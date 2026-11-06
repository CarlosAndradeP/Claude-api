'use strict';

const DEFAULT_LIMITS = Object.freeze({
    requestsPerMinute: 38,
    burst: 3,
    maxConcurrent: 2,
    maxAttempts: 3,
    baseCooldownMs: 65_000,
    maxCooldownMs: 15 * 60_000,
    staleAfterMs: 24 * 60 * 60_000
});

function createStats() {
    return {
        attempts: 0,
        successes: 0,
        failures: 0,
        rateLimits: 0,
        bytesIn: 0,
        bytesOut: 0,
        cancellations: 0,
        requestErrors: 0,
        modelErrors: 0,
        totalLatencyMs: 0,
        lastUsedAt: null,
        lastSuccessAt: null,
        lastFailureAt: null,
        lastStatus: null,
        lastError: null
    };
}

function normalizeKey(raw) {
    return {
        id: String(raw.id),
        key: String(raw.key || ''),
        role: ['main', 'sub', 'all'].includes(raw.role) ? raw.role : 'all',
        name: String(raw.name || 'Chave sem nome'),
        createdAt: raw.createdAt || new Date().toISOString(),
        stats: { ...createStats(), ...(raw.stats || {}) },
        health: {
            consecutiveFailures: 0,
            consecutiveRateLimits: 0,
            cooldownUntil: null,
            circuitOpenUntil: null,
            authVerifiedAt: null,
            lastFailureStatus: null,
            modelRestrictions: {},
            ...(raw.health || {})
        }
    };
}

function roleTier(key, agentType) {
    if (agentType === 'main') return key.role === 'main' || key.role === 'all' ? 0 : 1;
    if (agentType === 'sub') return key.role === 'sub' || key.role === 'all' ? 0 : 1;
    return 0;
}

class KeyPool {
    constructor(keys, options = {}) {
        this.keys = keys;
        this.limits = { ...DEFAULT_LIMITS, ...options };
        this.now = options.now || Date.now;
        this.onChange = options.onChange || (() => {});
        this.runtime = new Map();
        for (const key of keys) this.ensureRuntime(key);
    }

    setKeys(keys) {
        this.keys = keys;
        for (const key of keys) this.ensureRuntime(key);
        const ids = new Set(keys.map(key => key.id));
        for (const id of this.runtime.keys()) {
            if (!ids.has(id)) this.runtime.delete(id);
        }
    }

    setLimits(limits) {
        Object.assign(this.limits, limits);
        for (const state of this.runtime.values()) {
            state.tokens = Math.min(state.tokens, this.limits.burst);
        }
    }

    ensureRuntime(key) {
        if (!key.stats) key.stats = createStats();
        if (!key.health) key.health = normalizeKey(key).health;
        if (!this.runtime.has(key.id)) {
            this.runtime.set(key.id, {
                tokens: this.limits.burst,
                lastRefillAt: this.now(),
                inFlight: 0,
                lastSelectedAt: 0,
                probeInFlight: false,
                failureVersion: 0
            });
        }
        return this.runtime.get(key.id);
    }

    refill(state, now) {
        const elapsed = Math.max(0, now - state.lastRefillAt);
        const ratePerMs = this.limits.requestsPerMinute / 60_000;
        state.tokens = Math.min(this.limits.burst, state.tokens + elapsed * ratePerMs);
        state.lastRefillAt = now;
    }

    availability(key, now = this.now(), ignoreHealth = false, modelId = '') {
        const state = this.ensureRuntime(key);
        this.refill(state, now);
        const cooldownUntil = Date.parse(key.health.cooldownUntil || '') || 0;
        const circuitOpenUntil = Date.parse(key.health.circuitOpenUntil || '') || 0;
        // Even a manual authentication probe must respect an active Retry-After.
        if (cooldownUntil > now) return { available: false, retryAt: cooldownUntil, reason: 'cooldown' };
        if (!ignoreHealth && circuitOpenUntil > now) return { available: false, retryAt: circuitOpenUntil, reason: 'circuit_open' };
        const restriction = key.health.modelRestrictions?.[modelId];
        if (!ignoreHealth && restriction && Date.parse(restriction.until) > now) {
            return { available: false, retryAt: Date.parse(restriction.until), reason: 'model_forbidden' };
        }
        if (state.inFlight >= this.limits.maxConcurrent) return { available: false, retryAt: now + 1_000, reason: 'concurrency' };
        if (state.tokens < 1) {
            const wait = Math.ceil((1 - state.tokens) / (this.limits.requestsPerMinute / 60_000));
            return { available: false, retryAt: now + wait, reason: 'rate_limit' };
        }
        return { available: true, retryAt: now, reason: null };
    }

    capacity(excludedIds = new Set(), modelId = '') {
        const now = this.now();
        const availability = this.keys.filter(key => key.key && !excludedIds.has(key.id))
            .map(key => this.availability(key, now, false, modelId));
        const next = availability.reduce((earliest, item) => !earliest || item.retryAt < earliest.retryAt ? item : earliest, null);
        return {
            retryAfterMs: availability.length ? Math.max(1, Math.min(...availability.map(item => item.retryAt)) - now) : null,
            reason: availability.length && availability.every(item => item.reason === 'model_forbidden') ? 'model_forbidden' : next?.reason || 'capacity'
        };
    }

    acquire(agentType = 'none', excludedIds = new Set(), bytesIn = 0, probe = false, modelId = '') {
        const now = this.now();
        const candidates = [];
        let retryAt = Infinity;

        for (const key of this.keys) {
            if (!key.key || excludedIds.has(key.id)) continue;
            const state = this.ensureRuntime(key);
            if (probe && state.probeInFlight) continue;
            const availability = this.availability(key, now, probe, modelId);
            if (!availability.available) {
                retryAt = Math.min(retryAt, availability.retryAt);
                continue;
            }
            candidates.push({ key, state, tier: roleTier(key, agentType) });
        }

        candidates.sort((a, b) =>
            a.tier - b.tier ||
            a.state.inFlight - b.state.inFlight ||
            b.state.tokens - a.state.tokens ||
            a.state.lastSelectedAt - b.state.lastSelectedAt
        );

        const selected = candidates[0];
        if (!selected) {
            return { reservation: null, ...this.capacity(excludedIds, modelId), retryAfterMs: Number.isFinite(retryAt) ? Math.max(1, retryAt - now) : null };
        }

        selected.state.tokens -= 1;
        selected.state.inFlight += 1;
        selected.state.lastSelectedAt = now;
        if (probe) selected.state.probeInFlight = true;
        selected.key.stats.attempts += 1;
        selected.key.stats.bytesIn += Math.max(0, bytesIn);
        selected.key.stats.lastUsedAt = new Date(now).toISOString();
        this.onChange();

        let released = false;
        const failureVersion = selected.state.failureVersion;
        return {
            retryAfterMs: 0,
            reservation: {
                key: selected.key,
                finish: result => {
                    if (released) return;
                    released = true;
                    selected.state.inFlight = Math.max(0, selected.state.inFlight - 1);
                    if (probe) selected.state.probeInFlight = false;
                    this.recordResult(selected.key, { ...result, modelId, probe, failureVersion });
                }
            }
        };
    }

    recordResult(key, result) {
        const now = this.now();
        const status = Number(result.status) || null;
        const success = status >= 200 && status < 300 && result.success !== false;
        const state = this.ensureRuntime(key);
        key.stats.lastStatus = status;
        key.stats.totalLatencyMs += Math.max(0, Number(result.durationMs) || 0);
        key.stats.bytesOut += Math.max(0, Number(result.bytesOut) || 0);

        if (result.cancelled) {
            key.stats.cancellations += 1;
            this.onChange();
            return;
        }

        if (success) {
            key.stats.successes += 1;
            if (result.authenticated !== false) {
                key.stats.lastSuccessAt = new Date(now).toISOString();
                key.health.authVerifiedAt = key.stats.lastSuccessAt;
                if (result.failureVersion === state.failureVersion) {
                    key.stats.lastError = null;
                    key.health.consecutiveFailures = 0;
                    if (Date.parse(key.health.cooldownUntil || '') <= now || !key.health.cooldownUntil) {
                        key.health.cooldownUntil = null;
                        key.health.consecutiveRateLimits = 0;
                    }
                    if (result.probe || Date.parse(key.health.circuitOpenUntil || '') <= now) key.health.circuitOpenUntil = null;
                }
                if (result.failureVersion === state.failureVersion && result.modelId && key.health.modelRestrictions) delete key.health.modelRestrictions[result.modelId];
            }
        } else {
            key.stats.failures += 1;
            key.stats.lastFailureAt = new Date(now).toISOString();
            key.stats.lastError = String(result.error || (status ? `HTTP ${status}` : 'Falha de rede')).slice(0, 300);
            if (result.errorScope === 'model_key' && result.modelId) {
                state.failureVersion += 1;
                key.stats.modelErrors += 1;
                key.health.modelRestrictions ||= {};
                for (const [id, block] of Object.entries(key.health.modelRestrictions)) {
                    if (Date.parse(block.until) <= now) delete key.health.modelRestrictions[id];
                }
                if (Object.keys(key.health.modelRestrictions).length >= 128) delete key.health.modelRestrictions[Object.keys(key.health.modelRestrictions)[0]];
                Object.defineProperty(key.health.modelRestrictions, result.modelId, {
                    configurable: true, enumerable: true, writable: true,
                    value: { until: new Date(now + 5 * 60_000).toISOString(), status }
                });
                this.onChange();
                return;
            }
            // Invalid payloads and missing endpoints are request/model failures,
            // not evidence that this credential is unusable for other models.
            if (['request', 'model'].includes(result.errorScope) || (status && status >= 400 && status < 500 && ![401, 403, 408, 429].includes(status))) {
                key.stats.requestErrors += 1;
                this.onChange();
                return;
            }
            key.health.consecutiveFailures += 1;
            key.health.lastFailureStatus = status;
            state.failureVersion += 1;

            if (status === 429) {
                key.stats.rateLimits += 1;
                key.health.consecutiveRateLimits += 1;
                const exponential = this.limits.baseCooldownMs * (2 ** Math.min(3, key.health.consecutiveRateLimits - 1));
                const cooldownMs = Math.max(Math.min(this.limits.maxCooldownMs, exponential), result.retryAfterMs || 0);
                key.health.cooldownUntil = new Date(Math.max(Date.parse(key.health.cooldownUntil || '') || 0, now + cooldownMs)).toISOString();
            } else if (status === 401 || status === 403) {
                key.health.circuitOpenUntil = new Date(Math.max(Date.parse(key.health.circuitOpenUntil || '') || 0, now + this.limits.maxCooldownMs)).toISOString();
            } else if (!status || status === 408 || status >= 500) {
                const backoff = Math.min(60_000, 2_000 * (2 ** Math.min(5, key.health.consecutiveFailures - 1)));
                key.health.circuitOpenUntil = new Date(Math.max(Date.parse(key.health.circuitOpenUntil || '') || 0, now + backoff)).toISOString();
            }
        }
        this.onChange();
    }

    getHealth(key, now = this.now()) {
        const state = this.ensureRuntime(key);
        const cooldownUntil = Date.parse(key.health.cooldownUntil || '') || 0;
        const circuitOpenUntil = Date.parse(key.health.circuitOpenUntil || '') || 0;
        const lastSuccess = Date.parse(key.health.authVerifiedAt || '') || 0;
        let status = 'unknown';
        let reason = 'Ainda não foi testada';

        if (cooldownUntil > now) {
            status = 'rate_limited';
            reason = 'Em cooldown após rate limit';
        } else if (circuitOpenUntil > now) {
            status = 'unhealthy';
            reason = key.health.lastFailureStatus === 401 || key.health.lastFailureStatus === 403
                ? 'Autenticação rejeitada'
                : 'Circuito aberto após falhas';
        } else if (key.health.consecutiveFailures >= 3) {
            status = 'unhealthy';
            reason = `${key.health.consecutiveFailures} falhas consecutivas`;
        } else if (lastSuccess && now - lastSuccess > this.limits.staleAfterMs) {
            status = 'stale';
            reason = 'Sem sucesso recente';
        } else if (lastSuccess) {
            status = 'healthy';
            reason = 'Funcionando normalmente';
        } else if (key.health.consecutiveFailures > 0) {
            status = 'unhealthy';
            reason = 'Nunca concluiu uma chamada com sucesso';
        }

        this.refill(state, now);
        return {
            status,
            reason,
            inFlight: state.inFlight,
            availableTokens: Math.max(0, Number(state.tokens.toFixed(2))),
            cooldownUntil: key.health.cooldownUntil,
            circuitOpenUntil: key.health.circuitOpenUntil,
            consecutiveFailures: key.health.consecutiveFailures,
            authVerifiedAt: key.health.authVerifiedAt || null,
            restrictedModels: Object.entries(key.health.modelRestrictions || {}).filter(([, block]) => Date.parse(block.until) > now).map(([model]) => model)
        };
    }
}

module.exports = { KeyPool, DEFAULT_LIMITS, normalizeKey, createStats };
