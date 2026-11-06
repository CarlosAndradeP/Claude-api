'use strict';

const express = require('express');
const fs = require('fs');
const path = require('path');
const { randomUUID } = require('node:crypto');
const { KeyPool, DEFAULT_LIMITS, normalizeKey } = require('./lib/key-pool');
const { waitForDrainOrClose } = require('./lib/stream-flow');
const { classifyUpstreamError, errorHeaders } = require('./lib/proxy-errors');
const { loadConfig, saveConfig, updateConfig } = require('./lib/proxy-config');
const { CapacityQueue } = require('./lib/capacity-queue');

function envInteger(name, fallback, minimum = 1, maximum = Number.MAX_SAFE_INTEGER) {
    const value = Number(process.env[name]);
    return process.env[name] !== undefined && Number.isInteger(value) && value >= minimum && value <= maximum ? value : fallback;
}

const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const KEYS_FILE = path.join(DATA_DIR, 'keys.json');
const ADMIN_PORT = Number(process.env.ADMIN_PORT) || 88;
const PROXY_PORT = Number(process.env.PROXY_PORT) || 11431;
const UPSTREAM_ORIGIN = process.env.UPSTREAM_ORIGIN || 'https://integrate.api.nvidia.com';
const UPSTREAM_TIMEOUT_MS = envInteger('UPSTREAM_TIMEOUT_MS', 120_000);
const UPSTREAM_IDLE_TIMEOUT_MS = envInteger('UPSTREAM_IDLE_TIMEOUT_MS', UPSTREAM_TIMEOUT_MS);
const PROXY_CORS_ORIGIN = process.env.PROXY_CORS_ORIGIN || '';
// Bound queueing + upstream header retries below Web Agent's 120s chunk timeout.
// Healthy streams retain the existing rolling idle timeout after headers arrive.
const REQUEST_START_TIMEOUT_MS = envInteger('REQUEST_START_TIMEOUT_MS', 110_000);

fs.mkdirSync(DATA_DIR, { recursive: true });

function loadKeys() {
    if (!fs.existsSync(KEYS_FILE)) return [];
    try {
        const parsed = JSON.parse(fs.readFileSync(KEYS_FILE, 'utf8'));
        if (!Array.isArray(parsed)) return [];
        const normalized = parsed.map(normalizeKey).filter(item => item.key);
        return [...new Map(normalized.map(item => [item.key, item])).values()];
    } catch (error) {
        console.error('[Admin] Não foi possível carregar data/keys.json:', error.message);
        return [];
    }
}

let keys = loadKeys();
let saveTimer = null;

function saveKeysNow() {
    const tempFile = `${KEYS_FILE}.tmp`;
    fs.writeFileSync(tempFile, JSON.stringify(keys, null, 2));
    fs.renameSync(tempFile, KEYS_FILE);
}

function scheduleSave() {
    if (saveTimer) return;
    saveTimer = setTimeout(() => {
        saveTimer = null;
        try { saveKeysNow(); } catch (error) {
            console.error('[Admin] Falha ao persistir estatísticas:', error.message);
        }
    }, 500);
    saveTimer.unref?.();
}

const proxyConfig = loadConfig(DATA_DIR, {
    disableAbort: false,
    transparentMode: true,
    requestsPerMinute: envInteger('REQUESTS_PER_MINUTE', DEFAULT_LIMITS.requestsPerMinute, 1, 10_000),
    burst: envInteger('KEY_BURST', DEFAULT_LIMITS.burst, 1, 100),
    maxConcurrent: envInteger('MAX_CONCURRENT_PER_KEY', DEFAULT_LIMITS.maxConcurrent, 1, 100),
    maxAttempts: envInteger('MAX_ATTEMPTS', DEFAULT_LIMITS.maxAttempts, 1, 100),
    queueWaitMs: envInteger('QUEUE_WAIT_MS', 3_000, 0, 10_000),
    maxQueueLength: envInteger('MAX_QUEUE_LENGTH', 100, 0, 1_000),
    healthCheckModel: process.env.HEALTHCHECK_MODEL || 'openai/gpt-oss-20b'
});
let capacityQueue;
const pool = new KeyPool(keys, { ...proxyConfig, onChange: () => { scheduleSave(); capacityQueue?.wake(); } });
capacityQueue = new CapacityQueue(pool, () => proxyConfig);

function maskKey(key) {
    if (!key) return '';
    if (key.length <= 10) return `${key.slice(0, 2)}…${key.slice(-2)}`;
    return `${key.slice(0, 7)}…${key.slice(-4)}`;
}

function publicKey(key) {
    const attempts = key.stats.attempts || 0;
    return {
        id: key.id, name: key.name, role: key.role, maskedKey: maskKey(key.key), createdAt: key.createdAt,
        stats: {
            ...key.stats,
            successRate: attempts ? Number(((key.stats.successes / attempts) * 100).toFixed(1)) : null,
            averageLatencyMs: attempts ? Math.round(key.stats.totalLatencyMs / attempts) : null
        },
        health: pool.getHealth(key)
    };
}

function parseRetryAfter(value) {
    if (!value) return 0;
    const seconds = Number(value);
    if (Number.isFinite(seconds)) return Math.max(0, seconds * 1000);
    const date = Date.parse(value);
    return Number.isFinite(date) ? Math.max(0, date - Date.now()) : 0;
}

function upstreamHeaders(req, body) {
    const headers = new Headers();
    const ignored = new Set([
        'authorization', 'host', 'content-length', 'connection', 'transfer-encoding', 'keep-alive',
        'proxy-authenticate', 'proxy-authorization', 'te', 'trailer', 'upgrade', 'x-agent-type', 'x-request-timeout-ms'
    ]);
    for (const [name, value] of Object.entries(req.headers)) {
        if (!ignored.has(name.toLowerCase()) && value !== undefined) {
            headers.set(name, Array.isArray(value) ? value.join(', ') : value);
        }
    }
    headers.set('accept-encoding', 'identity');
    if (body?.length) headers.set('content-length', String(body.length));
    return headers;
}

function prepareBody(req) {
    if (req.method === 'GET' || req.method === 'HEAD' || !Buffer.isBuffer(req.body) || !req.body.length) return undefined;
    if (proxyConfig.transparentMode) return req.body;
    try {
        const parsed = JSON.parse(req.body.toString('utf8'));
        if (typeof parsed.model === 'string' && parsed.model.includes('glm-5.1')) {
            if (parsed.max_tokens > 16_384) parsed.max_tokens = 16_384;
            if (parsed.temperature === undefined) parsed.temperature = 1;
            if (parsed.top_p === undefined) parsed.top_p = 1;
        }
        return Buffer.from(JSON.stringify(parsed));
    } catch { return req.body; }
}

function isRetryableStatus(status) {
    return status === 401 || status === 403 || status === 408 || status === 429 || status >= 500;
}

async function readErrorBody(response) {
    return (await response.text()).slice(0, 4_000);
}

function sendUnavailable(res, retryAfterMs, message = 'Nenhuma chave possui capacidade disponível agora.', extra = {}) {
    if (retryAfterMs) res.setHeader('Retry-After', String(Math.max(1, Math.ceil(retryAfterMs / 1000))));
    const error = { type: 'proxy_capacity_exhausted', scope: 'proxy', retryable: true };
    errorHeaders(res, error);
    return res.status(429).json({ error: { message, ...error, retry_after_ms: retryAfterMs || null, request_id: res.getHeader('x-request-id'), ...extra } });
}

function copyUpstreamHeaders(upstream, res) {
    const excluded = new Set(['content-encoding', 'content-length', 'transfer-encoding', 'connection', 'x-request-id']);
    upstream.headers.forEach((value, name) => { if (!excluded.has(name.toLowerCase())) res.setHeader(name, value); });
}

function createAdminApp() {
    const app = express();
    app.use(express.json({ limit: '32kb' }));
    app.use(express.static(path.join(__dirname, 'public')));

    app.get('/api/config', (_req, res) => res.json(proxyConfig));
    app.post('/api/config', (req, res) => {
        let nextConfig;
        try { nextConfig = updateConfig(proxyConfig, req.body); }
        catch (error) { return res.status(400).json({ error: error.message }); }
        try { saveConfig(DATA_DIR, nextConfig); }
        catch { return res.status(500).json({ error: 'Não foi possível persistir a configuração. Nenhuma alteração foi aplicada.' }); }
        Object.assign(proxyConfig, nextConfig);
        pool.setLimits(proxyConfig);
        capacityQueue.wake();
        return res.json(proxyConfig);
    });

    app.get('/api/keys', (_req, res) => res.json(keys.map(publicKey)));
    app.post('/api/keys', (req, res) => {
        const keyValue = typeof req.body.key === 'string' ? req.body.key.trim() : '';
        const name = typeof req.body.name === 'string' ? req.body.name.trim() : '';
        const role = req.body.role;
        if (!keyValue || !name || !['main', 'sub', 'all'].includes(role)) {
            return res.status(400).json({ error: 'Informe name, key e um role válido (main, sub ou all).' });
        }
        if (keys.some(item => item.key === keyValue)) return res.status(409).json({ error: 'Esta chave já está cadastrada.' });
        const newKey = normalizeKey({ id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`, key: keyValue, role, name });
        keys.push(newKey);
        pool.setKeys(keys);
        saveKeysNow();
        return res.status(201).json(publicKey(newKey));
    });

    app.delete('/api/keys/:id', (req, res) => {
        const previousLength = keys.length;
        keys = keys.filter(key => key.id !== req.params.id);
        if (keys.length === previousLength) return res.status(404).json({ error: 'Chave não encontrada.' });
        pool.setKeys(keys);
        saveKeysNow();
        return res.json({ success: true });
    });

    app.post('/api/keys/:id/check', async (req, res) => {
        const key = keys.find(item => item.id === req.params.id);
        if (!key) return res.status(404).json({ error: 'Chave não encontrada.' });
        const excluded = new Set(keys.filter(item => item.id !== key.id).map(item => item.id));
        const model = typeof req.body?.model === 'string' && req.body.model.trim() ? req.body.model.trim() : proxyConfig.healthCheckModel;
        if (model.length > 256) return res.status(400).json({ error: 'Identificador do modelo inválido.' });
        const { reservation, retryAfterMs } = pool.acquire('none', excluded, 0, true, model);
        if (!reservation) return sendUnavailable(res, retryAfterMs, 'Esta chave está ocupada ou em cooldown.');

        const startedAt = Date.now();
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), Math.min(UPSTREAM_TIMEOUT_MS, 15_000));
        let clientClosed = false;
        const onClose = () => { if (!res.writableEnded) { clientClosed = true; controller.abort(); } };
        res.once('close', onClose);
        try {
            // /v1/models is public on NVIDIA and cannot validate a credential.
            const response = await fetch(`${UPSTREAM_ORIGIN}/v1/chat/completions`, {
                method: 'POST',
                headers: { authorization: `Bearer ${key.key}`, 'accept-encoding': 'identity', 'content-type': 'application/json' },
                body: JSON.stringify({ model, messages: [{ role: 'user', content: 'Hi' }], max_tokens: 1, stream: false }),
                signal: controller.signal
            });
            const text = (await response.text()).replaceAll(key.key, '[REDACTED]');
            let data;
            try { data = JSON.parse(text); } catch {}
            const success = response.ok && Array.isArray(data?.choices) && data.choices.length > 0;
            const classification = classifyUpstreamError(response.ok ? 502 : response.status, text, model);
            reservation.finish({
                status: response.ok && !success ? 502 : response.status, success, durationMs: Date.now() - startedAt,
                errorScope: classification.scope, authenticated: true,
                retryAfterMs: parseRetryAfter(response.headers.get('retry-after')), error: success ? '' : text.slice(0, 4_000)
            });
            if (success) return res.json(publicKey(key));
            errorHeaders(res, classification);
            return res.status(response.ok ? 502 : response.status).json({
                ...publicKey(key), error: { ...classification, message: 'A inferência de verificação falhou.', model, upstream_status: response.status }
            });
        } catch (error) {
            reservation.finish({ cancelled: clientClosed, durationMs: Date.now() - startedAt, error: error.message });
            if (clientClosed) return;
            return res.status(502).json({ error: error.name === 'AbortError' ? 'Check de saúde expirou.' : error.message, key: publicKey(key) });
        } finally { clearTimeout(timeout); res.removeListener('close', onClose); }
    });

    return app;
}

function createProxyApp() {
    const app = express();
    app.use((req, res, next) => {
        if (PROXY_CORS_ORIGIN) {
            res.setHeader('Access-Control-Allow-Origin', PROXY_CORS_ORIGIN);
            res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, PATCH, DELETE, OPTIONS');
            res.setHeader('Access-Control-Allow-Headers', '*');
            res.setHeader('Access-Control-Expose-Headers', 'Retry-After, x-request-id, x-proxy-error-type, x-proxy-error-scope, x-proxy-retryable');
            res.setHeader('Vary', 'Origin');
        }
        if (req.method === 'OPTIONS') return res.sendStatus(204);
        next();
    });
    app.use(express.raw({ type: '*/*', limit: '50mb' }));

    app.all('/v1/*', async (req, res) => {
        const suppliedId = String(req.headers['x-request-id'] || '');
        const requestId = /^[a-zA-Z0-9._:-]{1,128}$/.test(suppliedId) ? suppliedId : randomUUID();
        res.setHeader('x-request-id', requestId);
        if (!keys.length) {
            const error = { type: 'proxy_not_configured', scope: 'proxy', retryable: false };
            errorHeaders(res, error);
            return res.status(503).json({ error: { message: 'Nenhuma chave configurada no proxy.', ...error, request_id: requestId } });
        }

        const agentType = String(req.headers['x-agent-type'] || 'none').toLowerCase();
        const body = prepareBody(req);
        let modelId = '';
        try {
            const model = body && JSON.parse(body.toString('utf8')).model;
            if (typeof model === 'string' && model.length <= 256) modelId = model;
        } catch { /* Preserve transparent forwarding of non-JSON and invalid payloads. */ }
        const requestedBudget = Number(req.headers['x-request-timeout-ms']);
        const startBudget = Number.isFinite(requestedBudget) && requestedBudget > 0
            ? Math.min(REQUEST_START_TIMEOUT_MS, requestedBudget) : REQUEST_START_TIMEOUT_MS;
        const deadline = Date.now() + startBudget;
        const excludedIds = new Set();
        const targetUrl = `${UPSTREAM_ORIGIN}${req.originalUrl}`;
        const waitController = new AbortController();
        let currentController = null;
        let clientClosed = false;
        let lastError = null;
        let shortestRetryAfter = null;

        const onClientDisconnect = () => {
            clientClosed = true;
            waitController.abort();
            if (!proxyConfig.disableAbort) currentController?.abort();
        };
        const onResponseClose = () => {
            if (!res.writableEnded) onClientDisconnect();
        };
        req.once('aborted', onClientDisconnect);
        res.once('close', onResponseClose);

        try {
            for (let attempt = 1; attempt <= Math.min(proxyConfig.maxAttempts, keys.length); attempt += 1) {
                if (clientClosed) return;
                if (Date.now() >= deadline) break;
                const acquired = await capacityQueue.acquire({
                    agentType, excludedIds, bytesIn: body?.length || 0, modelId,
                    signal: waitController.signal, waitMs: deadline - Date.now()
                });
                if (acquired.cancelled || clientClosed) return;
                if (!acquired.reservation) {
                    shortestRetryAfter = acquired.retryAfterMs;
                    if (acquired.reason === 'model_forbidden') {
                        lastError ||= {
                            status: 403, message: 'As chaves disponíveis têm restrição temporária para este modelo.',
                            classification: { type: 'upstream_model_forbidden', scope: 'model_key', retryable: false }
                        };
                    }
                    break;
                }

                const { reservation } = acquired;
                const key = reservation.key;
                excludedIds.add(key.id);
                currentController = new AbortController();
                const controller = currentController;
                let timeout = setTimeout(() => controller.abort(), Math.max(1, Math.min(UPSTREAM_TIMEOUT_MS, deadline - Date.now())));
                const resetIdleTimeout = () => {
                    clearTimeout(timeout);
                    timeout = setTimeout(() => controller.abort(), UPSTREAM_IDLE_TIMEOUT_MS);
                };
                const startedAt = Date.now();
                let finished = false;
                const finish = result => {
                    if (finished) return;
                    finished = true;
                    reservation.finish({ authenticated: req.method !== 'GET' && req.method !== 'HEAD', ...result, durationMs: Date.now() - startedAt });
                };

                try {
                    const headers = upstreamHeaders(req, body);
                    headers.set('authorization', `Bearer ${key.key}`);
                    console.log(`[Proxy] ${requestId} ${req.method} ${req.path} tentativa ${attempt} com ${key.name} (${maskKey(key.key)})`);
                    const upstream = await fetch(targetUrl, { method: req.method, headers, body, signal: currentController.signal });
                    if (upstream.ok) resetIdleTimeout();
                    else {
                        clearTimeout(timeout);
                        timeout = setTimeout(() => controller.abort(), Math.max(1, deadline - Date.now()));
                    }

                    // Se o cliente sumiu mas pediu para não abortar, conclui apenas esta chamada
                    // e descarta o body. Não faz fallback inútil nem tenta escrever em um socket fechado.
                    if (clientClosed && proxyConfig.disableAbort) {
                        let bytesOut = 0;
                        if (upstream.body) {
                            const reader = upstream.body.getReader();
                            while (true) {
                                const { done, value } = await reader.read();
                                if (done) break;
                                resetIdleTimeout();
                                bytesOut += value.byteLength;
                            }
                        }
                        finish({
                            status: upstream.status,
                            success: upstream.ok,
                            errorScope: upstream.ok ? undefined : classifyUpstreamError(upstream.status, '', modelId).scope,
                            bytesOut,
                            retryAfterMs: parseRetryAfter(upstream.headers.get('retry-after')),
                            error: upstream.ok ? '' : `HTTP ${upstream.status}`
                        });
                        return;
                    }

                    if (!upstream.ok) {
                        const errorText = (await upstream.text()).replaceAll(key.key, '[REDACTED]');
                        const classification = classifyUpstreamError(upstream.status, errorText, modelId);
                        const retryAfterMs = parseRetryAfter(upstream.headers.get('retry-after'));
                        if (retryAfterMs > 0) {
                            shortestRetryAfter = shortestRetryAfter === null
                                ? retryAfterMs
                                : Math.min(shortestRetryAfter, retryAfterMs);
                        }
                        finish({ status: upstream.status, success: false, retryAfterMs, errorScope: classification.scope, error: errorText.slice(0, 4_000) });
                        lastError = { status: upstream.status, message: errorText.slice(0, 4_000) || `HTTP ${upstream.status}`, body: errorText, contentType: upstream.headers.get('content-type'), classification };
                        if (isRetryableStatus(upstream.status)) {
                            console.warn(`[Proxy] ${requestId} ${key.name}: HTTP ${upstream.status}, alcance ${classification.scope}; próxima chave elegível.`);
                            continue;
                        }
                        copyUpstreamHeaders(upstream, res);
                        errorHeaders(res, classification);
                        return res.status(upstream.status).send(errorText);
                    }

                    copyUpstreamHeaders(upstream, res);
                    res.status(upstream.status);
                    if (!upstream.body) {
                        finish({ status: upstream.status, success: upstream.ok });
                        return res.end();
                    }

                    let bytesOut = 0;
                    const reader = upstream.body.getReader();
                    try {
                        while (true) {
                            const { done, value } = await reader.read();
                            if (done) break;
                            resetIdleTimeout();
                            bytesOut += value.byteLength;
                            if (!clientClosed && !res.write(value)) await waitForDrainOrClose(res, controller.signal);
                        }
                        finish({ status: upstream.status, success: upstream.ok, bytesOut });
                        return res.end();
                    } catch (error) {
                        finish({ status: 502, cancelled: clientClosed, bytesOut, error: `Stream interrompido: ${error.message}` });
                        // A normal EOF would make truncated SSE/JSON look successful.
                        if (res.headersSent) return res.destroy();
                        if (clientClosed) return;
                        const classification = { type: 'upstream_stream_interrupted', scope: 'provider', retryable: true };
                        errorHeaders(res, classification);
                        return res.status(502).json({ error: { message: 'Stream interrompido no upstream.', ...classification, request_id: requestId } });
                    }
                } catch (error) {
                    clearTimeout(timeout);
                    finish({ cancelled: clientClosed, error: error.name === 'AbortError' ? 'Timeout ou cancelamento' : error.message });
                    if (clientClosed) return;
                    lastError = {
                        status: error.name === 'AbortError' ? 504 : 502,
                        message: error.name === 'AbortError' ? 'Timeout no upstream' : error.message,
                        classification: { type: error.name === 'AbortError' ? 'upstream_timeout' : 'upstream_connection_error', scope: 'provider', retryable: true }
                    };
                    console.warn(`[Proxy] ${requestId} Falha de rede em ${key.name}: ${lastError.message}`);
                } finally {
                    clearTimeout(timeout);
                }
            }
        } finally {
            req.removeListener('aborted', onClientDisconnect);
            res.removeListener('close', onResponseClose);
        }

        if (clientClosed || res.headersSent) return;
        if (lastError && [401, 403].includes(lastError.status)) {
            errorHeaders(res, lastError.classification);
            if (lastError.body !== undefined) return res.status(lastError.status).type(lastError.contentType || 'text/plain').send(lastError.body);
            return res.status(lastError.status).json({ error: { message: lastError.message, ...lastError.classification, model: modelId, request_id: requestId } });
        }
        if (lastError && lastError.status !== 429) {
            errorHeaders(res, lastError.classification);
            return res.status(lastError.status === 504 ? 504 : 502).json({ error: {
                message: 'As tentativas no upstream falharam.', ...lastError.classification,
                last_error: { status: lastError.status, message: lastError.message }, model: modelId, request_id: requestId
            } });
        }
        if (Date.now() >= deadline && !lastError) {
            const classification = { type: 'proxy_start_timeout', scope: 'proxy', retryable: true };
            errorHeaders(res, classification);
            return res.status(504).json({ error: { message: 'O prazo para iniciar a chamada expirou.', ...classification, request_id: requestId } });
        }
        // Recompute from the whole pool: excluded keys may now be cooling down,
        // and local backoff can be longer than the upstream's Retry-After.
        const now = Date.now();
        const availability = keys.map(key => pool.availability(key, now, false, modelId));
        if (availability.length && availability.every(item => !item.available)) {
            shortestRetryAfter = Math.max(1, Math.min(...availability.map(item => item.retryAt)) - now);
        }
        return sendUnavailable(res, shortestRetryAfter, 'Todas as chaves elegíveis estão limitadas, ocupadas ou em recuperação.', { model: modelId, upstream_status: lastError?.status || null });
    });
    return app;
}

function start() {
    const adminServer = createAdminApp().listen(ADMIN_PORT, () => console.log(`[Admin] Painel em http://localhost:${ADMIN_PORT}`));
    const proxyServer = createProxyApp().listen(PROXY_PORT, () => console.log(`[Proxy] API em http://localhost:${PROXY_PORT}`));
    return { adminServer, proxyServer };
}

if (require.main === module) start();
module.exports = { createAdminApp, createProxyApp, start, parseRetryAfter, prepareBody, publicKey, pool, capacityQueue };
