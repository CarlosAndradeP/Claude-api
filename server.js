'use strict';

const express = require('express');
const fs = require('fs');
const path = require('path');
const { KeyPool, DEFAULT_LIMITS, normalizeKey } = require('./lib/key-pool');
const { waitForDrainOrClose } = require('./lib/stream-flow');

const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const KEYS_FILE = path.join(DATA_DIR, 'keys.json');
const ADMIN_PORT = Number(process.env.ADMIN_PORT) || 88;
const PROXY_PORT = Number(process.env.PROXY_PORT) || 11431;
const UPSTREAM_ORIGIN = process.env.UPSTREAM_ORIGIN || 'https://integrate.api.nvidia.com';
const UPSTREAM_TIMEOUT_MS = Number(process.env.UPSTREAM_TIMEOUT_MS) || 120_000;
const UPSTREAM_IDLE_TIMEOUT_MS = Number(process.env.UPSTREAM_IDLE_TIMEOUT_MS) || UPSTREAM_TIMEOUT_MS;
const PROXY_CORS_ORIGIN = process.env.PROXY_CORS_ORIGIN || '';

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
    clearTimeout(saveTimer);
    saveTimer = setTimeout(() => {
        try { saveKeysNow(); } catch (error) {
            console.error('[Admin] Falha ao persistir estatísticas:', error.message);
        }
    }, 500);
    saveTimer.unref?.();
}

const proxyConfig = {
    disableAbort: false,
    transparentMode: true,
    requestsPerMinute: Number(process.env.REQUESTS_PER_MINUTE) || DEFAULT_LIMITS.requestsPerMinute,
    burst: Number(process.env.KEY_BURST) || DEFAULT_LIMITS.burst,
    maxConcurrent: Number(process.env.MAX_CONCURRENT_PER_KEY) || DEFAULT_LIMITS.maxConcurrent,
    maxAttempts: Number(process.env.MAX_ATTEMPTS) || DEFAULT_LIMITS.maxAttempts
};
const pool = new KeyPool(keys, { ...proxyConfig, onChange: scheduleSave });

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
        'proxy-authenticate', 'proxy-authorization', 'te', 'trailer', 'upgrade', 'x-agent-type'
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

function sendUnavailable(res, retryAfterMs, message = 'Nenhuma chave possui capacidade disponível agora.') {
    if (retryAfterMs) res.setHeader('Retry-After', String(Math.max(1, Math.ceil(retryAfterMs / 1000))));
    return res.status(429).json({ error: { message, type: 'proxy_capacity_exhausted', retry_after_ms: retryAfterMs || null } });
}

function createAdminApp() {
    const app = express();
    app.use(express.json({ limit: '32kb' }));
    app.use(express.static(path.join(__dirname, 'public')));

    app.get('/api/config', (_req, res) => res.json(proxyConfig));
    app.post('/api/config', (req, res) => {
        const nextConfig = { ...proxyConfig };
        for (const field of ['disableAbort', 'transparentMode']) {
            if (req.body[field] !== undefined) {
                if (typeof req.body[field] !== 'boolean') return res.status(400).json({ error: `${field} deve ser booleano` });
                nextConfig[field] = req.body[field];
            }
        }
        for (const field of ['requestsPerMinute', 'burst', 'maxConcurrent', 'maxAttempts']) {
            if (req.body[field] !== undefined) {
                const value = req.body[field];
                const maximum = field === 'requestsPerMinute' ? 10_000 : 100;
                if (!Number.isInteger(value) || value < 1 || value > maximum) {
                    return res.status(400).json({ error: `${field} deve ser um inteiro entre 1 e ${maximum}` });
                }
                nextConfig[field] = value;
            }
        }
        Object.assign(proxyConfig, nextConfig);
        pool.setLimits(proxyConfig);
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
        const { reservation, retryAfterMs } = pool.acquire('none', excluded, 0, true);
        if (!reservation) return sendUnavailable(res, retryAfterMs, 'Esta chave está ocupada ou em cooldown.');

        const startedAt = Date.now();
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), Math.min(UPSTREAM_TIMEOUT_MS, 15_000));
        try {
            const response = await fetch(`${UPSTREAM_ORIGIN}/v1/models`, {
                headers: { authorization: `Bearer ${key.key}`, 'accept-encoding': 'identity' }, signal: controller.signal
            });
            const errorText = response.ok ? '' : await readErrorBody(response);
            if (response.ok) await response.body?.cancel();
            reservation.finish({
                status: response.status, success: response.ok, durationMs: Date.now() - startedAt,
                retryAfterMs: parseRetryAfter(response.headers.get('retry-after')), error: errorText
            });
            return res.status(response.ok ? 200 : response.status).json(publicKey(key));
        } catch (error) {
            reservation.finish({ durationMs: Date.now() - startedAt, error: error.message });
            return res.status(502).json({ error: error.name === 'AbortError' ? 'Check de saúde expirou.' : error.message, key: publicKey(key) });
        } finally { clearTimeout(timeout); }
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
            res.setHeader('Vary', 'Origin');
        }
        if (req.method === 'OPTIONS') return res.sendStatus(204);
        next();
    });
    app.use(express.raw({ type: '*/*', limit: '50mb' }));

    app.all('/v1/*', async (req, res) => {
        if (!keys.length) return res.status(503).json({ error: { message: 'Nenhuma chave configurada no proxy.' } });

        const agentType = String(req.headers['x-agent-type'] || 'none').toLowerCase();
        const body = prepareBody(req);
        const excludedIds = new Set();
        const targetUrl = `${UPSTREAM_ORIGIN}${req.originalUrl}`;
        let currentController = null;
        let clientClosed = false;
        let lastError = null;
        let shortestRetryAfter = null;

        const onClientDisconnect = () => {
            clientClosed = true;
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
                const acquired = pool.acquire(agentType, excludedIds, body?.length || 0);
                if (!acquired.reservation) {
                    shortestRetryAfter = acquired.retryAfterMs;
                    break;
                }

                const { reservation } = acquired;
                const key = reservation.key;
                excludedIds.add(key.id);
                currentController = new AbortController();
                const controller = currentController;
                let timeout = setTimeout(() => controller.abort(), UPSTREAM_TIMEOUT_MS);
                const resetIdleTimeout = () => {
                    clearTimeout(timeout);
                    timeout = setTimeout(() => controller.abort(), UPSTREAM_IDLE_TIMEOUT_MS);
                };
                const startedAt = Date.now();
                let finished = false;
                const finish = result => {
                    if (finished) return;
                    finished = true;
                    reservation.finish({ ...result, durationMs: Date.now() - startedAt });
                };

                try {
                    const headers = upstreamHeaders(req, body);
                    headers.set('authorization', `Bearer ${key.key}`);
                    console.log(`[Proxy] ${req.method} ${req.originalUrl} tentativa ${attempt} com ${key.name} (${maskKey(key.key)})`);
                    const upstream = await fetch(targetUrl, { method: req.method, headers, body, signal: currentController.signal });
                    resetIdleTimeout();

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
                            bytesOut,
                            retryAfterMs: parseRetryAfter(upstream.headers.get('retry-after')),
                            error: upstream.ok ? '' : `HTTP ${upstream.status}`
                        });
                        return;
                    }

                    if (isRetryableStatus(upstream.status)) {
                        const errorText = await readErrorBody(upstream);
                        const retryAfterMs = parseRetryAfter(upstream.headers.get('retry-after'));
                        if (retryAfterMs > 0) {
                            shortestRetryAfter = shortestRetryAfter === null
                                ? retryAfterMs
                                : Math.min(shortestRetryAfter, retryAfterMs);
                        }
                        finish({ status: upstream.status, retryAfterMs, error: errorText });
                        lastError = { status: upstream.status, message: errorText || `HTTP ${upstream.status}` };
                        console.warn(`[Proxy] ${key.name} respondeu ${upstream.status}; chave isolada e próxima tentativa será selecionada.`);
                        continue;
                    }

                    const excludedHeaders = new Set(['content-encoding', 'content-length', 'transfer-encoding', 'connection']);
                    upstream.headers.forEach((value, name) => {
                        if (!excludedHeaders.has(name.toLowerCase())) res.setHeader(name, value);
                    });
                    res.status(upstream.status);
                    if (!upstream.body) {
                        finish({ status: upstream.status, success: upstream.status < 500 });
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
                        finish({ status: upstream.status, success: upstream.status < 500, bytesOut });
                        return res.end();
                    } catch (error) {
                        finish({ status: 502, cancelled: clientClosed, bytesOut, error: `Stream interrompido: ${error.message}` });
                        // A normal EOF would make truncated SSE/JSON look successful.
                        if (res.headersSent) return res.destroy();
                        if (clientClosed) return;
                        return res.status(502).json({ error: { message: 'Stream interrompido no upstream.' } });
                    }
                } catch (error) {
                    clearTimeout(timeout);
                    finish({ cancelled: clientClosed, error: error.name === 'AbortError' ? 'Timeout ou cancelamento' : error.message });
                    if (clientClosed) return;
                    lastError = { status: 502, message: error.name === 'AbortError' ? 'Timeout no upstream' : error.message };
                    console.warn(`[Proxy] Falha de rede em ${key.name}: ${lastError.message}`);
                } finally {
                    clearTimeout(timeout);
                }
            }
        } finally {
            req.removeListener('aborted', onClientDisconnect);
            res.removeListener('close', onResponseClose);
        }

        if (clientClosed || res.headersSent) return;
        if (lastError && lastError.status !== 429) {
            return res.status(502).json({ error: { message: 'As tentativas no upstream falharam.', last_error: lastError } });
        }
        // Recompute from the whole pool: excluded keys may now be cooling down,
        // and local backoff can be longer than the upstream's Retry-After.
        const now = Date.now();
        const availability = keys.map(key => pool.availability(key, now));
        if (availability.length && availability.every(item => !item.available)) {
            shortestRetryAfter = Math.max(1, Math.min(...availability.map(item => item.retryAt)) - now);
        }
        return sendUnavailable(res, shortestRetryAfter, 'Todas as chaves elegíveis estão limitadas, ocupadas ou em recuperação.');
    });
    return app;
}

function start() {
    const adminServer = createAdminApp().listen(ADMIN_PORT, () => console.log(`[Admin] Painel em http://localhost:${ADMIN_PORT}`));
    const proxyServer = createProxyApp().listen(PROXY_PORT, () => console.log(`[Proxy] API em http://localhost:${PROXY_PORT}`));
    return { adminServer, proxyServer };
}

if (require.main === module) start();
module.exports = { createAdminApp, createProxyApp, start, parseRetryAfter, prepareBody, publicKey, pool };
