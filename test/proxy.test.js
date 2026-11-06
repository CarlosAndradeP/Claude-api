'use strict';

const { test, before, after } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const { once } = require('node:events');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { normalizeKey, createStats } = require('../lib/key-pool');

const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'nim-proxy-test-'));
process.env.DATA_DIR = directory;
process.env.UPSTREAM_IDLE_TIMEOUT_MS = '150';
fs.writeFileSync(path.join(directory, 'keys.json'), JSON.stringify([
    { id: 'a', key: 'test-a', name: 'A', role: 'main' },
    { id: 'b', key: 'test-b', name: 'B', role: 'sub' }
]));
let mode = 'success';
let requests = [];
let proxy, admin, upstream, base, adminBase, pool, capacityQueue;

before(async () => {
    upstream = http.createServer(async (req, res) => {
        const buffers = [];
        for await (const chunk of req) buffers.push(chunk);
        let body;
        try { body = JSON.parse(Buffer.concat(buffers).toString()); } catch {}
        requests.push({ key: req.headers.authorization, agentType: req.headers['x-agent-type'], body, path: req.url });
        if (req.method === 'GET' && req.url === '/v1/models') {
            res.writeHead(200, { 'content-type': 'application/json' });
            return res.end('{"data":[{"id":"test/model"}]}');
        }
        if (mode === 'invalid-inference' || mode === 'invalid-403') {
            res.writeHead(mode === 'invalid-403' ? 403 : 401, { 'content-type': 'application/json' });
            return res.end('{"error":{"code":"invalid_api_key","message":"invalid inference key"}}');
        }
        if (mode === 'model-forbidden' && body?.model === 'restricted/model') {
            res.writeHead(403, { 'content-type': 'application/json' });
            return res.end('{"error":{"message":"Model access is forbidden"}}');
        }
        if (mode === 'not-found') {
            res.writeHead(404, { 'content-type': 'application/json' });
            return res.end('{"error":{"message":"Model not found"}}');
        }
        if (mode === 'probe') {
            res.writeHead(200, { 'content-type': 'application/json' });
            return res.end('{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}');
        }
        if (mode === 'slow-headers') {
            return setTimeout(() => { if (!res.destroyed) { res.writeHead(200); res.end('{}'); } }, 250);
        }
        if (mode === 'rate-limit') {
            res.writeHead(429, { 'retry-after': '1' });
            return res.end('limited');
        }
        if (mode === 'fallback' && req.headers.authorization === 'Bearer test-a') {
            res.writeHead(401);
            return res.end('invalid key');
        }
        if (mode === 'bad-request') {
            res.writeHead(400);
            return res.end('invalid payload');
        }
        res.writeHead(200, { 'content-type': 'text/event-stream' });
        res.write('data: {"chunk":1}\n\n');
        if (mode === 'finish-later') return setTimeout(() => { if (!res.destroyed) res.end('data: [DONE]\n\n'); }, 80);
        if (mode === 'long-stream') {
            let count = 0;
            const timer = setInterval(() => {
                if (res.destroyed) { clearInterval(timer); return; }
                res.write('data: {"chunk":2}\n\n');
                if (++count === 6) { clearInterval(timer); res.end('data: [DONE]\n\n'); }
            }, 40);
            return;
        }
        if (mode === 'stalled' || mode === 'cancel') return;
        if (mode === 'truncated') return setTimeout(() => res.destroy(), 20);
        res.end('data: [DONE]\n\n');
    });
    upstream.listen(0, '127.0.0.1');
    await once(upstream, 'listening');
    process.env.UPSTREAM_ORIGIN = `http://127.0.0.1:${upstream.address().port}`;
    const server = require('../server');
    pool = server.pool;
    capacityQueue = server.capacityQueue;
    proxy = server.createProxyApp().listen(0, '127.0.0.1');
    admin = server.createAdminApp().listen(0, '127.0.0.1');
    await Promise.all([once(proxy, 'listening'), once(admin, 'listening')]);
    base = `http://127.0.0.1:${proxy.address().port}/v1/chat/completions`;
    adminBase = `http://127.0.0.1:${admin.address().port}/api/config`;
});

after(async () => {
    for (const server of [proxy, admin, upstream]) {
        const closed = once(server, 'close');
        server.close();
        server.closeAllConnections();
        await closed;
    }
    // Let the debounced write finish before removing the isolated data directory.
    await new Promise(resolve => setTimeout(resolve, 600));
    fs.rmSync(directory, { recursive: true, force: true });
});

function reset(nextMode) {
    mode = nextMode;
    requests = [];
    pool.runtime.clear();
    for (const key of pool.keys) {
        key.health = normalizeKey({}).health;
        key.stats = createStats();
    }
}

function request(signal, model = 'test/model', headers = {}) {
    return fetch(base, { method: 'POST', headers: { 'content-type': 'application/json', 'x-agent-type': 'main', ...headers }, body: JSON.stringify({ model, stream: true }), signal });
}

test('fallback troca a chave e encaminha o stream completo', async () => {
    reset('fallback');
    const response = await request();
    assert.equal(response.status, 200);
    assert.match(await response.text(), /\[DONE\]/);
    assert.deepEqual(requests.map(item => item.key), ['Bearer test-a', 'Bearer test-b']);
    assert.ok(requests.every(item => item.agentType === undefined));
    assert.equal(pool.getHealth(pool.keys[0]).status, 'unhealthy');
});

test('400 não causa fallback nem multiplica a requisição', async () => {
    reset('bad-request');
    const response = await request();
    assert.equal(response.status, 400);
    assert.equal(await response.text(), 'invalid payload');
    assert.equal(requests.length, 1);
});

test('429 anuncia o cooldown real do pool mesmo com Retry-After curto', async () => {
    reset('rate-limit');
    const response = await request();
    assert.equal(response.status, 429);
    assert.ok(Number(response.headers.get('retry-after')) >= 64);
    await response.text();
});

test('stream truncado falha no cliente e libera todas as reservas', async () => {
    reset('truncated');
    const response = await request();
    await assert.rejects(response.text());
    assert.equal(pool.getHealth(pool.keys[0]).inFlight, 0);
    assert.equal(pool.keys[0].stats.failures, 1);
    assert.equal(requests.length, 1, 'não repetir geração após enviar chunks');
});

test('timeout de inatividade encerra stream parado após os headers', async () => {
    reset('stalled');
    const response = await request();
    await assert.rejects(response.text());
    assert.equal(pool.getHealth(pool.keys[0]).inFlight, 0);
});

test('cancelar o cliente libera a chave sem abrir circuito', async () => {
    reset('cancel');
    const controller = new AbortController();
    const response = await request(controller.signal);
    const reader = response.body.getReader();
    await reader.read();
    controller.abort();
    await assert.rejects(reader.read());
    for (let i = 0; i < 50 && pool.getHealth(pool.keys[0]).inFlight; i++) {
        await new Promise(resolve => setTimeout(resolve, 10));
    }
    assert.equal(pool.getHealth(pool.keys[0]).inFlight, 0);
    assert.equal(pool.keys[0].health.circuitOpenUntil, null);
    assert.equal(pool.keys[0].stats.failures, 0);
});

test('configuração inválida não altera opções nem os limites do pool', async () => {
    const previous = await (await fetch(adminBase)).json();
    const response = await fetch(adminBase, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ disableAbort: true, burst: 9, maxAttempts: 0 }) });
    assert.equal(response.status, 400);
    assert.deepEqual(await (await fetch(adminBase)).json(), previous);
    assert.equal(pool.limits.burst, previous.burst);
    for (const body of [{ burst: 1.5 }, { disableAbort: 'false' }]) {
        assert.equal((await fetch(adminBase, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) })).status, 400);
    }
});

test('404 mantém status e body, classifica o erro e não indica sucesso da chave', async () => {
    reset('not-found');
    const response = await request(undefined, 'test/model', { 'x-request-id': 'audit-request-404' });
    assert.equal(response.status, 404);
    assert.equal(response.headers.get('x-request-id'), 'audit-request-404');
    assert.equal(response.headers.get('x-proxy-error-scope'), 'model');
    assert.equal(response.headers.get('x-proxy-retryable'), 'false');
    assert.deepEqual(await response.json(), { error: { message: 'Model not found' } });
    assert.equal(requests.length, 1);
    assert.equal(pool.keys[0].stats.successes, 0);
    assert.equal(pool.keys[0].stats.requestErrors, 1);
    assert.equal(pool.keys[0].health.circuitOpenUntil, null);
});

test('403 de modelo não bloqueia outros modelos e não é reclassificado como falta de capacidade', async () => {
    reset('model-forbidden');
    const restricted = await request(undefined, 'restricted/model');
    assert.equal(restricted.status, 403);
    assert.equal(restricted.headers.get('x-proxy-error-scope'), 'model_key');
    await restricted.text();
    assert.equal(requests.length, 2);
    assert.ok(pool.keys.every(key => key.health.circuitOpenUntil === null));
    const cached = await request(undefined, 'restricted/model');
    assert.equal(cached.status, 403);
    await cached.text();
    assert.equal(requests.length, 2, 'restrição repetida não consome novas chamadas');
    const unrelated = await request(undefined, 'working/model');
    assert.equal(unrelated.status, 200);
    assert.match(await unrelated.text(), /DONE/);
});

test('403 com código explícito de chave inválida ainda isola a credencial', async () => {
    reset('invalid-403');
    const response = await request();
    assert.equal(response.status, 403);
    assert.equal(response.headers.get('x-proxy-error-scope'), 'key');
    await response.text();
    assert.ok(pool.keys.every(key => key.health.circuitOpenUntil));
});

test('catálogo público não valida chave e probe testa inferência real', async () => {
    reset('invalid-inference');
    const publicList = await fetch(base.replace('/chat/completions', '/models'));
    assert.equal(publicList.status, 200);
    await publicList.text();
    assert.equal(pool.getHealth(pool.keys[0]).status, 'unknown');
    const invalid = await fetch(adminBase.replace('/config', '/keys/a/check'), { method: 'POST' });
    assert.equal(invalid.status, 401);
    await invalid.text();
    assert.equal(pool.getHealth(pool.keys[0]).status, 'unhealthy');
    mode = 'probe';
    const valid = await fetch(adminBase.replace('/config', '/keys/a/check'), { method: 'POST' });
    assert.equal(valid.status, 200);
    const key = await valid.json();
    assert.equal(key.health.status, 'healthy');
    assert.ok(key.health.authVerifiedAt);
    assert.equal(requests.at(-1).path, '/v1/chat/completions');
    assert.equal(requests.at(-1).body.max_tokens, 1);
});

test('fila curta absorve concorrência e cancelar na fila não chama upstream', async () => {
    const previousBurst = pool.limits.burst;
    pool.setLimits({ burst: 10 });
    reset('success');
    const held = [];
    for (let i = 0; i < 4; i++) held.push(pool.acquire().reservation);
    assert.ok(held.every(Boolean));
    try {
        const waiting = request();
        for (let i = 0; i < 100 && !capacityQueue.waiters.length; i++) await new Promise(resolve => setTimeout(resolve, 5));
        assert.equal(capacityQueue.waiters.length, 1);
        assert.equal(requests.length, 0);
        held.shift().finish({ status: 200 });
        const response = await waiting;
        assert.equal(response.status, 200);
        await response.text();
        assert.equal(requests.length, 1);
        // Occupy the released slot again; the next request must be cancellable while queued.
        held.push(pool.acquire().reservation);
        assert.ok(held.at(-1));
        const controller = new AbortController();
        const cancelled = request(controller.signal);
        for (let i = 0; i < 100 && !capacityQueue.waiters.length; i++) await new Promise(resolve => setTimeout(resolve, 5));
        assert.equal(capacityQueue.waiters.length, 1);
        controller.abort();
        await assert.rejects(cancelled);
        for (let i = 0; i < 100 && capacityQueue.waiters.length; i++) await new Promise(resolve => setTimeout(resolve, 5));
        assert.equal(capacityQueue.waiters.length, 0);
        assert.equal(requests.length, 1);
    } finally {
        for (const active of held) active?.finish({ cancelled: true });
        pool.setLimits({ burst: previousBurst });
    }
});

test('prazo do cliente limita tentativas antes dos headers e preserva metadados de timeout', async () => {
    reset('slow-headers');
    const started = Date.now();
    const response = await request(undefined, 'test/model', { 'x-request-timeout-ms': '80' });
    assert.equal(response.status, 504);
    const data = await response.json();
    assert.equal(data.error.type, 'upstream_timeout');
    assert.equal(data.error.retryable, true);
    assert.equal(requests.length, 1);
    assert.ok(Date.now() - started < 1000);
    assert.ok(pool.keys.every(key => pool.getHealth(key).inFlight === 0));
});

test('configuração válida é persistida sem incluir chaves privadas', async () => {
    const response = await fetch(adminBase, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ queueWaitMs: 500, maxQueueLength: 20, healthCheckModel: 'custom/check-model' }) });
    assert.equal(response.status, 200);
    const saved = JSON.parse(fs.readFileSync(path.join(directory, 'proxy-config.json'), 'utf8'));
    assert.equal(saved.queueWaitMs, 500);
    assert.equal(saved.healthCheckModel, 'custom/check-model');
    assert.equal(JSON.stringify(saved).includes('test-a'), false);
});

test('prazo para headers não interrompe stream saudável que continua entregando chunks', async () => {
    reset('long-stream');
    const response = await request(undefined, 'test/model', { 'x-request-timeout-ms': '80' });
    assert.equal(response.status, 200);
    assert.match(await response.text(), /DONE/);
    assert.equal(requests.length, 1);
    assert.ok(pool.keys.every(key => pool.getHealth(key).inFlight === 0));
});

test('disableAbort preserva geração já iniciada e libera a chave ao terminar', async () => {
    reset('finish-later');
    await fetch(adminBase, { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{"disableAbort":true}' });
    try {
        const controller = new AbortController();
        const response = await request(controller.signal);
        const reader = response.body.getReader();
        await reader.read();
        controller.abort();
        await assert.rejects(reader.read());
        for (let i = 0; i < 100 && pool.getHealth(pool.keys[0]).inFlight; i++) await new Promise(resolve => setTimeout(resolve, 5));
        assert.equal(pool.getHealth(pool.keys[0]).inFlight, 0);
        assert.equal(pool.keys[0].stats.successes, 1);
        assert.equal(pool.keys[0].stats.failures, 0);
        assert.equal(requests.length, 1);
    } finally {
        await fetch(adminBase, { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{"disableAbort":false}' });
    }
});
