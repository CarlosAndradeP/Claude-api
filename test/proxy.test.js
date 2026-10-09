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
let proxy, admin, upstream, base, adminBase, pool;

before(async () => {
    upstream = http.createServer((req, res) => {
        requests.push({ key: req.headers.authorization, agentType: req.headers['x-agent-type'] });
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
        if (mode === 'stalled' || mode === 'cancel') return;
        if (mode === 'truncated') return setTimeout(() => res.destroy(), 20);
        res.end('data: [DONE]\n\n');
    });
    upstream.listen(0, '127.0.0.1');
    await once(upstream, 'listening');
    process.env.UPSTREAM_ORIGIN = `http://127.0.0.1:${upstream.address().port}`;
    const server = require('../server');
    pool = server.pool;
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

function request(signal) {
    return fetch(base, { method: 'POST', headers: { 'content-type': 'application/json', 'x-agent-type': 'main' }, body: '{"stream":true}', signal });
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
