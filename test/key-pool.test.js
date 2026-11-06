'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { KeyPool, normalizeKey } = require('../lib/key-pool');

function key(id, role = 'all') {
    return normalizeKey({ id, key: `secret-${id}`, name: `Key ${id}`, role });
}

test('distribui chamadas entre chaves com capacidade livre', () => {
    let now = 1_000;
    const keys = [key('a'), key('b')];
    const pool = new KeyPool(keys, { now: () => now, requestsPerMinute: 60, burst: 1, maxConcurrent: 1 });

    const first = pool.acquire('none').reservation;
    assert.equal(first.key.id, 'a');
    first.finish({ status: 200, success: true, durationMs: 20 });

    const second = pool.acquire('none').reservation;
    assert.equal(second.key.id, 'b');
    second.finish({ status: 200, success: true, durationMs: 30 });
    assert.equal(keys[0].stats.successes, 1);
    assert.equal(keys[1].stats.successes, 1);
});

test('não excede concorrência nem token bucket de uma chave', () => {
    let now = 5_000;
    const pool = new KeyPool([key('a')], { now: () => now, requestsPerMinute: 60, burst: 1, maxConcurrent: 1 });
    const active = pool.acquire('none').reservation;
    assert.ok(active);
    assert.equal(pool.acquire('none').reservation, null);

    active.finish({ status: 200, success: true });
    assert.equal(pool.acquire('none').reservation, null, 'token continua consumido após finalizar');
    now += 1_000;
    assert.ok(pool.acquire('none').reservation, 'um token é reposto depois de um segundo a 60 RPM');
});

test('429 abre cooldown adaptativo e respeita Retry-After', () => {
    let now = 10_000;
    const item = key('a');
    const pool = new KeyPool([item], {
        now: () => now, requestsPerMinute: 60, burst: 2, baseCooldownMs: 1_000, maxCooldownMs: 20_000
    });
    pool.acquire('none').reservation.finish({ status: 429, retryAfterMs: 5_000, error: 'limit' });

    const blocked = pool.acquire('none');
    assert.equal(blocked.reservation, null);
    assert.equal(blocked.retryAfterMs, 5_000);
    assert.equal(item.stats.rateLimits, 1);
    assert.equal(pool.getHealth(item).status, 'rate_limited');

    now += 5_000;
    assert.ok(pool.acquire('none').reservation);
});

test('falha de autenticação isola a chave mas probe manual pode recuperá-la', () => {
    let now = 20_000;
    const item = key('a');
    const pool = new KeyPool([item], { now: () => now, burst: 3, maxCooldownMs: 60_000 });
    pool.acquire('none').reservation.finish({ status: 401, error: 'invalid key' });
    assert.equal(pool.acquire('none').reservation, null);
    assert.equal(pool.getHealth(item).status, 'unhealthy');

    const probe = pool.acquire('none', new Set(), 0, true).reservation;
    assert.ok(probe);
    probe.finish({ status: 200, success: true });
    assert.equal(pool.getHealth(item).status, 'healthy');
});

test('prioriza o papel do agente sem perder fallback', () => {
    const keys = [key('main', 'main'), key('sub', 'sub')];
    const pool = new KeyPool(keys, { burst: 3 });
    const forSub = pool.acquire('sub').reservation;
    assert.equal(forSub.key.id, 'sub');
    forSub.finish({ status: 500 });
    const fallback = pool.acquire('sub', new Set(['sub'])).reservation;
    assert.equal(fallback.key.id, 'main');
});

test('Retry-After maior que o teto local continua sendo respeitado', () => {
    const now = 1_000;
    const pool = new KeyPool([key('a')], { now: () => now, baseCooldownMs: 100, maxCooldownMs: 1_000 });
    pool.acquire().reservation.finish({ status: 429, retryAfterMs: 5_000 });
    assert.equal(pool.acquire().retryAfterMs, 5_000);
});

test('cancelamento libera a reserva sem isolar uma chave saudável', () => {
    const item = key('a');
    const pool = new KeyPool([item]);
    pool.acquire().reservation.finish({ status: 200, success: true });
    pool.acquire().reservation.finish({ status: 502, cancelled: true });
    assert.equal(pool.getHealth(item).status, 'healthy');
    assert.equal(pool.getHealth(item).inFlight, 0);
    assert.equal(item.stats.failures, 0);
    assert.equal(item.stats.cancellations, 1);
});

test('falha explícita não é convertida em sucesso pelo status HTTP', () => {
    const item = key('a');
    const pool = new KeyPool([item]);
    pool.acquire().reservation.finish({ status: 200, success: false, error: 'incomplete body' });
    assert.equal(item.stats.successes, 0);
    assert.equal(item.stats.failures, 1);
});

test('sucesso paralelo não apaga Retry-After nem falha de autenticação mais recente', () => {
    for (const status of [429, 401]) {
        const item = key('a');
        const pool = new KeyPool([item]);
        const failure = pool.acquire().reservation;
        const earlier = pool.acquire().reservation;
        failure.finish({ status, retryAfterMs: 65_000 });
        earlier.finish({ status: 200 });
        assert.equal(pool.acquire().reservation, null);
        assert.equal(item.health.consecutiveFailures, 1);
        assert.equal(pool.getHealth(item).status, status === 429 ? 'rate_limited' : 'unhealthy');
    }
});

test('Retry-After em respostas fora de ordem nunca encurta cooldown ativo', () => {
    let now = 1_000;
    const item = key('a');
    const pool = new KeyPool([item], { now: () => now, baseCooldownMs: 1_000 });
    const first = pool.acquire().reservation;
    const second = pool.acquire().reservation;
    first.finish({ status: 429, retryAfterMs: 80_000 });
    now += 100;
    second.finish({ status: 429, retryAfterMs: 1_000 });
    assert.equal(Date.parse(item.health.cooldownUntil), 81_000);
    assert.equal(pool.acquire('none', new Set(), 0, true).reservation, null, 'probe também respeita cooldown');
});

test('403 de um modelo não bloqueia outros e sucesso antigo não remove a restrição', () => {
    const item = key('a');
    const pool = new KeyPool([item], { burst: 10 });
    const denied = pool.acquire('none', new Set(), 0, false, 'restricted/model').reservation;
    const old = pool.acquire('none', new Set(), 0, false, 'restricted/model').reservation;
    denied.finish({ status: 403, errorScope: 'model_key' });
    old.finish({ status: 200 });
    assert.equal(pool.acquire('none', new Set(), 0, false, 'restricted/model').reservation, null);
    const unrelated = pool.acquire('none', new Set(), 0, false, 'working/model').reservation;
    assert.ok(unrelated);
    unrelated.finish({ status: 200 });
    assert.equal(item.health.circuitOpenUntil, null);
    assert.deepEqual(pool.getHealth(item).restrictedModels, ['restricted/model']);
});

test('400 e 404 contam falhas da requisição sem declarar uma chave saudável ou inválida', () => {
    const item = key('a');
    const pool = new KeyPool([item]);
    for (const status of [400, 404]) pool.acquire().reservation.finish({ status });
    assert.equal(item.stats.successes, 0);
    assert.equal(item.stats.failures, 2);
    assert.equal(item.stats.requestErrors, 2);
    assert.equal(item.health.consecutiveFailures, 0);
    assert.equal(pool.getHealth(item).status, 'unknown');
});

test('sucesso em endpoint público não comprova autenticação nem recupera circuito', () => {
    const item = key('a');
    const pool = new KeyPool([item]);
    const inference = pool.acquire().reservation;
    const publicList = pool.acquire().reservation;
    inference.finish({ status: 401 });
    publicList.finish({ status: 200, authenticated: false });
    assert.equal(item.health.authVerifiedAt, null);
    assert.equal(pool.getHealth(item).status, 'unhealthy');
});
