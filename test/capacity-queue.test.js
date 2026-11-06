'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { KeyPool, normalizeKey } = require('../lib/key-pool');
const { CapacityQueue } = require('../lib/capacity-queue');

function setup(overrides = {}) {
    const key = normalizeKey({ id: 'a', key: 'fake-only' });
    let queue;
    const pool = new KeyPool([key], { burst: 10, requestsPerMinute: 10_000, maxConcurrent: 1, onChange: () => queue?.wake() });
    queue = new CapacityQueue(pool, () => ({ queueWaitMs: 100, maxQueueLength: 2, ...overrides }));
    const request = signal => queue.acquire({ agentType: 'main', excludedIds: new Set(), modelId: 'test/model', bytesIn: 1, signal });
    return { pool, queue, request };
}

test('fila espera capacidade e atende em ordem sem reservar tokens enquanto espera', async () => {
    const { pool, queue, request } = setup();
    const active = pool.acquire().reservation;
    const first = request();
    const second = request();
    assert.equal(queue.waiters.length, 2);
    assert.equal(pool.keys[0].stats.attempts, 1);
    active.finish({ status: 200 });
    const one = await first;
    assert.ok(one.reservation);
    assert.equal(queue.waiters.length, 1);
    one.reservation.finish({ status: 200 });
    const two = await second;
    assert.ok(two.reservation);
    two.reservation.finish({ status: 200 });
    assert.equal(queue.waiters.length, 0);
});

test('fila cheia e prazo de espera não aumentam tentativas upstream', async () => {
    const { pool, queue, request } = setup({ queueWaitMs: 25, maxQueueLength: 1 });
    const active = pool.acquire().reservation;
    const waiting = request();
    const overflow = await request();
    assert.equal(overflow.reason, 'queue_full');
    assert.equal((await waiting).reason, 'queue_timeout');
    assert.equal(pool.keys[0].stats.attempts, 1);
    assert.equal(queue.waiters.length, 0);
    active.finish({ status: 200 });
});

test('cancelar na fila não inicia inferência nem mantém listeners', async () => {
    const { pool, queue, request } = setup();
    const active = pool.acquire().reservation;
    const controller = new AbortController();
    const waiting = request(controller.signal);
    controller.abort();
    assert.equal((await waiting).cancelled, true);
    assert.equal(queue.waiters.length, 0);
    assert.equal(pool.keys[0].stats.attempts, 1);
    active.finish({ status: 200 });
});

test('cooldown longo responde cedo e não ocupa a fila curta', async () => {
    const { pool, queue, request } = setup();
    pool.acquire().reservation.finish({ status: 429, retryAfterMs: 65_000 });
    const result = await request();
    assert.equal(result.reservation, null);
    assert.ok(result.retryAfterMs > 64_000);
    assert.equal(queue.waiters.length, 0);
});
