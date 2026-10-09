'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const { waitForDrainOrClose } = require('../lib/stream-flow');

for (const event of ['drain', 'close']) {
    test(`backpressure termina em ${event} e remove listeners`, async () => {
        const response = new EventEmitter();
        const pending = waitForDrainOrClose(response, new AbortController().signal);
        response.emit(event);
        await pending;
        assert.equal(response.listenerCount('drain'), 0);
        assert.equal(response.listenerCount('close'), 0);
    });
}

test('timeout interrompe espera por cliente lento e libera listeners', async () => {
    const response = new EventEmitter();
    const controller = new AbortController();
    const pending = waitForDrainOrClose(response, controller.signal);
    const rejected = assert.rejects(pending, { name: 'AbortError' });
    controller.abort();
    await rejected;
    assert.equal(response.listenerCount('drain'), 0);
    assert.equal(response.listenerCount('close'), 0);
    await assert.rejects(waitForDrainOrClose(response, controller.signal), { name: 'AbortError' });
});
