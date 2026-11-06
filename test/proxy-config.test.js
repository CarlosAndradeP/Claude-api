'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { loadConfig, saveConfig, updateConfig } = require('../lib/proxy-config');

test('configuração persistida sobrevive a novo carregamento e valida alterações atomicamente', t => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'nim-config-test-'));
    t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
    const defaults = { disableAbort: false, transparentMode: true, burst: 3, queueWaitMs: 3000, healthCheckModel: 'test/model' };
    const updated = updateConfig(defaults, { burst: 8, queueWaitMs: 100, disableAbort: true });
    saveConfig(directory, updated);
    assert.deepEqual(loadConfig(directory, defaults), updated);
    assert.throws(() => updateConfig(updated, { burst: 5, queueWaitMs: -1 }));
    assert.deepEqual(loadConfig(directory, defaults), updated);
    assert.throws(() => updateConfig(updated, { maxQueueLength: 1001 }));
    assert.throws(() => updateConfig(updated, { healthCheckModel: '' }));
    assert.throws(() => updateConfig(updated, { disableAbort: 'false' }));
    assert.deepEqual(updateConfig(updated, { queueWaitMs: 0, maxQueueLength: 0 }).queueWaitMs, 0);
});
