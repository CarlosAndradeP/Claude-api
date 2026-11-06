'use strict';

const fs = require('node:fs');
const path = require('node:path');

const NUMBER_BOUNDS = {
    requestsPerMinute: [1, 10_000], burst: [1, 100], maxConcurrent: [1, 100], maxAttempts: [1, 100],
    queueWaitMs: [0, 10_000], maxQueueLength: [0, 1_000]
};

function updateConfig(previous, patch) {
    if (!patch || typeof patch !== 'object' || Array.isArray(patch)) throw new Error('Configuração deve ser um objeto');
    const next = { ...previous };
    for (const field of ['disableAbort', 'transparentMode']) {
        if (patch[field] === undefined) continue;
        if (typeof patch[field] !== 'boolean') throw new Error(`${field} deve ser booleano`);
        next[field] = patch[field];
    }
    for (const [field, [minimum, maximum]] of Object.entries(NUMBER_BOUNDS)) {
        if (patch[field] === undefined) continue;
        if (!Number.isInteger(patch[field]) || patch[field] < minimum || patch[field] > maximum) {
            throw new Error(`${field} deve ser um inteiro entre ${minimum} e ${maximum}`);
        }
        next[field] = patch[field];
    }
    if (patch.healthCheckModel !== undefined) {
        if (typeof patch.healthCheckModel !== 'string' || !patch.healthCheckModel.trim() || patch.healthCheckModel.length > 256) {
            throw new Error('healthCheckModel deve identificar um modelo de chat disponível');
        }
        next.healthCheckModel = patch.healthCheckModel.trim();
    }
    return next;
}

function saveConfig(directory, config) {
    const destination = path.join(directory, 'proxy-config.json');
    fs.writeFileSync(`${destination}.tmp`, JSON.stringify(config, null, 2), { mode: 0o600 });
    fs.renameSync(`${destination}.tmp`, destination);
}

function loadConfig(directory, defaults) {
    const filename = path.join(directory, 'proxy-config.json');
    if (!fs.existsSync(filename)) return { ...defaults };
    try { return updateConfig(defaults, JSON.parse(fs.readFileSync(filename, 'utf8'))); }
    catch (error) {
        console.error('[Admin] Configuração salva inválida; usando valores iniciais:', error.message);
        return { ...defaults };
    }
}

module.exports = { loadConfig, saveConfig, updateConfig };
