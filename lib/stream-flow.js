'use strict';

function waitForDrainOrClose(response, signal) {
    if (signal?.aborted) return Promise.reject(signal.reason);
    return new Promise((resolve, reject) => {
        const cleanup = () => {
            response.removeListener('drain', done);
            response.removeListener('close', done);
            signal?.removeEventListener('abort', aborted);
        };
        const done = () => { cleanup(); resolve(); };
        const aborted = () => { cleanup(); reject(signal.reason); };
        response.once('drain', done);
        response.once('close', done);
        signal?.addEventListener('abort', aborted, { once: true });
    });
}

module.exports = { waitForDrainOrClose };
