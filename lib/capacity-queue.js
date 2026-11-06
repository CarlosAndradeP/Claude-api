'use strict';

/** Short, bounded FIFO waiting room. No key/token is held while waiting. */
class CapacityQueue {
    constructor(pool, getLimits) {
        this.pool = pool;
        this.getLimits = getLimits;
        this.waiters = [];
        this.timer = null;
        this.scheduled = false;
    }

    acquire({ agentType, excludedIds, bytesIn, modelId, signal, waitMs }) {
        if (signal?.aborted) return Promise.resolve({ reservation: null, cancelled: true });
        const options = this.getLimits();
        const remaining = Math.max(0, Math.min(waitMs ?? options.queueWaitMs, options.queueWaitMs));
        if (!this.waiters.length) {
            const result = this.pool.acquire(agentType, excludedIds, bytesIn, false, modelId);
            if (result.reservation || !remaining || !options.maxQueueLength || (result.retryAfterMs > remaining && result.reason !== 'concurrency') || result.reason === 'model_forbidden') {
                return Promise.resolve(result);
            }
        }
        if (!remaining || this.waiters.length >= options.maxQueueLength) {
            return Promise.resolve({ reservation: null, ...this.pool.capacity(excludedIds, modelId), reason: 'queue_full' });
        }
        return new Promise(resolve => {
            const entry = { agentType, excludedIds, bytesIn, modelId, signal, resolve, deadline: Date.now() + remaining };
            entry.abort = () => { this.finish(entry, { reservation: null, cancelled: true }); this.wake(); };
            signal?.addEventListener('abort', entry.abort, { once: true });
            this.waiters.push(entry);
            this.wake();
        });
    }

    finish(entry, result) {
        const index = this.waiters.indexOf(entry);
        if (index < 0) return;
        this.waiters.splice(index, 1);
        entry.signal?.removeEventListener('abort', entry.abort);
        entry.resolve(result);
    }

    wake() {
        if (this.scheduled) return;
        this.scheduled = true;
        queueMicrotask(() => { this.scheduled = false; this.drain(); });
    }

    drain() {
        clearTimeout(this.timer);
        this.timer = null;
        const now = Date.now();
        let nextAt = now + 250;
        for (const entry of [...this.waiters]) {
            if (entry.signal?.aborted) { this.finish(entry, { reservation: null, cancelled: true }); continue; }
            if (now >= entry.deadline) {
                this.finish(entry, { reservation: null, ...this.pool.capacity(entry.excludedIds, entry.modelId), reason: 'queue_timeout' });
                continue;
            }
            const result = this.pool.acquire(entry.agentType, entry.excludedIds, entry.bytesIn, false, entry.modelId);
            if (result.reservation || result.reason === 'model_forbidden') this.finish(entry, result);
            else nextAt = Math.min(nextAt, entry.deadline, now + (result.retryAfterMs || 250));
        }
        if (this.waiters.length) this.timer = setTimeout(() => this.drain(), Math.max(1, nextAt - Date.now()));
    }
}

module.exports = { CapacityQueue };
