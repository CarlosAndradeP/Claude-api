'use strict';

function classifyUpstreamError(status, body = '', model = '') {
    let data;
    try { data = JSON.parse(body); } catch {}
    const code = String(data?.error?.code || data?.error?.type || data?.code || '').toLowerCase();
    const detail = String(data?.error?.message || data?.message || body);
    const invalidCredential = status === 401 || (status === 403 && (
        ['invalid_api_key', 'invalid_token', 'expired_token', 'authentication_error', 'unauthorized'].includes(code)
        || /invalid api key|invalid authentication token|expired authentication token/i.test(detail)
    ));
    if (invalidCredential) return { type: 'upstream_authentication_error', scope: 'key', retryable: false };
    if (status === 403) return { type: 'upstream_model_forbidden', scope: model ? 'model_key' : 'request', retryable: false };
    if (status === 404) return { type: 'upstream_endpoint_not_found', scope: 'model', retryable: false };
    if (status === 429) return { type: 'upstream_rate_limited', scope: 'key', retryable: true };
    if (status === 408 || status === 504) return { type: 'upstream_timeout', scope: 'provider', retryable: true };
    if (status >= 500) return { type: 'upstream_unavailable', scope: 'provider', retryable: true };
    return { type: 'upstream_request_error', scope: 'request', retryable: false };
}

function errorHeaders(res, error) {
    res.setHeader('x-proxy-error-type', error.type);
    res.setHeader('x-proxy-error-scope', error.scope);
    res.setHeader('x-proxy-retryable', String(error.retryable));
}

module.exports = { classifyUpstreamError, errorHeaders };
