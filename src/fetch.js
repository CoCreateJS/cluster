import { decodeContent, getContentType } from '@cocreate/utils';

async function readResponseData(response) {
    const contentType = getContentType({ contentType: response.headers.get('content-type') });
    const value = decodeContent({ value: await response.arrayBuffer(), contentType });
    if (!/[/+]json$/.test(contentType) || typeof value !== 'string' || !value) return value;
    try { return JSON.parse(value); }
    catch { return value; }
}

/** Fetch with a default timeout and optional parsed HTTP errors; success returns a native Response. */
export default async function Fetch(input, init = {}) {
    const { timeout = 15000, throwHttpErrors = false, signal, ...options } = init;
    const timeoutSignal = Number.isFinite(timeout) && timeout > 0 ? AbortSignal.timeout(timeout) : undefined;
    const requestSignal = signal && timeoutSignal
        ? AbortSignal.any([signal, timeoutSignal])
        : signal || timeoutSignal;

    let response;
    try {
        response = await globalThis.fetch(input, { ...options, ...(requestSignal && { signal: requestSignal }) });
    } catch (error) {
        if (timeoutSignal?.aborted && !signal?.aborted) {
            throw new Error(`HTTP request timed out after ${timeout} ms.`, { cause: error });
        }
        throw error;
    }

    if (!throwHttpErrors || response.ok) return response;

    const data = await readResponseData(response.clone());
    const detail = typeof data === 'string' ? data.trim() : data?.message;
    const error = new Error(detail || `HTTP request failed (${response.status} ${response.statusText}).`);
    error.name = 'FetchError';
    error.response = {
        status: response.status,
        statusText: response.statusText,
        headers: response.headers,
        url: response.url,
        data
    };
    throw error;
}
