import delay from './timeout'
import Logger from './logger'
import type { ClientConfig } from '../types'

// In seconds
const MAX_RETRY_TIMEOUT = 15

// Step in seconds
const RETRY_STEP = 5

export default function requestWithRety(url: string, options?: globalThis.RequestInit, maxRetries = 3): Promise<Response> {
    return retry(0, url, options, maxRetries)
}

async function retry(retryCount = 0, url: string, options?: globalThis.RequestInit, maxRetries = 3): Promise<Response> {
    let response: Response

    try {
        // Only the fetch call itself belongs inside the try. Anything thrown
        // below has to reach the caller: catching it here would turn an
        // exhausted HTTP failure into a "network error" and restart the loop
        // without ever increasing retryCount, so the request would never
        // settle.
        response = await fetch(url, options)
    } catch {
        Logger.debug('Request failed with network error. Wait 10s and retry')
        // Fetch throws only for network errors. In that case we wait a bit and retry without increasing the count
        await delay(10_000) // 10 seconds

        return retry(retryCount, url, options, maxRetries)
    }

    // Server responded
    if (response.ok) return response

    Logger.debug(`Request failed : ${response.statusText}`)

    // Response not ok. This means server responded but with an error. We retry with increased retry count
    if (retryCount >= maxRetries) {
        throw new Error(`Request to ${url} failed after ${maxRetries + 1} attempts: ${response.status} ${response.statusText}`)
    }

    const timeout = Math.min(retryCount * RETRY_STEP, MAX_RETRY_TIMEOUT)

    Logger.debug(`Retrying in ${timeout} seconds`)

    await delay(timeout * 1000)

    return retry(retryCount + 1, url, options, maxRetries)
}

export const getEndpoint = (config: ClientConfig, baseUrl: string, path = '') => (
    `${baseUrl.endsWith('/') ? baseUrl : `${baseUrl}/`}projects/${config.firebase.projectId}/${path}`
)
