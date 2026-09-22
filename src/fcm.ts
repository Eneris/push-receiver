import crypto from 'crypto'
import request, { getEndpoint } from './utils/request'

import type * as Types from './types'

const FCM_API = 'https://fcm.googleapis.com/'
const FCM_REGISTRATION = 'https://fcmregistrations.googleapis.com/v1/'
const FCM_INSTALLATION = 'https://firebaseinstallations.googleapis.com/v1/'
const AUTH_VERSION = 'FIS_v2'
const SDK_VERSION = 'w:0.6.6'

// TODO: FIXME it is optional to send it but better to implement proper heatbeat in the future
const getEmptyHeatbeat = () => btoa(JSON.stringify({ heartbeats: [], version: 2 })).toString()

function encodeBase64URL(value: string): string {
    return String(value).replace(/=/g, '').replace(/\+/g, '-').replace(/\//g, '_')
}

function generateFirebaseFID() {
    // A valid FID has exactly 22 base64 characters, which is 132 bits, or 16.5
    // bytes. our implementation generates a 17 byte array instead.
    const fid = crypto.randomBytes(17)

    // Replace the first 4 random bits with the constant FID header of 0b0111.
    fid[0] = 0b01110000 + (fid[0] % 0b00010000)

    // Firebase requires a FID to match /^[cdef][\w-]{21}$/, so base64url and
    // exactly 22 characters. Drop the 23rd, which only carries the extra 4 bits
    // of the 17th byte.
    return encodeBase64URL(fid.toString('base64')).substring(0, 22)
}

// The installation auth token is only valid for 7 days. Refresh a bit early so
// a token that is about to expire is not handed out as still valid.
const INSTALLATION_REFRESH_MARGIN = 60 * 60 * 1000 // in ms

export function isInstallationTokenExpired(installation: Types.InstallationData, margin = INSTALLATION_REFRESH_MARGIN): boolean {
    if (!installation.createdAt || !installation.expiresIn) return true

    return installation.createdAt + installation.expiresIn - margin <= Date.now()
}

export async function refreshFCMInstallationToken(installation: Types.InstallationData, config: Types.ClientConfig): Promise<Types.InstallationData> {
    // The FIS endpoint for this is projects/<projectId>/installations/<fid>/authTokens:generate.
    // Omitting the `installations/` segment is what made the previous
    // implementation fail with a 404 (see #27). The FID is encoded because it is
    // not known whether FIS echoed back the malformed FIDs this client sent
    // before the base64url fix, and '+' or '/' in a path segment would 404 too.
    const response = await request(getEndpoint(config, FCM_INSTALLATION, `installations/${encodeURIComponent(installation.fid)}/authTokens:generate`), {
        method: 'POST',
        headers: {
            Authorization: `${AUTH_VERSION} ${installation.refreshToken}`,
            'x-firebase-client': getEmptyHeatbeat(),
            'x-goog-api-key': config.firebase.apiKey,
        },
        body: JSON.stringify({
            installation: {
                sdkVersion: SDK_VERSION,
                appId: config.firebase.appId,
            }
        })
    })

    const data = await response.json() as Types.FcmInstallationAuthTokenResponse

    // FIS sends expiresIn as a duration string, '604800s'.
    const expiresIn = Number.parseInt(data.expiresIn) * 1000 // in ms

    // Without this the caller would persist an undefined token over one that is
    // merely expired, and broadcast it as a credentials change.
    if (!data.token || !Number.isFinite(expiresIn)) {
        throw new Error(`FCM installation token refresh returned an unusable response (token ${data.token ? 'present' : 'missing'}, expiresIn ${data.expiresIn})`)
    }

    return {
        ...installation,
        token: data.token,
        createdAt: (new Date()).getTime(), // in ms
        expiresIn,
    }
}

export async function installFCM(config: Types.ClientConfig): Promise<Types.InstallationData> {
    const response = await request(getEndpoint(config, FCM_INSTALLATION, 'installations'), {
        method: 'POST',
        headers: {
            'x-firebase-client': getEmptyHeatbeat(),
            'x-goog-api-key': config.firebase.apiKey
        },
        body: JSON.stringify({
            appId: config.firebase.appId,
            authVersion: AUTH_VERSION,
            fid: generateFirebaseFID(),
            sdkVersion: SDK_VERSION
        }),
    })

    const data = await response.json() as Types.FcmInstallationResponse

    return {
        token: data.authToken.token,
        createdAt: (new Date()).getTime(), // in ms
        expiresIn: Number.parseInt(data.authToken.expiresIn) * 1000, // in ms
        refreshToken: data.refreshToken,
        fid: data.fid,
    }
}

export async function registerFCM(gcmData: Types.GcmData, installation: Types.InstallationData, keys: Types.Keys, config: Types.ClientConfig): Promise<Types.FcmRegistrationResponse> {
    const requestOptions = {
        method: 'POST',
        headers: {
            'x-goog-api-key': config.firebase.apiKey,
            'x-goog-firebase-installations-auth': installation.token,
        },
        body: JSON.stringify({
            web: {
                // Include VAPID only if it's not default key, otherwise FCM registration will fail
                applicationPubKey: config.vapidKey || undefined,
                auth: encodeBase64URL(keys.authSecret),
                /**
                 * TODO
                 * Shouldn't endpoint be migrated to v1 too??? But official JS module still uses the old one...
                 * https://firebase.google.com/docs/cloud-messaging/migrate-v1
                 * Currently not working with
                 * Works - https://fcm.googleapis.com/fcm/send
                 * Does not work - https://fcm.googleapis.com/v1/projects/{projectId}/messages:send
                 */
                endpoint: `${FCM_API}fcm/send/${gcmData.token}`,
                p256dh: encodeBase64URL(keys.publicKey),
            }
        })
    }

    const response = await request(getEndpoint(config, FCM_REGISTRATION, 'registrations'), requestOptions)

    const data = await response.json()

    if (data.error) {
        throw new Error('FCM registration failed... ' + data.error.message)
    }

    return data
}

export default async function register(gcm: Types.GcmData, keys: Types.Keys, config: Types.ClientConfig): Promise<Types.FcmData> {
    const installation = await installFCM(config)
    const registration = await registerFCM(gcm, installation, keys, config)

    return {
        token: registration.token,
        installation
    }
}