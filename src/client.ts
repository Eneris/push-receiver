import Long from 'long'
import ProtobufJS from 'protobufjs'
import tls from 'tls'
import registerGCM, { checkIn } from './gcm'
import registerFCM, { refreshFCMInstallationToken } from './fcm'
import createKeys from './keys'
import Parser from './parser'
import decrypt from './utils/decrypt'
import Logger from './utils/logger'
import Protos, { mcs_proto } from './protos'
import defer from './utils/defer'

import Emitter from './emitter'

import { Variables, MCSProtoTag } from './constants'

import type * as Types from './types'

export {
    Types
}

ProtobufJS.util.Long = Long
ProtobufJS.configure()

const HOST = 'mtalk.google.com'
const PORT = 5228
const MAX_RETRY_TIMEOUT = 15

interface ClientEvents {
    ON_MESSAGE_RECEIVED: (data: Types.MessageEnvelope) => void
    ON_CREDENTIALS_CHANGE: (data: Types.EventChangeCredentials) => void
    ON_CONNECT: (data: void) => void
    ON_DISCONNECT: (data: void) => void
    ON_READY: (data: void) => void
    ON_HEARTBEAT: (data: void) => void
}

export default class PushReceiver extends Emitter<ClientEvents> {
    #config: Types.ClientConfig
    #socket: tls.TLSSocket
    #retryCount = 0
    #retryTimeout: NodeJS.Timeout
    #parser: Parser
    #heartbeatTimer?: NodeJS.Timeout
    #heartbeatTimeout?: NodeJS.Timeout
    #tokenRefreshTimer?: NodeJS.Timeout
    #streamId = 0
    #lastStreamIdReported = -1
    #ready = defer()

    persistentIds: Types.PersistentId[]

    get fcmToken() {
        return this.#config.credentials?.fcm?.token
    }

    constructor(config: Types.ClientConfig) {
        super()

        this.setDebug(config.debug)
        Logger.debug('constructor', config)

        this.#config = {
            bundleId: 'receiver.push.com',
            chromeId: 'org.chromium.linux',
            chromeVersion: '94.0.4606.51',
            vapidKey: '',
            persistentIds: [],
            heartbeatIntervalMs: 5 * 60 * 1000, // 5 min
            ...config
        }

        this.persistentIds = this.#config.persistentIds
    }

    get whenReady() {
        return this.#ready.promise;
    }

    setDebug(enabled?: boolean) {
        Logger.setDebug(enabled)
    }

    onNotification(listener: (data: Types.MessageEnvelope) => void): Types.DisposeFunction {
        return this.on('ON_MESSAGE_RECEIVED', listener)
    }

    onCredentialsChanged(listener: (data: Types.EventChangeCredentials) => void): Types.DisposeFunction {
        return this.on('ON_CREDENTIALS_CHANGE', listener)
    }

    onReady(listener: () => void): Types.DisposeFunction {
        return this.on('ON_READY', listener)
    }

    connect = async (): Promise<void> => {
        if (this.#socket) return

        await this.registerIfNeeded()

        Logger.debug('connect')

        this.#lastStreamIdReported = -1

        this.#socket = new tls.TLSSocket(null)
        this.#socket.setKeepAlive(true)
        this.#socket.on('connect', () => this.#handleSocketConnect())
        this.#socket.on('close', () => this.#handleSocketClose())
        this.#socket.on('error', (err) => this.#handleSocketError(err))
        this.#socket.connect({ host: HOST, port: PORT })

        this.#parser = new Parser(this.#socket)
        this.#parser.on('message', (data) => this.#handleMessage(data))
        this.#parser.on('error', (err) => this.#handleParserError(err))

        this.#sendLogin()

        return new Promise((res) => {
            const dispose = this.onReady(() => {
                dispose()
                res()
            })
        })
    }

    destroy = () => {
        this.#clearReady();

        clearTimeout(this.#retryTimeout)
        this.#clearHeartbeat()
        this.#clearTokenRefresh()

        if (this.#socket) {
            this.#socket.removeAllListeners()
            this.#socket.end()
            this.#socket.destroy()
            this.#socket = null
        }

        if (this.#parser) {
            this.#parser.destroy()
            this.#parser = null
        }
    }

    get #configMetaData() {
        return {
            bundleId: this.#config.bundleId,
            projectId: this.#config.firebase.projectId,
            vapidKey: this.#config.vapidKey
        }
    }

    checkCredentials(credentials: Types.Credentials) {
        // Structure check
        if (!credentials) return false
        if (!credentials.fcm || !credentials.gcm || !credentials.keys) return false
        if (!credentials.fcm.installation) return false
        if (!credentials.config) return false

        // Config data
        if (JSON.stringify(credentials.config) !== JSON.stringify(this.#configMetaData)) return false

        return true
    }

    /**
     * Clears the locally stored credentials, including the current FCM token reference.
     *
     * This does NOT revoke or delete the token on FCM/GCM servers; it only resets the
     * local registration state. If the client is currently connected, this will first
     * disconnect and destroy the existing connection.
     *
     * Call connect() again to register and obtain a new FCM token.
     */
    deleteToken(): void {
        const wasConnected = !!this.#socket

        if (wasConnected) {
            // Ensure no further messages are processed with invalidated credentials
            this.destroy()
        }

        const oldCredentials = this.#config.credentials
        this.#config.credentials = undefined

        if (oldCredentials) {
            if (wasConnected) {
                Logger.debug('Token deleted and existing connection destroyed')
            } else {
                Logger.debug('Token deleted')
            }
        }
    }

    /**
     * Refreshes the FCM installation token using the existing refresh token.
     * This refreshes the installation auth token that expires after 7 days, without
     * generating new encryption keys or performing a full re-registration.
     *
     * This is the proper way to refresh tokens and maintains the same Firebase Installation ID (FID).
     * Emits ON_CREDENTIALS_CHANGE event with the updated credentials.
     *
     * Note: The client automatically refreshes tokens before expiration. This method
     * is exposed for manual refresh if needed, but typically automatic refresh handles this.
     *
     * @returns Promise that resolves with the updated credentials
     * @throws Error if no existing credentials are found or if the refresh fails
     */
    async refreshToken(): Promise<Types.Credentials> {
        if (!this.#config.credentials?.fcm) {
            throw new Error('No existing credentials to refresh. Call connect() first to obtain initial credentials.')
        }

        Logger.debug('Refreshing FCM installation token')

        const oldCredentials = this.#config.credentials

        try {
            // Refresh the installation token using the refresh token
            const newInstallation = await refreshFCMInstallationToken(
                oldCredentials.fcm,
                this.#config
            )

            // Update credentials with new installation token
            const newCredentials: Types.Credentials = {
                ...oldCredentials,
                fcm: {
                    ...oldCredentials.fcm,
                    installation: newInstallation
                }
            }

            this.#config.credentials = newCredentials

            this.emit('ON_CREDENTIALS_CHANGE', {
                oldCredentials,
                newCredentials
            })

            Logger.debug('FCM installation token refreshed')

            // Reschedule automatic token refresh for the new token
            this.#scheduleTokenRefresh()

            return newCredentials
        } catch (error) {
            Logger.error('Failed to refresh FCM installation token:', error)
            throw error
        }
    }

    async registerIfNeeded(): Promise<Types.Credentials> {
        if (this.checkCredentials(this.#config.credentials)) {
            await checkIn(this.#config)

            // Schedule automatic token refresh for existing credentials
            this.#scheduleTokenRefresh()

            return this.#config.credentials
        }

        const keys = await createKeys()
        const gcm = await registerGCM(this.#config)
        const fcm = await registerFCM(gcm, keys, this.#config)

        const credentials: Types.Credentials = {
            keys,
            gcm,
            fcm,
            config: this.#configMetaData,
        }

        this.emit('ON_CREDENTIALS_CHANGE', {
            oldCredentials: this.#config.credentials,
            newCredentials: credentials
        })

        this.#config.credentials = credentials

        Logger.debug('got credentials', credentials)

        // Schedule automatic token refresh
        this.#scheduleTokenRefresh()

        return this.#config.credentials
    }

    #clearReady() {
        if (!this.#ready.isResolved) {
            this.#ready.reject(new Error('Client destroyed'))
        }

        this.#ready = defer()
    }

    #clearHeartbeat() {
        clearTimeout(this.#heartbeatTimer)
        this.#heartbeatTimer = undefined

        clearTimeout(this.#heartbeatTimeout)
        this.#heartbeatTimeout = undefined
    }

    #startHeartbeat() {
        this.#clearHeartbeat()

        if (!this.#config.heartbeatIntervalMs) return

        this.#heartbeatTimer = setTimeout(() => this.#sendHeartbeatPing(), this.#config.heartbeatIntervalMs)
        this.#heartbeatTimeout = setTimeout(() => this.#socketRetry(), this.#config.heartbeatIntervalMs * 2)
    }

    #clearTokenRefresh() {
        clearTimeout(this.#tokenRefreshTimer)
        this.#tokenRefreshTimer = undefined
    }

    #scheduleTokenRefresh() {
        this.#clearTokenRefresh()

        if (!this.#config.credentials?.fcm?.installation) return

        const { createdAt, expiresIn } = this.#config.credentials.fcm.installation
        const expiresAt = createdAt + expiresIn
        const now = Date.now()

        // Refresh token 1 day before expiration (or immediately if already expired)
        const refreshIn = Math.max(0, expiresAt - now - (24 * 60 * 60 * 1000))

        Logger.debug(`Scheduling token refresh in ${refreshIn / 1000 / 60 / 60} hours`)

        this.#tokenRefreshTimer = setTimeout(async () => {
            try {
                Logger.debug('Automatic token refresh triggered')
                await this.refreshToken()
                // refreshToken() already calls #scheduleTokenRefresh() on success
            } catch (error) {
                Logger.error('Automatic token refresh failed:', error)
                // Retry in 1 hour
                this.#tokenRefreshTimer = setTimeout(() => this.#scheduleTokenRefresh(), 60 * 60 * 1000)
            }
        }, refreshIn)
    }

    #handleSocketConnect = (): void => {
        this.#retryCount = 0
        this.emit('ON_CONNECT')
        this.#startHeartbeat()
    }

    #handleSocketClose = (): void => {
        this.emit('ON_DISCONNECT')
        this.#clearHeartbeat()
        this.#socketRetry()
    }

    #handleSocketError = (err: Error): void => {
        Logger.error(err)
        // ignore, the close handler takes care of retry
    }

    #socketRetry() {
        this.destroy()
        const timeout = Math.min(++this.#retryCount, MAX_RETRY_TIMEOUT) * 1000
        this.#retryTimeout = setTimeout(() => this.connect(), timeout)
    }

    #getStreamId(): number {
        this.#lastStreamIdReported = this.#streamId
        return this.#streamId
    }

    #newStreamIdAvailable(): boolean {
        return this.#lastStreamIdReported != this.#streamId
    }

    #sendHeartbeatPing() {
        const heartbeatPingRequest: Record<string, unknown> = {}

        if (this.#newStreamIdAvailable()) {
            heartbeatPingRequest.last_stream_id_received = this.#getStreamId()
        }

        Logger.debug('Heartbeat send pong', heartbeatPingRequest)

        const HeartbeatPingRequestType = Protos.mcs_proto.HeartbeatPing
        const errorMessage = HeartbeatPingRequestType.verify(heartbeatPingRequest)

        if (errorMessage) {
            throw new Error(errorMessage)
        }

        const buffer = HeartbeatPingRequestType.encodeDelimited(heartbeatPingRequest).finish()

        Logger.debug('HEARTBEAT sending PING', heartbeatPingRequest)

        this.#socket.write(Buffer.concat([
            Buffer.from([MCSProtoTag.kHeartbeatPingTag]),
            buffer,
        ]))
    }

    #sendHeartbeatPong(object) {
        const heartbeatAckRequest: Record<string, any> = {}

        if (this.#newStreamIdAvailable()) {
            heartbeatAckRequest.last_stream_id_received = this.#getStreamId()
        }

        if (object?.status) {
            heartbeatAckRequest.status = object.status
        }

        Logger.debug('Heartbeat send pong', heartbeatAckRequest)

        const HeartbeatAckRequestType = Protos.mcs_proto.HeartbeatAck
        const errorMessage = HeartbeatAckRequestType.verify(heartbeatAckRequest)
        if (errorMessage) {
            throw new Error(errorMessage)
        }

        const buffer = HeartbeatAckRequestType.encodeDelimited(heartbeatAckRequest).finish()

        Logger.debug('HEARTBEAT sending PONG', heartbeatAckRequest)

        this.#socket.write(Buffer.concat([
            Buffer.from([MCSProtoTag.kHeartbeatAckTag]),
            buffer
        ]))
    }

    #sendLogin() {
        const gcm = this.#config.credentials.gcm
        const LoginRequestType = Protos.mcs_proto.LoginRequest
        const hexAndroidId = Long.fromString(gcm.androidId).toString(16)
        const loginRequest: mcs_proto.ILoginRequest = {
            adaptiveHeartbeat: false,
            authService: 2,
            authToken: gcm.securityToken,
            id: `chrome-${this.#config.chromeVersion}`,
            domain: 'mcs.android.com',
            deviceId: `android-${hexAndroidId}`,
            networkType: 1,
            resource: gcm.androidId,
            user: gcm.androidId,
            useRmq2: true,
            setting: [{ name: 'new_vc', value: '1' }],
            clientEvent: [],
            // Id of the last notification received
            receivedPersistentId: this.#config.persistentIds,
        }

        if (this.#config.heartbeatIntervalMs) {
            loginRequest.heartbeatStat = {
                ip: '',
                timeout: true,
                intervalMs: this.#config.heartbeatIntervalMs,
            }
        }

        const errorMessage = LoginRequestType.verify(loginRequest)
        if (errorMessage) {
            throw new Error(errorMessage)
        }

        const buffer = LoginRequestType.encodeDelimited(loginRequest).finish()

        this.#socket.write(Buffer.concat([
            Buffer.from([Variables.kMCSVersion, MCSProtoTag.kLoginRequestTag]),
            buffer,
        ]))
    }

    #handleMessage = ({ tag, object }: Types.DataPacket): void => {
        // any message will reset the client side heartbeat timeout.
        this.#startHeartbeat()

        switch (tag) {
            case MCSProtoTag.kLoginResponseTag:
                // clear persistent ids, as we just sent them to the server while logging in
                this.#config.persistentIds = []
                this.emit('ON_READY')
                this.#startHeartbeat()
                this.#ready.resolve()
                break

            case MCSProtoTag.kDataMessageStanzaTag:
                this.#handleDataMessage(object)
                break

            case MCSProtoTag.kHeartbeatPingTag:
                this.emit('ON_HEARTBEAT')
                Logger.debug('HEARTBEAT PING', object)
                this.#sendHeartbeatPong(object)
                break

            case MCSProtoTag.kHeartbeatAckTag:
                this.emit('ON_HEARTBEAT')
                Logger.debug('HEARTBEAT PONG', object)
                break

            case MCSProtoTag.kCloseTag:
                Logger.debug('Close: Server requested close! message: ', JSON.stringify(object))
                this.#handleSocketClose()
                break

            case MCSProtoTag.kLoginRequestTag:
                Logger.debug('Login request: message: ', JSON.stringify(object))
                break

            case MCSProtoTag.kIqStanzaTag:
                Logger.debug('IqStanza: ', JSON.stringify(object))
                // FIXME: If anyone knows what is this and how to respond, please let me know
                break

            default:
                Logger.error('Unknown message: ', JSON.stringify(object))
                return

            // no default
        }

        this.#streamId++
    }

    #handleDataMessage = (object): void => {
        if (this.persistentIds.includes(object.persistentId)) {
            return
        }

        let message
        try {
            message = decrypt(object, this.#config.credentials.keys)
        } catch (error) {
            switch (true) {
                case error.message.includes('Unsupported state or unable to authenticate data'):
                case error.message.includes('crypto-key is missing'):
                case error.message.includes('salt is missing'):
                    // NOTE(ibash) Periodically we're unable to decrypt notifications. In
                    // all cases we've been able to receive future notifications using the
                    // same keys. So, we silently drop this notification.
                    Logger.warn('Message dropped as it could not be decrypted: ' + error.message)
                    return
                default:
                    throw error
            }
        }

        // Maintain persistentIds updated with the very last received value
        this.persistentIds.push(object.persistentId)
        // Send notification
        this.emit('ON_MESSAGE_RECEIVED', {
            message,
            // Needs to be saved by the client
            persistentId: object.persistentId,
        })
    }

    #handleParserError = (error) => {
        Logger.error(error)
        this.#socketRetry()
    }
}

export { PushReceiver }
