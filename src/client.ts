import Long from 'long'
import ProtobufJS from 'protobufjs'
import tls from 'tls'
import registerGCM, { checkIn } from './gcm'
import registerFCM, { isInstallationTokenExpired, refreshFCMInstallationToken } from './fcm'
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
const STREAM_ACK_EXTENSION_ID = 13
const STREAM_ACK_BATCH_SIZE = 10
const STREAM_ACK_DELAY_MS = 1000

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
    #streamId = 0
    #lastStreamIdReported = -1
    #unackedDataCount = 0
    #ackTimer?: NodeJS.Timeout
    #ready = defer()

    persistentIds: Types.PersistentId[]

    get fcmToken() {
        return this.#config.credentials?.fcm?.token
    }

    constructor(config: Types.ClientConfig) {
        super()

        this.setDebug(config.debug)

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

        this.#streamId = 0
        this.#lastStreamIdReported = -1
        this.#unackedDataCount = 0

        this.#socket = new tls.TLSSocket(null)
        // Probe after 30s idle instead of the OS default (7200s on Linux).
        // Without an explicit initialDelay, half-open connections caused by
        // residential NAT aging go undetected for 2+ hours. 30s makes a
        // stalled socket trigger 'error'/'close' → existing retry logic
        // within ~60-90s.
        this.#socket.setKeepAlive(true, 30_000)
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
        this.#clearAckTimer()
        this.#clearHeartbeat()

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

    async registerIfNeeded(): Promise<Types.Credentials> {
        if (this.checkCredentials(this.#config.credentials)) {
            await checkIn(this.#config)

            return this.#refreshInstallationIfNeeded(this.#config.credentials)
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

        return this.#config.credentials
    }

    // Persisted credentials keep working for MCS, but their installation auth
    // token expires after 7 days. Nothing in here consumes it yet (a fresh
    // registration always installs a new one), so this keeps it valid for
    // consumers that read it out of the credentials we hand them.
    async #refreshInstallationIfNeeded(credentials: Types.Credentials): Promise<Types.Credentials> {
        if (!isInstallationTokenExpired(credentials.fcm.installation)) {
            return credentials
        }

        try {
            const installation = await refreshFCMInstallationToken(credentials.fcm.installation, this.#config)
            const newCredentials: Types.Credentials = {
                ...credentials,
                fcm: {
                    ...credentials.fcm,
                    installation,
                },
            }

            this.emit('ON_CREDENTIALS_CHANGE', {
                oldCredentials: credentials,
                newCredentials
            })

            this.#config.credentials = newCredentials

            Logger.debug('refreshed expired FCM installation token')

            return newCredentials
        } catch (error) {
            // Keep the existing credentials: they are still usable for
            // receiving messages, and connecting must not depend on FIS.
            Logger.warn('Failed to refresh FCM installation token', error)

            return credentials
        }
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

    #clearAckTimer() {
        clearTimeout(this.#ackTimer)
        this.#ackTimer = undefined
    }

    #writeAcknowledgement(tag: MCSProtoTag, buffer: Uint8Array, streamId?: number) {
        if (!this.#socket) return

        try {
            // A false return means backpressure, not failure: the bytes are queued.
            this.#socket.write(Buffer.concat([Buffer.from([tag]), buffer]))
        } catch (error) {
            Logger.error('Failed to send push acknowledgement', error)
            this.#socketRetry()
            return
        }

        // Commit only after the write accepts the packet. Keep persistent IDs
        // for reconnect in case the socket fails before the server receives it.
        if (streamId !== undefined) {
            this.#lastStreamIdReported = streamId
            this.#unackedDataCount = 0
            this.#clearAckTimer()
        }
    }

    #newStreamIdAvailable(): boolean {
        return this.#lastStreamIdReported != this.#streamId
    }

    #sendHeartbeatPing() {
        const heartbeatPingRequest: mcs_proto.IHeartbeatPing = {}

        if (this.#newStreamIdAvailable()) {
            heartbeatPingRequest.lastStreamIdReceived = this.#streamId
        }

        const HeartbeatPingRequestType = Protos.mcs_proto.HeartbeatPing
        const errorMessage = HeartbeatPingRequestType.verify(heartbeatPingRequest)

        if (errorMessage) {
            throw new Error(errorMessage)
        }

        const buffer = HeartbeatPingRequestType.encodeDelimited(heartbeatPingRequest).finish()

        Logger.debug('HEARTBEAT sending PING', heartbeatPingRequest)

        this.#writeAcknowledgement(MCSProtoTag.kHeartbeatPingTag, buffer, heartbeatPingRequest.lastStreamIdReceived)
    }

    #sendHeartbeatPong(object) {
        const heartbeatAckRequest: mcs_proto.IHeartbeatAck = {}

        if (this.#newStreamIdAvailable()) {
            heartbeatAckRequest.lastStreamIdReceived = this.#streamId
        }

        if (object?.status !== undefined) {
            // Parser emits int64 fields as strings; preserve all bits and zero.
            heartbeatAckRequest.status = Long.fromValue(object.status)
        }

        const HeartbeatAckRequestType = Protos.mcs_proto.HeartbeatAck
        const errorMessage = HeartbeatAckRequestType.verify(heartbeatAckRequest)
        if (errorMessage) {
            throw new Error(errorMessage)
        }

        const buffer = HeartbeatAckRequestType.encodeDelimited(heartbeatAckRequest).finish()

        Logger.debug('HEARTBEAT sending PONG', heartbeatAckRequest)

        this.#writeAcknowledgement(MCSProtoTag.kHeartbeatAckTag, buffer, heartbeatAckRequest.lastStreamIdReceived)
    }

    #sendStreamAck() {
        if (!this.#socket || !this.#unackedDataCount) return

        // MCS stream ACK: IQ SET with extension 13 and a cumulative receive ID.
        const IqStanza = Protos.mcs_proto.IqStanza
        const acknowledgement: mcs_proto.IIqStanza = {
            type: IqStanza.IqType.SET,
            id: '',
            extension: { id: STREAM_ACK_EXTENSION_ID, data: Buffer.alloc(0) },
            lastStreamIdReceived: this.#streamId,
        }
        const errorMessage = IqStanza.verify(acknowledgement)
        if (errorMessage) throw new Error(errorMessage)

        this.#writeAcknowledgement(
            MCSProtoTag.kIqStanzaTag,
            IqStanza.encodeDelimited(acknowledgement).finish(),
            acknowledgement.lastStreamIdReceived,
        )
    }

    #acknowledgeDataMessage(object: mcs_proto.IDataMessageStanza) {
        this.#unackedDataCount++
        if (object.immediateAck || this.#unackedDataCount >= STREAM_ACK_BATCH_SIZE) {
            this.#sendStreamAck()
        } else if (!this.#ackTimer) {
            // Flush short bursts without waiting for the next heartbeat.
            this.#ackTimer = setTimeout(() => {
                this.#ackTimer = undefined
                this.#sendStreamAck()
            }, STREAM_ACK_DELAY_MS)
        }
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
            // Include IDs received since earlier logins, not a stale config array.
            receivedPersistentId: this.persistentIds,
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
        // Every incoming packet, including this one, contributes to stream ACKs.
        this.#streamId++
        // any message will reset the client side heartbeat timeout.
        this.#startHeartbeat()

        switch (tag) {
            case MCSProtoTag.kLoginResponseTag:
                // Retain the public ID history for deduplication and reconnects.
                this.emit('ON_READY')
                this.#startHeartbeat()
                this.#ready.resolve()
                break

            case MCSProtoTag.kDataMessageStanzaTag:
                // Transport receipt does not depend on decryption or user callbacks.
                this.#acknowledgeDataMessage(object)
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
    }

    #handleDataMessage = (object): void => {
        if (object.persistentId && this.persistentIds.includes(object.persistentId)) {
            return
        }

        // Google deletion controls have no encrypted body or encryption headers.
        // Do not mistake a malformed/encrypted notification for this control.
        const appData = object.appData || []
        if (!object.rawData?.length &&
            appData.some(item => item.key === 'message_type' && item.value === 'deleted_messages') &&
            !appData.some(item => item.key === 'crypto-key' || item.key === 'encryption')) {
            Logger.warn('Push server deleted queued notifications; some past events may be unavailable.')
            if (object.persistentId) this.persistentIds.push(object.persistentId)
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
                    Logger.debug('Message dropped as it could not be decrypted: ' + error.message)
                    return
                default:
                    throw error
            }
        }

        // Maintain persistentIds updated with the very last received value
        if (object.persistentId) this.persistentIds.push(object.persistentId)
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
