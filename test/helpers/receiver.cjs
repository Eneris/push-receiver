// Exercise the built receiver, parser and protobufs without credentials or network access.
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { createRequire } = require('node:module');
const { EventEmitter } = require('node:events');
const source = path.resolve(__dirname, '../../dist/client.js');
const requireBuilt = createRequire(source);
const proto = requireBuilt('./protos').mcs_proto;
const { Variables, MCSProtoTag: tags } = requireBuilt('./constants');
const ece = requireBuilt('http_ece');

async function receiverHarness(t, options = {}) {
    const key = crypto.createECDH('prime256v1');
    const sender = crypto.createECDH('prime256v1');
    key.generateKeys();
    sender.generateKeys();
    const salt = crypto.randomBytes(16).toString('base64url');
    const authSecret = crypto.randomBytes(16).toString('base64url');
    const message = {
        rawData: ece.encrypt(Buffer.from('{"synthetic":true}'), {
            version: 'aesgcm', privateKey: sender, dh: key.getPublicKey(), salt, authSecret,
        }),
        appData: [
            { key: 'crypto-key', value: 'dh=' + sender.getPublicKey().toString('base64url') + '; p256ecdsa=synthetic-signature' },
            { key: 'encryption', value: 'salt=' + salt },
        ],
    };
    const sockets = [], timers = new Set(), received = [], warnings = [], errors = [], debug = [];
    let now = 0;
    let nextLogin = { id: 'synthetic-login' };
    const logger = {
        setDebug() {}, log() {}, debug: (...args) => debug.push(args),
        warn: (...args) => warnings.push(args), error: (...args) => errors.push(args),
    };
    // Listener exceptions are intentionally caught and logged by the real Emitter.
    t.mock.method(console, 'error', (...args) => errors.push(args));

    class Socket extends EventEmitter {
        writes = [];
        writable = true;
        destroyed = false;
        writeResult = true;
        writeError = null;
        constructor() { super(); sockets.push(this); }
        setKeepAlive() {}
        connect() {
            queueMicrotask(() => {
                this.emit('connect');
                this.emit('data', Buffer.concat([
                    Buffer.from([Variables.kMCSVersion, tags.kLoginResponseTag]),
                    proto.LoginResponse.encodeDelimited(nextLogin).finish(),
                ]));
            });
        }
        write(bytes) {
            if (this.writeError) throw this.writeError;
            if (this.destroyed || !this.writable) throw new Error('write after close');
            this.writes.push(Buffer.from(bytes));
            return this.writeResult;
        }
        end() { this.writable = false; }
        destroy() { this.destroyed = true; }
    }

    const setTimer = (fn, delay) => {
        const timer = { fn, at: now + delay };
        timers.add(timer);
        return timer;
    };
    const clearTimer = timer => timers.delete(timer);
    const module = { exports: {} };
    new Function('require', 'module', 'exports', 'setTimeout', 'clearTimeout', fs.readFileSync(source, 'utf8'))(
        name => name === 'tls' ? { TLSSocket: Socket } : name === './utils/logger' ? logger : requireBuilt(name),
        module, module.exports, setTimer, clearTimer,
    );
    const receiver = new module.exports.PushReceiver({
        firebase: { projectId: 'synthetic' },
        heartbeatIntervalMs: options.heartbeatIntervalMs ?? 300000,
        persistentIds: options.persistentIds || [],
        credentials: {
            keys: { privateKey: key.getPrivateKey().toString('base64'), authSecret },
            gcm: { androidId: '123', securityToken: 'synthetic' },
        },
    });
    // Registration is the only public network entry point outside the fake TLS socket.
    receiver.registerIfNeeded = async () => {};
    receiver.onNotification(value => received.push(value));
    receiver.whenReady.catch(() => {});
    t.after(() => { receiver.whenReady.catch(() => {}); receiver.destroy(); });
    await receiver.connect();

    const types = {
        [tags.kLoginRequestTag]: proto.LoginRequest,
        [tags.kLoginResponseTag]: proto.LoginResponse,
        [tags.kDataMessageStanzaTag]: proto.DataMessageStanza,
        [tags.kHeartbeatPingTag]: proto.HeartbeatPing,
        [tags.kHeartbeatAckTag]: proto.HeartbeatAck,
        [tags.kIqStanzaTag]: proto.IqStanza,
        [tags.kCloseTag]: proto.Close,
    };
    function frame(tag, object) {
        const error = types[tag].verify(object);
        if (error) throw new Error(error);
        return Buffer.concat([Buffer.from([tag]), types[tag].encodeDelimited(object).finish()]);
    }
    function send(tag, object) { sockets.at(-1).emit('data', frame(tag, object)); }
    function data(id, extra = {}) {
        send(tags.kDataMessageStanzaTag, {
            from: 'synthetic', category: 'synthetic', ...message, persistentId: id, ...extra,
        });
    }
    function writes(index = sockets.length - 1) {
        return sockets[index].writes.map(bytes => {
            const offset = bytes[0] === Variables.kMCSVersion ? 1 : 0;
            const tag = bytes[offset];
            const type = types[tag];
            return { tag, value: type.toObject(type.decodeDelimited(bytes.subarray(offset + 1)), { longs: String, bytes: Buffer }) };
        });
    }
    function acks() { return writes().filter(x => x.tag === tags.kIqStanzaTag).map(x => x.value); }
    async function tick(ms) {
        const end = now + ms;
        for (;;) {
            const next = [...timers].filter(t => t.at <= end).sort((a, b) => a.at - b.at)[0];
            if (!next) break;
            now = next.at;
            timers.delete(next);
            await next.fn();
        }
        now = end;
    }
    async function reconnect(login = { id: 'synthetic-login' }) {
        nextLogin = login;
        sockets.at(-1).emit('close');
        receiver.whenReady.catch(() => {});
        await tick(1000);
    }
    return { receiver, send, data, frame, message, writes, acks, tick, reconnect, received, warnings, errors, debug, sockets, timers };
}

module.exports = { receiverHarness, proto, tags };
