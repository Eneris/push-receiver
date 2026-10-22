const test = require('node:test');
const assert = require('node:assert/strict');
const Long = require('long');
const { receiverHarness, proto, tags } = require('./helpers/receiver.cjs');

test('heartbeat replies acknowledge the current packet and preserve 64-bit status', async t => {
    const h = await receiverHarness(t);
    h.data('one');
    h.send(tags.kHeartbeatPingTag, { status: Long.fromString('9007199254740993') });
    const reply = h.writes().find(x => x.tag === tags.kHeartbeatAckTag)?.value;
    assert.equal(reply?.lastStreamIdReceived, 3);
    assert.equal(reply.status, '9007199254740993');
    await h.tick(1000);
    assert.equal(h.acks().length, 0, 'heartbeat cancels the pending stream ACK');
});

test('heartbeat requests use the protobuf acknowledgement field', async t => {
    const h = await receiverHarness(t, { heartbeatIntervalMs: 500 });
    h.data('one');
    await h.tick(500);
    assert.equal(h.writes().find(x => x.tag === tags.kHeartbeatPingTag)?.value.lastStreamIdReceived, 2);
    assert.equal(h.acks().length, 0);
});

test('zero heartbeat status is echoed', async t => {
    const h = await receiverHarness(t);
    h.send(tags.kHeartbeatPingTag, { status: Long.ZERO });
    assert.equal(h.writes().find(x => x.tag === tags.kHeartbeatAckTag)?.value.status, '0');
});

test('101 messages produce ten stream ACKs and one bounded tail ACK', async t => {
    const h = await receiverHarness(t);
    for (let i = 0; i < 101; i++) h.data('message-' + i);
    assert.equal(h.received.length, 101);
    assert.deepEqual(h.acks().map(x => x.lastStreamIdReceived), Array.from({ length: 10 }, (_, i) => 11 + i * 10));
    await h.tick(999);
    assert.equal(h.acks().length, 10);
    await h.tick(1);
    const last = h.acks().at(-1);
    assert.equal(last.lastStreamIdReceived, 102);
    assert.equal(last.type, proto.IqStanza.IqType.SET);
    assert.equal(last.id, '');
    assert.equal(last.extension.id, 13);
    assert.deepEqual(last.extension.data, Buffer.alloc(0));
    await h.tick(1000);
    assert.equal(h.acks().length, 11);
});

test('server-requested immediate ACK bypasses the batch and timer', async t => {
    const h = await receiverHarness(t);
    h.data('urgent', { immediateAck: true });
    assert.equal(h.acks()[0]?.lastStreamIdReceived, 2);
    await h.tick(1000);
    assert.equal(h.acks().length, 1);
});

test('a partial batch flushes even when heartbeats are disabled', async t => {
    const h = await receiverHarness(t, { heartbeatIntervalMs: 0 });
    h.data('one');
    await h.tick(1000);
    assert.equal(h.acks()[0]?.lastStreamIdReceived, 2);
    assert.equal(h.timers.size, 0);
});

test('duplicate deliveries are suppressed but each packet is acknowledged', async t => {
    const h = await receiverHarness(t);
    h.data('same');
    h.data('same');
    assert.equal(h.received.length, 1);
    await h.tick(1000);
    assert.equal(h.acks()[0]?.lastStreamIdReceived, 3);
});

test('reconnect sends current persistent IDs and resets the receive counter', async t => {
    const h = await receiverHarness(t, { persistentIds: ['previous-session'] });
    assert.deepEqual(h.writes()[0].value.receivedPersistentId, ['previous-session']);
    h.data('before-reconnect');
    await h.reconnect();
    assert.ok(h.writes()[0].value.receivedPersistentId?.includes('before-reconnect'));
    h.data('after-reconnect');
    h.send(tags.kHeartbeatPingTag, {});
    assert.equal(h.writes().find(x => x.tag === tags.kHeartbeatAckTag)?.value.lastStreamIdReceived, 3);
    await h.reconnect();
    assert.ok(h.writes()[0].value.receivedPersistentId?.includes('after-reconnect'));
});

test('rejected login does not retire pending IDs', async t => {
    const h = await receiverHarness(t);
    h.data('pending');
    await h.reconnect({ id: 'rejected', error: { code: 401 } });
    await h.reconnect();
    assert.ok(h.writes()[0].value.receivedPersistentId?.includes('pending'));
});

test('destroy cancels the ACK timer and does not write to the old socket', async t => {
    const h = await receiverHarness(t);
    h.data('one');
    const count = h.writes().length;
    h.receiver.destroy();
    await h.tick(1000);
    assert.equal(h.writes().length, count);
    assert.equal(h.timers.size, 0);
});

test('all incoming packet types contribute to the cumulative receive counter', async t => {
    const h = await receiverHarness(t);
    h.data('one');
    h.send(tags.kHeartbeatAckTag, { lastStreamIdReceived: 1 });
    h.send(tags.kIqStanzaTag, { type: proto.IqStanza.IqType.SET, id: '', extension: { id: 13, data: Buffer.alloc(0) } });
    h.data('two');
    await h.tick(1000);
    assert.equal(h.acks()[0]?.lastStreamIdReceived, 5);
});

test('socket backpressure still accepts an ACK without duplicating it', async t => {
    const h = await receiverHarness(t);
    h.sockets.at(-1).writeResult = false;
    h.data('one');
    await h.tick(2000);
    assert.equal(h.acks().length, 1);
    assert.equal(h.errors.length, 0);
});

test('a synchronous ACK write failure is logged and reconnect retains IDs', async t => {
    const h = await receiverHarness(t);
    h.data('pending');
    h.sockets.at(-1).writeError = new Error('synthetic write failure');
    await h.tick(1000);
    assert.equal(h.errors.length, 1);
    assert.match(h.errors.flat().join(' '), /synthetic write failure/);
    h.receiver.whenReady.catch(() => {});
    await h.tick(1000);
    assert.equal(h.sockets.length, 2);
    assert.ok(h.writes()[0].value.receivedPersistentId?.includes('pending'));
});

test('an asynchronous socket failure retains IDs for reconnect', async t => {
    const h = await receiverHarness(t);
    h.data('pending', { immediateAck: true });
    h.sockets.at(-1).emit('error', new Error('synthetic async write failure'));
    await h.reconnect();
    assert.equal(h.errors.length, 1);
    assert.ok(h.writes()[0].value.receivedPersistentId?.includes('pending'));
});

test('Google deletion controls warn once and receive transport ACKs', async t => {
    const h = await receiverHarness(t);
    const control = { rawData: Buffer.alloc(0), appData: [{ key: 'message_type', value: 'deleted_messages' }] };
    h.data('control', control);
    h.data('control', control);
    assert.equal(h.warnings.length, 1);
    assert.match(h.warnings[0][0], /deleted queued notifications/);
    assert.equal(h.received.length, 0);
    await h.tick(1000);
    assert.equal(h.acks()[0]?.lastStreamIdReceived, 3);
});

test('a deletion marker cannot bypass decryption of an encrypted notification', async t => {
    const h = await receiverHarness(t);
    // Leave the encrypted body in place but remove its key: this must use the decrypt diagnostic.
    h.data('not-a-control', { appData: [{ key: 'message_type', value: 'deleted_messages' }] });
    assert.equal(h.warnings.length, 0);
    assert.match(h.debug.flat().join(' '), /crypto-key is missing/);
    await h.tick(1000);
    assert.equal(h.acks()[0]?.lastStreamIdReceived, 2);
});

test('unknown empty messages retain their decryption diagnostic', async t => {
    const h = await receiverHarness(t);
    h.data('unknown', { rawData: Buffer.alloc(0), appData: [{ key: 'message_type', value: 'unknown' }] });
    assert.equal(h.warnings.length, 0);
    assert.match(h.debug.flat().join(' '), /crypto-key is missing/);
    await h.tick(1000);
    assert.equal(h.acks()[0]?.lastStreamIdReceived, 2);
});

test('an invalid encryption key remains an error and does not prevent transport ACKs', async t => {
    const h = await receiverHarness(t);
    h.data('invalid', { appData: [
        { key: 'crypto-key', value: 'dh=invalid' },
        { key: 'encryption', value: 'salt=invalid' },
    ] });
    assert.equal(h.errors.length, 1);
    await h.tick(1000);
    assert.equal(h.acks()[0]?.lastStreamIdReceived, 2);
});

test('messages without optional persistent IDs are delivered independently', async t => {
    const h = await receiverHarness(t);
    h.data(undefined);
    h.data(undefined);
    assert.equal(h.received.length, 2);
    assert.deepEqual(h.receiver.persistentIds, []);
    await h.tick(1000);
    assert.equal(h.acks()[0]?.lastStreamIdReceived, 3);
});

test('application listener errors stay visible without preventing ACKs', async t => {
    const h = await receiverHarness(t);
    h.receiver.onNotification(() => { throw new Error('synthetic application failure'); });
    h.data('one');
    assert.equal(h.errors.length, 1);
    await h.tick(1000);
    assert.equal(h.acks()[0]?.lastStreamIdReceived, 2);
});

test('fragmented and coalesced wire frames acknowledge only complete packets', async t => {
    const h = await receiverHarness(t);
    const frame = h.frame(tags.kDataMessageStanzaTag, {
        from: 'synthetic', category: 'synthetic', persistentId: 'fragmented', ...h.message,
    });
    h.sockets.at(-1).emit('data', frame.subarray(0, 2));
    await h.tick(1000);
    assert.equal(h.acks().length, 0);
    assert.equal(h.received.length, 0);
    h.sockets.at(-1).emit('data', Buffer.concat([
        frame.subarray(2), h.frame(tags.kHeartbeatPingTag, {}),
    ]));
    assert.equal(h.received.length, 1);
    assert.equal(h.writes().at(-1).value.lastStreamIdReceived, 3);
});

test('encrypted messages with a deletion marker are still decrypted and delivered', async t => {
    const h = await receiverHarness(t);
    h.data('encrypted', { appData: [...h.message.appData, { key: 'message_type', value: 'deleted_messages' }] });
    assert.equal(h.received.length, 1);
    assert.deepEqual(h.received[0].message, { synthetic: true });
    assert.equal(h.warnings.length, 0);
});

test('header-bearing empty messages are not treated as deletion controls', async t => {
    const h = await receiverHarness(t);
    h.data('header-only', { rawData: Buffer.alloc(0), appData: [
        { key: 'message_type', value: 'deleted_messages' },
        { key: 'crypto-key', value: 'dh=invalid' },
    ] });
    assert.equal(h.warnings.length, 0);
    assert.match(h.debug.flat().join(' '), /salt is missing/);
});

test('deletion notices without an ID do not corrupt deduplication', async t => {
    const h = await receiverHarness(t);
    h.data(undefined, { rawData: undefined, appData: [{ key: 'message_type', value: 'deleted_messages' }] });
    h.data(undefined);
    assert.equal(h.warnings.length, 1);
    assert.equal(h.received.length, 1);
    assert.deepEqual(h.receiver.persistentIds, []);
});

test('reconnect cancels the old tail ACK without writing to the old socket', async t => {
    const h = await receiverHarness(t);
    h.data('before-reconnect');
    const oldSocket = h.sockets.at(-1);
    const count = oldSocket.writes.length;
    await h.reconnect();
    await h.tick(1000);
    assert.equal(oldSocket.writes.length, count);
    assert.equal(h.acks().length, 0);
});

test('a failed immediate ACK does not discard the notification or its reconnect ID', async t => {
    const h = await receiverHarness(t);
    h.sockets.at(-1).writeError = new Error('synthetic immediate write failure');
    h.data('urgent', { immediateAck: true });
    assert.equal(h.received.length, 1);
    assert.equal(h.errors.length, 1);
    h.receiver.whenReady.catch(() => {});
    await h.tick(1000);
    assert.ok(h.writes()[0].value.receivedPersistentId?.includes('urgent'));
});
