# @eneris/push-receiver

## 4.4.0

### Patch Changes

- Fixed a stack overflow (`RangeError: Maximum call stack size exceeded`) in the MCS parser on a burst of buffered frames or a TCP segment that split a varint size header - the parser state machine now loops instead of recursing
- Fixed installation auth tokens never refreshing after their 7-day expiry, and corrected the Firebase Installations refresh endpoint URL, which previously returned 404
- Fixed FID generation to match Firebase's required format (`/^[cdef][\w-]{21}$/`) instead of returning plain base64
- Fixed `retry()` swallowing exhausted-retry failures, causing an endpoint that keeps failing to be retried forever instead of surfacing an error
- Fixed TCP keepalive falling back to the OS default (up to 2 hours on Linux) instead of 30s, which could leave dead sockets behind NAT undetected for hours
- Lowered the harmless "message dropped as it could not be decrypted" log from `warn` to `debug`
- Removed a debug log line that printed full credentials, including secrets, when `debug: true`

## 4.0.1-beta
### Braking changes
- New parametrer `firebase` is now requried in the config - this is used mainly for FCM installation
- FCM `installation` data were added into `fcm` part of `Credentials` containing Firebase Install credentials - Token refreshing is in TODO
- Removed the `logLevel` option and replaced it with `debug: boolean`
- Moved `sentTestMessage` into separate class called `PushSender`

### Major changes
- Replaced `axios` with native `fetch` in Node v20+
- Added `index` export for `PushReceiver` and `PushSender`

## 3.1.0

### Minor Changes

- Added automated Heartbeat messages
  - new option `heartbeatIntervalMs` DEFAULT: 5 * 60 * 1000
  - new events `ON_HEARTBEAT` - this is emited when socket recieves `ping` or `ack` messages

### Patch Changes

- Updated devDependencies