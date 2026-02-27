# push-receiver

A library to subscribe to GCM/FCM and receive notifications within a node process.

## When should I use `push-receiver` ?

- I want to **receive** push notifications sent using Firebase Cloud Messaging in an [electron](https://github.com/electron/electron) desktop application.
- I want to communicate with a node process/server using Firebase Cloud Messaging infrastructure.

## When should I not use `push-receiver` ?

- I want to **send** push notifications (use the firebase SDK instead)
- My application is running on a FCM supported platform (Android, iOS, Web).

## Install

`
npm i -S @eneris/push-receiver
`

## Requirements 

- Node v20 (async/await/randomUUID/fetch support)
- Firebase credentials from `Step 1` - https://firebase.google.com/docs/web/setup

## Acknowledgements 
- https://github.com/MatthieuLemoine - for creating initial module on wich uppon in iterated

## Usage

### ClientConfig

```typescript
interface ClientConfig {
    credentials?: Credentials // Will be generated if missing - save this after first use!
    persistentIds?: PersistentId[] // Default - []
    bundleId?: string // Default - 'receiver.push.com'
    chromeId?: string // Default - 'org.chromium.linux'
    chromeVersion?: string // Default - '94.0.4606.51'
    debug?: boolean // Enables debug console logs
    heartbeatIntervalMs?: number // Default - 5 * 60 * 1000
    firebase: FirebaseConfig // Full client firebase credentials are now needed
}
```

### Node example

```javascript
import { PushReceiver } from '@eneris/push-receiver'

(async () => {
    const instance = new PushReceiver({
        debug: true,
        persistentIds: [], // Recover stored ids of all previous notifications
        firebase: {
            // ...Firebase web credentials
        },
        credentials: null, // Insert credentials here after the first run
    })

    const stopListeningToCredentials = instance.onCredentialsChanged(({ oldCredentials, newCredentials }) => {
        console.log('Client generated new credentials.', newCredentials)
        // Save them somewhere! And decide if thing are needed to re-subscribe
    })

    const stopListeningToNotifications = instance.onNotification(notification => {
        // Do someting with the notification
        console.log('Notification received', notification)
    })

    await instance.connect()

    
    await instance.connect()

    console.log('connected')

    const sender = new PushSender({
        // Firebase service account credentials here
    })

    console.log('server created')

    await sender.testMessage(instance.config.credentials.fcm.token)

    console.log('message sent')

    stopListeningToCredentials()
    stopListeningToNotifications()

    instance.destroy()
})()
```

### Token Management

The library automatically manages FCM installation token lifecycle and handles token expiration:

#### Automatic Token Refresh

The client automatically refreshes the FCM installation auth token before it expires (1 day before the 7-day expiration). When a token is refreshed, the `onCredentialsChanged` event is emitted with the updated credentials.

```javascript
// Listen for automatic token refresh
instance.onCredentialsChanged(({ oldCredentials, newCredentials }) => {
    console.log('Credentials updated (automatic refresh or new registration)')
    // Save the updated credentials for future use
    saveCredentials(newCredentials)
})
```

The automatic refresh:
- Maintains the same Firebase Installation ID (FID) and encryption keys
- Refreshes only the installation auth token (which expires after 7 days)
- Emits `ON_CREDENTIALS_CHANGE` event when completed
- Automatically schedules the next refresh cycle

#### Manual Token Refresh

You can also manually trigger a token refresh if needed:

```javascript
// Manually refresh the FCM installation token
const updatedCredentials = await instance.refreshToken()
console.log('Installation token refreshed, expires in:', updatedCredentials.fcm.installation.expiresIn / 1000 / 60 / 60 / 24, 'days')
```

#### Delete Token

Use `deleteToken()` to clear locally stored credentials and perform a full re-registration. This does NOT revoke the token on FCM servers—it only resets the local state. If the client is connected, it will automatically disconnect first.

```javascript
// Delete the current token and disconnect
instance.deleteToken()

// Connect again to perform a full re-registration with new credentials
await instance.connect()
```
