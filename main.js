const WebSocket = require('ws');
const { connect, StringCodec } = require('nats');
const mysql = require('mysql2/promise');
require('dotenv').config();

// Database pool
const pool = mysql.createPool({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASS,
    database: process.env.DB_NAME,
});

async function startServer() {
    // Connect to NATS
    const nc = await connect({ servers: 'nats://127.0.0.1:4222' });
    const sc = StringCodec();
    // Start WebSocket server
    const wss = new WebSocket.Server({ port: 8081 });
    console.log('WebSocket server running on ws://localhost:8081');

    // Track connected clients by channel
    const channels = new Map(); // channelName -> Set<WebSocket>

    // Shared NATS subscriptions per channel (Imp 2)
    // channelName -> { sub: NatsSubscription, refCount: number }
    const channelSubs = new Map();

    /**
     * Get or create a shared NATS subscription for a channel.
     * Messages are fanned out to all WS clients in that channel.
     */
    function getOrCreateChannelSub(channelName) {
        if (channelSubs.has(channelName)) {
            channelSubs.get(channelName).refCount++;
            return;
        }

        const sub = nc.subscribe(`meeting.${channelName}`);
        channelSubs.set(channelName, { sub, refCount: 1 });

        // Fan-out: deliver each NATS message to all WS clients in the channel
        (async () => {
            for await (const m of sub) {
                const clients = channels.get(channelName);
                if (!clients) continue;

                const payload = sc.decode(m.data);
                const parsed = JSON.parse(payload);

                for (const client of clients) {
                    if (client.readyState !== WebSocket.OPEN) continue;

                    // Imp 3: targeted delivery for device-toggled
                    // Only send to the target user, not everyone
                    if (parsed.action === 'device-toggled') {
                        if (client.clientUid === parsed.data?.targetUid) {
                            client.send(payload);
                        }
                        continue;
                    }

                    client.send(payload);
                }
            }
        })();
    }

    /**
     * Release a reference to a channel's shared NATS subscription.
     * Unsubscribes when no more clients are using it.
     */
    function releaseChannelSub(channelName) {
        const entry = channelSubs.get(channelName);
        if (!entry) return;

        entry.refCount--;
        if (entry.refCount <= 0) {
            entry.sub.unsubscribe();
            channelSubs.delete(channelName);
            console.log(`[NATS] Unsubscribed from meeting.${channelName} (no more clients)`);
        }
    }

    wss.on('connection', (ws) => {
        let clientChannel = null;

        // Store uid on the ws object for targeted delivery (Imp 3)
        ws.clientUid = null;

        ws.on('message', async (raw) => {
            try {
                const msg = JSON.parse(raw);
                const { action, channelName, uid } = msg;

                switch (action) {
                    // ─── JOIN/SUBSCRIBE TO A CHANNEL ───
                    case 'join':
                        // Bug 6: Clean up previous subscription if re-joining
                        if (clientChannel && clientChannel !== channelName) {
                            // Remove from old channel's client set
                            if (channels.has(clientChannel)) {
                                channels.get(clientChannel).delete(ws);
                            }
                            // Release ref on old channel's NATS sub
                            releaseChannelSub(clientChannel);
                        }

                        clientChannel = channelName;
                        ws.clientUid = uid;

                        if (!channels.has(channelName)) {
                            channels.set(channelName, new Set());
                        }
                        channels.get(channelName).add(ws);

                        // Get or create shared NATS subscription for this channel
                        getOrCreateChannelSub(channelName);

                        ws.send(JSON.stringify({ action: 'joined', channelName }));
                        break;

                    // ─── DATABASE: GET DATA ───
                    case 'get-messages':
                        const [rows] = await pool.query(
                            'SELECT * FROM chat_messages WHERE channel_name = ? ORDER BY created_at DESC LIMIT ?',
                            [channelName, msg.limit || 50]
                        );
                        ws.send(JSON.stringify({
                            action: 'messages',
                            channelName,
                            data: rows,
                        }));
                        break;

                    // ─── DATABASE: INSERT DATA ───
                    case 'send-message':
                        const [result] = await pool.query(
                            'INSERT INTO chat_messages (channel_name, uid, username, message) VALUES (?, ?, ?, ?)',
                            [channelName, uid, msg.username, msg.message]
                        );
                        const newMsg = {
                            id: result.insertId,
                            channel_name: channelName,
                            uid, username: msg.username,
                            message: msg.message,
                            created_at: new Date().toISOString(),
                        };
                        // Publish to NATS → all subscribers get it
                        nc.publish(
                            `meeting.${channelName}`,
                            sc.encode(JSON.stringify({
                                action: 'new-message',
                                channelName,
                                data: newMsg,
                            }))
                        );
                        break;

                    // ─── DATABASE: UPDATE DATA ───
                    case 'update-status':
                        await pool.query(
                            'UPDATE meeting_participants SET is_camera_off = ?, is_muted = ? WHERE channel_name = ? AND uid = ?',
                            [msg.isCameraOff, msg.isMuted, channelName, uid]
                        );
                        // Broadcast status change to all clients in channel
                        nc.publish(
                            `meeting.${channelName}`,
                            sc.encode(JSON.stringify({
                                action: 'participant-status-changed',
                                channelName,
                                data: { uid, isCameraOff: msg.isCameraOff, isMuted: msg.isMuted },
                            }))
                        );
                        break;

                    // ─── DATABASE: DELETE DATA ───
                    case 'delete-message':
                        await pool.query(
                            'DELETE FROM chat_messages WHERE id = ? AND channel_name = ?',
                            [msg.messageId, channelName]
                        );
                        nc.publish(
                            `meeting.${channelName}`,
                            sc.encode(JSON.stringify({
                                action: 'message-deleted',
                                channelName,
                                data: { messageId: msg.messageId },
                            }))
                        );
                        break;

                    // ─── REMOTE DEVICE CONTROL ───
                    case 'toggle-device':
                        // First, update the database so new joiners get the correct status
                        if (msg.device === 'camera') {
                            await pool.query(
                                'UPDATE meeting_participants SET is_camera_off = ? WHERE channel_name = ? AND uid = ?',
                                [!msg.state, channelName, String(msg.targetUid)] // if turning on (msg.state=true), is_camera_off is false
                            );
                        } else if (msg.device === 'mic') {
                            await pool.query(
                                'UPDATE meeting_participants SET is_muted = ? WHERE channel_name = ? AND uid = ?',
                                [!msg.state, channelName, String(msg.targetUid)] // if turning on (msg.state=true), is_muted is false
                            );
                        }

                        // Send the direct toggle command — delivered only to targetUid via fan-out filter (Imp 3)
                        nc.publish(
                            `meeting.${channelName}`,
                            sc.encode(JSON.stringify({
                                action: 'device-toggled',
                                channelName,
                                data: {
                                    targetUid: msg.targetUid,
                                    device: msg.device, // 'mic' or 'camera'
                                    state: msg.state // true (on) or false (off)
                                },
                            }))
                        );

                        // Also broadcast the new status so everyone's UI updates immediately
                        setTimeout(() => {
                            nc.publish(
                                `meeting.${channelName}`,
                                sc.encode(JSON.stringify({
                                    action: 'participant-status-changed',
                                    channelName,
                                    data: {
                                        uid: msg.targetUid,
                                        isCameraOff: msg.device === 'camera' ? !msg.state : undefined,
                                        isMuted: msg.device === 'mic' ? !msg.state : undefined
                                    },
                                }))
                            );
                        }, 100);
                        break;

                    case 'end-meeting':
                        console.log(`[WS] End meeting: channel=${channelName}, by uid=${uid} (${msg.username})`);
                        // Broadcast meeting-ended to all clients in the channel via NATS
                        nc.publish(
                            `meeting.${channelName}`,
                            sc.encode(JSON.stringify({
                                action: 'meeting-ended',
                                channelName,
                                endedBy: msg.username || 'Host',
                            }))
                        );
                        break;

                    case 'break-approved':
                        console.log(`[WS] Break approved: channel=${channelName}, targetUid=${msg.targetUid}, duration=${msg.durationMinutes}m`);
                        nc.publish(
                            `meeting.${channelName}`,
                            sc.encode(JSON.stringify({
                                action: 'break-approved',
                                channelName,
                                data: {
                                    requestId: msg.requestId,
                                    targetUid: msg.targetUid,
                                    username: msg.username,
                                    durationMinutes: msg.durationMinutes,
                                    expiresAt: msg.expiresAt,
                                },
                            }))
                        );
                        break;

                    case 'participant-face-warning':
                        console.log(`[WS] Face warning: channel=${channelName}, uid=${uid}, isFaceMissing=${msg.isFaceMissing}`);
                        // Broadcast face detection warning to all clients in channel
                        nc.publish(
                            `meeting.${channelName}`,
                            sc.encode(JSON.stringify({
                                action: 'participant-face-warning',
                                channelName,
                                data: { uid, isFaceMissing: msg.isFaceMissing },
                            }))
                        );
                        break;

                    // ─── REAL-TIME REACTIONS (replaces HTTP polling) ───
                    case 'raise-hand':
                        nc.publish(
                            `meeting.${channelName}`,
                            sc.encode(JSON.stringify({
                                action: 'hand-raised',
                                channelName,
                                data: {
                                    uid,
                                    username: msg.username,
                                    isRaised: msg.isRaised, // true = raised, false = lowered
                                },
                            }))
                        );
                        break;

                    case 'send-reaction':
                        nc.publish(
                            `meeting.${channelName}`,
                            sc.encode(JSON.stringify({
                                action: 'reaction-received',
                                channelName,
                                data: {
                                    uid,
                                    username: msg.username,
                                    emoji: msg.emoji,
                                    createdAt: new Date().toISOString(),
                                },
                            }))
                        );
                        break;

                    // ─── REAL-TIME BREAK REQUESTS (replaces HTTP polling) ───
                    case 'break-submitted':
                        // Student submitted a break request — notify teacher
                        nc.publish(
                            `meeting.${channelName}`,
                            sc.encode(JSON.stringify({
                                action: 'break-request-new',
                                channelName,
                                data: {
                                    requestId: msg.requestId,
                                    uid,
                                    username: msg.username,
                                },
                            }))
                        );
                        break;

                    case 'break-denied':
                        // Teacher denied a break request — notify student
                        nc.publish(
                            `meeting.${channelName}`,
                            sc.encode(JSON.stringify({
                                action: 'break-denied',
                                channelName,
                                data: {
                                    requestId: msg.requestId,
                                    targetUid: msg.targetUid,
                                },
                            }))
                        );
                        break;
                }
            } catch (err) {
                console.error('WebSocket message error:', err);
                ws.send(JSON.stringify({ action: 'error', message: err.message }));
            }
        });

        ws.on('close', () => {
            if (clientChannel && channels.has(clientChannel)) {
                channels.get(clientChannel).delete(ws);
            }
            // Release shared subscription reference
            if (clientChannel) {
                releaseChannelSub(clientChannel);
            }
        });
    });
}

startServer().catch(console.error);