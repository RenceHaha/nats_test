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

    wss.on('connection', (ws) => {
        let clientChannel = null;
        let clientUid = null;

        ws.on('message', async (raw) => {
            try {
                const msg = JSON.parse(raw);
                const { action, channelName, uid } = msg;

                switch (action) {
                    // ─── JOIN/SUBSCRIBE TO A CHANNEL ───
                    case 'join':
                        clientChannel = channelName;
                        clientUid = uid;
                        if (!channels.has(channelName)) {
                            channels.set(channelName, new Set());
                        }
                        channels.get(channelName).add(ws);

                        // Subscribe to NATS for this channel
                        const sub = nc.subscribe(`meeting.${channelName}`);
                        (async () => {
                            for await (const m of sub) {
                                if (ws.readyState === WebSocket.OPEN) {
                                    ws.send(sc.decode(m.data));
                                }
                            }
                        })();

                        ws.natsSub = sub;
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

                        // Send the direct toggle command exclusively to the target user so their hardware turns on/off
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

                        // Wait a tiny bit and also broadcast the new status so everyone's UI updates immediately
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

                    // ─── END MEETING FOR ALL ───
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
            if (ws.natsSub) ws.natsSub.unsubscribe();
        });
    });
}

startServer().catch(console.error);