const WebSocket = require('ws');
const { connect, StringCodec } = require('nats');
const http = require('http');
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

    // Track connected clients by channel (the live source of truth)
    const channels = new Map(); // channelName -> Set<WebSocket>

    // ── HTTP server: handles /check-teacher/:channel (and WebSocket upgrades) ──
    const server = http.createServer((req, res) => {
        // CORS headers so Flutter can reach this from any origin
        res.setHeader('Access-Control-Allow-Origin', '*');
        res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');

        if (req.method === 'OPTIONS') {
            res.writeHead(204);
            res.end();
            return;
        }

        // GET /check-teacher/<channelName>
        const match = req.url && req.url.match(/^\/check-teacher\/(.+)$/);
        if (req.method === 'GET' && match) {
            const channelName = decodeURIComponent(match[1]);
            const clients = channels.get(channelName) || new Set();
            // Check live connections for teacher or admin role
            const teacherPresent = [...clients].some(
                (c) => c.clientRole === 'teacher' || c.clientRole === 'admin'
            );
            const count = clients.size;
            console.log(`[HTTP] check-teacher/${channelName} → teacherPresent=${teacherPresent}, connected=${count}`);
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ teacherPresent, connectedCount: count }));
            return;
        }

        // GET /get-user/<channelName>/<uid>
        const userMatch = req.url && req.url.match(/^\/get-user\/([^/]+)\/(.+)$/);
        if (req.method === 'GET' && userMatch) {
            const channelName = decodeURIComponent(userMatch[1]);
            const uid = parseInt(userMatch[2], 10);
            const clients = channels.get(channelName) || new Set();
            const client = [...clients].find((c) => c.clientUid === uid);
            if (client) {
                res.writeHead(200, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({
                    username: client.clientUsername || 'Participant',
                    role: client.clientRole || 'student',
                    isScreenSharing: false,
                }));
            } else {
                // Client not found in live connections — return 404 so Flutter falls back to DB
                res.writeHead(404, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ error: 'not found' }));
            }
            return;
        }

        // Health check
        if (req.method === 'GET' && req.url === '/health') {
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ status: 'ok', channels: channels.size }));
            return;
        }

        // GET /active-activity/<channelName> — polling endpoint for late-joiners
        const activityMatch = req.url && req.url.match(/^\/active-activity\/(.+)$/);
        if (req.method === 'GET' && activityMatch) {
            const channelName = decodeURIComponent(activityMatch[1]);
            const activity = channelActivities.get(channelName);
            if (activity) {
                res.writeHead(200, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ success: true, data: activity }));
            } else {
                res.writeHead(200, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ success: false, data: null }));
            }
            console.log(`[HTTP] active-activity/${channelName} → ${activity ? 'ACTIVE' : 'none'}`);
            return;
        }

        res.writeHead(404);
        res.end('Not found');
    });

    // Attach WebSocket server to the same HTTP server
    const wss = new WebSocket.Server({ server });
    server.listen(8081, () => {
        console.log('Server running on port 8081 (HTTP + WebSocket)');
    });


    // Shared NATS subscriptions per channel (Imp 2)
    // channelName -> { sub: NatsSubscription, refCount: number }
    const channelSubs = new Map();

    // ─── MEETING MODE PERSISTENCE ───
    // Persists mode even if all clients disconnect (crash fallback).
    // Values: 'standard' | 'supervised' | 'focus'
    const channelModes = new Map(); // channelName -> mode string

    // ─── IN-MEETING ACTIVITY PERSISTENCE ───
    // Tracks the currently active classwork activity per channel.
    // Cleared when teacher stops activity or all clients disconnect.
    const channelActivities = new Map(); // channelName -> { classwork_id, title, type, ... }
    const channelActivityTimers = new Map(); // channelName -> setTimeout handle

    /**
     * Stop an activity centrally: clear state, broadcast to clients, and update DB.
     */
    async function stopActivityCentrally(channelName) {
        const activity = channelActivities.get(channelName);
        if (!activity) return;

        console.log(`[Timer] Auto-stopping activity for channel: ${channelName}`);

        // 1. Clear in-memory state
        channelActivities.delete(channelName);
        const timer = channelActivityTimers.get(channelName);
        if (timer) {
            clearTimeout(timer);
            channelActivityTimers.delete(channelName);
        }

        // 2. Broadcast stop event via NATS
        nc.publish(
            `meeting.${channelName}`,
            sc.encode(
                JSON.stringify({
                    action: 'activity-stopped',
                    channelName,
                })
            )
        );

        // 3. Update DB to close response acceptance
        const classworkId = activity.classwork_id || activity.quiz_id;
        if (classworkId) {
            try {
                // Determine if it was a quiz or activity to update the correct table/flag
                // The user specifically mentioned the 'accepting_responses' flag in class_work.
                await pool.query(
                    "UPDATE class_work SET accepting_responses = 0 WHERE class_work_id = ? OR quiz_id = ?",
                    [classworkId, classworkId]
                );
                console.log(`[DB] Closed accepting_responses for classwork/quiz: ${classworkId}`);
            } catch (err) {
                console.error(`[DB] Failed to close activity in DB:`, err);
            }
        }
    }

    function getChannelMode(channelName) {
        return channelModes.get(channelName) || 'standard';
    }

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

                    // Targeted delivery: device-toggled, device-permission-result -> target uid only
                    if (parsed.action === 'device-toggled') {
                        if (client.clientUid === parsed.data?.targetUid) {
                            client.send(payload);
                        }
                        continue;
                    }

                    if (parsed.action === 'device-permission-result') {
                        if (client.clientUid === parsed.data?.targetUid) {
                            client.send(payload);
                        }
                        continue;
                    }

                    // Supervised mode: permission requests only go to teacher/admin clients
                    if (parsed.action === 'device-permission-requested' || parsed.action === 'quiz-submitted') {
                        if (client.clientRole === 'teacher' || client.clientRole === 'admin') {
                            client.send(payload);
                        }
                        continue;
                    }

                    // Targeted: break-approved → target student only
                    if (parsed.action === 'break-approved') {
                        if (client.clientUid === parsed.data?.targetUid) {
                            client.send(payload);
                        }
                        continue;
                    }

                    // Targeted: break-denied → target student only
                    if (parsed.action === 'break-denied') {
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

        // Store uid/role on the ws object for targeted delivery
        ws.clientUid = null;
        ws.clientRole = null;

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
                        ws.clientRole = msg.role || null;       // store for targeted delivery & HTTP endpoints
                        ws.clientUsername = msg.username || null; // store for /get-user endpoint
                        ws.clientUserId = msg.user_id != null ? msg.user_id : null;   // store exact database ID
                        // Store initial device state so participants-list has the real
                        // camera/mic status from the moment of joining (not undefined).
                        // These will be updated later by 'update-status' messages.
                        ws.clientIsCameraOff = msg.isCameraOff === true;
                        ws.clientIsMuted = msg.isMuted !== false; // default muted if not specified

                        if (!channels.has(channelName)) {
                            channels.set(channelName, new Set());
                        }
                        channels.get(channelName).add(ws);

                        // Get or create shared NATS subscription for this channel
                        getOrCreateChannelSub(channelName);

                        // Send join ack with current mode — crash fallback for reconnects
                        const currentMode = getChannelMode(channelName);
                        ws.send(JSON.stringify({ action: 'joined', channelName, currentMode }));

                        // Push the current participants list directly to the new joiner
                        // so they immediately know everyone already in the channel
                        const existingParticipants = [...channels.get(channelName)]
                            .filter(c => c !== ws && c.clientUid != null && c.clientUsername)
                            .map(c => ({ 
                                uid: c.clientUid, 
                                username: c.clientUsername, 
                                role: c.clientRole,
                                user_id: c.clientUserId,
                                isCameraOff: c.clientIsCameraOff === true, // Default to false if technically unknown, but we'll store it explicitly
                                isMuted: c.clientIsMuted === true
                            }));
                        if (existingParticipants.length > 0) {
                            ws.send(JSON.stringify({
                                action: 'participants-list',
                                channelName,
                                data: existingParticipants,
                            }));
                            console.log(`[WS] participants-list sent to uid=${uid}: ${existingParticipants.map(p => p.username).join(', ')}`);
                        }

                        // Broadcast to all other clients so they can immediately show the correct name
                        nc.publish(
                            `meeting.${channelName}`,
                            sc.encode(JSON.stringify({
                                action: 'participant-joined',
                                channelName,
                                data: {
                                    uid,
                                    username: ws.clientUsername,
                                    role: ws.clientRole,
                                    user_id: ws.clientUserId,
                                    isCameraOff: ws.clientIsCameraOff === true,
                                    isMuted: ws.clientIsMuted === true,
                                },
                            }))
                        );
                        console.log(`[WS] participant-joined: uid=${uid}, username=${ws.clientUsername}, role=${ws.clientRole}, user_id=${ws.clientUserId}, isCameraOff=${ws.clientIsCameraOff}, channel=${channelName}`);
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
                        // Store on the socket so late-joiners get the correct initial state
                        ws.clientIsCameraOff = msg.isCameraOff;
                        ws.clientIsMuted = msg.isMuted;
                        ws.clientIsScreenSharing = msg.isScreenSharing === true;
                        
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
                                data: {
                                    uid,
                                    isCameraOff: msg.isCameraOff,
                                    isMuted: msg.isMuted,
                                    isScreenSharing: msg.isScreenSharing === true,
                                },
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

                        // Also update the in-memory cache for late-joiners (ws.clientIsCameraOff/ws.clientIsMuted)
                        const channelClients = channels.get(channelName);
                        if (channelClients) {
                            for (const client of channelClients) {
                                if (client.clientUid === msg.targetUid) {
                                    if (msg.device === 'camera') {
                                        client.clientIsCameraOff = !msg.state;
                                    } else if (msg.device === 'mic') {
                                        client.clientIsMuted = !msg.state;
                                    }
                                    break;
                                }
                            }
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

                    // ─── MEETING MODE CONTROL (teacher/admin only) ───
                    case 'set-meeting-mode':
                        // Validate role server-side
                        if (ws.clientRole !== 'teacher' && ws.clientRole !== 'admin') {
                            ws.send(JSON.stringify({ action: 'error', message: 'Not authorized to set mode.' }));
                            break;
                        }
                        const newMode = msg.mode;
                        if (!['standard', 'supervised', 'focus'].includes(newMode)) break;

                        // Persist mode — survives client reconnects and crashes
                        channelModes.set(channelName, newMode);
                        console.log(`[Mode] ${channelName} → ${newMode} (by uid=${uid})`);

                        // Broadcast to all participants in the channel
                        nc.publish(
                            `meeting.${channelName}`,
                            sc.encode(JSON.stringify({
                                action: 'meeting-mode-changed',
                                channelName,
                                data: { mode: newMode, changedBy: uid },
                            }))
                        );
                        break;

                    // ─── WAITING ROOM / ADMISSION CONTROL (teacher/admin only) ───
                    case 'toggle-waiting-room':
                        if (ws.clientRole !== 'teacher' && ws.clientRole !== 'admin') {
                            ws.send(JSON.stringify({ action: 'error', message: 'Not authorized.' }));
                            break;
                        }

                        nc.publish(
                            `meeting.${channelName}`,
                            sc.encode(JSON.stringify({
                                action: 'waiting-room-toggled',
                                channelName,
                                data: { enabled: msg.enabled, changedBy: uid },
                            }))
                        );
                        break;

                    case 'join-request-new':
                        // Broadcast the new pending join request to all teachers/admins in the room
                        nc.publish(
                            `meeting.${channelName}`,
                            sc.encode(JSON.stringify({
                                action: 'join-request-new',
                                channelName,
                                data: {
                                    uid,
                                    username: msg.username,
                                    role: msg.role,
                                },
                            }))
                        );
                        break;

                    case 'admission-handled':
                        nc.publish(
                            `meeting.${channelName}`,
                            sc.encode(JSON.stringify({
                                action: 'admission-handled',
                                channelName,
                                data: { targetUid: msg.targetUid, status: msg.status },
                            }))
                        );
                        break;

                    // ─── SUPERVISED MODE: STUDENT REQUESTS DEVICE PERMISSION ───
                    case 'device-permission-request':
                        // Broadcast to teacher/admin clients only (fan-out filter handles it)
                        nc.publish(
                            `meeting.${channelName}`,
                            sc.encode(JSON.stringify({
                                action: 'device-permission-requested',
                                channelName,
                                data: {
                                    requestId: msg.requestId,
                                    studentUid: uid,
                                    username: msg.username,
                                    device: msg.device, // 'mic' | 'camera' | 'screenshare'
                                },
                            }))
                        );
                        break;

                    // ─── SUPERVISED MODE: TEACHER APPROVES/DENIES PERMISSION ───
                    case 'device-permission-result':
                        // Deliver only to the student who requested (fan-out filter handles it)
                        nc.publish(
                            `meeting.${channelName}`,
                            sc.encode(JSON.stringify({
                                action: 'device-permission-result',
                                channelName,
                                data: {
                                    requestId: msg.requestId,
                                    targetUid: msg.targetUid,
                                    device: msg.device,
                                    approved: msg.approved, // true or false
                                },
                            }))
                        );
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

                    case 'participant-verifying':
                        console.log(`[WS] Participant verifying: channel=${channelName}, uid=${uid}, isVerifying=${msg.isVerifying}`);
                        nc.publish(
                            `meeting.${channelName}`,
                            sc.encode(JSON.stringify({
                                action: 'participant-verifying',
                                channelName,
                                data: { uid, isVerifying: msg.isVerifying },
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
                        
                    case 'quiz-submitted':
                        // Student submitted quiz, send their attempt data to teacher/admin
                        nc.publish(
                            `meeting.${channelName}`,
                            sc.encode(JSON.stringify({
                                action: 'quiz-submitted',
                                channelName,
                                data: msg.data,
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

                    case 'break-cancelled':
                        // Student cancelled a break request — notify teacher
                        nc.publish(
                            `meeting.${channelName}`,
                            sc.encode(JSON.stringify({
                                action: 'break-cancelled',
                                channelName,
                                data: {
                                    requestId: msg.requestId,
                                    uid,
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

                    // ─── RECITATION: PICK A STUDENT ───
                    case 'recitation-pick':
                        console.log(`[WS] Recitation pick: channel=${channelName}, target=${msg.targetUsername} (uid=${msg.targetUid})`);
                        nc.publish(
                            `meeting.${channelName}`,
                            sc.encode(JSON.stringify({
                                action: 'recitation-picked',
                                channelName,
                                data: {
                                    targetUid: msg.targetUid,
                                    targetUsername: msg.targetUsername,
                                },
                            }))
                        );
                        break;

                    // ─── IN-MEETING CLASSWORK ACTIVITY (teacher launches assignment/question) ───
                    case 'activity-launched':
                        if (ws.clientRole !== 'teacher' && ws.clientRole !== 'admin') {
                            ws.send(JSON.stringify({ action: 'error', message: 'Not authorized to launch activity.' }));
                            break;
                        }
                        const activityData = msg.data || {};
                        const durationMins = parseInt(activityData.duration_minutes || 30, 10);

                        // Sync launch time if not provided
                        if (!activityData.launched_at) {
                            activityData.launched_at = new Date().toISOString();
                        }

                        // Calculate expiration for students
                        const expiresAt = new Date(new Date(activityData.launched_at).getTime() + durationMins * 60 * 1000);
                        activityData.expires_at = expiresAt.toISOString();

                        // Persist in memory so late-joiners can poll for it
                        channelActivities.set(channelName, activityData);

                        // Clear any existing timer for this channel
                        if (channelActivityTimers.has(channelName)) {
                            clearTimeout(channelActivityTimers.get(channelName));
                        }

                        // Schedule auto-stop
                        const msLeft = expiresAt.getTime() - Date.now();
                        if (msLeft > 0) {
                            const timer = setTimeout(() => {
                                stopActivityCentrally(channelName);
                            }, msLeft);
                            channelActivityTimers.set(channelName, timer);
                        }

                        console.log(`[WS] Activity launched: channel=${channelName}, classwork_id=${activityData.classwork_id}, title="${activityData.title}", expires_at=${activityData.expires_at}`);

                        // Broadcast to all clients in the channel
                        nc.publish(
                            `meeting.${channelName}`,
                            sc.encode(JSON.stringify({
                                action: 'activity-launched',
                                channelName,
                                data: activityData,
                            }))
                        );
                        break;

                    case 'activity-stopped':
                        if (ws.clientRole !== 'teacher' && ws.clientRole !== 'admin') {
                            ws.send(JSON.stringify({ action: 'error', message: 'Not authorized to stop activity.' }));
                            break;
                        }
                        stopActivityCentrally(channelName);
                        console.log(`[WS] Activity manually stopped: channel=${channelName}`);
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