import 'dotenv/config';
import express from 'express';
import http from 'http';
import path from 'path';
import fs from 'fs';
import os from 'os';
import { fileURLToPath } from 'url';
import { spawn } from 'child_process';
import { WebSocketServer } from 'ws';
import { EventEmitter } from 'events';
import cookieParser from 'cookie-parser';
import jwt from 'jsonwebtoken';
import bcrypt from 'bcryptjs';
import { db } from './db/index.js';
import { users, streamSessions } from './db/schema.js';
import { initDb } from './db/migrate.js';
import { eq, desc, sql } from 'drizzle-orm';

EventEmitter.defaultMaxListeners = 50;

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const server = http.createServer(app);

const PORT = process.env.PORT || 3000;
const NODE_ENV = process.env.NODE_ENV || 'development';
const APP_URL = process.env.APP_URL || `http://localhost:${PORT}`;
const IS_PROD = NODE_ENV === 'production';
const JWT_SECRET = process.env.JWT_SECRET || 'cowatch_super_secret_jwt_key_2026';

// Directories setup
const liveDir = path.join(__dirname, 'public', 'live');
if (!fs.existsSync(liveDir)) {
    fs.mkdirSync(liveDir, { recursive: true });
}

// Configure Express Middlewares & Views
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));

app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS, HEAD');
    res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Range');
    if (req.method === 'OPTIONS') return res.sendStatus(200);
    next();
});

app.use(express.static(path.join(__dirname, 'public')));
app.use('/stream', express.raw({ type: '*/*', limit: '100mb' }));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(cookieParser());

// Initialize Database schema and seed default Admin (watch@watch.in / HexWatch78)
initDb().catch(console.error);

// ─── Authentication Middlewares ──────────────────────────────────────────────
function getAuthUser(req) {
    const token = req.cookies.auth_token;
    if (!token) return null;
    try { return jwt.verify(token, JWT_SECRET); } catch (e) { return null; }
}

async function requireAuth(req, res, next) {
    const userPayload = getAuthUser(req);
    if (!userPayload) return res.redirect('/login');
    try {
        const dbUser = await db.select().from(users).where(eq(users.id, userPayload.id));
        if (dbUser.length === 0 || (!dbUser[0].hasAccess && dbUser[0].role !== 'admin')) {
            res.clearCookie('auth_token');
            return res.redirect('/login?error=access_denied');
        }
        req.user = dbUser[0];
        next();
    } catch (e) { return res.redirect('/login'); }
}

async function requireAdmin(req, res, next) {
    const userPayload = getAuthUser(req);
    if (!userPayload) return res.redirect('/login');
    try {
        const dbUser = await db.select().from(users).where(eq(users.id, userPayload.id));
        if (dbUser.length === 0 || dbUser[0].role !== 'admin') {
            return res.status(403).send('Forbidden: Admin access required');
        }
        req.user = dbUser[0];
        next();
    } catch (e) { return res.status(403).send('Forbidden'); }
}

// ─── Multi-Stream Session Manager ──────────────────────────────────────────────
const activeStreams = new Map();

function getOrCreateStreamSession(streamKey, title = 'Live Movie Stream', user = null) {
    if (!streamKey || streamKey === 'undefined') streamKey = 'default';
    if (activeStreams.has(streamKey)) {
        const s = activeStreams.get(streamKey);
        if (title && title !== 'Live Movie Stream') s.title = title;
        if (user) { s.hostId = user.id; s.hostName = user.name; }
        return s;
    }

    const session = {
        streamKey,
        title,
        hostId: user ? user.id : null,
        hostName: user ? user.name : 'Host',
        isLive: false,
        ffmpegProcess: null,
        disconnectTimer: null,
        oneHourTimer: null,
        liveDir: path.join(liveDir, streamKey),
        createdAt: new Date(),
        chunks1080pCount: 0,
        chunks720pCount: 0,
        chunks480pCount: 0,
        totalChunksCount: 0,
        failureCount: 0,
        qualityViewers: { '1080p': new Set(), '720p': new Set(), '480p': new Set() },
        initSegments: { '1080p': null, '720p': null, '480p': null }
    };

    if (!fs.existsSync(session.liveDir)) {
        fs.mkdirSync(session.liveDir, { recursive: true });
    }

    activeStreams.set(streamKey, session);
    return session;
}

function clearLiveFolder(session) {
    if (fs.existsSync(session.liveDir)) {
        const files = fs.readdirSync(session.liveDir);
        for (const file of files) {
            try { fs.unlinkSync(path.join(session.liveDir, file)); } catch (err) { }
        }
    }
}

// ─── WebSocket Servers (Status, Viewers Count & Telemetries) ───────────────────
const wss = new WebSocketServer({ noServer: true });
const streamWss = new WebSocketServer({ noServer: true });
const viewWss = new WebSocketServer({ noServer: true });

server.on('upgrade', (request, socket, head) => {
    try {
        const urlObj = new URL(request.url, `http://${request.headers.host || 'localhost'}`);
        const pathname = urlObj.pathname;
        const streamKey = urlObj.searchParams.get('key') || 'default';
        const role = urlObj.searchParams.get('role') || 'viewer';

        if (pathname === '/status-ws') {
            wss.handleUpgrade(request, socket, head, (ws) => {
                ws.streamKey = streamKey;
                ws.role = role;
                wss.emit('connection', ws, request);
            });
        } else if (pathname === '/stream-ws') {
            streamWss.handleUpgrade(request, socket, head, (ws) => {
                ws.streamKey = streamKey;
                ws.role = 'host';
                streamWss.emit('connection', ws, request);
            });
        } else if (pathname === '/view-ws') {
            viewWss.handleUpgrade(request, socket, head, (ws) => {
                ws.streamKey = streamKey;
                ws.quality = urlObj.searchParams.get('quality') || '1080p';
                ws.role = 'viewer';
                viewWss.emit('connection', ws, request);
            });
        } else {
            socket.destroy();
        }
    } catch (err) { socket.destroy(); }
});

wss.on('connection', (ws) => {
    const key = ws.streamKey || 'default';
    if (key !== 'admin') {
        const session = activeStreams.get(key);
        if (session) broadcastStatus(session, session.isLive);
    }
    broadcastAdminTelemetry();

    ws.on('close', () => {
        if (key !== 'admin') {
            const session = activeStreams.get(key);
            if (session) broadcastStatus(session, session.isLive);
        }
        broadcastAdminTelemetry();
    });
    ws.on('error', (err) => console.warn(`[Status WS Error ${key}]:`, err.message));
});

function broadcastStatus(session, liveState) {
    if (!session) return;
    session.isLive = liveState;
    const viewerCount = getTotalViewerCount(session);

    const msg = JSON.stringify({
        type: 'STATUS',
        live: session.isLive,
        streamKey: session.streamKey,
        viewers: viewerCount
    });

    for (const client of wss.clients) {
        if (client.streamKey === session.streamKey && client.readyState === 1) {
            client.send(msg);
        }
    }
}

function broadcastAdminTelemetry() {
    const adminData = Array.from(activeStreams.values()).map(s => {
        const statusClients = Array.from(wss.clients).filter(c => c.streamKey === s.streamKey && c.readyState === 1 && c.role === 'viewer');
        return {
            streamKey: s.streamKey,
            title: s.title,
            hostName: s.hostName || 'Host',
        isLive: s.isLive,
            viewerCount: statusClients.length
        };
    });

    const msg = JSON.stringify({ type: 'ADMIN_TELEMETRY', activeStreams: adminData });
    for (const client of wss.clients) {
        if (client.streamKey === 'admin' && client.readyState === 1) {
            client.send(msg);
        }
    }
}

streamWss.on('connection', (ws) => {
    const key = ws.streamKey || 'default';
    console.log(`⚡ Host connected to WebSocket Ingest for [${key}]`);

    const session = activeStreams.get(key);
    if (session) {
        if (session.disconnectTimer) {
            console.log(`🔄 Host reconnected to WebSocket Ingest for [${key}]! Cancelled 90-second grace timer.`);
            clearTimeout(session.disconnectTimer);
            session.disconnectTimer = null;
        }
        broadcastStatus(session, true);
        broadcastAdminTelemetry();
    }

    ws.on('message', (data) => {
        const session = activeStreams.get(key);
        if (session) {
            if (session.disconnectTimer) {
                clearTimeout(session.disconnectTimer);
                session.disconnectTimer = null;
            }
            if (session.ffmpegProcess && session.ffmpegProcess.stdin && session.ffmpegProcess.stdin.writable) {
                try {
                    const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
                    session.ffmpegProcess.stdin.write(buf);
                } catch (e) { }
            }
        }
    });
    ws.on('error', (err) => {
        console.warn(`[Stream WS Error ${key}]:`, err.message);
        const session = activeStreams.get(key);
        if (session) session.failureCount = (session.failureCount || 0) + 1;
    });
    ws.on('close', () => {
        console.log(`⚠️ Host WebSocket disconnected for [${key}]. Keeping FFmpeg process alive for 90s reconnection window...`);
        const session = activeStreams.get(key);
        if (session && session.isLive) {
            if (session.disconnectTimer) clearTimeout(session.disconnectTimer);
            session.disconnectTimer = setTimeout(() => {
                console.log(`🛑 90-second reconnection window expired for [${key}]. Stopping FFmpeg process...`);
                stopFfmpegLive(session);
                broadcastStatus(session, false);
                broadcastAdminTelemetry();
                session.disconnectTimer = null;
            }, 90 * 1000);
        }
    });
});

// ─── Viewer WebSocket Handler (streams fMP4 chunks to viewers) ──────────────
function broadcastToQuality(session, quality, data) {
    if (!session || !session.qualityViewers) return;
    const viewers = session.qualityViewers[quality];
    if (!viewers) return;
    for (const viewer of viewers) {
        if (viewer.readyState === 1) {
            try { viewer.send(data); } catch (e) { }
        }
    }
}

function getTotalViewerCount(session) {
    if (!session || !session.qualityViewers) return 0;
    return (session.qualityViewers['1080p']?.size || 0) +
           (session.qualityViewers['720p']?.size || 0) +
           (session.qualityViewers['480p']?.size || 0);
}

viewWss.on('connection', (ws) => {
    const key = ws.streamKey || 'default';
    const quality = ws.quality || '1080p';
    const session = activeStreams.get(key);
    if (!session) {
        ws.close(1000, 'Stream not found');
        return;
    }

    if (!session.qualityViewers[quality]) session.qualityViewers[quality] = new Set();
    session.qualityViewers[quality].add(ws);
    console.log(`👁 Viewer connected to [${key}] ${quality} (${getTotalViewerCount(session)} total)`);

    if (session.initSegments[quality]) {
        try { ws.send(session.initSegments[quality]); } catch (e) { }
    }

    broadcastStatus(session, session.isLive);

    ws.on('close', () => {
        session.qualityViewers[quality].delete(ws);
        console.log(`👁 Viewer disconnected from [${key}] ${quality} (${getTotalViewerCount(session)} total)`);
        broadcastStatus(session, session.isLive);
    });
    ws.on('error', (err) => {
        session.qualityViewers[quality].delete(ws);
    });
});

setInterval(() => {
    broadcastAdminTelemetry();
}, 2000);

// ─── FFmpeg Live Process Manager ────────────────────────────────────────────────
function stopFfmpegLive(session) {
    if (session.disconnectTimer) {
        clearTimeout(session.disconnectTimer);
        session.disconnectTimer = null;
    }
    if (session.ffmpegProcess) {
        try {
            if (session.ffmpegProcess.stdin) {
                session.ffmpegProcess.stdin.removeAllListeners('error');
                session.ffmpegProcess.stdin.on('error', () => { });
                session.ffmpegProcess.stdin.end();
            }
            session.ffmpegProcess.kill('SIGKILL');
        } catch (err) { }
        session.ffmpegProcess = null;
    }
    session.isLive = false;
    if (session.oneHourTimer) { clearTimeout(session.oneHourTimer); session.oneHourTimer = null; }
}

async function startFfmpegLive(session) {
    if (session.cleanupTimer) { clearTimeout(session.cleanupTimer); session.cleanupTimer = null; }

    stopFfmpegLive(session);
    clearLiveFolder(session);

    // 1-Hour Stream Limit Timer: If stream runs > 1 hour (3600s), increment user stream count in DB!
    session.countedAgainstLimit = false;
    session.oneHourTimer = setTimeout(async () => {
        console.log(`⏰ Stream [${session.streamKey}] reached 1 hour duration. Incrementing stream count for host ${session.hostId}...`);
        if (session.hostId && !session.countedAgainstLimit) {
            session.countedAgainstLimit = true;
            try {
                await db.update(users).set({ streamCount: sql`${users.streamCount} + 1` }).where(eq(users.id, session.hostId));
            } catch (e) { console.error('Error updating user streamCount:', e); }
        }
    }, 3600 * 1000);

    const args = [
        '-y',
        '-threads', '0',
        '-fflags', '+genpts+discardcorrupt+nobuffer',
        '-probesize', '2M',
        '-analyzeduration', '1000000',
        '-i', 'pipe:0',
        '-f', 'lavfi',
        '-i', 'anullsrc=channel_layout=stereo:sample_rate=48000',

        '-filter_complex',
        '[0:v]format=yuv420p,fps=30,split=3[v1080][v720][v480];' +
        '[v720]scale=1280:720:flags=bilinear[sv720];' +
        '[v480]scale=854:480:flags=bilinear[sv480];' +
        '[0:a][1:a]amix=inputs=2:duration=first:dropout_transition=0:normalize=0,aresample=async=1:first_pts=0,asplit=3[a1080][a720][a480]',

        // 1080p → pipe:3
        '-map', '[v1080]', '-map', '[a1080]',
        '-c:v', 'libx264', '-preset', 'ultrafast', '-profile:v', 'high', '-level', '4.2',
        '-crf', '23', '-maxrate', '4500k', '-bufsize', '4500k',
        '-g', '60', '-sc_threshold', '0',
        '-x264-params', 'no-scenecut=1:open-gop=0:keyint=60:min-keyint=60:rc-lookahead=0:bframes=0',
        '-c:a', 'aac', '-b:a', '192k', '-ar', '48000',
        '-f', 'mp4', '-mov_flags', 'frag_keyframe+empty_moof+default_base_moof',
        '-frag_duration', '1000000', '-max_muxing_queue_size', '4096',
        'pipe:3',

        // 720p → pipe:4
        '-map', '[sv720]', '-map', '[a720]',
        '-c:v', 'libx264', '-preset', 'ultrafast', '-profile:v', 'main',
        '-crf', '24', '-maxrate', '2500k', '-bufsize', '2500k',
        '-g', '60', '-sc_threshold', '0',
        '-x264-params', 'no-scenecut=1:open-gop=0:keyint=60:min-keyint=60:rc-lookahead=0:bframes=0',
        '-c:a', 'aac', '-b:a', '128k', '-ar', '48000',
        '-f', 'mp4', '-mov_flags', 'frag_keyframe+empty_moof+default_base_moof',
        '-frag_duration', '1000000', '-max_muxing_queue_size', '4096',
        'pipe:4',

        // 480p → pipe:5
        '-map', '[sv480]', '-map', '[a480]',
        '-c:v', 'libx264', '-preset', 'ultrafast', '-profile:v', 'baseline',
        '-crf', '26', '-maxrate', '1200k', '-bufsize', '1200k',
        '-g', '60', '-sc_threshold', '0',
        '-x264-params', 'no-scenecut=1:open-gop=0:keyint=60:min-keyint=60:rc-lookahead=0:bframes=0',
        '-c:a', 'aac', '-b:a', '96k', '-ar', '48000',
        '-f', 'mp4', '-mov_flags', 'frag_keyframe+empty_moof+default_base_moof',
        '-frag_duration', '1000000', '-max_muxing_queue_size', '4096',
        'pipe:5'
    ];

    console.log(`⚡ Spawning Live Stream Generator for [${session.streamKey}]...`);
    session.ffmpegProcess = spawn('ffmpeg', args, { stdio: ['pipe', 'pipe', 'pipe', 'pipe', 'pipe', 'pipe'] });
    session.isLive = true;
    session.initSegments = { '1080p': null, '720p': null, '480p': null };

    if (session.ffmpegProcess.stdin) {
        session.ffmpegProcess.stdin.on('error', (err) => {});
    }

    const pipeQualityMap = { 3: '1080p', 4: '720p', 5: '480p' };
    for (const [fd, quality] of Object.entries(pipeQualityMap)) {
        const pipe = session.ffmpegProcess.stdio[fd];
        if (pipe) {
            pipe.on('data', (chunk) => {
                if (!session.initSegments[quality]) {
                    session.initSegments[quality] = chunk;
                    console.log(`📦 Init segment stored for [${session.streamKey}] ${quality} (${chunk.length} bytes)`);
                }
                broadcastToQuality(session, quality, chunk);
            });
        }
    }

    if (session.ffmpegProcess.stderr) {
        let lastLoggedTime = 0;
        session.ffmpegProcess.stderr.on('data', (data) => {
            const lines = data.toString().split(/\r?\n/);
            for (const line of lines) {
                const msg = line.trim();
                if (!msg) continue;
                if (msg.includes('fps=') || msg.includes('speed=')) {
                    const now = Date.now();
                    if (now - lastLoggedTime > 2000) {
                        lastLoggedTime = now;
                        const fpsMatch = msg.match(/fps=\s*([\d.]+)/);
                        const speedMatch = msg.match(/speed=\s*([\d.x]+)/);
                        const fps = fpsMatch ? parseFloat(fpsMatch[1]).toFixed(1) : '30.0';
                        const speed = speedMatch ? speedMatch[1] : '1.0x';
                        console.log(`[FFmpeg ${session.streamKey}] Speed: ${speed} | FPS: ${fps}`);
                    }
                }
            }
        });
    }

    session.ffmpegProcess.on('exit', (code, signal) => {
        session.ffmpegProcess = null;
        session.isLive = false;
    });
}

// ─── Authentication & User Routes ──────────────────────────────────────────────

app.get('/login', (req, res) => {
    const user = getAuthUser(req);
    if (user) return res.redirect('/');
    res.render('login');
});

app.get('/logout', (req, res) => {
    res.clearCookie('auth_token');
    res.redirect('/login');
});

app.post('/api/auth/register', async (req, res) => {
    try {
        const { name, email, password } = req.body || {};
        if (!name || !email || !password) {
            return res.status(400).json({ success: false, error: 'All fields are required' });
        }

        const existing = await db.select().from(users).where(eq(users.email, email.toLowerCase().trim()));
        if (existing.length > 0) {
            return res.status(400).json({ success: false, error: 'Email already registered' });
        }

        const hashedPassword = await bcrypt.hash(password, 10);
        await db.insert(users).values({
            name: name.trim(),
            email: email.toLowerCase().trim(),
            password: hashedPassword,
            role: 'user',
            hasAccess: false,
            streamLimit: 5,
            streamCount: 0
        });

        res.json({ success: true, message: 'Account registered. Waiting for Admin approval.' });
    } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

app.post('/api/auth/login', async (req, res) => {
    try {
        const { email, password } = req.body || {};
        if (!email || !password) {
            return res.status(400).json({ success: false, error: 'Email and password required' });
        }

        const dbUser = await db.select().from(users).where(eq(users.email, email.toLowerCase().trim()));
        if (dbUser.length === 0) {
            return res.status(400).json({ success: false, error: 'Invalid email or password' });
        }

        const user = dbUser[0];
        const match = await bcrypt.compare(password, user.password);
        if (!match) {
            return res.status(400).json({ success: false, error: 'Invalid email or password' });
        }

        if (!user.hasAccess && user.role !== 'admin') {
            return res.status(403).json({ success: false, error: 'Account pending Admin access approval.' });
        }

        const token = jwt.sign({ id: user.id, email: user.email, role: user.role, name: user.name }, JWT_SECRET, { expiresIn: '7d' });
        res.cookie('auth_token', token, { httpOnly: true, maxAge: 7 * 24 * 3600 * 1000 });

        res.json({ success: true, redirect: '/' });
    } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

// Admin Panel Routes
app.get('/admin', requireAdmin, async (req, res) => {
    try {
        const allUsers = await db.select().from(users).orderBy(desc(users.createdAt));
        const activeList = Array.from(activeStreams.values()).map(s => {
            const statusClients = Array.from(wss.clients).filter(c => c.streamKey === s.streamKey && c.readyState === 1);
            return {
                streamKey: s.streamKey,
                title: s.title,
                hostName: s.hostName || 'Host',
                isLive: s.isLive,
                viewerCount: statusClients.length
            };
        });
        res.render('admin', { users: allUsers, activeStreams: activeList, currentUser: req.user });
    } catch (err) { res.status(500).send('Admin error: ' + err.message); }
});

app.post('/api/admin/users/access', requireAdmin, async (req, res) => {
    try {
        const { userId, hasAccess } = req.body;
        await db.update(users).set({ hasAccess: !!hasAccess }).where(eq(users.id, userId));
        res.json({ success: true });
    } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

app.post('/api/admin/users/limit', requireAdmin, async (req, res) => {
    try {
        const { userId, streamLimit } = req.body;
        await db.update(users).set({ streamLimit: parseInt(streamLimit) }).where(eq(users.id, userId));
        res.json({ success: true });
    } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

app.post('/api/admin/users/reset-count', requireAdmin, async (req, res) => {
    try {
        const { userId } = req.body;
        await db.update(users).set({ streamCount: 0 }).where(eq(users.id, userId));
        res.json({ success: true });
    } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

// Admin User Telemetry Accordion Route
app.get(['/user', '/users'], requireAdmin, async (req, res) => {
    try {
        const allUsers = await db.select().from(users).orderBy(desc(users.createdAt));
        const allDbSessions = await db.select().from(streamSessions).orderBy(desc(streamSessions.startedAt));

        // Group database sessions by host_id
        const userSessionsMap = new Map();
        for (const s of allDbSessions) {
            if (!s.hostId) continue;
            if (!userSessionsMap.has(s.hostId)) userSessionsMap.set(s.hostId, []);
            userSessionsMap.get(s.hostId).push(s);
        }

        const usersListWithTelemetry = allUsers.map(u => {
            const dbSessions = userSessionsMap.get(u.id) || [];
            
            // Check active live streams for this user
            const activeUserStreams = Array.from(activeStreams.values()).filter(s => s.hostId === u.id);
            const isLiveNow = activeUserStreams.some(s => s.isLive);

            // Combine DB sessions + active live sessions
            const sessions = [...dbSessions];
            for (const activeS of activeUserStreams) {
                if (!sessions.some(s => s.streamKey === activeS.streamKey)) {
                    const statusClients = Array.from(wss.clients).filter(c => c.streamKey === activeS.streamKey && c.readyState === 1);
                    sessions.unshift({
                        id: 'live_' + activeS.streamKey,
                        streamKey: activeS.streamKey,
                        title: activeS.title,
                        isLive: activeS.isLive,
                        startedAt: activeS.createdAt || new Date(),
                        durationSeconds: Math.floor((Date.now() - (activeS.createdAt || Date.now())) / 1000),
                        viewerCount: statusClients.length,
                        peakViewers: Math.max(statusClients.length, activeS.peakViewersCount || 0),
                        totalChunks: activeS.totalChunksCount || 0,
                        chunks1080p: activeS.chunks1080pCount || 0,
                        chunks720p: activeS.chunks720pCount || 0,
                        chunks480p: activeS.chunks480pCount || 0,
                        failureCount: activeS.failureCount || 0
                    });
                }
            }

            // Calculate aggregations
            let totalStreamSeconds = 0;
            let totalViewersCount = 0;
            let peakViewers = 0;
            let totalChunks = 0;
            let chunks1080p = 0;
            let chunks720p = 0;
            let chunks480p = 0;
            let failureCount = 0;

            for (const s of sessions) {
                const dur = s.durationSeconds || 0;
                totalStreamSeconds += dur;
                const v = s.viewerCount || 0;
                const peakV = s.peakViewers || v;
                totalViewersCount += v;
                if (peakV > peakViewers) peakViewers = peakV;

                totalChunks += (s.totalChunks || 0);
                chunks1080p += (s.chunks1080p || 0);
                chunks720p += (s.chunks720p || 0);
                chunks480p += (s.chunks480p || 0);
                failureCount += (s.failureCount || 0);
            }

            const totalStreamMinutes = Math.round(totalStreamSeconds / 60);
            const totalStreamHours = (totalStreamSeconds / 3600).toFixed(1);

            // Stability Rating
            let stabilityRating = 100;
            if (sessions.length > 0 && failureCount > 0) {
                stabilityRating = Math.max(0, Math.round(100 - (failureCount * 2.5)));
            }

            return {
                ...u,
                isLiveNow,
                totalStreamSeconds,
                totalStreamMinutes,
                totalStreamHours,
                totalSessionsCount: sessions.length,
                totalViewersCount,
                peakViewers,
                totalChunks,
                chunks1080p,
                chunks720p,
                chunks480p,
                failureCount,
                stabilityRating,
                sessions
            };
        });

        res.render('users', { usersList: usersListWithTelemetry, currentUser: req.user });
    } catch (err) {
        console.error('User telemetry route error:', err);
        res.status(500).send('User telemetry error: ' + err.message);
    }
});

// ─── Application Core Routes ─────────────────────────────────────────────────────

// Home Dashboard — Requires Auth
app.get('/', requireAuth, (req, res) => {
    const streamsList = Array.from(activeStreams.values()).map(s => ({
        streamKey: s.streamKey,
        title: s.title,
        isLive: s.isLive,
        createdAt: s.createdAt
    }));
    res.render('home', { streams: streamsList, user: req.user });
});

// Create Stream API
app.post('/api/streams/create', requireAuth, async (req, res) => {
    let { title, streamKey } = req.body || {};
    if (!title) title = 'Untitled Live Movie Stream';
    if (!streamKey) streamKey = 'str_' + Math.random().toString(36).substring(2, 9);

    // Check monthly stream limit for normal users
    if (req.user.role !== 'admin' && req.user.streamCount >= req.user.streamLimit) {
        return res.status(403).json({
            success: false,
            error: `Monthly stream limit reached (${req.user.streamCount}/${req.user.streamLimit}). Contact Admin to increase limit.`
        });
    }

    const session = getOrCreateStreamSession(streamKey, title, req.user);
    broadcastAdminTelemetry();
    res.json({
        success: true,
        streamKey: session.streamKey,
        title: session.title
    });
});

// Delete Stream API
app.post(['/api/streams/delete/:streamKey', '/api/streams/delete'], requireAuth, async (req, res) => {
    try {
        const streamKey = req.params.streamKey || req.body?.streamKey;
        if (!streamKey) return res.status(400).json({ success: false, error: 'Stream key required' });

        const session = activeStreams.get(streamKey);
        if (session) {
            stopFfmpegLive(session);
            clearLiveFolder(session);
            activeStreams.delete(streamKey);
        } else {
            const dummySession = { streamKey, liveDir: path.join(liveDir, streamKey) };
            clearLiveFolder(dummySession);
        }

        try {
            await db.delete(streamSessions).where(eq(streamSessions.streamKey, streamKey));
        } catch (e) {}

        broadcastAdminTelemetry();
        console.log(`🗑️ Stream [${streamKey}] deleted.`);
        res.json({ success: true, message: `Stream [${streamKey}] deleted.` });
    } catch (err) {
        console.error('Delete stream error:', err);
        res.status(500).json({ success: false, error: err.message });
    }
});

// Broadcaster Studio Route — Requires Auth
app.get(['/stream', '/stream/:streamKey'], requireAuth, (req, res) => {
    const key = req.params.streamKey || 'default';
    const session = getOrCreateStreamSession(key, 'Live Channel', req.user);

    // Check monthly limit before broadcasting
    if (req.user.role !== 'admin' && req.user.streamCount >= req.user.streamLimit) {
        return res.status(403).send(`<h1>Monthly Stream Limit Reached</h1><p>You have used ${req.user.streamCount} out of ${req.user.streamLimit} streams allowed this month. Please contact your Admin.</p><a href="/">Back to Home</a>`);
    }

    res.render('index', {
        title: `${session.title} - Broadcaster Studio`,
        streamKey: session.streamKey,
        streamTitle: session.title,
        user: req.user
    });
});

// Express Local Live File Fallback
app.get('/live-file/:streamKey/:filename', (req, res) => {
    const { streamKey, filename } = req.params;
    const filePath = path.join(liveDir, streamKey, filename);
    if (!fs.existsSync(filePath)) return res.status(404).send('File not found');
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Content-Type', 'video/mp2t');
    res.setHeader('Cache-Control', 'public, max-age=3600');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('Keep-Alive', 'timeout=30, max=100');
    res.sendFile(filePath);
});

// Dynamic Express HLS Playlist Server — Serves .m3u8 instantly
app.get(['/live-playlist/:streamKey/:playlist', '/live-playlist/:playlist'], (req, res) => {
    const streamKey = req.params.streamKey || 'default';
    const playlistName = req.params.playlist || 'master.m3u8';

    const session = activeStreams.get(streamKey);
    if (!session) return res.status(404).send('#EXTM3U\n#EXT-X-ERROR: Stream session not found');

    const playlistPath = path.join(session.liveDir, playlistName);
    if (!fs.existsSync(playlistPath)) return res.status(404).send('#EXTM3U\n#EXT-X-ERROR: Playlist not generated yet');

    try {
        const isTs = playlistName.endsWith('.ts');
        if (isTs) {
            res.setHeader('Content-Type', 'video/mp2t');
            res.setHeader('Cache-Control', 'public, max-age=3600');
            res.setHeader('Pragma', 'public');
            res.setHeader('Expires', '3600');
            res.setHeader('Access-Control-Allow-Origin', '*');
            return res.sendFile(playlistPath);
        }
        const rawContent = fs.readFileSync(playlistPath, 'utf-8');
        res.setHeader('Content-Type', 'application/vnd.apple.mpegurl');
        res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate, max-age=0');
        res.setHeader('Pragma', 'no-cache');
        res.setHeader('Expires', '0');
        res.setHeader('Connection', 'keep-alive');
        res.setHeader('Keep-Alive', 'timeout=30, max=100');
        return res.send(rawContent);
    } catch (err) { return res.status(500).send('#EXTM3U\n#EXT-X-ERROR: Error reading playlist'); }
});

// Public Live Player Route (HLS Master Playlist served via Express)
app.get(['/live', '/live/:streamKey'], (req, res) => {
    const key = req.params.streamKey || 'default';
    const session = getOrCreateStreamSession(key);
    const hlsBaseUrl = `/live-playlist/${key}`;
    res.render('live', {
        stream: { title: session.title, streamKey: session.streamKey },
        hlsBaseUrl: hlsBaseUrl,
        streamKey: session.streamKey
    });
});

// Live Status Check API
app.get(['/live/status', '/live/status/:streamKey'], (req, res) => {
    const key = req.params.streamKey || req.query.streamKey || 'default';
    const session = activeStreams.get(key);
    res.json({ live: (session && session.isLive), streamKey: key });
});

// Reset & Start Stream Process
app.post(['/reset-stream', '/reset-stream/:streamKey'], async (req, res) => {
    const key = req.params.streamKey || req.body?.streamKey || 'default';
    const user = getAuthUser(req);
    const session = getOrCreateStreamSession(key, 'Live Stream', user);

    try {
        await startFfmpegLive(session);
        broadcastStatus(session, true);
        res.json({ success: true, message: `Live stream ${key} started`, streamKey: key });
    } catch (err) {
        console.error(`Error starting stream ${key}:`, err);
        res.status(500).json({ success: false, error: err.message });
    }
});

// Stop Stream Process
app.post(['/stop-stream', '/stop-stream/:streamKey'], async (req, res) => {
    const key = req.params.streamKey || req.body?.streamKey || 'default';
    const session = activeStreams.get(key);
    if (!session) return res.json({ success: true });

    try {
        stopFfmpegLive(session);
        broadcastStatus(session, false);
        broadcastAdminTelemetry();
        console.log(`🔴 Stream [${key}] stopped and FFmpeg process killed immediately.`);

        if (session.cleanupTimer) clearTimeout(session.cleanupTimer);
        session.cleanupTimer = setTimeout(async () => {
            console.log(`🧹 Purging local HLS files for [${key}]...`);
            clearLiveFolder(session);
        }, 10 * 60 * 1000);

        res.json({ success: true, message: `Stream [${key}] stopped.` });
    } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

// Binary Chunk Stream HTTP Fallback
app.post(['/stream', '/stream/:streamKey'], (req, res) => {
    const key = req.params.streamKey || req.headers['x-stream-key'] || 'default';
    const chunkIndex = parseInt(req.headers['x-chunk-index'] || '0');

    res.status(200).json({ success: true, chunkIndex });

    const session = activeStreams.get(key);
    if (session) {
        if (session.disconnectTimer) {
            console.log(`🔄 Incoming HTTP chunk stream for [${key}] during network drop! Refreshing 90-second grace timer.`);
            clearTimeout(session.disconnectTimer);
            session.disconnectTimer = setTimeout(() => {
                console.log(`🛑 90-second reconnection window expired for [${key}]. Stopping FFmpeg process...`);
                stopFfmpegLive(session);
                broadcastStatus(session, false);
                broadcastAdminTelemetry();
                session.disconnectTimer = null;
            }, 90 * 1000);
        }
        if (session.ffmpegProcess && session.ffmpegProcess.stdin && session.ffmpegProcess.stdin.writable) {
            try {
                const buf = Buffer.isBuffer(req.body) ? req.body : Buffer.from(req.body);
                session.ffmpegProcess.stdin.write(buf);
            } catch (e) { }
        }
    }
});

app.use((err, req, res, next) => {
    if (err && (err.type === 'aborted' || err.status === 400)) {
        return res.status(400).json({ success: false, error: 'Request aborted' });
    }
    next(err);
});

server.listen(PORT, () => {
    console.log(`🚀 CoWatch Multi-Stream Platform running at ${APP_URL} (Port ${PORT})`);
});
