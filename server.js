import 'dotenv/config';
import express from 'express';
import http from 'http';
import path from 'path';
import fs from 'fs';
import os from 'os';
import { fileURLToPath } from 'url';
import { spawn } from 'child_process';
import { WebSocketServer } from 'ws';
import { S3Client, PutObjectCommand, DeleteObjectCommand, ListObjectsV2Command } from '@aws-sdk/client-s3';
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

// ExCloud S3 Object Storage CDN Configuration
const S3_ENDPOINT = process.env.S3_ENDPOINT || 'https://buckets.excloud.dev';
const S3_REGION = process.env.S3_REGION || 'default';
const S3_BUCKET = process.env.S3_BUCKET || 'live';
const S3_ACCESS_KEY = process.env.S3_ACCESS_KEY || 'EXCNLC2REYVL5FSFT57AQRKPP24TE';
const S3_SECRET_KEY = process.env.S3_SECRET_KEY || 'DiVv1AV3CJI/Y58jkiDzqCyUI9osj3NXS1xXxCA3';
const S3_PUBLIC_BASE_URL = process.env.S3_PUBLIC_BASE_URL || 'https://1834.objects.excloud.dev/public/live';
const S3_ENABLED = !!(S3_ACCESS_KEY && S3_SECRET_KEY);

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

function getOrCreateStreamSession(streamKey, title = 'Live Stream', hostUser = null) {
    if (!streamKey || streamKey === 'undefined') streamKey = 'default';
    if (activeStreams.has(streamKey)) {
        const session = activeStreams.get(streamKey);
        if (title && title !== 'Live Stream') session.title = title;
        if (hostUser) { session.hostId = hostUser.id; session.hostName = hostUser.name; }
        return session;
    }

    const streamLiveDir = path.join(liveDir, streamKey);
    if (!fs.existsSync(streamLiveDir)) {
        fs.mkdirSync(streamLiveDir, { recursive: true });
    }

    const session = {
        streamKey,
        title,
        hostId: hostUser ? hostUser.id : null,
        hostName: hostUser ? hostUser.name : 'Host',
        createdAt: new Date(),
        isLive: false,
        liveDir: streamLiveDir,
        ffmpegProcess: null,
        s3Watcher: null,
        s3SegmentQueue: new Set(),
        uploadedS3Segments: new Set(),
        s3UploadLog: [], // Telemetry items: { timestamp, filename, is720p, is480p }
        isS3Uploading: false,
        cleanupTimer: null,
        oneHourTimer: null,
        countedAgainstLimit: false
    };

    activeStreams.set(streamKey, session);
    return session;
}

// ─── ExCloud S3 Bucket Sync & Telemetry Manager ──────────────────────────────────
let s3Client = null;
if (S3_ENABLED) {
    s3Client = new S3Client({
        endpoint: S3_ENDPOINT,
        region: S3_REGION,
        credentials: { accessKeyId: S3_ACCESS_KEY, secretAccessKey: S3_SECRET_KEY },
        forcePathStyle: true
    });
    console.log(`📦 ExCloud S3 Storage Enabled (${S3_ENDPOINT} / ${S3_BUCKET})`);
}

function getSessionS3Telemetry(session) {
    if (!session || !session.s3UploadLog) return { chunksPerSec: '0.0', p720Count: 0, p480Count: 0 };
    const now = Date.now();
    const recentLogs = session.s3UploadLog.filter(item => now - item.timestamp < 5000);
    const count720p = recentLogs.filter(item => item.is720p).length;
    const count480p = recentLogs.filter(item => item.is480p).length;
    const totalCount = recentLogs.length;
    return {
        chunksPerSec: (totalCount / 5).toFixed(1),
        p720Count: count720p,
        p480Count: count480p,
        total: totalCount
    };
}

async function uploadSingleSegment(session, filename) {
    const filePath = path.join(session.liveDir, filename);
    if (!fs.existsSync(filePath)) return false;

    try {
        const fileBuffer = fs.readFileSync(filePath);
        const s3Key = `${session.streamKey}/${filename}`;
        await s3Client.send(new PutObjectCommand({
            Bucket: S3_BUCKET,
            Key: s3Key,
            Body: fileBuffer,
            ContentType: 'video/mp2t',
            CacheControl: 'public, max-age=3600'
        }));
        session.uploadedS3Segments.add(filename);

        const now = Date.now();
        const is720p = filename.includes('720p');
        const is480p = filename.includes('480p');
        session.s3UploadLog.push({ timestamp: now, filename, is720p, is480p });
        session.s3UploadLog = session.s3UploadLog.filter(item => now - item.timestamp < 10000);

        return true;
    } catch (err) {
        console.warn(`[S3 Upload Error ${session.streamKey}] ${filename}:`, err.message);
        return false;
    }
}

async function processS3Queue(session) {
    if (session.isS3Uploading || !s3Client) return;
    if (session.s3SegmentQueue.size === 0) return;
    session.isS3Uploading = true;

    const segmentsToUpload = Array.from(session.s3SegmentQueue);
    session.s3SegmentQueue.clear();

    await Promise.all(segmentsToUpload.map(filename => uploadSingleSegment(session, filename)));
    session.isS3Uploading = false;

    if (session.s3SegmentQueue.size > 0) {
        processS3Queue(session);
    }
}

function startS3Watcher(session) {
    if (!S3_ENABLED) return;
    if (session.s3Watcher) { session.s3Watcher.close(); session.s3Watcher = null; }
    console.log(`📡 Starting ExCloud S3 Telemetry Watcher for [${session.streamKey}]...`);

    try {
        const files = fs.readdirSync(session.liveDir);
        for (const file of files) {
            if (file.endsWith('.ts')) session.s3SegmentQueue.add(file);
        }
        processS3Queue(session);
    } catch (e) { }

    session.s3Watcher = fs.watch(session.liveDir, (eventType, filename) => {
        if (!filename) return;
        if (filename.endsWith('.ts')) {
            session.s3SegmentQueue.add(filename);
            processS3Queue(session);
        }
    });
}

function stopS3Watcher(session) {
    if (session.s3Watcher) { session.s3Watcher.close(); session.s3Watcher = null; }
    session.s3SegmentQueue.clear();
    session.uploadedS3Segments.clear();
}

async function cleanS3Bucket(session) {
    if (!S3_ENABLED || !s3Client) return;
    try {
        const prefix = `${session.streamKey}/`;
        const listRes = await s3Client.send(new ListObjectsV2Command({ Bucket: S3_BUCKET, Prefix: prefix }));
        if (listRes.Contents && listRes.Contents.length > 0) {
            for (const item of listRes.Contents) {
                await s3Client.send(new DeleteObjectCommand({ Bucket: S3_BUCKET, Key: item.Key }));
            }
            console.log(`🧹 S3 Bucket folder cleaned for [${session.streamKey}]`);
        }
    } catch (err) { console.warn(`[S3 Clean Error ${session.streamKey}]:`, err.message); }
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
    const viewerCount = Array.from(wss.clients).filter(c => c.streamKey === session.streamKey && c.readyState === 1 && c.role === 'viewer').length;
    const telemetry = getSessionS3Telemetry(session);

    const msg = JSON.stringify({
        type: 'STATUS',
        live: session.isLive,
        streamKey: session.streamKey,
        viewers: viewerCount,
        s3Telemetry: telemetry
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
            isLive: s.isLive || fs.existsSync(path.join(s.liveDir, 'master.m3u8')),
            viewerCount: statusClients.length,
            s3Telemetry: getSessionS3Telemetry(s)
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

    ws.on('message', (data) => {
        const session = activeStreams.get(key);
        if (session && session.ffmpegProcess && session.ffmpegProcess.stdin && session.ffmpegProcess.stdin.writable) {
            try {
                const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
                session.ffmpegProcess.stdin.write(buf);
            } catch (e) { }
        }
    });
    ws.on('error', (err) => console.warn(`[Stream WS Error ${key}]:`, err.message));
    ws.on('close', () => console.log(`⚡ Host Disconnected for [${key}]`));
});

// Periodic S3 Upload Telemetry Console & Admin Broadcast Logger
setInterval(() => {
    broadcastAdminTelemetry();
    for (const session of activeStreams.values()) {
        if (session.isLive) {
            const telemetry = getSessionS3Telemetry(session);
            console.log(`[Telemetry ${session.streamKey}] Speed: ${telemetry.chunksPerSec} chunks/sec | 720p: ${telemetry.p720Count} | 480p: ${telemetry.p480Count}`);
        }
    }
}, 1500);

// ─── FFmpeg Live Process Manager ────────────────────────────────────────────────
function stopFfmpegLive(session) {
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
    session.uploadedS3Segments.clear();

    if (S3_ENABLED) {
        await cleanS3Bucket(session);
    }

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
        '[0:v]format=yuv420p,split=2[v720in][v480in];' +
        '[v720in]fps=30,scale=1280:720[v720out];' +
        '[v480in]fps=30,scale=854:480[v480out];' +
        '[0:a][1:a]amix=inputs=2:duration=first:dropout_transition=0:normalize=0,aresample=async=1:first_pts=0,asplit=2[a720][a480]',

        // 720p — Optimized 3Mbps stream for ultra-fast S3 uploads (30fps, GOP 60, zero lookahead)
        '-map', '[v720out]',
        '-c:v:0', 'libx264',
        '-preset', 'ultrafast',
        '-profile:v:0', 'high',
        '-level:v:0', '4.1',
        '-crf:v:0', '25',
        '-maxrate:v:0', '3000k',
        '-bufsize:v:0', '6000k',
        '-g:v:0', '60',
        '-sc_threshold:v:0', '0',
        '-x264-params:v:0', 'no-scenecut=1:open-gop=0:keyint=60:min-keyint=60:rc-lookahead=0:bframes=0',
        '-map', '[a720]',
        '-c:a:0', 'aac',
        '-b:a:0', '192k',

        // 480p — Optimized 1.2Mbps stream (30fps, GOP 60 for 1:1 segment lock-step alignment)
        '-map', '[v480out]',
        '-c:v:1', 'libx264',
        '-preset', 'ultrafast',
        '-profile:v:1', 'baseline',
        '-crf:v:1', '28',
        '-maxrate:v:1', '1200k',
        '-bufsize:v:1', '2400k',
        '-g:v:1', '60',
        '-sc_threshold:v:1', '0',
        '-x264-params:v:1', 'no-scenecut=1:open-gop=0:keyint=60:min-keyint=60:rc-lookahead=0:bframes=0',
        '-map', '[a480]',
        '-c:a:1', 'aac',
        '-b:a:1', '128k',

        '-f', 'hls',
        '-hls_time', '2',
        '-hls_list_size', '30',
        '-hls_flags', 'delete_segments+omit_endlist+independent_segments',
        '-hls_segment_type', 'mpegts',
        '-master_pl_name', 'master.m3u8',
        '-var_stream_map', 'v:0,a:0,name:720p v:1,a:1,name:480p',
        path.join(session.liveDir, 'stream_%v.m3u8')
    ];

    console.log(`⚡ Spawning Live Stream Generator for [${session.streamKey}]...`);
    session.ffmpegProcess = spawn('ffmpeg', args);
    session.isLive = true;

    if (S3_ENABLED) {
        startS3Watcher(session);
    }

    if (session.ffmpegProcess.stdin) {
        session.ffmpegProcess.stdin.on('error', (err) => {});
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
        stopS3Watcher(session);
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
                viewerCount: statusClients.length,
                s3Telemetry: getSessionS3Telemetry(s)
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

// ─── Application Core Routes ─────────────────────────────────────────────────────

// Home Dashboard — Requires Auth
app.get('/', requireAuth, (req, res) => {
    const streamsList = Array.from(activeStreams.values()).map(s => ({
        streamKey: s.streamKey,
        title: s.title,
        isLive: s.isLive || fs.existsSync(path.join(s.liveDir, 'master.m3u8')),
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

// Delete Stream & Purge S3 API
app.post(['/api/streams/delete/:streamKey', '/api/streams/delete'], requireAuth, async (req, res) => {
    try {
        const streamKey = req.params.streamKey || req.body?.streamKey;
        if (!streamKey) return res.status(400).json({ success: false, error: 'Stream key required' });

        const session = activeStreams.get(streamKey);
        if (session) {
            stopFfmpegLive(session);
            stopS3Watcher(session);
            clearLiveFolder(session);
            if (S3_ENABLED) await cleanS3Bucket(session);
            activeStreams.delete(streamKey);
        } else {
            const dummySession = { streamKey, liveDir: path.join(liveDir, streamKey) };
            clearLiveFolder(dummySession);
            if (S3_ENABLED) await cleanS3Bucket(dummySession);
        }

        try {
            await db.delete(streamSessions).where(eq(streamSessions.streamKey, streamKey));
        } catch (e) {}

        broadcastAdminTelemetry();
        console.log(`🗑️ Stream [${streamKey}] deleted and S3 files purged.`);
        res.json({ success: true, message: `Stream [${streamKey}] deleted and S3 storage purged.` });
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

// Dynamic Express HLS Playlist Server — Serves .m3u8 instantly with zero CDN delay
app.get(['/live-playlist/:streamKey/:playlist', '/live-playlist/:playlist'], (req, res) => {
    const streamKey = req.params.streamKey || 'default';
    const playlistName = req.params.playlist || 'master.m3u8';

    const session = activeStreams.get(streamKey);
    if (!session) return res.status(404).send('#EXTM3U\n#EXT-X-ERROR: Stream session not found');

    const playlistPath = path.join(session.liveDir, playlistName);
    if (!fs.existsSync(playlistPath)) return res.status(404).send('#EXTM3U\n#EXT-X-ERROR: Playlist not generated yet');

    try {
        const rawContent = fs.readFileSync(playlistPath, 'utf-8');
        res.setHeader('Content-Type', 'application/vnd.apple.mpegurl');
        res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate, max-age=0');
        res.setHeader('Pragma', 'no-cache');
        res.setHeader('Expires', '0');

        if (playlistName === 'master.m3u8') return res.send(rawContent);

        const s3CdnBase = `${S3_PUBLIC_BASE_URL}/${streamKey}`;
        const lines = rawContent.split(/\r?\n/);
        const rewrittenLines = [];

        for (let i = 0; i < lines.length; i++) {
            const line = lines[i].trim();
            if (line.endsWith('.ts')) {
                if (S3_ENABLED && !session.uploadedS3Segments.has(line)) {
                    if (rewrittenLines.length > 0 && rewrittenLines[rewrittenLines.length - 1].startsWith('#EXTINF:')) {
                        rewrittenLines.pop();
                    }
                    continue;
                }
                const cdnUrl = S3_ENABLED ? `${s3CdnBase}/${line}` : line;
                rewrittenLines.push(cdnUrl);
            } else {
                rewrittenLines.push(line);
            }
        }

        return res.send(rewrittenLines.join('\n'));
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
    const masterExists = session ? fs.existsSync(path.join(session.liveDir, 'master.m3u8')) : false;
    res.json({ live: (session && session.isLive) || masterExists, streamKey: key });
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
        broadcastStatus(session, false);
        stopS3Watcher(session);
        console.log(`🔴 Stream [${key}] stopped.`);

        if (session.cleanupTimer) clearTimeout(session.cleanupTimer);
        session.cleanupTimer = setTimeout(async () => {
            console.log(`🧹 Purging HLS files for [${key}]...`);
            stopFfmpegLive(session);
            clearLiveFolder(session);
            if (S3_ENABLED) await cleanS3Bucket(session);
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
    if (session && session.ffmpegProcess && session.ffmpegProcess.stdin && session.ffmpegProcess.stdin.writable) {
        try {
            const buf = Buffer.isBuffer(req.body) ? req.body : Buffer.from(req.body);
            session.ffmpegProcess.stdin.write(buf);
        } catch (e) { }
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
    if (S3_ENABLED) console.log(`📦 ExCloud S3 Multi-Stream CDN Base: ${S3_PUBLIC_BASE_URL}`);
});
