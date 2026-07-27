import 'dotenv/config';
import express from 'express';
import http from 'http';
import path from 'path';
import fs from 'fs';
import os from 'os';
import { fileURLToPath } from 'url';
import { spawn, spawnSync } from 'child_process';
import { WebSocketServer } from 'ws';
import { EventEmitter } from 'events';
import cookieParser from 'cookie-parser';
import jwt from 'jsonwebtoken';
import bcrypt from 'bcryptjs';
import { db } from './db/index.js';
import { users, streamSessions } from './db/schema.js';
import logger, { getCpuUsage, getSystemInfo } from './logger.js';
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

app.use(express.static(path.join(__dirname, 'public'), {
    setHeaders: (res, path) => {
        if (path.endsWith('.ts')) {
            res.setHeader('Cache-Control', 'public, max-age=3');
            res.setHeader('Accept-Ranges', 'bytes');
        } else if (path.endsWith('.m3u8')) {
            res.setHeader('Cache-Control', 'no-cache');
        }
    }
}));
app.use('/stream', express.raw({ type: '*/*', limit: '100mb' }));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(cookieParser());

// Initialize Database schema and seed default Admin (watch@watch.in / HexWatch78)
initDb().catch(err => logger.error('DB init failed', err));

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
        ffmpegProcesses: { '720p': null, '480p': null },
        disconnectTimer: null,
        oneHourTimer: null,
        liveDir: path.join(liveDir, streamKey),
        ffmpegSpawnedAt: 0,
        createdAt: new Date(),
        chunks720pCount: 0,
        chunks480pCount: 0,
        totalChunksCount: 0,
        failureCount: 0,
        ffmpegRestartBlocked: false,
        hostIngestStats: {
            bytes: 0,
            chunks: 0,
            lastBytes: 0,
            lastChunks: 0,
            lastLoggedAt: 0,
            lastChunkAt: 0
        },
        qualityViewers: { '720p': new Set(), '480p': new Set() },
        initSegments: { '720p': null, '480p': null },
        hostAlive: false
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
                ws.quality = urlObj.searchParams.get('quality') || '720p';
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
    ws.on('error', (err) => logger.warn(`[Status WS Error ${key}]: ${err.message}`));
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

function logHostIngestStats(session, source, chunkBytes) {
    if (!session.hostIngestStats) {
        session.hostIngestStats = { bytes: 0, chunks: 0, lastBytes: 0, lastChunks: 0, lastLoggedAt: 0, lastChunkAt: 0 };
    }

    const now = Date.now();
    const stats = session.hostIngestStats;
    const gapMs = stats.lastChunkAt ? now - stats.lastChunkAt : 0;
    stats.lastChunkAt = now;
    stats.bytes += chunkBytes;
    stats.chunks += 1;

    if (!stats.lastLoggedAt) {
        stats.lastLoggedAt = now;
        stats.lastBytes = stats.bytes;
        stats.lastChunks = stats.chunks;
        return;
    }

    const elapsedSec = (now - stats.lastLoggedAt) / 1000;
    if (elapsedSec < 5) return;

    const bytesDelta = stats.bytes - stats.lastBytes;
    const chunksDelta = stats.chunks - stats.lastChunks;
    const mbps = (bytesDelta * 8) / elapsedSec / 1000000;
    const chunksPerSec = chunksDelta / elapsedSec;

    logger.info(
        `[Host Ingest ${session.streamKey}] Source: ${source} | ${mbps.toFixed(2)} Mbps | ` +
        `${chunksPerSec.toFixed(2)} chunks/s | Last chunk: ${(chunkBytes / 1024).toFixed(1)} KB | Gap: ${gapMs}ms`
    );

    stats.lastLoggedAt = now;
    stats.lastBytes = stats.bytes;
    stats.lastChunks = stats.chunks;
}

function resetHostIngestStats(session) {
    session.hostIngestStats = {
        bytes: 0,
        chunks: 0,
        lastBytes: 0,
        lastChunks: 0,
        lastLoggedAt: 0,
        lastChunkAt: 0
    };
    session._hostChunkLog = [];
}

function tryHandleHostControlMessage(session, data, isBinary) {
    if (isBinary) return false;
    try {
        const raw = typeof data === 'string' ? data : data.toString();
        const parsed = JSON.parse(raw);
        if (parsed.type === 'HOST_MEDIA_SETTINGS') {
            session.hostMediaSettings = parsed;
            logger.info(`[Host Media ${session.streamKey}] ${JSON.stringify({
                mimeType: parsed.mimeType,
                videoBitsPerSecond: parsed.videoBitsPerSecond,
                audioBitsPerSecond: parsed.audioBitsPerSecond,
                trackSettings: parsed.trackSettings
            })}`);
            return true;
        }
        if (parsed.type === 'HOST_CAPTURE_STATS') {
            logger.info(`[Host Capture ${session.streamKey}] ${JSON.stringify({
                measuredFps: parsed.measuredFps,
                totalVideoFrames: parsed.totalVideoFrames,
                droppedVideoFrames: parsed.droppedVideoFrames,
                chunkIndex: parsed.chunkIndex
            })}`);
            return true;
        }
        return false;
    } catch (e) {
        return false;
    }
}

function hasRunningFfmpeg(session) {
    return !!(session.ffmpegProcess && session.ffmpegProcess.stdin && session.ffmpegProcess.stdin.writable);
}

function writeChunkToFfmpeg(session, buf) {
    if (session.ffmpegProcess && session.ffmpegProcess.stdin && session.ffmpegProcess.stdin.writable) {
        try { session.ffmpegProcess.stdin.write(buf); } catch (e) { }
    }
}

streamWss.on('connection', (ws) => {
    const key = ws.streamKey || 'default';
    logger.info(`Host connected to WebSocket Ingest for [${key}]`);

    const session = activeStreams.get(key);
    if (session) {
        session.hostAlive = true;
        if (session.disconnectTimer) {
            logger.info(`Host reconnected to WebSocket Ingest for [${key}]! Cancelled 90-second grace timer.`);
            clearTimeout(session.disconnectTimer);
            session.disconnectTimer = null;
        }
        session.isLive = true;
        broadcastStatus(session, true);
        broadcastAdminTelemetry();
    }

    ws.on('message', async (data, isBinary) => {
        const session = activeStreams.get(key);
        if (session) {
            if (tryHandleHostControlMessage(session, data, isBinary)) return;

            if (session.disconnectTimer) {
                clearTimeout(session.disconnectTimer);
                session.disconnectTimer = null;
            }

            const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
            logHostIngestStats(session, 'ws', buf.length);

            if (!hasRunningFfmpeg(session) && !session.ffmpegRestartBlocked) {
                try {
                    await startFfmpegLive(session);
                    broadcastStatus(session, true);
                } catch (e) {
                    logger.error(`Failed to start FFmpeg on first chunk for [${key}]: ${e.message}`);
                }
            }

            writeChunkToFfmpeg(session, buf);

            // Track host chunk rate for slow upload detection
            if (!session._hostChunkLog) session._hostChunkLog = [];
            session._hostChunkLog.push(Date.now());
            // Keep only last 10 seconds of chunk timestamps
            const cutoff = Date.now() - 10000;
            session._hostChunkLog = session._hostChunkLog.filter(t => t > cutoff);
        }
    });

    ws.on('error', (err) => {
        logger.warn(`[Stream WS Error ${key}]: ${err.message}`);
        const session = activeStreams.get(key);
        if (session) session.failureCount = (session.failureCount || 0) + 1;
    });
    ws.on('close', () => {
        logger.info(`Host WebSocket disconnected for [${key}]. Keeping FFmpeg process alive for 90s reconnection window...`);
        const session = activeStreams.get(key);
        if (session) {
            session.hostAlive = false;
        }
        if (session && session.isLive) {
            if (session.disconnectTimer) clearTimeout(session.disconnectTimer);
            session.disconnectTimer = setTimeout(() => {
                logger.info(`90-second reconnection window expired for [${key}]. Stopping FFmpeg process...`);
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
    return (session.qualityViewers['720p']?.size || 0) +
        (session.qualityViewers['720p']?.size || 0) +
        (session.qualityViewers['480p']?.size || 0);
}

viewWss.on('connection', (ws) => {
    const key = ws.streamKey || 'default';
    const quality = ws.quality || '720p';
    const session = activeStreams.get(key);
    if (!session) {
        ws.close(1000, 'Stream not found');
        return;
    }

    if (!session.qualityViewers[quality]) session.qualityViewers[quality] = new Set();
    session.qualityViewers[quality].add(ws);
        logger.info(`Viewer connected to [${key}] ${quality} (${getTotalViewerCount(session)} total)`);

    if (session.initSegments[quality]) {
        try { ws.send(session.initSegments[quality]); } catch (e) { }
    }

    broadcastStatus(session, session.isLive);

    ws.on('message', (msg) => {
        try {
            const parsed = JSON.parse(msg);
            if (parsed.type === 'CHANGE_QUALITY' && parsed.quality) {
                const oldQ = ws.currentQuality || quality;
                const newQ = parsed.quality;
                if (oldQ !== newQ) {
                    if (session.qualityViewers[oldQ]) session.qualityViewers[oldQ].delete(ws);
                    if (!session.qualityViewers[newQ]) session.qualityViewers[newQ] = new Set();
                    session.qualityViewers[newQ].add(ws);
                    ws.currentQuality = newQ;
                    logger.info(`Viewer switched quality: [${key}] ${oldQ} -> ${newQ}`);
                    if (session.initSegments[newQ]) {
                        try { ws.send(session.initSegments[newQ]); } catch (e) { }
                    }
                }
            }
        } catch (e) { }
    });

    ws.on('close', () => {
        const currentQ = ws.currentQuality || quality;
        if (session.qualityViewers[currentQ]) session.qualityViewers[currentQ].delete(ws);
        logger.info(`Viewer disconnected from [${key}] ${currentQ} (${getTotalViewerCount(session)} total)`);
        broadcastStatus(session, session.isLive);
    });
    ws.on('error', (err) => logger.warn(`[Viewer WS Error ${key}]: ${err.message}`));
});

setInterval(() => {
    broadcastAdminTelemetry();
    broadcastHostHealth();
}, 2000);

function broadcastHostHealth() {
    for (const session of activeStreams.values()) {
        if (!session.isLive) continue;
        const chunkLog = session._hostChunkLog || [];
        const recentChunks = chunkLog.filter(t => t > Date.now() - 5000);
        const chunksPerSec = recentChunks.length / 5;
        let health = 'good';
        if (chunksPerSec < 0.5) health = 'poor';
        else if (chunksPerSec < 1.5) health = 'slow';

        const msg = JSON.stringify({ type: 'HOST_HEALTH', health, chunksPerSec: Math.round(chunksPerSec) });
        // Broadcast to all status-ws viewers watching this stream
        for (const client of wss.clients) {
            if (client.streamKey === session.streamKey && client.readyState === 1 && client.role === 'viewer') {
                try { client.send(msg); } catch (e) { }
            }
        }
    }
}

// ─── FFmpeg Live Process Manager ────────────────────────────────────────────────
function stopFfmpegLive(session) {
    if (session.disconnectTimer) {
        clearTimeout(session.disconnectTimer);
        session.disconnectTimer = null;
    }
    if (session.ffmpegProcesses) {
        for (const quality of Object.keys(session.ffmpegProcesses)) {
            const proc = session.ffmpegProcesses[quality];
            if (!proc) continue;
            try {
                if (proc.stdin) {
                    proc.stdin.removeAllListeners('error');
                    proc.stdin.on('error', () => { });
                    proc.stdin.end();
                }
                proc.kill('SIGKILL');
            } catch (err) { }
            session.ffmpegProcesses[quality] = null;
        }
        session.ffmpegProcess = null;
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
    session.hostAlive = false;
    session.ffmpegSpawnedAt = 0;
    if (session.oneHourTimer) { clearTimeout(session.oneHourTimer); session.oneHourTimer = null; }
}

function commandSucceeds(command, args = []) {
    try {
        const result = spawnSync(command, args, { stdio: 'ignore', timeout: 3000 });
        return result.status === 0;
    } catch (e) {
        return false;
    }
}

function detectH264Encoder() {
    const forced = (process.env.FFMPEG_H264_ENCODER || '').toLowerCase().trim();
    if (['libx264', 'h264_nvenc', 'h264_vaapi'].includes(forced)) {
        return { name: forced, reason: 'forced by FFMPEG_H264_ENCODER' };
    }

    const encoders = spawnSync('ffmpeg', ['-hide_banner', '-encoders'], { encoding: 'utf8', timeout: 5000 });
    const list = `${encoders.stdout || ''}\n${encoders.stderr || ''}`;

    if (list.includes('h264_nvenc') && commandSucceeds('nvidia-smi')) {
        return { name: 'h264_nvenc', reason: 'NVIDIA NVENC available' };
    }

    if (list.includes('h264_vaapi') && fs.existsSync('/dev/dri/renderD128')) {
        return { name: 'h264_vaapi', reason: 'VAAPI render device available' };
    }

    return { name: 'libx264', reason: 'no usable hardware H.264 encoder detected' };
}

const H264_ENCODER = detectH264Encoder();

function writeMasterPlaylist(session) {
    const master = [
        '#EXTM3U',
        '#EXT-X-VERSION:3',
        '#EXT-X-STREAM-INF:BANDWIDTH=6128000,AVERAGE-BANDWIDTH=6128000,RESOLUTION=1280x720,FRAME-RATE=30.000,CODECS="avc1.4d401f,mp4a.40.2"',
        'stream_720p.m3u8',
        '#EXT-X-STREAM-INF:BANDWIDTH=2096000,AVERAGE-BANDWIDTH=2096000,RESOLUTION=854x480,FRAME-RATE=24.000,CODECS="avc1.4d401e,mp4a.40.2"',
        'stream_480p.m3u8',
        ''
    ].join('\n');
    fs.writeFileSync(path.join(session.liveDir, 'master.m3u8'), master);
}

function getEncoderArgs(quality, bitrate, bufsize) {
    if (H264_ENCODER.name === 'h264_nvenc') {
        return [
            '-c:v', 'h264_nvenc', '-preset', 'p1', '-tune', 'll', '-rc', 'cbr',
            '-b:v', bitrate, '-maxrate', bitrate, '-bufsize', bufsize,
            '-profile:v', 'main', '-pix_fmt', 'yuv420p'
        ];
    }

    if (H264_ENCODER.name === 'h264_vaapi') {
        return [
            '-vaapi_device', '/dev/dri/renderD128',
            '-c:v', 'h264_vaapi',
            '-b:v', bitrate, '-maxrate', bitrate, '-bufsize', bufsize,
            '-profile:v', '578'
        ];
    }

    const keyint = quality === '720p' ? '30' : '24';
    return [
        '-c:v', 'libx264', '-threads:v', '0', '-preset', 'ultrafast', '-tune', 'zerolatency',
        '-profile:v', 'main', '-pix_fmt', 'yuv420p',
        '-b:v', bitrate, '-maxrate', bitrate, '-bufsize', bufsize,
        '-x264-params', `keyint=${keyint}:min-keyint=${keyint}:scenecut=0:bframes=0:rc-lookahead=0:ref=1:me=dia:subme=0:trellis=0:mixed-refs=0:8x8dct=0:weightb=0:b-adapt=0:direct=none:no-mbtree=1:force-cfr=1:aq-mode=0:partitions=none:no-deblock=1`
    ];
}

function getEncoderArgsForStream(quality, streamIndex, bitrate, bufsize) {
    if (H264_ENCODER.name === 'h264_nvenc') {
        return [
            `-c:v:${streamIndex}`, 'h264_nvenc', `-preset:v:${streamIndex}`, 'p1',
            `-tune:v:${streamIndex}`, 'll', `-rc:v:${streamIndex}`, 'cbr',
            `-b:v:${streamIndex}`, bitrate, `-maxrate:v:${streamIndex}`, bitrate, `-bufsize:v:${streamIndex}`, bufsize,
            `-profile:v:${streamIndex}`, 'main', `-pix_fmt:v:${streamIndex}`, 'yuv420p'
        ];
    }

    if (H264_ENCODER.name === 'h264_vaapi') {
        return [
            `-c:v:${streamIndex}`, 'h264_vaapi',
            `-b:v:${streamIndex}`, bitrate, `-maxrate:v:${streamIndex}`, bitrate, `-bufsize:v:${streamIndex}`, bufsize,
            `-profile:v:${streamIndex}`, '578'
        ];
    }

    const keyint = quality === '720p' ? '30' : '24';
    return [
        `-c:v:${streamIndex}`, 'libx264', `-threads:v:${streamIndex}`, '0',
        `-preset:v:${streamIndex}`, 'ultrafast', `-tune:v:${streamIndex}`, 'zerolatency',
        `-profile:v:${streamIndex}`, 'main', `-pix_fmt:v:${streamIndex}`, 'yuv420p',
        `-b:v:${streamIndex}`, bitrate, `-maxrate:v:${streamIndex}`, bitrate, `-bufsize:v:${streamIndex}`, bufsize,
        `-x264-params:v:${streamIndex}`, `keyint=${keyint}:min-keyint=${keyint}:scenecut=0:bframes=0:rc-lookahead=0:ref=1:me=dia:subme=0:trellis=0:mixed-refs=0:8x8dct=0:weightb=0:b-adapt=0:direct=none:no-mbtree=1:force-cfr=1:aq-mode=0:partitions=none:no-deblock=1`
    ];
}

function buildFfmpegArgs(session) {
    const hlsDir = session.liveDir.replace(/\\/g, '/');
    const v720Filter = H264_ENCODER.name === 'h264_vaapi'
        ? 'fps=30,format=nv12,hwupload[v720]'
        : 'fps=30[v720]';
    const v480Filter = H264_ENCODER.name === 'h264_vaapi'
        ? 'scale=854:480:flags=fast_bilinear,fps=24,format=nv12,hwupload[v480]'
        : 'scale=854:480:flags=fast_bilinear,fps=24[v480]';
    const filterComplex = `[0:v]split=2[v720src][v480src];[v720src]${v720Filter};[v480src]${v480Filter};[0:a][1:a]amix=inputs=2:duration=first:dropout_transition=0:normalize=0,aresample=async=1:first_pts=0,asplit=2[a720][a480]`;

    return [
        '-y',
        ...(H264_ENCODER.name === 'h264_vaapi' ? ['-vaapi_device', '/dev/dri/renderD128'] : []),
        '-fflags', '+genpts+discardcorrupt',
        '-probesize', '2M',
        '-analyzeduration', '1000000',
        '-thread_queue_size', '1024',
        '-i', 'pipe:0',
        '-f', 'lavfi',
        '-i', 'anullsrc=channel_layout=stereo:sample_rate=48000',
        '-filter_complex', filterComplex,

        '-map', '[v720]', '-map', '[a720]',
        '-r:v:0', '30', '-g:v:0', '30', '-keyint_min:v:0', '30', '-sc_threshold:v:0', '0',
        ...getEncoderArgsForStream('720p', 0, '6000k', '12000k'),
        '-c:a:0', 'aac', '-b:a:0', '128k', '-ar:a:0', '48000', '-ac:a:0', '2',

        '-map', '[v480]', '-map', '[a480]',
        '-r:v:1', '24', '-g:v:1', '24', '-keyint_min:v:1', '24', '-sc_threshold:v:1', '0',
        ...getEncoderArgsForStream('480p', 1, '2000k', '4000k'),
        '-c:a:1', 'aac', '-b:a:1', '96k', '-ar:a:1', '48000', '-ac:a:1', '2',

        '-f', 'hls',
        '-hls_time', '1',
        '-hls_list_size', '20',
        '-hls_flags', 'delete_segments+independent_segments',
        '-hls_segment_filename', `${hlsDir}/stream_%v%d.ts`,
        '-master_pl_name', 'master.m3u8',
        '-var_stream_map', 'v:0,a:0,name:720p v:1,a:1,name:480p',
        `${hlsDir}/stream_%v.m3u8`
    ];
}

function attachFfmpegLogging(session, quality, proc) {
    if (proc.stdin) proc.stdin.on('error', () => { });

    if (proc.stderr) {
        let lastLoggedTime = 0;
        proc.stderr.on('data', (data) => {
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
                        const fps = fpsMatch ? parseFloat(fpsMatch[1]).toFixed(1) : '0.0';
                        const speed = speedMatch ? speedMatch[1] : '1.0x';
                        const cpu = getCpuUsage();
                        const load = os.loadavg().map(l => l.toFixed(2)).join(' ');
                        logger.info(`[FFmpeg ${session.streamKey} ${quality}] Encoder: ${H264_ENCODER.name} | Speed: ${speed} | FPS: ${fps} | CPU: ${cpu}% | Load: ${load}`);
                    }
                } else if (msg.includes('Error') || msg.includes('Invalid') || msg.includes('failed')) {
                    logger.error(`[FFmpeg Error ${session.streamKey} ${quality}]: ${msg}`);
                }
            }
        });
    }

    proc.on('exit', (code, signal) => {
        logger.warn(`FFmpeg ${quality} exited for [${session.streamKey}] (code=${code}, signal=${signal}). Host WS alive: ${session.hostAlive}`);
        if (session.ffmpegProcesses) session.ffmpegProcesses[quality] = null;
        const anyRunning = session.ffmpegProcesses && Object.values(session.ffmpegProcesses).some(Boolean);
        session.ffmpegProcess = anyRunning ? session.ffmpegProcess : null;
        session.ffmpegRestartBlocked = true;
        logger.warn(`FFmpeg restart blocked for [${session.streamKey}] until stream reset; current MediaRecorder chunks may not include a fresh container header.`);
        if (!session.hostAlive && !anyRunning) session.isLive = false;
    });
}

async function startFfmpegLive(session, opts = {}) {
    if (session.cleanupTimer) { clearTimeout(session.cleanupTimer); session.cleanupTimer = null; }

    stopFfmpegLive(session);
    clearLiveFolder(session);
    session.ffmpegRestartBlocked = false;
    resetHostIngestStats(session);

    // 1-Hour Stream Limit Timer
    session.countedAgainstLimit = false;
    session.oneHourTimer = setTimeout(async () => {
        logger.info(`Stream [${session.streamKey}] reached 1 hour duration. Incrementing stream count for host ${session.hostId}...`);
        if (session.hostId && !session.countedAgainstLimit) {
            session.countedAgainstLimit = true;
            try {
                await db.update(users).set({ streamCount: sql`${users.streamCount} + 1` }).where(eq(users.id, session.hostId));
            } catch (e) { logger.error('Error updating user streamCount', e); }
        }
    }, 3600 * 1000);

    logger.info(`Spawning optimized shared Live Stream Generator for [${session.streamKey}] with ${H264_ENCODER.name} (${H264_ENCODER.reason}, transcode-720p-480p)...`, { sys: getSystemInfo() });
    session.ffmpegProcesses = { '720p': null, '480p': null };
    session.ffmpegProcess = spawn('ffmpeg', buildFfmpegArgs(session), { stdio: ['pipe', 'pipe', 'pipe'] });
    session.isLive = true;
    session.ffmpegSpawnedAt = Date.now();

    attachFfmpegLogging(session, '720p+480p', session.ffmpegProcess);
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

app.post('/api/admin/users/limit-adjust', requireAdmin, async (req, res) => {
    try {
        const { userId, delta } = req.body;
        const d = parseInt(delta) || 0;
        await db.update(users).set({ streamLimit: sql`GREATEST(1, ${users.streamLimit} + ${d})` }).where(eq(users.id, userId));
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
                chunks720p,
                chunks480p,
                failureCount,
                stabilityRating,
                sessions
            };
        });

        res.render('users', { usersList: usersListWithTelemetry, currentUser: req.user });
    } catch (err) {
        logger.error('User telemetry route error', err);
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

        broadcastAdminTelemetry();
        logger.info(`Stream [${streamKey}] deleted.`);
        res.json({ success: true, message: `Stream [${streamKey}] deleted.` });
    } catch (err) {
        logger.error('Delete stream error', err);
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
    try {
        const stat = fs.statSync(filePath);
        res.writeHead(200, {
            'Content-Type': 'video/mp2t',
            'Content-Length': stat.size,
            'Cache-Control': 'public, max-age=3',
            'Access-Control-Allow-Origin': '*',
            'Connection': 'keep-alive'
        });
        fs.createReadStream(filePath).pipe(res);
    } catch (err) {
        if (!res.headersSent) return res.status(404).send('File not found');
    }
});

// Dynamic Express HLS Playlist & Segment Server — Serves .m3u8 and .ts instantly
app.get(['/live-playlist/:streamKey/:playlist', '/live-playlist/:playlist'], (req, res) => {
    const streamKey = req.params.streamKey || 'default';
    const playlistName = req.params.playlist || 'master.m3u8';

    const session = activeStreams.get(streamKey);
    if (!session) return res.status(404).send('#EXTM3U\n#EXT-X-ERROR: Stream session not found');

    const filePath = path.join(session.liveDir, playlistName);

    try {
        res.setHeader('Access-Control-Allow-Origin', '*');
        res.setHeader('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Range');
        res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS, HEAD');

        if (playlistName.endsWith('.ts')) {
            const stat = fs.statSync(filePath);
            res.writeHead(200, {
                'Content-Type': 'video/mp2t',
                'Content-Length': stat.size,
                'Cache-Control': 'public, max-age=3',
                'Connection': 'keep-alive'
            });
            fs.createReadStream(filePath).pipe(res);
        } else {
            res.setHeader('Content-Type', 'application/vnd.apple.mpegurl');
            res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate, max-age=0');
            res.setHeader('Pragma', 'no-cache');
            res.setHeader('Expires', '0');
            const rawContent = fs.readFileSync(filePath, 'utf-8');
            return res.send(rawContent);
        }
    } catch (err) {
        if (!res.headersSent) return res.status(404).send('File not found');
    }
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
    const hostConnected = session && Array.from(streamWss.clients).some(c => c.streamKey === key && c.readyState === 1);
    res.json({ live: (session && (session.isLive || hostConnected)), streamKey: key });
});

// Reset & Start Stream Process
app.post(['/reset-stream', '/reset-stream/:streamKey'], async (req, res) => {
    const key = req.params.streamKey || req.body?.streamKey || 'default';
    const user = getAuthUser(req);
    const session = getOrCreateStreamSession(key, 'Live Stream', user);

    try {
        stopFfmpegLive(session);
        clearLiveFolder(session);
        session.ffmpegRestartBlocked = false;
        resetHostIngestStats(session);
        broadcastStatus(session, false);
        res.json({ success: true, message: `Stream ${key} reset`, streamKey: key });
    } catch (err) {
        logger.error(`Error resetting stream ${key}`, err);
        res.status(500).json({ success: false, error: err.message });
    }
});

// Stop Stream Process
app.post(['/stop-stream', '/stop-stream/:streamKey'], async (req, res) => {
    const key = req.params.streamKey || req.body?.streamKey || 'default';
    const session = activeStreams.get(key);
    if (!session) return res.json({ success: true });

    // Debounce: ignore stop-stream if FFmpeg was spawned less than 1 second ago
    // This prevents a stale stop-stream from a refreshing page from killing a newly spawned FFmpeg
    if (session.ffmpegSpawnedAt && (Date.now() - session.ffmpegSpawnedAt) < 1000) {
        logger.info(`Ignoring stale /stop-stream for [${key}] — FFmpeg spawned ${Date.now() - session.ffmpegSpawnedAt}ms ago`);
        return res.json({ success: true, message: `Stream [${key}] stop ignored (recent spawn).` });
    }

    try {
        stopFfmpegLive(session);
        broadcastStatus(session, false);
        broadcastAdminTelemetry();
        logger.info(`Stream [${key}] stopped and FFmpeg process killed immediately.`);

        if (session.cleanupTimer) clearTimeout(session.cleanupTimer);
        session.cleanupTimer = setTimeout(async () => {
            logger.info(`Purging local HLS files for [${key}]...`);
            clearLiveFolder(session);
        }, 10 * 60 * 1000);

        res.json({ success: true, message: `Stream [${key}] stopped.` });
    } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

// Binary Chunk Stream HTTP Fallback
app.post(['/stream', '/stream/:streamKey'], async (req, res) => {
    const key = req.params.streamKey || req.headers['x-stream-key'] || 'default';
    const chunkIndex = parseInt(req.headers['x-chunk-index'] || '0');

    res.status(200).json({ success: true, chunkIndex });

    const session = activeStreams.get(key);
    if (session) {
        if (session.disconnectTimer) {
                logger.info(`Incoming HTTP chunk stream for [${key}] during network drop! Refreshing 90-second grace timer.`);
            clearTimeout(session.disconnectTimer);
            session.disconnectTimer = setTimeout(() => {
                logger.info(`90-second reconnection window expired for [${key}]. Stopping FFmpeg process...`);
                stopFfmpegLive(session);
                broadcastStatus(session, false);
                broadcastAdminTelemetry();
                session.disconnectTimer = null;
            }, 90 * 1000);
        }
        if (!hasRunningFfmpeg(session) && !session.ffmpegRestartBlocked) {
            try {
                await startFfmpegLive(session);
                broadcastStatus(session, true);
            } catch (e) {
                logger.error(`Failed to start FFmpeg on HTTP chunk for [${key}]: ${e.message}`);
            }
        }
        if (hasRunningFfmpeg(session)) {
            const buf = Buffer.isBuffer(req.body) ? req.body : Buffer.from(req.body);
            logHostIngestStats(session, 'http', buf.length);
            writeChunkToFfmpeg(session, buf);
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
    logger.info(`CoWatch Multi-Stream Platform running at ${APP_URL} (Port ${PORT})`, { sys: getSystemInfo() });
});
