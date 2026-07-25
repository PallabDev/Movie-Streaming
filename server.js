import 'dotenv/config';
import express from 'express';
import http from 'http';
import path from 'path';
import fs from 'fs';
import os from 'os';
import { fileURLToPath } from 'url';
import { spawn } from 'child_process';
import { WebSocketServer } from 'ws';
import SftpClient from 'ssh2-sftp-client';
import { S3Client, PutObjectCommand, DeleteObjectCommand, ListObjectsV2Command } from '@aws-sdk/client-s3';
import { EventEmitter } from 'events';

// Increase default max listeners to prevent SFTP event listener warnings
EventEmitter.defaultMaxListeners = 50;

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const server = http.createServer(app);

const PORT = process.env.PORT || 3000;
const NODE_ENV = process.env.NODE_ENV || 'development';
const APP_URL = process.env.APP_URL || `http://localhost:${PORT}`;
const IS_PROD = NODE_ENV === 'production';

// SFTP CDN Configuration
const SFTP_HOST = process.env.SFTP_HOST;
const SFTP_PORT = parseInt(process.env.SFTP_PORT || '22');
const SFTP_USER = process.env.SFTP_USER;
const SFTP_PASSWORD = process.env.SFTP_PASSWORD;
const SFTP_BASE_PATH = process.env.SFTP_BASE_PATH;
const SFTP_ENABLED = !!(SFTP_HOST && SFTP_USER && SFTP_PASSWORD && SFTP_BASE_PATH);

// ExCloud S3 Object Storage CDN Configuration
const S3_ENDPOINT = process.env.S3_ENDPOINT || 'https://buckets.excloud.dev';
const S3_REGION = process.env.S3_REGION || 'default';
const S3_BUCKET = process.env.S3_BUCKET || 'live';
const S3_ACCESS_KEY = process.env.S3_ACCESS_KEY || 'EXCNLC2REYVL5FSFT57AQRKPP24TE';
const S3_SECRET_KEY = process.env.S3_SECRET_KEY || 'DiVv1AV3CJI/Y58jkiDzqCyUI9osj3NXS1xXxCA3';
const S3_PUBLIC_BASE_URL = process.env.S3_PUBLIC_BASE_URL || 'https://1834.objects.excloud.dev/public/live';
const HLS_CDN_URL = process.env.HLS_CDN_URL || S3_PUBLIC_BASE_URL;
const S3_ENABLED = !!(S3_ACCESS_KEY && S3_SECRET_KEY);

// Directories setup
const liveDir = path.join(__dirname, 'public', 'live');

if (!fs.existsSync(liveDir)) {
    fs.mkdirSync(liveDir, { recursive: true });
}

// Configure EJS view engine
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));

// CORS Middleware for Express server (Allows cross-domain playback from anywhere)
app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS, HEAD');
    res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Range');
    if (req.method === 'OPTIONS') {
        return res.sendStatus(200);
    }
    next();
});

// File-based HLS Playlist Delivery with Keep-Alive headers
app.use(express.static(path.join(__dirname, 'public')));
app.use('/live', express.static(liveDir, {
    maxAge: '1h',
    setHeaders: (res) => {
        res.setHeader('Access-Control-Allow-Origin', '*');
        res.setHeader('Connection', 'keep-alive');
        res.setHeader('Keep-Alive', 'timeout=120, max=2000');
    }
}));

// Middleware for parsing raw video/binary stream data up to 100MB per chunk
app.use('/stream', express.raw({ type: '*/*', limit: '100mb' }));
app.use(express.json());

// Live Status & Cleanups
let isLiveStreamActive = false;
let cleanupTimer = null;
let cpuMonitorInterval = null;
const CLEANUP_DELAY_MS = 10 * 60 * 1000;

// ─── SFTP CDN Sync Manager ─────────────────────────────────────────────────────
let sftp = null;
let sftpConnected = false;
let sftpWatcher = null;
const uploadQueue = new Map();

async function ensureSftpConnected() {
    if (sftpConnected && sftp) return true;
    try {
        sftp = new SftpClient();
        if (sftp.client) {
            sftp.client.setMaxListeners(100);
        }
        await sftp.connect({
            host: SFTP_HOST,
            port: SFTP_PORT,
            username: SFTP_USER,
            password: SFTP_PASSWORD,
            readyTimeout: 10000,
            retries: 3
        });
        sftpConnected = true;
        console.log(`📡 SFTP connected to ${SFTP_HOST}:${SFTP_PORT}`);
        return true;
    } catch (err) {
        console.warn(`[SFTP] Connection failed: ${err.message}`);
        sftpConnected = false;
        sftp = null;
        return false;
    }
}

async function disconnectSftp() {
    if (sftp) {
        try { await sftp.end(); } catch (e) { }
        sftp = null;
        sftpConnected = false;
    }
}

async function cleanSftpFolder() {
    if (!SFTP_ENABLED) return;
    try {
        const connected = await ensureSftpConnected();
        if (!connected) return;

        const exists = await sftp.exists(SFTP_BASE_PATH);
        if (exists) {
            const files = await sftp.list(SFTP_BASE_PATH);
            for (const file of files) {
                if (file.name.endsWith('.ts') || file.name.endsWith('.m3u8')) {
                    try {
                        await sftp.delete(`${SFTP_BASE_PATH}/${file.name}`);
                    } catch (e) { }
                }
            }
        } else {
            await sftp.mkdir(SFTP_BASE_PATH, true);
        }

        // Upload .htaccess with robust CORS 'always set' headers for Hostinger CDN
        const htaccess = `# Allow all origins for HLS video streaming
<IfModule mod_headers.c>
    Header always set Access-Control-Allow-Origin "*"
    Header always set Access-Control-Allow-Methods "GET, HEAD, OPTIONS"
    Header always set Access-Control-Allow-Headers "Range, Origin, Content-Type, Accept"
    Header always set Access-Control-Expose-Headers "Content-Length, Content-Range"
</IfModule>

# Correct MIME types for HLS
<IfModule mod_mime.c>
    AddType application/vnd.apple.mpegurl .m3u8
    AddType video/mp2t .ts
</IfModule>
`;
        await sftp.put(Buffer.from(htaccess), `${SFTP_BASE_PATH}/.htaccess`);
        console.log('🧹 SFTP CDN folder cleaned + .htaccess CORS (Header always set) uploaded');
    } catch (err) {
        console.warn('[SFTP] Clean folder error:', err.message);
    }
}

function queueSftpUpload(filename) {
    if (!SFTP_ENABLED) return;
    if (filename.endsWith('.tmp')) return;

    if (uploadQueue.has(filename)) {
        clearTimeout(uploadQueue.get(filename));
    }

    // Delay .m3u8 upload by 80ms to guarantee .ts segment files arrive on Hostinger CDN first
    const delay = filename.endsWith('.m3u8') ? 80 : 10;

    uploadQueue.set(filename, setTimeout(async () => {
        uploadQueue.delete(filename);
        const localPath = path.join(liveDir, filename);
        const remotePath = `${SFTP_BASE_PATH}/${filename}`;

        try {
            const connected = await ensureSftpConnected();
            if (!connected) return;

            if (fs.existsSync(localPath)) {
                const data = fs.readFileSync(localPath);
                await sftp.put(Buffer.from(data), remotePath);
            } else {
                try { await sftp.delete(remotePath); } catch (e) { }
            }
        } catch (err) {
            if (err.message && err.message.includes('No SFTP')) {
                sftpConnected = false;
                sftp = null;
            }
            console.warn(`[SFTP] Sync error (${filename}): ${err.message}`);
        }
    }, 50));
}

function startSftpWatcher() {
    if (!SFTP_ENABLED) return;
    if (sftpWatcher) { sftpWatcher.close(); sftpWatcher = null; }

    sftpWatcher = fs.watch(liveDir, (eventType, filename) => {
        if (!filename) return;
        if (filename.endsWith('.ts') || filename.endsWith('.m3u8')) {
            queueSftpUpload(filename);
        }
    });

    sftpWatcher.on('error', (err) => {
        console.warn('[SFTP Watcher] Error:', err.message);
    });

    console.log('👁️ SFTP file watcher started on liveDir');
}

function stopSftpWatcher() {
    if (sftpWatcher) {
        sftpWatcher.close();
        sftpWatcher = null;
    }
    for (const timer of uploadQueue.values()) {
        clearTimeout(timer);
    }
    uploadQueue.clear();
}

// ─── Multi-Stream Session Manager ──────────────────────────────────────────────
const activeStreams = new Map();

function getOrCreateStreamSession(streamKey, title = 'Live Stream') {
    if (!streamKey || streamKey === 'undefined') streamKey = 'default';
    if (activeStreams.has(streamKey)) {
        const session = activeStreams.get(streamKey);
        if (title && title !== 'Live Stream') session.title = title;
        return session;
    }

    const streamLiveDir = path.join(liveDir, streamKey);
    if (!fs.existsSync(streamLiveDir)) {
        fs.mkdirSync(streamLiveDir, { recursive: true });
    }

    const session = {
        streamKey,
        title,
        createdAt: new Date(),
        isLive: false,
        liveDir: streamLiveDir,
        ffmpegProcess: null,
        s3Watcher: null,
        s3SegmentQueue: new Set(),
        s3PlaylistQueue: new Set(),
        uploadedS3Segments: new Set(),
        isS3Uploading: false,
        cleanupTimer: null
    };

    activeStreams.set(streamKey, session);
    return session;
}

// Initialize default stream on server startup
getOrCreateStreamSession('default', 'Main Live Channel');

// ─── ExCloud S3 Bucket Sync Manager ─────────────────────────────────────────────
let s3Client = null;
if (S3_ENABLED) {
    s3Client = new S3Client({
        endpoint: S3_ENDPOINT,
        region: S3_REGION,
        credentials: {
            accessKeyId: S3_ACCESS_KEY,
            secretAccessKey: S3_SECRET_KEY
        },
        forcePathStyle: true
    });
    console.log(`📦 ExCloud S3 Bucket Storage Enabled (Endpoint: ${S3_ENDPOINT}, Bucket: ${S3_BUCKET})`);
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
        console.log(`⚡ S3 Segment Synced [${session.streamKey}]: ${filename}`);
        return true;
    } catch (err) {
        console.warn(`[S3 Segment Error ${session.streamKey}] ${filename}:`, err.message);
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
    console.log(`📡 Starting ExCloud S3 File Sync Watcher for [${session.streamKey}]...`);

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
    if (session.s3Watcher) {
        session.s3Watcher.close();
        session.s3Watcher = null;
    }
    session.s3SegmentQueue.clear();
    session.s3PlaylistQueue.clear();
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
            console.log(`🧹 ExCloud S3 Bucket folder cleaned for [${session.streamKey}]`);
        }
    } catch (err) {
        console.warn(`[S3 Clean Error ${session.streamKey}]:`, err.message);
    }
}

function clearLiveFolder(session) {
    if (fs.existsSync(session.liveDir)) {
        const files = fs.readdirSync(session.liveDir);
        for (const file of files) {
            try { fs.unlinkSync(path.join(session.liveDir, file)); } catch (err) { }
        }
    }
}

// ─── WebSocket Servers (Status & High-Speed Stream Ingest) ─────────────────────
const wss = new WebSocketServer({ noServer: true });
const streamWss = new WebSocketServer({ noServer: true });

server.on('upgrade', (request, socket, head) => {
    try {
        const urlObj = new URL(request.url, `http://${request.headers.host || 'localhost'}`);
        const pathname = urlObj.pathname;
        const streamKey = urlObj.searchParams.get('key') || 'default';

        if (pathname === '/status-ws') {
            wss.handleUpgrade(request, socket, head, (ws) => {
                ws.streamKey = streamKey;
                wss.emit('connection', ws, request);
            });
        } else if (pathname === '/stream-ws') {
            streamWss.handleUpgrade(request, socket, head, (ws) => {
                ws.streamKey = streamKey;
                streamWss.emit('connection', ws, request);
            });
        } else {
            socket.destroy();
        }
    } catch (err) {
        socket.destroy();
    }
});

wss.on('connection', (ws) => {
    const key = ws.streamKey || 'default';
    const session = activeStreams.get(key);
    ws.send(JSON.stringify({ type: 'STATUS', live: !!(session && session.isLive), streamKey: key }));
    ws.on('error', (err) => console.warn(`[Status WS Error ${key}]:`, err.message));
});

function broadcastStatus(session, liveState) {
    session.isLive = liveState;
    const msg = JSON.stringify({ type: 'STATUS', live: session.isLive, streamKey: session.streamKey });
    for (const client of wss.clients) {
        if (client.streamKey === session.streamKey && client.readyState === 1) {
            client.send(msg);
        }
    }
}

streamWss.on('connection', (ws) => {
    const key = ws.streamKey || 'default';
    console.log(`⚡ Host connected to High-Speed WebSocket Stream Ingest for [${key}]`);
    
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
    ws.on('close', () => console.log(`⚡ Host WebSocket Stream Ingest Disconnected for [${key}]`));
});

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
}

async function startFfmpegLive(session) {
    if (session.cleanupTimer) { clearTimeout(session.cleanupTimer); session.cleanupTimer = null; }

    stopFfmpegLive(session);
    clearLiveFolder(session);
    session.uploadedS3Segments.clear();

    if (S3_ENABLED) {
        await cleanS3Bucket(session);
    }

    const args = [
        '-y',
        '-threads', '0',
        '-fflags', '+genpts+discardcorrupt',
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

        // 480p — Optimized 1.2Mbps stream (30fps, GOP 60 for identical segment sequence numbers)
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
                        console.log(`[Stream ${session.streamKey}] Speed: ${speed} | FPS: ${fps}`);
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

// ─── Routes ─────────────────────────────────────────────────────────────────────

// Home Dashboard — List all active streams & create stream modal
app.get('/', (req, res) => {
    const streamsList = Array.from(activeStreams.values()).map(s => ({
        streamKey: s.streamKey,
        title: s.title,
        isLive: s.isLive || fs.existsSync(path.join(s.liveDir, 'master.m3u8')),
        createdAt: s.createdAt
    }));
    res.render('home', { streams: streamsList });
});

// Create Stream API
app.post('/api/streams/create', (req, res) => {
    let { title, streamKey } = req.body || {};
    if (!title) title = 'Untitled Live Movie Stream';
    if (!streamKey) streamKey = 'str_' + Math.random().toString(36).substring(2, 9);

    const session = getOrCreateStreamSession(streamKey, title);
    res.json({
        success: true,
        streamKey: session.streamKey,
        title: session.title
    });
});

// List Streams API
app.get('/api/streams', (req, res) => {
    const streamsList = Array.from(activeStreams.values()).map(s => ({
        streamKey: s.streamKey,
        title: s.title,
        isLive: s.isLive || fs.existsSync(path.join(s.liveDir, 'master.m3u8')),
        createdAt: s.createdAt
    }));
    res.json({ success: true, streams: streamsList });
});

// Broadcaster Studio Route
app.get(['/stream', '/stream/:streamKey'], (req, res) => {
    const key = req.params.streamKey || 'default';
    const session = getOrCreateStreamSession(key);
    res.render('index', {
        title: `${session.title} - Broadcaster Studio`,
        streamKey: session.streamKey,
        streamTitle: session.title
    });
});

// Dynamic Express HLS Playlist Server — Serves .m3u8 instantly with zero CDN delay
// Rewrites segment URLs to ExCloud S3 bucket CDN while filtering unverified S3 segments
app.get(['/live-playlist/:streamKey/:playlist', '/live-playlist/:playlist'], (req, res) => {
    const streamKey = req.params.streamKey || 'default';
    const playlistName = req.params.playlist || 'master.m3u8';

    const session = activeStreams.get(streamKey);
    if (!session) {
        return res.status(404).send('#EXTM3U\n#EXT-X-ERROR: Stream session not found');
    }

    const playlistPath = path.join(session.liveDir, playlistName);
    if (!fs.existsSync(playlistPath)) {
        return res.status(404).send('#EXTM3U\n#EXT-X-ERROR: Playlist not generated yet');
    }

    try {
        const rawContent = fs.readFileSync(playlistPath, 'utf-8');
        res.setHeader('Content-Type', 'application/vnd.apple.mpegurl');
        res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate, max-age=0');
        res.setHeader('Pragma', 'no-cache');
        res.setHeader('Expires', '0');

        if (playlistName === 'master.m3u8') {
            return res.send(rawContent);
        }

        // Variant playlist (stream_720p.m3u8, stream_480p.m3u8)
        // Rewrite relative segment lines (stream_720p0.ts) to ExCloud S3 CDN absolute URLs
        const s3CdnBase = `${S3_PUBLIC_BASE_URL}/${streamKey}`;
        const lines = rawContent.split(/\r?\n/);
        const rewrittenLines = [];

        for (let i = 0; i < lines.length; i++) {
            const line = lines[i].trim();
            if (line.endsWith('.ts')) {
                // If segment is not verified uploaded on S3 yet, omit it to prevent 404s!
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
    } catch (err) {
        return res.status(500).send('#EXTM3U\n#EXT-X-ERROR: Internal error reading playlist');
    }
});

// Live Player Route (HLS Master Playlist served via Express)
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

// Live Status Check
app.get(['/live/status', '/live/status/:streamKey'], (req, res) => {
    const key = req.params.streamKey || req.query.streamKey || 'default';
    const session = activeStreams.get(key);
    const masterExists = session ? fs.existsSync(path.join(session.liveDir, 'master.m3u8')) : false;
    res.json({ live: (session && session.isLive) || masterExists, streamKey: key });
});

// Reset & Start Stream Process
app.post(['/reset-stream', '/reset-stream/:streamKey'], async (req, res) => {
    const key = req.params.streamKey || req.body?.streamKey || 'default';
    const session = getOrCreateStreamSession(key);
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
        console.log(`🔴 Stream [${key}] stopped. Cleanup scheduled in 10 minutes.`);

        if (session.cleanupTimer) clearTimeout(session.cleanupTimer);
        session.cleanupTimer = setTimeout(async () => {
            console.log(`🧹 Purging HLS files for [${key}]...`);
            stopFfmpegLive(session);
            clearLiveFolder(session);
            if (S3_ENABLED) {
                await cleanS3Bucket(session);
            }
        }, CLEANUP_DELAY_MS);

        res.json({ success: true, message: `Stream [${key}] stopped.` });
    } catch (err) {
        console.error(`Error stopping stream ${key}:`, err);
        res.status(500).json({ success: false, error: err.message });
    }
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
    if (S3_ENABLED) {
        console.log(`📦 ExCloud S3 Multi-Stream CDN Base: ${S3_PUBLIC_BASE_URL}`);
    }
});
