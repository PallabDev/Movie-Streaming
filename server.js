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

let s3Watcher = null;
const s3SegmentQueue = new Set();
const s3PlaylistQueue = new Set();
const uploadedS3Segments = new Set();
let isS3Uploading = false;

async function uploadSingleSegment(filename) {
    const filePath = path.join(liveDir, filename);
    if (!fs.existsSync(filePath)) return false;

    try {
        const fileBuffer = fs.readFileSync(filePath);
        await s3Client.send(new PutObjectCommand({
            Bucket: S3_BUCKET,
            Key: filename,
            Body: fileBuffer,
            ContentType: 'video/mp2t',
            CacheControl: 'public, max-age=3600'
        }));
        uploadedS3Segments.add(filename);
        console.log(`⚡ S3 Segment Synced: ${filename}`);
        return true;
    } catch (err) {
        console.warn(`[S3 Segment Error] ${filename}:`, err.message);
        return false;
    }
}

async function processS3Queue() {
    if (isS3Uploading || !s3Client) return;
    if (s3SegmentQueue.size === 0 && s3PlaylistQueue.size === 0) return;
    isS3Uploading = true;

    // STEP 1: Upload all queued .ts video segments
    if (s3SegmentQueue.size > 0) {
        const segmentsToUpload = Array.from(s3SegmentQueue);
        s3SegmentQueue.clear();

        await Promise.all(segmentsToUpload.map(filename => uploadSingleSegment(filename)));
    }

    // STEP 2: Process playlists & ensure ALL referenced .ts segments exist on S3 first!
    if (s3PlaylistQueue.size > 0) {
        const playlistsToUpload = Array.from(s3PlaylistQueue);
        s3PlaylistQueue.clear();

        for (const filename of playlistsToUpload) {
            const filePath = path.join(liveDir, filename);
            if (!fs.existsSync(filePath)) continue;

            try {
                const playlistContent = fs.readFileSync(filePath, 'utf-8');
                
                // Extract all .ts segment files referenced in this playlist
                const lines = playlistContent.split(/\r?\n/);
                const referencedSegments = lines
                    .map(l => l.trim())
                    .filter(l => l && l.endsWith('.ts'));

                // Verify every referenced segment is uploaded before pushing the playlist
                for (const segFile of referencedSegments) {
                    if (!uploadedS3Segments.has(segFile)) {
                        console.log(`⏳ Segment ${segFile} needed by ${filename} not on S3 yet. Uploading now...`);
                        await uploadSingleSegment(segFile);
                    }
                }

                // Now safe to push playlist to S3!
                await s3Client.send(new PutObjectCommand({
                    Bucket: S3_BUCKET,
                    Key: filename,
                    Body: Buffer.from(playlistContent),
                    ContentType: 'application/vnd.apple.mpegurl',
                    CacheControl: 'no-cache, no-store, must-revalidate, max-age=0'
                }));
                console.log(`⚡ S3 Playlist Synced: ${filename}`);
            } catch (err) {
                console.warn(`[S3 Playlist Error] ${filename}:`, err.message);
            }
        }
    }

    isS3Uploading = false;

    if (s3SegmentQueue.size > 0 || s3PlaylistQueue.size > 0) {
        processS3Queue();
    }
}

function startS3Watcher() {
    if (!S3_ENABLED) return;
    if (s3Watcher) { s3Watcher.close(); s3Watcher = null; }
    console.log('📡 Starting ExCloud S3 Bucket File Sync Watcher (TS Pre-Verification Mode)...');

    // Initial scan of liveDir so master.m3u8 and early segments sync immediately
    try {
        const files = fs.readdirSync(liveDir);
        for (const file of files) {
            if (file.endsWith('.ts')) s3SegmentQueue.add(file);
            else if (file.endsWith('.m3u8')) s3PlaylistQueue.add(file);
        }
        processS3Queue();
    } catch (e) { }

    s3Watcher = fs.watch(liveDir, (eventType, filename) => {
        if (!filename) return;
        if (filename.endsWith('.ts')) {
            s3SegmentQueue.add(filename);
            processS3Queue();
        } else if (filename.endsWith('.m3u8')) {
            s3PlaylistQueue.add(filename);
            setTimeout(() => {
                processS3Queue();
            }, 100);
        }
    });
}

function stopS3Watcher() {
    if (s3Watcher) {
        s3Watcher.close();
        s3Watcher = null;
    }
    s3SegmentQueue.clear();
    s3PlaylistQueue.clear();
    uploadedS3Segments.clear();
}

async function cleanS3Bucket() {
    if (!S3_ENABLED || !s3Client) return;
    try {
        const listRes = await s3Client.send(new ListObjectsV2Command({ Bucket: S3_BUCKET }));
        if (listRes.Contents && listRes.Contents.length > 0) {
            for (const item of listRes.Contents) {
                if (item.Key.endsWith('.ts') || item.Key.endsWith('.m3u8')) {
                    await s3Client.send(new DeleteObjectCommand({ Bucket: S3_BUCKET, Key: item.Key }));
                }
            }
            console.log('🧹 ExCloud S3 Bucket live folder cleaned');
        }
    } catch (err) {
        console.warn('[S3 Clean Error]:', err.message);
    }
}

// ─── CPU Usage Monitor (Production Only) ────────────────────────────────────────
let prevCpuTimes = null;

function getCpuUsage() {
    const cpus = os.cpus();
    let totalIdle = 0, totalTick = 0;
    for (const cpu of cpus) {
        for (const type in cpu.times) totalTick += cpu.times[type];
        totalIdle += cpu.times.idle;
    }
    const currentTimes = { idle: totalIdle, total: totalTick };
    if (prevCpuTimes) {
        const idleDiff = currentTimes.idle - prevCpuTimes.idle;
        const totalDiff = currentTimes.total - prevCpuTimes.total;
        const usage = totalDiff > 0 ? ((1 - idleDiff / totalDiff) * 100).toFixed(1) : '0.0';
        prevCpuTimes = currentTimes;
        return usage;
    }
    prevCpuTimes = currentTimes;
    return null;
}

function startCpuMonitor() {
    if (!IS_PROD) return;
    prevCpuTimes = null;
    getCpuUsage();
    cpuMonitorInterval = setInterval(() => {
        const usage = getCpuUsage();
        if (usage !== null) {
            const freeMem = (os.freemem() / 1024 / 1024).toFixed(0);
            const totalMem = (os.totalmem() / 1024 / 1024).toFixed(0);
            console.log(`[VPS Resource] CPU: ${usage}% | RAM: ${freeMem}MB / ${totalMem}MB`);
        }
    }, 3000);
}

function stopCpuMonitor() {
    if (cpuMonitorInterval) { clearInterval(cpuMonitorInterval); cpuMonitorInterval = null; }
}

// ─── WebSocket Servers (Status & High-Speed Stream Ingest) ─────────────────────
const wss = new WebSocketServer({ noServer: true });
const streamWss = new WebSocketServer({ noServer: true });

server.on('upgrade', (request, socket, head) => {
    try {
        const { pathname } = new URL(request.url, `http://${request.headers.host || 'localhost'}`);
        if (pathname === '/status-ws') {
            wss.handleUpgrade(request, socket, head, (ws) => {
                wss.emit('connection', ws, request);
            });
        } else if (pathname === '/stream-ws') {
            streamWss.handleUpgrade(request, socket, head, (ws) => {
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
    ws.send(JSON.stringify({ type: 'STATUS', live: isLiveStreamActive }));
    ws.on('error', (err) => console.warn('[Status WS Error]:', err.message));
});

function broadcastStatus(liveState) {
    isLiveStreamActive = liveState;
    const msg = JSON.stringify({ type: 'STATUS', live: isLiveStreamActive });
    for (const client of wss.clients) {
        if (client.readyState === 1) client.send(msg);
    }
}

// Ultra-low latency binary WebSocket stream ingest
streamWss.on('connection', (ws) => {
    console.log('⚡ Host connected to High-Speed WebSocket Stream Ingest');
    ws.on('message', (data) => {
        if (ffmpegLiveProcess && ffmpegLiveProcess.stdin && ffmpegLiveProcess.stdin.writable) {
            try {
                const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
                ffmpegLiveProcess.stdin.write(buf);
            } catch (e) { }
        }
    });
    ws.on('error', (err) => console.warn('[Stream WS Error]:', err.message));
    ws.on('close', () => console.log('⚡ Host WebSocket Stream Ingest Disconnected'));
});

// ─── FFmpeg Live Process ────────────────────────────────────────────────────────
let ffmpegLiveProcess = null;

function clearLiveFolder() {
    if (fs.existsSync(liveDir)) {
        const files = fs.readdirSync(liveDir);
        for (const file of files) {
            try { fs.unlinkSync(path.join(liveDir, file)); } catch (err) { }
        }
    }
}

function stopFfmpegLive() {
    if (ffmpegLiveProcess) {
        try {
            if (ffmpegLiveProcess.stdin) {
                ffmpegLiveProcess.stdin.removeAllListeners('error');
                ffmpegLiveProcess.stdin.on('error', () => { });
                ffmpegLiveProcess.stdin.end();
            }
            ffmpegLiveProcess.kill('SIGKILL');
        } catch (err) { }
        ffmpegLiveProcess = null;
    }
}

// Start High-Quality 720p Stream Generator (CRF + High Profile)
async function startFfmpegLive() {
    if (cleanupTimer) { clearTimeout(cleanupTimer); cleanupTimer = null; }

    stopFfmpegLive();
    clearLiveFolder();
    uploadedS3Segments.clear();

    if (SFTP_ENABLED) {
        await cleanSftpFolder();
    }
    if (S3_ENABLED) {
        await cleanS3Bucket();
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

        // Dual-resolution filter graph — 720p@30fps + 480p@24fps
        '-filter_complex',
        '[0:v]format=yuv420p,split=2[v720in][v480in];' +
        '[v720in]fps=30,scale=1280:720[v720out];' +
        '[v480in]fps=24,scale=854:480[v480out];' +
        '[0:a][1:a]amix=inputs=2:duration=first:dropout_transition=0:normalize=0,aresample=async=1:first_pts=0,asplit=2[a720][a480]',

        // 720p — CRF quality, high profile, all cores
        '-map', '[v720out]',
        '-c:v:0', 'libx264',
        '-preset', 'ultrafast',
        '-profile:v:0', 'high',
        '-level:v:0', '4.1',
        '-crf:v:0', '23',
        '-maxrate:v:0', '4500k',
        '-bufsize:v:0', '9000k',
        '-g:v:0', '60',
        '-sc_threshold:v:0', '0',
        '-x264-params:v:0', 'no-scenecut=1:open-gop=0:keyint=60:min-keyint=60:rc-lookahead=10',
        '-map', '[a720]',
        '-c:a:0', 'aac',
        '-b:a:0', '192k',

        // 480p — lightweight (baseline, CRF 28, 24fps)
        '-map', '[v480out]',
        '-c:v:1', 'libx264',
        '-preset', 'ultrafast',
        '-profile:v:1', 'baseline',
        '-crf:v:1', '28',
        '-maxrate:v:1', '1200k',
        '-bufsize:v:1', '2400k',
        '-g:v:1', '48',
        '-sc_threshold:v:1', '0',
        '-x264-params:v:1', 'no-scenecut=1:open-gop=0:keyint=48:min-keyint=48:rc-lookahead=0:bframes=0',
        '-map', '[a480]',
        '-c:a:1', 'aac',
        '-b:a:1', '96k',

        // HLS Packaging with Master Playlist + Variant Streams
        '-f', 'hls',
        '-hls_time', '2',
        '-hls_list_size', '30',
        '-hls_flags', 'delete_segments+omit_endlist+independent_segments',
        '-hls_segment_type', 'mpegts',
        '-master_pl_name', 'master.m3u8',
        '-var_stream_map', 'v:0,a:0,name:720p v:1,a:1,name:480p',
        path.join(liveDir, 'stream_%v.m3u8')
    ];

    console.log('⚡ Spawning Dual-Quality (720p CRF + 480p Lite) Live Stream Generator...');
    ffmpegLiveProcess = spawn('ffmpeg', args);

    if (SFTP_ENABLED) {
        startSftpWatcher();
    }
    if (S3_ENABLED) {
        startS3Watcher();
    }

    startCpuMonitor();

    if (ffmpegLiveProcess.stdin) {
        ffmpegLiveProcess.stdin.on('error', (err) => {
            if (err.code !== 'EPIPE' && err.code !== 'EOF') { }
        });
    }

    if (ffmpegLiveProcess.stderr) {
        let lastLoggedTime = 0;
        ffmpegLiveProcess.stderr.on('data', (data) => {
            const lines = data.toString().split(/\r?\n/);
            for (const line of lines) {
                const msg = line.trim();
                if (!msg) continue;
                if (msg.includes('fps=') || msg.includes('speed=')) {
                    const now = Date.now();
                    if (now - lastLoggedTime > 1500) {
                        lastLoggedTime = now;
                        const fpsMatch = msg.match(/fps=\s*([\d.]+)/);
                        const speedMatch = msg.match(/speed=\s*([\d.x]+)/);
                        const fps = fpsMatch ? parseFloat(fpsMatch[1]).toFixed(1) : '30.0';
                        const speed = speedMatch ? speedMatch[1] : '1.0x';
                        console.log(`[Multi-Stream] Speed: ${speed} | FPS: ${fps}`);
                    }
                } else {
                    console.error('[FFmpeg]:', msg);
                }
            }
        });
    }

    ffmpegLiveProcess.on('error', (err) => {
        console.error('FFmpeg error:', err.message);
        ffmpegLiveProcess = null;
    });

    ffmpegLiveProcess.on('exit', (code, signal) => {
        if (code !== 0 && signal !== 'SIGKILL') console.log(`FFmpeg exited (code: ${code})`);
        ffmpegLiveProcess = null;
        stopCpuMonitor();
        stopSftpWatcher();
        stopS3Watcher();
    });
}

// ─── Routes ─────────────────────────────────────────────────────────────────────
app.get('/', (req, res) => {
    res.render('index', { title: 'Movie Streaming - Broadcaster Studio' });
});

app.get(['/live', '/live/'], (req, res) => {
    const hlsBaseUrl = HLS_CDN_URL || '/live';
    res.render('live', {
        stream: { title: 'Live Screen Stream', streamKey: 'live' },
        hlsBaseUrl: hlsBaseUrl
    });
});

app.get('/live/status', (req, res) => {
    const masterExists = fs.existsSync(path.join(liveDir, 'master.m3u8'));
    res.json({ live: isLiveStreamActive || masterExists });
});

app.post('/reset-stream', async (req, res) => {
    try {
        await startFfmpegLive();
        broadcastStatus(true);
        res.json({ success: true, message: 'Live stream started' });
    } catch (err) {
        console.error('Error starting stream:', err);
        res.status(500).json({ success: false, error: err.message });
    }
});

app.post('/stop-stream', async (req, res) => {
    try {
        broadcastStatus(false);
        stopCpuMonitor();
        stopSftpWatcher();
        stopS3Watcher();
        console.log('🔴 Stream stopped. Segment cleanup scheduled in 10 minutes.');

        if (cleanupTimer) clearTimeout(cleanupTimer);
        cleanupTimer = setTimeout(async () => {
            console.log('🧹 Purging HLS segment files (local + SFTP + ExCloud S3 Bucket)...');
            stopFfmpegLive();
            clearLiveFolder();
            if (SFTP_ENABLED) {
                await cleanSftpFolder();
                await disconnectSftp();
            }
            if (S3_ENABLED) {
                await cleanS3Bucket();
            }
        }, CLEANUP_DELAY_MS);

        res.json({ success: true, message: 'Stream stopped. Deletion scheduled in 10 minutes.' });
    } catch (err) {
        console.error('Error stopping stream:', err);
        res.status(500).json({ success: false, error: err.message });
    }
});

app.post('/stream', (req, res) => {
    req.on('aborted', () => { });
    const chunkIndex = parseInt(req.headers['x-chunk-index'] || '0');

    // Respond IMMEDIATELY to release Chrome's HTTP socket connection instantly (< 1ms)
    res.status(200).json({ success: true, chunkIndex });

    // Write binary chunk asynchronously to FFmpeg stdin without blocking HTTP socket
    if (ffmpegLiveProcess && ffmpegLiveProcess.stdin && ffmpegLiveProcess.stdin.writable) {
        try {
            const buf = Buffer.isBuffer(req.body) ? req.body : Buffer.from(req.body);
            ffmpegLiveProcess.stdin.write(buf);
        } catch (e) { }
    }
});

// Global Express error handler to swallow raw-body BadRequestError on aborted chunk uploads
app.use((err, req, res, next) => {
    if (err && (err.type === 'aborted' || err.status === 400)) {
        return res.status(400).json({ success: false, error: 'Request aborted' });
    }
    next(err);
});

server.listen(PORT, () => {
    console.log(`🚀 Server running in [${NODE_ENV}] mode at ${APP_URL} (Port ${PORT})`);
    if (IS_PROD) {
        const cpus = os.cpus();
        console.log(`📊 VPS: ${cpus.length} cores (${cpus[0].model}) | ${(os.totalmem() / 1024 / 1024).toFixed(0)}MB RAM`);
    }
    if (SFTP_ENABLED) {
        console.log(`📡 SFTP CDN: ${SFTP_HOST}:${SFTP_PORT} → ${HLS_CDN_URL}`);
        console.log(`📁 SFTP CDN: Disabled (serving HLS locally from /live)`);
    }
});
