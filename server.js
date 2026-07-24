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
const HLS_CDN_URL = process.env.HLS_CDN_URL || '';
const SFTP_ENABLED = !!(SFTP_HOST && SFTP_USER && SFTP_PASSWORD && SFTP_BASE_PATH);

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

// ─── In-Memory RAM Segment Cache & High-Speed Keep-Alive Delivery ────────────
const segmentRamCache = new Map();
const MAX_RAM_CACHE_ITEMS = 30;

// High performance RAM cache & persistent Keep-Alive for HLS files
app.get('/live/:file', (req, res, next) => {
    const filename = req.params.file;

    // Set persistent HTTP Keep-Alive and CORS headers for instant chunk delivery
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('Keep-Alive', 'timeout=120, max=2000');

    if (filename.endsWith('.ts') && segmentRamCache.has(filename)) {
        res.setHeader('Content-Type', 'video/mp2t');
        res.setHeader('Cache-Control', 'public, max-age=3600');
        return res.send(segmentRamCache.get(filename));
    }
    next();
});

// File watcher populates Node V8 RAM cache in < 1ms as soon as FFmpeg writes segments
fs.watch(liveDir, (eventType, filename) => {
    if (filename && filename.endsWith('.ts')) {
        const filePath = path.join(liveDir, filename);
        setTimeout(() => {
            if (fs.existsSync(filePath)) {
                try {
                    const buf = fs.readFileSync(filePath);
                    segmentRamCache.set(filename, buf);
                    if (segmentRamCache.size > MAX_RAM_CACHE_ITEMS) {
                        const oldestKey = segmentRamCache.keys().next().value;
                        segmentRamCache.delete(oldestKey);
                    }
                } catch (e) {}
            }
        }, 30);
    }
});

// Static folder setup
app.use(express.static(path.join(__dirname, 'public')));
app.use('/live', express.static(liveDir, {
    maxAge: '1h',
    setHeaders: (res) => {
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
        try { await sftp.end(); } catch (e) {}
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
                    } catch (e) {}
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
                try { await sftp.delete(remotePath); } catch (e) {}
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

// ─── WebSocket Server ───────────────────────────────────────────────────────────
const wss = new WebSocketServer({ server, path: '/status-ws' });

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

// ─── FFmpeg Live Process ────────────────────────────────────────────────────────
let ffmpegLiveProcess = null;

function clearLiveFolder() {
    segmentRamCache.clear();
    if (fs.existsSync(liveDir)) {
        const files = fs.readdirSync(liveDir);
        for (const file of files) {
            try { fs.unlinkSync(path.join(liveDir, file)); } catch (err) {}
        }
    }
}

function stopFfmpegLive() {
    if (ffmpegLiveProcess) {
        try {
            if (ffmpegLiveProcess.stdin) {
                ffmpegLiveProcess.stdin.removeAllListeners('error');
                ffmpegLiveProcess.stdin.on('error', () => {});
                ffmpegLiveProcess.stdin.end();
            }
            ffmpegLiveProcess.kill('SIGKILL');
        } catch (err) {}
        ffmpegLiveProcess = null;
    }
}

// Start Single 720p Stream Generator (Ultra-fast, lightweight 720p encoding)
async function startFfmpegLive() {
    if (cleanupTimer) { clearTimeout(cleanupTimer); cleanupTimer = null; }

    stopFfmpegLive();
    clearLiveFolder();

    if (SFTP_ENABLED) {
        await cleanSftpFolder();
    }

    const args = [
        '-y',
        '-threads', '2',
        '-probesize', '64k',
        '-analyzeduration', '0',
        '-i', 'pipe:0',

        // Explicitly map input video 0:v and audio 0:a? to v:0 and a:0
        '-map', '0:v',
        '-c:v:0', 'libx264',
        '-threads:v:0', '2',
        '-r:v:0', '30',
        '-preset', 'ultrafast',
        '-tune', 'zerolatency',
        '-profile:v:0', 'high',
        '-crf:v:0', '18',
        '-b:v:0', '6500k',
        '-maxrate:v:0', '7500k',
        '-bufsize:v:0', '12000k',
        '-g:v:0', '30',
        '-keyint_min:v:0', '30',
        '-sc_threshold:v:0', '0',

        '-map', '0:a?',
        '-c:a:0', 'aac',
        '-b:a:0', '128k',

        '-f', 'hls',
        '-hls_time', '1',
        '-hls_list_size', '60',
        '-hls_flags', 'delete_segments+omit_endlist+independent_segments',
        '-hls_segment_type', 'mpegts',
        '-master_pl_name', 'master.m3u8',
        '-var_stream_map', 'v:0,a:0,name:720p',
        path.join(liveDir, 'stream_%v.m3u8')
    ];

    console.log('⚡ Spawning Single 720p Stream Live Generator...');
    ffmpegLiveProcess = spawn('ffmpeg', args);

    if (SFTP_ENABLED) {
        startSftpWatcher();
    }

    startCpuMonitor();

    if (ffmpegLiveProcess.stdin) {
        ffmpegLiveProcess.stdin.on('error', (err) => {
            if (err.code !== 'EPIPE' && err.code !== 'EOF') {}
        });
    }

    if (ffmpegLiveProcess.stderr) {
        let lastLoggedTime = 0;
        ffmpegLiveProcess.stderr.on('data', (data) => {
            const msg = data.toString().trim();
            if (msg.includes('fps=') || msg.includes('speed=')) {
                const now = Date.now();
                if (now - lastLoggedTime > 1500) {
                    lastLoggedTime = now;
                    const fpsMatch = msg.match(/fps=\s*([\d.]+)/);
                    const speedMatch = msg.match(/speed=\s*([\d.x]+)/);
                    const fps = fpsMatch ? parseFloat(fpsMatch[1]).toFixed(1) : '30.0';
                    const speed = speedMatch ? speedMatch[1] : '1.0x';
                    console.log(`[720p Stream] Speed: ${speed} | FPS: ${fps}`);
                }
            } else if (msg.includes('Error') || msg.includes('Invalid') || msg.includes('Unrecognized')) {
                console.error('[FFmpeg Error]:', msg);
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
    res.json({ live: isLiveStreamActive });
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
        console.log('🔴 Stream stopped. Segment cleanup scheduled in 10 minutes.');

        if (cleanupTimer) clearTimeout(cleanupTimer);
        cleanupTimer = setTimeout(async () => {
            console.log('🧹 Purging HLS segment files (local + SFTP CDN)...');
            stopFfmpegLive();
            clearLiveFolder();
            if (SFTP_ENABLED) {
                await cleanSftpFolder();
                await disconnectSftp();
            }
        }, CLEANUP_DELAY_MS);

        res.json({ success: true, message: 'Stream stopped. Deletion scheduled in 10 minutes.' });
    } catch (err) {
        console.error('Error stopping stream:', err);
        res.status(500).json({ success: false, error: err.message });
    }
});

app.post('/stream', (req, res) => {
    try {
        const chunkIndex = parseInt(req.headers['x-chunk-index'] || '0');
        if (ffmpegLiveProcess && ffmpegLiveProcess.stdin && ffmpegLiveProcess.stdin.writable) {
            try { ffmpegLiveProcess.stdin.write(req.body); } catch (e) {}
        }
        res.json({ success: true, chunkIndex });
    } catch (err) {
        console.error('Error piping stream chunk:', err);
        res.status(500).json({ success: false, error: err.message });
    }
});

server.listen(PORT, () => {
    console.log(`🚀 Server running in [${NODE_ENV}] mode at ${APP_URL} (Port ${PORT})`);
    if (IS_PROD) {
        const cpus = os.cpus();
        console.log(`📊 VPS: ${cpus.length} cores (${cpus[0].model}) | ${(os.totalmem() / 1024 / 1024).toFixed(0)}MB RAM`);
    }
    if (SFTP_ENABLED) {
        console.log(`📡 SFTP CDN: ${SFTP_HOST}:${SFTP_PORT} → ${HLS_CDN_URL}`);
    } else {
        console.log(`📁 SFTP CDN: Disabled (serving HLS locally from /live)`);
    }
});
