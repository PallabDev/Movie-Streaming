import 'dotenv/config';
import express from 'express';
import http from 'http';
import path from 'path';
import fs from 'fs';
import os from 'os';
import { fileURLToPath } from 'url';
import { spawn } from 'child_process';
import { WebSocketServer } from 'ws';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const server = http.createServer(app);

const PORT = process.env.PORT || 3000;
const NODE_ENV = process.env.NODE_ENV || 'development';
const APP_URL = process.env.APP_URL || `http://localhost:${PORT}`;
const IS_PROD = NODE_ENV === 'production';

// Directories setup
const liveDir = path.join(__dirname, 'public', 'live');

if (!fs.existsSync(liveDir)) {
    fs.mkdirSync(liveDir, { recursive: true });
}

// Configure EJS view engine
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));

// Static folder setup
app.use(express.static(path.join(__dirname, 'public')));
app.use('/live', express.static(liveDir));

// Middleware for parsing raw video/binary stream data up to 100MB per chunk
app.use('/stream', express.raw({ type: '*/*', limit: '100mb' }));
app.use(express.json());

// Live Status & Cleanups
let isLiveStreamActive = false;
let cleanupTimer = null;
let cpuMonitorInterval = null;
const CLEANUP_DELAY_MS = 10 * 60 * 1000; // 10 minutes after stream ends

// ─── CPU Usage Monitor (Production Only) ───────────────────────────────────────
let prevCpuTimes = null;

function getCpuUsage() {
    const cpus = os.cpus();
    let totalIdle = 0, totalTick = 0;

    for (const cpu of cpus) {
        for (const type in cpu.times) {
            totalTick += cpu.times[type];
        }
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
    return null; // First call, no delta yet
}

function startCpuMonitor() {
    if (!IS_PROD) return;
    prevCpuTimes = null;
    getCpuUsage(); // Prime the first reading
    cpuMonitorInterval = setInterval(() => {
        const usage = getCpuUsage();
        if (usage !== null) {
            const freeMem = (os.freemem() / 1024 / 1024).toFixed(0);
            const totalMem = (os.totalmem() / 1024 / 1024).toFixed(0);
            console.log(`[VPS Resource] CPU: ${usage}% | RAM: ${freeMem}MB / ${totalMem}MB`);
        }
    }, 3000); // Log every 3 seconds
}

function stopCpuMonitor() {
    if (cpuMonitorInterval) {
        clearInterval(cpuMonitorInterval);
        cpuMonitorInterval = null;
    }
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
        if (client.readyState === 1) {
            client.send(msg);
        }
    }
}

// ─── FFmpeg Live Process ────────────────────────────────────────────────────────
let ffmpegLiveProcess = null;

function clearLiveFolder() {
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

// Start Optimized Single-Pass (720p & 480p) ABR Generator
// Input: 720p 30fps VP8 from browser (lightweight decode)
// Output: 720p passthrough-res + 480p downscale, both ultrafast H.264
function startFfmpegLive() {
    if (cleanupTimer) {
        clearTimeout(cleanupTimer);
        cleanupTimer = null;
    }

    stopFfmpegLive();
    clearLiveFolder();

    // Optimized for 720p input: no upscale/downscale on 720p rendition, only 480p needs scaling
    const args = [
        '-y',
        '-threads', '2',
        '-probesize', '64k',
        '-analyzeduration', '0',
        '-i', 'pipe:0',

        '-filter_complex',
        '[0:v]fps=30,split=2[v720][v480];' +
        '[v480]scale=854:-2[v480out]',

        // Rendition 0: 720p passthrough (no scale needed — input is already 720p)
        '-map', '[v720]', '-c:v:0', 'libx264', '-threads:v:0', '1', '-preset', 'ultrafast', '-tune', 'zerolatency', '-profile:v:0', 'main', '-b:v:0', '3000k', '-maxrate:v:0', '3600k', '-bufsize:v:0', '6000k', '-g:v:0', '30', '-sc_threshold:v:0', '0',
        '-map', '0:a?', '-c:a:0', 'aac', '-b:a:0', '128k',

        // Rendition 1: 480p downscale (lightweight 720p→480p scale)
        '-map', '[v480out]', '-c:v:1', 'libx264', '-threads:v:1', '1', '-preset', 'ultrafast', '-tune', 'zerolatency', '-profile:v:1', 'baseline', '-b:v:1', '1000k', '-maxrate:v:1', '1200k', '-bufsize:v:1', '2000k', '-g:v:1', '30', '-sc_threshold:v:1', '0',
        '-map', '0:a?', '-c:a:1', 'aac', '-b:a:1', '96k',

        '-f', 'hls',
        '-hls_time', '1',
        '-hls_list_size', '5',
        '-hls_flags', 'delete_segments+omit_endlist+independent_segments',
        '-hls_segment_type', 'mpegts',
        '-master_pl_name', 'master.m3u8',
        '-var_stream_map', 'v:0,a:0,name:720p v:1,a:1,name:480p',
        path.join(liveDir, 'stream_%v.m3u8')
    ];

    console.log('⚡ Spawning Optimized 720p+480p Live Generator (720p input, 2 threads)...');
    ffmpegLiveProcess = spawn('ffmpeg', args);

    // Start CPU monitoring in production
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
                    const fps = fpsMatch ? fpsMatch[1] : 'N/A';
                    const speed = speedMatch ? speedMatch[1] : '1.0x';
                    console.log(`[FFmpeg Speed] [720p & 480p] Speed: ${speed} | FPS: ${fps}`);
                }
            }
        });
    }

    ffmpegLiveProcess.on('error', (err) => {
        console.error('FFmpeg error:', err.message);
        ffmpegLiveProcess = null;
    });

    ffmpegLiveProcess.on('exit', (code, signal) => {
        if (code !== 0 && signal !== 'SIGKILL') {
            console.log(`FFmpeg exited (code: ${code})`);
        }
        ffmpegLiveProcess = null;
        stopCpuMonitor();
    });
}

// ─── Routes ─────────────────────────────────────────────────────────────────────
app.get('/', (req, res) => {
    res.render('index', { title: 'Movie Streaming - Broadcaster Studio' });
});

app.get(['/live', '/live/'], (req, res) => {
    res.render('live', {
        stream: { title: 'Live Screen Stream', streamKey: 'live' }
    });
});

app.get('/live/status', (req, res) => {
    res.json({ live: isLiveStreamActive });
});

app.post('/reset-stream', (req, res) => {
    try {
        startFfmpegLive();
        broadcastStatus(true);
        res.json({ success: true, message: 'Live stream started' });
    } catch (err) {
        console.error('Error starting stream:', err);
        res.status(500).json({ success: false, error: err.message });
    }
});

app.post('/stop-stream', (req, res) => {
    try {
        broadcastStatus(false);
        stopCpuMonitor();
        console.log('🔴 Stream stopped. Segment cleanup scheduled in 10 minutes.');

        if (cleanupTimer) clearTimeout(cleanupTimer);
        cleanupTimer = setTimeout(() => {
            console.log('🧹 Purging live HLS segment files...');
            stopFfmpegLive();
            clearLiveFolder();
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
});
