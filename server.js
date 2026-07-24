import 'dotenv/config';
import express from 'express';
import http from 'http';
import path from 'path';
import fs from 'fs';
import os from 'os';
import session from 'express-session';
import pgSimple from 'connect-pg-simple';
import { fileURLToPath } from 'url';
import { spawn } from 'child_process';
import { WebSocketServer } from 'ws';
import { pool } from './src/db/index.js';
import { injectUser } from './src/middleware/auth.js';
import authRoutes from './src/routes/auth.js';
import dashboardRoutes from './src/routes/dashboard.js';
import adminRoutes from './src/routes/admin.js';
import streamRoutes from './src/routes/stream.js';

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
app.set('views', path.join(__dirname, 'src', 'views'));

// Ensure sessions table exists for connect-pg-simple
async function ensureSessionsTable() {
    try {
        await pool.query(`
            CREATE TABLE IF NOT EXISTS "sessions" (
                "sid" varchar NOT NULL COLLATE "default",
                "sess" json NOT NULL,
                "expire" timestamp(6) NOT NULL,
                CONSTRAINT "sessions_pkey" PRIMARY KEY ("sid")
            );
            CREATE INDEX IF NOT EXISTS "IDX_session_expire" ON "sessions" ("expire");
        `);
        console.log('Sessions table ready');
    } catch (err) {
        console.error('Failed to create sessions table:', err.message);
    }
}

// Session middleware
const PgSession = pgSimple(session);
const sessionStore = new PgSession({ pool, tableName: 'sessions' });
sessionStore.on('error', (err) => {
    console.error('Session store error:', err.message);
});
app.use(session({
    store: sessionStore,
    secret: process.env.SESSION_SECRET || 'cowatch-dev-secret',
    resave: false,
    saveUninitialized: false,
    cookie: { maxAge: 7 * 24 * 60 * 60 * 1000, secure: IS_PROD, sameSite: 'lax' },
}));

// Make APP_URL available to all views
app.use((req, res, next) => {
    res.locals.APP_URL = APP_URL;
    next();
});

// Auth middleware - inject user into all views
app.use(injectUser);

// Body parsing
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// CORS Middleware
app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS, HEAD');
    res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Range');
    if (req.method === 'OPTIONS') {
        return res.sendStatus(200);
    }
    next();
});

// Static files + HLS segments served via Caddy reverse proxy
app.use(express.static(path.join(__dirname, 'public')));
app.use('/live', express.static(liveDir, {
    setHeaders: (res, filePath) => {
        res.setHeader('Access-Control-Allow-Origin', '*');
        res.setHeader('Connection', 'keep-alive');
        res.setHeader('Keep-Alive', 'timeout=120, max=2000');

        if (filePath.endsWith('.m3u8')) {
            res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate, max-age=0');
            res.setHeader('Pragma', 'no-cache');
            res.setHeader('Expires', '0');
        } else {
            res.setHeader('Cache-Control', 'public, max-age=3600');
        }
    }
}));

// Middleware for parsing raw video/binary stream data up to 100MB per chunk
app.use('/stream', express.raw({ type: '*/*', limit: '100mb' }));

// Live Status
let isLiveStreamActive = false;
let cleanupTimer = null;
let cpuMonitorInterval = null;

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
    const pathname = request.url;

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

streamWss.on('connection', (ws) => {
    ws.on('message', (data) => {
        if (ffmpegLiveProcess && ffmpegLiveProcess.stdin && ffmpegLiveProcess.stdin.writable) {
            try {
                ffmpegLiveProcess.stdin.write(data);
            } catch (e) { }
        }
    });
    ws.on('error', (err) => console.warn('[Stream WS Error]:', err.message));
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

async function startFfmpegLive() {
    if (cleanupTimer) { clearTimeout(cleanupTimer); cleanupTimer = null; }

    stopFfmpegLive();
    clearLiveFolder();

    const args = [
        '-y',
        '-threads', '4',
        '-fflags', '+genpts+discardcorrupt',
        '-probesize', '64k',
        '-analyzeduration', '0',
        '-i', 'pipe:0',

        // Multi-resolution filter graph (1080p, 720p, 480p)
        '-filter_complex',
        '[0:v]fps=30,split=3[v1080in][v720in][v480in];' +
        '[v1080in]copy[v1080out];' +
        '[v720in]scale=1280:720[v720out];' +
        '[v480in]scale=854:480:force_original_aspect_ratio=decrease,scale=trunc(iw/2)*2:trunc(ih/2)*2[v480out];' +
        '[0:a]aformat=sample_rates=48000:channel_layouts=stereo,aresample=async=1:min_comp=0.001:comp_duration=1,asplit=3[a1080][a720][a480]',

        // 1080p (CRF 15, 14Mbps)
        '-map', '[v1080out]',
        '-c:v:0', 'libx264',
        '-threads:v:0', '2',
        '-preset', 'superfast',
        '-tune', 'zerolatency',
        '-profile:v:0', 'high',
        '-crf:v:0', '15',
        '-b:v:0', '14000k',
        '-maxrate:v:0', '16000k',
        '-bufsize:v:0', '24000k',
        '-g:v:0', '30',
        '-keyint_min:v:0', '30',
        '-sc_threshold:v:0', '0',
        '-x264-params:v:0', 'no-scenecut=1:open-gop=0:keyint=30:min-keyint=30',
        '-map', '[a1080]',
        '-c:a:0', 'aac',
        '-b:a:0', '320k',
        '-ar:a:0', '48000',
        '-ac:a:0', '2',

        // 720p (6Mbps)
        '-map', '[v720out]',
        '-c:v:1', 'libx264',
        '-threads:v:1', '2',
        '-preset', 'ultrafast',
        '-tune', 'zerolatency',
        '-profile:v:1', 'main',
        '-b:v:1', '6000k',
        '-maxrate:v:1', '7000k',
        '-bufsize:v:1', '12000k',
        '-g:v:1', '30',
        '-keyint_min:v:1', '30',
        '-sc_threshold:v:1', '0',
        '-x264-params:v:1', 'no-scenecut=1:open-gop=0:keyint=30:min-keyint=30',
        '-map', '[a720]',
        '-c:a:1', 'aac',
        '-b:a:1', '256k',
        '-ar:a:1', '48000',
        '-ac:a:1', '2',

        // 480p (2.5Mbps)
        '-map', '[v480out]',
        '-c:v:2', 'libx264',
        '-threads:v:2', '2',
        '-preset', 'ultrafast',
        '-tune', 'zerolatency',
        '-profile:v:2', 'main',
        '-b:v:2', '2500k',
        '-maxrate:v:2', '3000k',
        '-bufsize:v:2', '5000k',
        '-g:v:2', '30',
        '-keyint_min:v:2', '30',
        '-sc_threshold:v:2', '0',
        '-x264-params:v:2', 'no-scenecut=1:open-gop=0:keyint=30:min-keyint=30',
        '-map', '[a480]',
        '-c:a:2', 'aac',
        '-b:a:2', '192k',
        '-ar:a:2', '48000',
        '-ac:a:2', '2',

        // HLS fMP4
        '-f', 'hls',
        '-hls_time', '2',
        '-hls_list_size', '60',
        '-hls_flags', 'delete_segments+omit_endlist+independent_segments+temp_file',
        '-hls_segment_type', 'fmp4',
        '-master_pl_name', 'master.m3u8',
        '-var_stream_map', 'v:0,a:0,name:1080p v:1,a:1,name:720p v:2,a:2,name:480p',
        path.join(liveDir, 'stream_%v.m3u8')
    ];

    console.log('Spawning Triple-Quality (1080p CRF 15 / 720p 6Mbps / 480p 2.5Mbps) Live Generator...');
    ffmpegLiveProcess = spawn('ffmpeg', args);

    startCpuMonitor();

    if (ffmpegLiveProcess.stdin) {
        ffmpegLiveProcess.stdin.on('error', (err) => {
            if (err.code !== 'EPIPE' && err.code !== 'EOF') { }
        });
    }

    if (ffmpegLiveProcess.stderr) {
        let lastLoggedTime = 0;
        let stderrBuffer = '';
        ffmpegLiveProcess.stderr.on('data', (data) => {
            stderrBuffer += data.toString();
            const lines = stderrBuffer.split(/\r?\n/);
            stderrBuffer = lines.pop();

            for (const line of lines) {
                if (line.includes('fps=') || line.includes('speed=')) {
                    const now = Date.now();
                    if (now - lastLoggedTime > 2000) {
                        lastLoggedTime = now;
                        const fpsMatch = line.match(/fps=\s*([\d.]+)/);
                        const speedMatch = line.match(/speed=\s*([\d.x]+)/);
                        const bitrateMatch = line.match(/bitrate=\s*([\d.]+)kbits\/s/);
                        const frameMatch = line.match(/frame=\s*(\d+)/);
                        const timeMatch = line.match(/time=\s*(\d+:\d+:\d+\.\d+)/);
                        const sizeMatch = line.match(/size=\s*(\d+)kB/);

                        const realFps = fpsMatch ? fpsMatch[1] : 'N/A';
                        const realSpeed = speedMatch ? speedMatch[1] : 'N/A';
                        const actualBitrate = bitrateMatch ? (parseFloat(bitrateMatch[1]) / 1000).toFixed(1) : 'N/A';
                        const frame = frameMatch ? frameMatch[1] : 'N/A';
                        const time = timeMatch ? timeMatch[1] : 'N/A';
                        const size = sizeMatch ? sizeMatch[1] : 'N/A';

                        console.log(`[FFmpeg] FPS: ${realFps} | Speed: ${realSpeed}x | Bitrate: ${actualBitrate} Mbps | Frame: ${frame} | Time: ${time} | Size: ${size}kB`);
                    }
                } else if (line.includes('Error') || line.includes('Invalid') || line.includes('Unrecognized')) {
                    console.error('[FFmpeg Error]:', line);
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
    });
}

// ─── Routes ─────────────────────────────────────────────────────────────────────
app.get('/', (req, res) => {
    if (!req.session?.user) return res.redirect('/auth/login');
    res.redirect('/dashboard');
});

// MVC Routes
app.use('/auth', authRoutes);
app.use('/dashboard', dashboardRoutes);
app.use('/admin', adminRoutes);
app.use('/stream', streamRoutes);

// Viewer live page (no auth required)
app.get(['/live', '/live/'], (req, res) => {
    res.render('live', {
        stream: { title: 'Live Stream', streamKey: 'live' },
        hlsBaseUrl: '/live',
        user: req.session?.user || null,
    });
});

// Stream key viewer page
app.get('/live/:key', async (req, res) => {
    try {
        const { db } = await import('./src/db/index.js');
        const { streams } = await import('./src/db/schema.js');
        const { eq } = await import('drizzle-orm');
        const [stream] = await db.select().from(streams).where(eq(streams.streamKey, req.params.key));
        if (!stream) return res.status(404).render('partials/404', { title: 'Stream Not Found', user: req.session?.user || null });
        res.render('live', { stream, hlsBaseUrl: '/live', user: req.session?.user || null });
    } catch (err) {
        console.error('Live page error:', err);
        res.status(500).send('Error loading stream');
    }
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
        if (cleanupTimer) { clearTimeout(cleanupTimer); cleanupTimer = null; }

        console.log('Stream stopped. Purging HLS segment files...');
        stopFfmpegLive();
        clearLiveFolder();

        res.json({ success: true, message: 'Stream stopped and HLS cleaned.' });
    } catch (err) {
        console.error('Error stopping stream:', err);
        res.status(500).json({ success: false, error: err.message });
    }
});

app.post('/stream', (req, res) => {
    req.on('aborted', () => { });
    const chunkIndex = parseInt(req.headers['x-chunk-index'] || '0');

    res.status(200).json({ success: true, chunkIndex });

    if (ffmpegLiveProcess && ffmpegLiveProcess.stdin && ffmpegLiveProcess.stdin.writable) {
        try {
            ffmpegLiveProcess.stdin.write(req.body);
        } catch (e) { }
    }
});

// Global Express error handler
app.use((err, req, res, next) => {
    if (err && (err.type === 'aborted' || err.status === 400)) {
        return res.status(400).json({ success: false, error: 'Request aborted' });
    }
    next(err);
});

ensureSessionsTable().then(() => {
    server.listen(PORT, () => {
        console.log(`Server running in [${NODE_ENV}] mode at ${APP_URL} (Port ${PORT})`);
        if (IS_PROD) {
            const cpus = os.cpus();
            console.log(`VPS: ${cpus.length} cores (${cpus[0].model}) | ${(os.totalmem() / 1024 / 1024).toFixed(0)}MB RAM`);
        }
    });
});
