import 'dotenv/config';
import express from 'express';
import http from 'http';
import path from 'path';
import fs from 'fs';
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
const CLEANUP_DELAY_MS = 10 * 60 * 1000; // 10 minutes after stream ends

// WebSocket Server for Real-Time Status Travel between Host & Viewers
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

// Active FFmpeg process
let ffmpegLiveProcess = null;

// Helper function to clean live HLS folder
function clearLiveFolder() {
    if (fs.existsSync(liveDir)) {
        const files = fs.readdirSync(liveDir);
        for (const file of files) {
            try {
                fs.unlinkSync(path.join(liveDir, file));
            } catch (err) {
                // Silent clean
            }
        }
    }
}

// Stop active FFmpeg live process
function stopFfmpegLive() {
    if (ffmpegLiveProcess) {
        try {
            if (ffmpegLiveProcess.stdin) {
                ffmpegLiveProcess.stdin.removeAllListeners('error');
                ffmpegLiveProcess.stdin.on('error', () => {});
                ffmpegLiveProcess.stdin.end();
            }
            ffmpegLiveProcess.kill('SIGKILL');
        } catch (err) {
            // Silent stop
        }
        ffmpegLiveProcess = null;
    }
}

// Start Single-Pass 2-Core Multi-Threaded Dual-Quality (720p & 480p) Generator
function startFfmpegLive() {
    if (cleanupTimer) {
        clearTimeout(cleanupTimer);
        cleanupTimer = null;
    }
    
    stopFfmpegLive();
    clearLiveFolder();

    const masterPath = path.join(liveDir, 'master.m3u8');
    
    // Dedicated 2-Core Multi-Threaded Single-Pass ABR pipeline for 720p & 480p (Speed > 1.4x)
    const args = [
        '-y',
        '-threads', '2',                     // Lock FFmpeg to exactly 2 CPU cores
        '-filter_complex_threads', '2',      // Allocate 2 dedicated threads for scaling filter
        '-probesize', '64k',
        '-analyzeduration', '0',
        '-i', 'pipe:0',
        
        '-filter_complex',
        '[0:v]split=2[v720][v480];' +
        '[v720]fps=30,scale=1280:-2[v720out];' +
        '[v480]fps=30,scale=854:-2[v480out]',

        // Rendition 0: Crisp 720p 30fps (3000k Bitrate)
        '-map', '[v720out]', '-c:v:0', 'libx264', '-threads:v:0', '2', '-preset', 'ultrafast', '-tune', 'zerolatency', '-profile:v:0', 'main', '-b:v:0', '3000k', '-maxrate:v:0', '3600k', '-bufsize:v:0', '6000k', '-g:v:0', '30', '-sc_threshold:v:0', '0',
        '-map', '0:a?', '-c:a:0', 'aac', '-b:a:0', '128k',

        // Rendition 1: Clean 480p 30fps (1000k Bitrate)
        '-map', '[v480out]', '-c:v:1', 'libx264', '-threads:v:1', '2', '-preset', 'ultrafast', '-tune', 'zerolatency', '-profile:v:1', 'baseline', '-b:v:1', '1000k', '-maxrate:v:1', '1200k', '-bufsize:v:1', '2000k', '-g:v:1', '30', '-sc_threshold:v:1', '0',
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

    console.log('⚡ Spawning 2-Core Multi-Threaded Dual-Quality (720p & 480p) Live Generator...');
    ffmpegLiveProcess = spawn('ffmpeg', args);

    if (ffmpegLiveProcess.stdin) {
        ffmpegLiveProcess.stdin.on('error', (err) => {
            if (err.code !== 'EPIPE' && err.code !== 'EOF') {
                // Suppress non-critical EPIPE
            }
        });
    }

    if (ffmpegLiveProcess.stderr) {
        let lastLoggedTime = 0;
        ffmpegLiveProcess.stderr.on('data', (data) => {
            const msg = data.toString().trim();
            if (msg.includes('fps=') || msg.includes('speed=')) {
                const now = Date.now();
                if (now - lastLoggedTime > 1500) { // Log clean summary every 1.5 seconds
                    lastLoggedTime = now;
                    const fpsMatch = msg.match(/fps=\s*([\d.]+)/);
                    const speedMatch = msg.match(/speed=\s*([\d.x]+)/);
                    const fps = fpsMatch ? fpsMatch[1] : 'N/A';
                    const speed = speedMatch ? speedMatch[1] : '1.0x';
                    console.log(`[FFmpeg 2-Core Speed] [720p & 480p] Speed: ${speed} | FPS: ${fps}`);
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
            console.log(`FFmpeg process exited (code: ${code})`);
        }
        ffmpegLiveProcess = null;
    });
}

// Routes
app.get('/', (req, res) => {
    res.render('index', {
        title: 'Movie Streaming - Broadcaster Studio'
    });
});

// Live Watch Page Route
app.get(['/live', '/live/'], (req, res) => {
    res.render('live', {
        stream: {
            title: 'Live Screen Stream',
            streamKey: 'live'
        }
    });
});

// Endpoint to check live stream status
app.get('/live/status', (req, res) => {
    res.json({ live: isLiveStreamActive });
});

// Endpoint called when host starts sharing screen
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

// Endpoint called when host stops sharing screen (Schedules 10-minute HLS segment cleanup)
app.post('/stop-stream', (req, res) => {
    try {
        broadcastStatus(false);
        console.log('🔴 Stream stopped by host. Segment cleanup scheduled in 10 minutes.');

        if (cleanupTimer) clearTimeout(cleanupTimer);
        cleanupTimer = setTimeout(() => {
            console.log('🧹 10 minutes elapsed. Purging live HLS segment files...');
            stopFfmpegLive();
            clearLiveFolder();
        }, CLEANUP_DELAY_MS);

        res.json({ success: true, message: 'Stream stopped. Deletion scheduled in 10 minutes.' });
    } catch (err) {
        console.error('Error stopping stream:', err);
        res.status(500).json({ success: false, error: err.message });
    }
});

// Endpoint to receive video chunks and pipe DIRECTLY to FFmpeg stdin (Cleaned & Silent)
app.post('/stream', (req, res) => {
    try {
        const chunkIndexRaw = req.headers['x-chunk-index'] || '0';
        const chunkIndex = parseInt(chunkIndexRaw);

        // Direct memory pipe to FFmpeg stdin
        if (ffmpegLiveProcess && ffmpegLiveProcess.stdin && ffmpegLiveProcess.stdin.writable) {
            try {
                ffmpegLiveProcess.stdin.write(req.body);
            } catch (writeErr) {
                // Suppress stdin write catch
            }
        }

        res.json({ success: true, chunkIndex });
    } catch (err) {
        console.error('Error piping stream chunk:', err);
        res.status(500).json({ success: false, error: err.message });
    }
});

server.listen(PORT, () => {
    console.log(`🚀 Server running in [${NODE_ENV}] mode at ${APP_URL} (Port ${PORT})`);
});
