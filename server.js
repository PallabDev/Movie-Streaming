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

// Active FFmpeg processes for separate 720p & 480p variant logging
let ffmpeg720Process = null;
let ffmpeg480Process = null;

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

// Stop active FFmpeg live processes
function stopFfmpegLive() {
    if (ffmpeg720Process) {
        try {
            if (ffmpeg720Process.stdin) {
                ffmpeg720Process.stdin.removeAllListeners('error');
                ffmpeg720Process.stdin.on('error', () => {});
                ffmpeg720Process.stdin.end();
            }
            ffmpeg720Process.kill('SIGKILL');
        } catch (err) {}
        ffmpeg720Process = null;
    }

    if (ffmpeg480Process) {
        try {
            if (ffmpeg480Process.stdin) {
                ffmpeg480Process.stdin.removeAllListeners('error');
                ffmpeg480Process.stdin.on('error', () => {});
                ffmpeg480Process.stdin.end();
            }
            ffmpeg480Process.kill('SIGKILL');
        } catch (err) {}
        ffmpeg480Process = null;
    }
}

// Create HLS Master Playlist for Adaptive Bitrate Switching
function writeMasterPlaylist() {
    const masterPath = path.join(liveDir, 'master.m3u8');
    const content = `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-STREAM-INF:BANDWIDTH=3800000,RESOLUTION=1280x720,NAME="720p"
stream_720p.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=1200000,RESOLUTION=854x480,NAME="480p"
stream_480p.m3u8
`;
    fs.writeFileSync(masterPath, content);
}

// Start Separate 720p & 480p FFmpeg Generators for Variant-Wise Speed Logging
function startFfmpegLive() {
    if (cleanupTimer) {
        clearTimeout(cleanupTimer);
        cleanupTimer = null;
    }
    
    stopFfmpegLive();
    clearLiveFolder();
    writeMasterPlaylist();

    // 1. FFmpeg Process for 720p Stream
    const args720 = [
        '-y', '-probesize', '64k', '-analyzeduration', '0', '-i', 'pipe:0',
        '-vf', 'scale=1280:-2', '-r', '30',
        '-c:v', 'libx264', '-preset', 'ultrafast', '-tune', 'zerolatency', '-profile:v', 'main',
        '-b:v', '3800k', '-maxrate', '4500k', '-bufsize', '7000k', '-g', '30', '-sc_threshold', '0',
        '-c:a', 'aac', '-b:a', '160k',
        '-f', 'hls', '-hls_time', '1', '-hls_list_size', '5',
        '-hls_flags', 'delete_segments+omit_endlist+independent_segments',
        '-hls_segment_type', 'mpegts',
        path.join(liveDir, 'stream_720p.m3u8')
    ];

    // 2. FFmpeg Process for 480p Stream
    const args480 = [
        '-y', '-probesize', '64k', '-analyzeduration', '0', '-i', 'pipe:0',
        '-vf', 'scale=854:-2', '-r', '30',
        '-c:v', 'libx264', '-preset', 'ultrafast', '-tune', 'zerolatency', '-profile:v', 'baseline',
        '-b:v', '1200k', '-maxrate', '1500k', '-bufsize', '2500k', '-g', '30', '-sc_threshold', '0',
        '-c:a', 'aac', '-b:a', '96k',
        '-f', 'hls', '-hls_time', '1', '-hls_list_size', '5',
        '-hls_flags', 'delete_segments+omit_endlist+independent_segments',
        '-hls_segment_type', 'mpegts',
        path.join(liveDir, 'stream_480p.m3u8')
    ];

    console.log('⚡ Spawning 720p & 480p Separate Variant Live Stream Generators...');
    ffmpeg720Process = spawn('ffmpeg', args720);
    ffmpeg480Process = spawn('ffmpeg', args480);

    let last720Log = 0;
    let last480Log = 0;

    // Listen to 720p Speed Log
    if (ffmpeg720Process.stderr) {
        ffmpeg720Process.stderr.on('data', (data) => {
            const msg = data.toString().trim();
            if (msg.includes('fps=') || msg.includes('speed=')) {
                const now = Date.now();
                if (now - last720Log > 1500) {
                    last720Log = now;
                    const fpsMatch = msg.match(/fps=\s*([\d.]+)/);
                    const speedMatch = msg.match(/speed=\s*([\d.x]+)/);
                    const fps = fpsMatch ? fpsMatch[1] : 'N/A';
                    const speed = speedMatch ? speedMatch[1] : '1.0x';
                    console.log(`[720p Stream] Speed: ${speed} | FPS: ${fps}`);
                }
            }
        });
    }

    // Listen to 480p Speed Log
    if (ffmpeg480Process.stderr) {
        ffmpeg480Process.stderr.on('data', (data) => {
            const msg = data.toString().trim();
            if (msg.includes('fps=') || msg.includes('speed=')) {
                const now = Date.now();
                if (now - last480Log > 1500) {
                    last480Log = now;
                    const fpsMatch = msg.match(/fps=\s*([\d.]+)/);
                    const speedMatch = msg.match(/speed=\s*([\d.x]+)/);
                    const fps = fpsMatch ? fpsMatch[1] : 'N/A';
                    const speed = speedMatch ? speedMatch[1] : '1.0x';
                    console.log(`[480p Stream] Speed: ${speed} | FPS: ${fps}`);
                }
            }
        });
    }

    ffmpeg720Process.on('exit', () => { ffmpeg720Process = null; });
    ffmpeg480Process.on('exit', () => { ffmpeg480Process = null; });
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

// Endpoint to receive video chunks and pipe DIRECTLY to BOTH 720p & 480p FFmpeg stdins
app.post('/stream', (req, res) => {
    try {
        const chunkIndexRaw = req.headers['x-chunk-index'] || '0';
        const chunkIndex = parseInt(chunkIndexRaw);

        // Pipe to 720p FFmpeg process
        if (ffmpeg720Process && ffmpeg720Process.stdin && ffmpeg720Process.stdin.writable) {
            try { ffmpeg720Process.stdin.write(req.body); } catch (e) {}
        }

        // Pipe to 480p FFmpeg process
        if (ffmpeg480Process && ffmpeg480Process.stdin && ffmpeg480Process.stdin.writable) {
            try { ffmpeg480Process.stdin.write(req.body); } catch (e) {}
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
