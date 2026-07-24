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
    // Send current status immediately upon connection
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
                console.warn(`Could not delete live file ${file}:`, err.message);
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
            console.warn('Error stopping FFmpeg live process:', err.message);
        }
        ffmpegLiveProcess = null;
    }
}

// Start Multi-Quality ABR HLS Generator
function startFfmpegLive() {
    if (cleanupTimer) {
        clearTimeout(cleanupTimer);
        cleanupTimer = null;
    }
    
    stopFfmpegLive();
    clearLiveFolder();

    const masterPath = path.join(liveDir, 'master.m3u8');
    
    const args = [
        '-y',
        '-probesize', '64k',
        '-analyzeduration', '0',
        '-i', 'pipe:0',
        
        '-filter_complex',
        '[0:v]split=3[v1080][v720][v480];' +
        '[v1080]fps=30,scale=1920:-2[v1080out];' +
        '[v720]fps=30,scale=1280:-2[v720out];' +
        '[v480]fps=30,scale=854:-2[v480out]',

        // Rendition 0: Pristine 1080p 30fps
        '-map', '[v1080out]', '-c:v:0', 'libx264', '-preset', 'superfast', '-tune', 'zerolatency', '-profile:v:0', 'high', '-level:v:0', '4.1', '-crf:v:0', '19', '-maxrate:v:0', '12000k', '-bufsize:v:0', '20000k', '-g:v:0', '30', '-sc_threshold:v:0', '0',
        '-map', '0:a?', '-c:a:0', 'aac', '-b:a:0', '192k',
        
        // Rendition 1: Clean 720p 30fps
        '-map', '[v720out]', '-c:v:1', 'libx264', '-preset', 'superfast', '-tune', 'zerolatency', '-profile:v:1', 'main', '-crf:v:1', '22', '-maxrate:v:1', '4000k', '-bufsize:v:1', '7000k', '-g:v:1', '30', '-sc_threshold:v:1', '0',
        '-map', '0:a?', '-c:a:1', 'aac', '-b:a:1', '128k',

        // Rendition 2: Fast 480p 30fps
        '-map', '[v480out]', '-c:v:2', 'libx264', '-preset', 'superfast', '-tune', 'zerolatency', '-crf:v:2', '25', '-maxrate:v:2', '1500k', '-bufsize:v:2', '3000k', '-g:v:2', '30', '-sc_threshold:v:2', '0',
        '-map', '0:a?', '-c:a:2', 'aac', '-b:a:2', '96k',

        '-f', 'hls',
        '-hls_time', '1',
        '-hls_list_size', '5',
        '-hls_flags', 'delete_segments+omit_endlist+independent_segments',
        '-hls_segment_type', 'mpegts',
        '-master_pl_name', 'master.m3u8',
        '-var_stream_map', 'v:0,a:0,name:1080p v:1,a:1,name:720p v:2,a:2,name:480p',
        path.join(liveDir, 'stream_%v.m3u8')
    ];

    console.log('Spawning Multi-Quality ABR FFmpeg Live Generator...');
    ffmpegLiveProcess = spawn('ffmpeg', args);

    if (ffmpegLiveProcess.stdin) {
        ffmpegLiveProcess.stdin.on('error', (err) => {
            if (err.code !== 'EPIPE' && err.code !== 'EOF') {
                console.warn('FFmpeg stdin pipe error:', err.message);
            }
        });
    }

    if (ffmpegLiveProcess.stderr) {
        ffmpegLiveProcess.stderr.on('data', (data) => {
            const msg = data.toString().trim();
            if (msg.includes('fps=') || msg.includes('speed=')) {
                const fpsMatch = msg.match(/fps=\s*([\d.]+)/);
                const speedMatch = msg.match(/speed=\s*([\d.x]+)/);
                const bitrateMatch = msg.match(/bitrate=\s*([\d.]+kbits\/s)/);
                const fps = fpsMatch ? fpsMatch[1] : 'N/A';
                const speed = speedMatch ? speedMatch[1] : 'N/A';
                const bitrate = bitrateMatch ? bitrateMatch[1] : 'N/A';
                console.log(`[FFmpeg Speed] FPS: ${fps} | Speed: ${speed} | Bitrate: ${bitrate}`);
            } else if (msg.includes('Error') || msg.includes('Opening')) {
                console.log('[FFmpeg Live]', msg);
            }
        });
    }

    ffmpegLiveProcess.on('error', (err) => {
        console.error('FFmpeg Live process error:', err);
        ffmpegLiveProcess = null;
    });

    ffmpegLiveProcess.on('exit', (code, signal) => {
        console.log(`FFmpeg Live process exited with code ${code}, signal ${signal}`);
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
        console.log(`Stream stopped by host. Scheduling HLS segment cleanup in ${CLEANUP_DELAY_MS / 60000} minutes...`);

        if (cleanupTimer) clearTimeout(cleanupTimer);
        cleanupTimer = setTimeout(() => {
            console.log('10 minutes elapsed. Cleaning up live HLS segment files...');
            stopFfmpegLive();
            clearLiveFolder();
        }, CLEANUP_DELAY_MS);

        res.json({ success: true, message: 'Stream stopped. Deletion scheduled in 10 minutes.' });
    } catch (err) {
        console.error('Error stopping stream:', err);
        res.status(500).json({ success: false, error: err.message });
    }
});

// Endpoint to receive video chunks and pipe DIRECTLY to FFmpeg stdin (No disk storage!)
app.post('/stream', (req, res) => {
    try {
        const chunkIndexRaw = req.headers['x-chunk-index'] || '0';
        const chunkIndex = parseInt(chunkIndexRaw);
        const startTime = Date.now();

        // Direct memory pipe to FFmpeg stdin - NO CHUNK WRITTEN TO DISK!
        if (ffmpegLiveProcess && ffmpegLiveProcess.stdin && ffmpegLiveProcess.stdin.writable) {
            try {
                ffmpegLiveProcess.stdin.write(req.body, (err) => {
                    if (err && err.code !== 'EPIPE' && err.code !== 'EOF') {
                        console.warn('FFmpeg stdin write warning:', err.message);
                    }
                });
            } catch (writeErr) {
                console.warn('FFmpeg stdin write catch:', writeErr.message);
            }
        }

        const elapsed = Date.now() - startTime;
        console.log(`Piped chunk #${chunkIndex} directly to FFmpeg (${req.body.length} bytes) in ${elapsed}ms | Disk write: 0 bytes`);
        res.json({ success: true, chunkIndex });
    } catch (err) {
        console.error('Error piping stream chunk:', err);
        res.status(500).json({ success: false, error: err.message });
    }
});

server.listen(PORT, () => {
    console.log(`Server running in [${NODE_ENV}] mode at ${APP_URL} (Port ${PORT})`);
});
