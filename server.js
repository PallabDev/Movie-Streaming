import express from 'express';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';
import { exec, spawn } from 'child_process';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3000;

// Directories setup
const mediaDir = path.join(__dirname, 'media');
const liveDir = path.join(__dirname, 'public', 'live');

if (!fs.existsSync(mediaDir)) {
    fs.mkdirSync(mediaDir, { recursive: true });
}
if (!fs.existsSync(liveDir)) {
    fs.mkdirSync(liveDir, { recursive: true });
}

// Configure EJS view engine
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));

// Static folder setup
app.use(express.static(path.join(__dirname, 'public')));
app.use('/media', express.static(mediaDir));

// Middleware for parsing raw video/binary stream data up to 100MB per chunk
app.use('/stream', express.raw({ type: '*/*', limit: '100mb' }));
app.use(express.json());

// Active FFmpeg process & compilation locks
let ffmpegLiveProcess = null;
let activeCompilePromise = null;

// Helper function to safely clean media folder without locking crashes
function clearMediaFolder() {
    if (fs.existsSync(mediaDir)) {
        const files = fs.readdirSync(mediaDir);
        for (const file of files) {
            try {
                fs.unlinkSync(path.join(mediaDir, file));
            } catch (err) {
                console.warn(`Could not delete media file ${file}:`, err.message);
            }
        }
    }
}

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

// Start FFmpeg process for Low-Latency HLS live generation
function startFfmpegLive() {
    stopFfmpegLive();
    clearLiveFolder();

    const hlsPath = path.join(liveDir, 'stream.m3u8');
    
    // FFmpeg options for low latency HLS generation from raw WebM stdin pipe
    const args = [
        '-y',
        '-f', 'matroska',            // WebM / Matroska format hint for unseekable pipe
        '-probesize', '32k',         // Minimal probe size to start HLS immediately
        '-analyzeduration', '0',
        '-i', 'pipe:0',
        '-c:v', 'libx264',
        '-preset', 'ultrafast',
        '-tune', 'zerolatency',
        '-g', '30',
        '-sc_threshold', '0',
        '-c:a', 'aac',
        '-b:a', '192k',
        '-f', 'hls',
        '-hls_time', '1',
        '-hls_list_size', '5',
        '-hls_flags', 'delete_segments+omit_endlist+independent_segments',
        '-hls_segment_type', 'mpegts',
        hlsPath
    ];

    console.log('Spawning FFmpeg Live HLS generator...');
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
            if (msg.includes('Error') || msg.includes('Opening')) {
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

function compileStream() {
    if (activeCompilePromise) {
        console.log('Reusing active FFmpeg compilation task...');
        return activeCompilePromise;
    }

    activeCompilePromise = new Promise((resolve, reject) => {
        try {
            const files = fs.readdirSync(mediaDir)
                .filter(f => f.startsWith('chunk_') && f.endsWith('.webm'))
                .sort((a, b) => a.localeCompare(b));

            if (files.length === 0) {
                activeCompilePromise = null;
                return reject(new Error('No stream chunks found to compile.'));
            }

            const rawCombinedPath = path.join(mediaDir, 'raw_combined.webm');
            const outputFilePath = path.join(mediaDir, `recompiled_recording_${Date.now()}.mp4`);

            // Combine raw binary byte stream of WebM chunks into raw_combined.webm
            const writeStream = fs.createWriteStream(rawCombinedPath);
            for (const file of files) {
                const chunkData = fs.readFileSync(path.join(mediaDir, file));
                writeStream.write(chunkData);
            }
            writeStream.end();

            writeStream.on('finish', () => {
                const ffmpegCmd = `ffmpeg -y -i "${rawCombinedPath}" -c:v libx264 -preset ultrafast -c:a aac -b:a 256k "${outputFilePath}"`;
                console.log('Running FFmpeg:', ffmpegCmd);

                exec(ffmpegCmd, (error, stdout, stderr) => {
                    if (error) {
                        console.error('FFmpeg execution error:', error, stderr);
                        activeCompilePromise = null;
                        return resolve(rawCombinedPath);
                    }

                    console.log('FFmpeg compilation completed successfully!');
                    activeCompilePromise = null;
                    resolve(outputFilePath);
                });
            });

            writeStream.on('error', (err) => {
                activeCompilePromise = null;
                reject(err);
            });
        } catch (err) {
            activeCompilePromise = null;
            reject(err);
        }
    });

    return activeCompilePromise;
}

// Routes
app.get('/', (req, res) => {
    res.render('index', {
        title: 'Movie Streaming - Broadcaster Studio'
    });
});

// Live Watch Page Route (handles both /live and /live/)
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
    const hlsPlaylist = path.join(liveDir, 'stream.m3u8');
    const isLive = fs.existsSync(hlsPlaylist) && ffmpegLiveProcess !== null;
    res.json({ live: isLive });
});

// Endpoint to reset media & live folders when a new stream starts
app.post('/reset-stream', (req, res) => {
    try {
        activeCompilePromise = null;
        clearMediaFolder();
        startFfmpegLive();
        res.json({ success: true, message: 'Stream reset and live FFmpeg started' });
    } catch (err) {
        console.error('Error resetting stream:', err);
        res.status(500).json({ success: false, error: err.message });
    }
});

// Endpoint to receive video chunks
app.post('/stream', (req, res) => {
    try {
        const chunkIndex = req.headers['x-chunk-index'] || Date.now();
        const paddedIndex = String(chunkIndex).padStart(6, '0');
        const chunkPath = path.join(mediaDir, `chunk_${paddedIndex}.webm`);

        // 1. Save chunk to media directory
        fs.writeFileSync(chunkPath, req.body);

        // 2. Pipe chunk directly to FFmpeg Live stdin safely
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

        console.log(`Saved & piped chunk: chunk_${paddedIndex}.webm (${req.body.length} bytes)`);
        res.json({ success: true, chunkIndex });
    } catch (err) {
        console.error('Error handling stream chunk:', err);
        res.status(500).json({ success: false, error: err.message });
    }
});

// Endpoint to recompile chunks using ffmpeg and trigger download
app.get('/download', async (req, res) => {
    try {
        const filePath = await compileStream();
        const ext = path.extname(filePath);
        const fileName = `screen_recording${ext}`;
        
        res.download(filePath, fileName, (err) => {
            if (err && !res.headersSent) {
                console.error('Download sending error:', err);
            }
        });
    } catch (err) {
        console.error('Error in /download:', err);
        if (!res.headersSent) {
            res.status(500).send(err.message || 'Server error during download processing.');
        }
    }
});

app.listen(PORT, () => {
    console.log(`Server is running on http://localhost:${PORT}`);
});
