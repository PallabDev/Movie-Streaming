import { RTCPeerConnection, useH264, useOPUS, useVP8 } from 'werift';
import dgram from 'dgram';
import fs from 'fs';
import path from 'path';
import logger from './logger.js';

const WEBRTC_ICE_SERVERS = [{ urls: 'stun:stun.l.google.com:19302' }];
const WEBRTC_CODECS = {
    audio: [useOPUS()],
    video: [useH264(), useVP8()]
};

export class WebRtcIngestSession {
    constructor(session, ws, onWebRtcReady) {
        this.session = session;
        this.ws = ws;
        this.onWebRtcReady = onWebRtcReady;
        this.pc = null;
        this.videoSocket = null;
        this.audioSocket = null;
        this.videoPort = 0;
        this.audioPort = 0;
        this.videoPt = 96;
        this.audioPt = 111;
        this.videoCodec = 'vp8';
        this.stats = { bytes: 0, packets: 0 };
        this.onVideoRtp = null;
        this.onAudioRtp = null;
    }

    async initialize() {
        this.videoSocket = dgram.createSocket('udp4');
        this.audioSocket = dgram.createSocket('udp4');

        this.videoPort = 50000 + Math.floor(Math.random() * 15000);
        this.audioPort = 50000 + Math.floor(Math.random() * 15000);

        logger.info(`[WebRTC ${this.session.streamKey}] Allocated UDP ports for FFmpeg: Video=${this.videoPort}, Audio=${this.audioPort}`);

        this.pc = new RTCPeerConnection({
            iceServers: WEBRTC_ICE_SERVERS,
            codecs: WEBRTC_CODECS
        });

        this.pc.onIceCandidate.subscribe(candidate => {
            if (candidate && this.ws && this.ws.readyState === 1) {
                try {
                    this.ws.send(JSON.stringify({ type: 'WEBRTC_ICE_CANDIDATE', candidate }));
                } catch (e) { }
            }
        });

        this.pc.onTrack.subscribe(track => {
            logger.info(`[WebRTC ${this.session.streamKey}] Received remote track: kind=${track.kind}`);
            if (track.kind === 'video') {
                track.onReceiveRtp.subscribe(rtp => {
                    const buf = rtp.serialize();
                    this.stats.bytes += buf.length;
                    this.stats.packets++;
                    if (this.videoSocket) {
                        try { this.videoSocket.send(buf, this.videoPort, '127.0.0.1'); } catch (e) { }
                    }
                    if (this.onVideoRtp) {
                        try { this.onVideoRtp(rtp); } catch (e) { }
                    }
                });
            } else if (track.kind === 'audio') {
                track.onReceiveRtp.subscribe(rtp => {
                    const buf = rtp.serialize();
                    this.stats.bytes += buf.length;
                    this.stats.packets++;
                    if (this.audioSocket) {
                        try { this.audioSocket.send(buf, this.audioPort, '127.0.0.1'); } catch (e) { }
                    }
                    if (this.onAudioRtp) {
                        try { this.onAudioRtp(rtp); } catch (e) { }
                    }
                });
            }
        });
    }

    async handleOffer(sdpOffer) {
        if (!this.pc) await this.initialize();

        const vMatch = sdpOffer.match(/a=rtpmap:(\d+)\s+(VP8|H264|vp8|h264)\/90000/i);
        if (vMatch) {
            this.videoPt = parseInt(vMatch[1]);
            this.videoCodec = vMatch[2].toLowerCase();
        }
        const aMatch = sdpOffer.match(/a=rtpmap:(\d+) opus\/48000/i);
        if (aMatch) this.audioPt = parseInt(aMatch[1]);

        this.writeSdpFile();

        if (typeof this.onWebRtcReady === 'function') {
            await this.onWebRtcReady();
        }

        await this.pc.setRemoteDescription({ type: 'offer', sdp: sdpOffer });
        const answer = await this.pc.createAnswer();
        await this.pc.setLocalDescription(answer);

        if (this.ws && this.ws.readyState === 1) {
            this.ws.send(JSON.stringify({ type: 'WEBRTC_ANSWER', sdp: answer.sdp }));
        }

        logger.info(`[WebRTC ${this.session.streamKey}] SDP Answer sent. VideoPT=${this.videoPt} (${this.videoCodec}), AudioPT=${this.audioPt}`);
    }

    async handleIceCandidate(candidate) {
        if (this.pc && candidate) {
            try { await this.pc.addIceCandidate(candidate); } catch (e) {
                logger.warn(`[WebRTC ${this.session.streamKey}] Error adding ICE candidate: ${e.message}`);
            }
        }
    }

    writeSdpFile() {
        const codecName = this.videoCodec === 'h264' ? 'H264/90000' : 'VP8/90000';
        const sdpContent = [
            'v=0',
            'o=- 0 0 IN IP4 127.0.0.1',
            's=WebRTC Stream',
            'c=IN IP4 127.0.0.1',
            't=0 0',
            `m=video ${this.videoPort} RTP/AVP ${this.videoPt}`,
            `a=rtpmap:${this.videoPt} ${codecName}`,
            `m=audio ${this.audioPort} RTP/AVP ${this.audioPt}`,
            `a=rtpmap:${this.audioPt} opus/48000/2`,
            ''
        ].join('\n');

        const sdpPath = path.join(this.session.liveDir, 'input.sdp');
        fs.writeFileSync(sdpPath, sdpContent, 'utf-8');
        logger.info(`[WebRTC ${this.session.streamKey}] Created input.sdp (${this.videoCodec}) at ${sdpPath}`);
    }

    close() {
        if (this.pc) { try { this.pc.close(); } catch (e) { } this.pc = null; }
        if (this.videoSocket) { try { this.videoSocket.close(); } catch (e) { } this.videoSocket = null; }
        if (this.audioSocket) { try { this.audioSocket.close(); } catch (e) { } this.audioSocket = null; }
    }
}
