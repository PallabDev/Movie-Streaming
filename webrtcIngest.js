import { RTCPeerConnection, useH264, useOPUS, useVP8 } from 'werift';
import dgram from 'dgram';
import fs from 'fs';
import path from 'path';
import logger from './logger.js';

const WEBRTC_ICE_SERVERS = [
    { urls: 'stun:stun.l.google.com:19302' },
    { urls: 'stun:stun1.l.google.com:19302' },
    { urls: 'turn:openrelay.metered.ca:80', username: 'openrelayproject', credential: 'openrelayproject' },
    { urls: 'turn:openrelay.metered.ca:443', username: 'openrelayproject', credential: 'openrelayproject' },
    { urls: 'turn:openrelay.metered.ca:443?transport=tcp', username: 'openrelayproject', credential: 'openrelayproject' }
];
const WEBRTC_CODECS = {
    audio: [useOPUS()],
    video: [useVP8(), useH264()]
};

function extractNegotiatedPt(sdp, mediaType) {
    const mediaLineRegex = new RegExp(`m=${mediaType}.*SAVPF\\s+([\\d\\s]+)`);
    const match = sdp.match(mediaLineRegex);
    if (!match) return null;
    const pts = match[1].trim().split(/\s+/).map(Number);
    return pts[0];
}

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
            codecs: WEBRTC_CODECS,
            iceTransportPolicy: 'relay'
        });

        this.pc.onIceCandidate.subscribe(candidate => {
            if (candidate && this.ws && this.ws.readyState === 1) {
                try {
                    this.ws.send(JSON.stringify({ type: 'WEBRTC_ICE_CANDIDATE', candidate }));
                } catch (e) { }
            }
        });

        this.pc.iceConnectionStateChange.subscribe(state => {
            logger.info(`[WebRTC ${this.session.streamKey}] ICE connection state: ${state}`);
        });

        this.pc.onTrack.subscribe(track => {
            logger.info(`[WebRTC ${this.session.streamKey}] Received remote track: kind=${track.kind}`);
            if (track.kind === 'video') {
                let videoPacketCount = 0;
                track.onReceiveRtp.subscribe(rtp => {
                    const buf = rtp.serialize();
                    this.stats.bytes += buf.length;
                    this.stats.packets++;
                    if (this.videoSocket) {
                        try { this.videoSocket.send(buf, this.videoPort, '127.0.0.1'); } catch (e) { }
                    }
                    if (this.onVideoRtp) {
                        try { this.onVideoRtp(rtp); } catch (e) { 
                            logger.error(`[WebRTC ${this.session.streamKey}] onVideoRtp callback error: ${e.message}`);
                        }
                    }
                    videoPacketCount++;
                    if (videoPacketCount <= 3) {
                        logger.info(`[WebRTC ${this.session.streamKey}] ingest video RTP #${videoPacketCount}: pt=${rtp.header.payloadType} ssrc=${rtp.header.ssrc} len=${rtp.payload?.length} hasCallback=${!!this.onVideoRtp}`);
                    }
                });
            } else if (track.kind === 'audio') {
                let audioPacketCount = 0;
                track.onReceiveRtp.subscribe(rtp => {
                    const buf = rtp.serialize();
                    this.stats.bytes += buf.length;
                    this.stats.packets++;
                    if (this.audioSocket) {
                        try { this.audioSocket.send(buf, this.audioPort, '127.0.0.1'); } catch (e) { }
                    }
                    if (this.onAudioRtp) {
                        try { this.onAudioRtp(rtp); } catch (e) { 
                            logger.error(`[WebRTC ${this.session.streamKey}] onAudioRtp callback error: ${e.message}`);
                        }
                    }
                    audioPacketCount++;
                    if (audioPacketCount <= 3) {
                        logger.info(`[WebRTC ${this.session.streamKey}] ingest audio RTP #${audioPacketCount}: pt=${rtp.header.payloadType} ssrc=${rtp.header.ssrc}`);
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

        const transceivers = this.pc.getTransceivers();
        for (const t of transceivers) {
            if (t.kind === 'video') {
                this.videoSender = t.sender;
                this.videoSsrc = t.sender.ssrc;
            } else if (t.kind === 'audio') {
                t.direction = 'sendrecv';
                this.audioSender = t.sender;
                this.audioSsrc = t.sender.ssrc;
            }
        }

        const answer = await this.pc.createAnswer();
        await this.pc.setLocalDescription(answer);

        this.videoPt = extractNegotiatedPt(answer.sdp, 'video') || 96;
        this.audioPt = extractNegotiatedPt(answer.sdp, 'audio') || 111;

        if (this.videoSender) {
            this.videoSender.codec = { payloadType: this.videoPt };
        }
        if (this.audioSender) {
            this.audioSender.codec = { payloadType: this.audioPt };
        }

        if (this.ws && this.ws.readyState === 1) {
            this.ws.send(JSON.stringify({ type: 'WEBRTC_ANSWER', sdp: answer.sdp }));
        }

        logger.info(`[WebRTC ${this.session.streamKey}] SDP Answer sent. VideoPT=${this.videoPt} (${this.videoCodec}), AudioPT=${this.audioPt} AudioSSRC=${this.audioSsrc}`);
    }

    async handleIceCandidate(candidate) {
        if (this.pc && candidate) {
            try { await this.pc.addIceCandidate(candidate); } catch (e) {
                logger.warn(`[WebRTC ${this.session.streamKey}] Error adding ICE candidate: ${e.message}`);
            }
        }
    }

    requestVideoKeyframe() {
        try {
            const receiver = this.pc?.getReceivers?.().find(r => r.kind === 'video' || r.track?.kind === 'video');
            const mediaSsrc = receiver?.track?.ssrc || Object.keys(receiver?.remoteStreams || {})[0];
            if (!receiver || !mediaSsrc || typeof receiver.sendRtcpPLI !== 'function') return false;
            receiver.sendRtcpPLI(Number(mediaSsrc)).catch(() => { });
            logger.info(`[WebRTC ${this.session.streamKey}] Requested video keyframe from host`);
            return true;
        } catch (e) {
            logger.warn(`[WebRTC ${this.session.streamKey}] Failed to request keyframe: ${e.message}`);
            return false;
        }
    }

    sendAudioRtp(rtp) {
        if (!this.audioSender || !this.pc) return;
        try {
            if (this.audioSender.dtlsTransport?.state !== 'connected') return;

            const cloned = { header: { ...rtp.header, ssrc: this.audioSsrc, payloadType: this.audioPt }, payload: rtp.payload };

            this.audioSender.sendRtp(cloned).then(() => {
                if (!this._audioLogged) {
                    this._audioLogged = true;
                    logger.info(`[WebRTC Ingest ${this.session.streamKey}] Forwarded viewer audio packet to host successfully!`);
                }
            }).catch(e => {
                logger.error(`[WebRTC Ingest ${this.session.streamKey}] sendAudioRtp error: ${e.message}`);
            });
        } catch (e) {
            logger.error(`[WebRTC Ingest ${this.session.streamKey}] sendAudioRtp exception: ${e.message}`);
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
