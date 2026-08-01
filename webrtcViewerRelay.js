import { RTCPeerConnection, RtpPacket, useH264, useOPUS, useVP8 } from 'werift';
import logger from './logger.js';

const WEBRTC_ICE_SERVERS = [{ urls: 'stun:stun.l.google.com:19302' }];

function createRelayPeerConnection(videoCodec = 'h264') {
    const preferH264 = videoCodec === 'h264';
    return new RTCPeerConnection({
        iceServers: WEBRTC_ICE_SERVERS,
        codecs: {
            audio: [useOPUS()],
            video: preferH264 ? [useH264(), useVP8()] : [useVP8(), useH264()]
        }
    });
}

function cloneRtpPacket(rtp) {
    return RtpPacket.deSerialize(rtp.serialize());
}

export class WebRtcViewerSession {
    constructor(streamKey, ws, videoCodec = 'h264') {
        this.streamKey = streamKey;
        this.ws = ws;
        this.videoCodec = videoCodec;
        this.pc = null;
        this.videoSender = null;
        this.audioSender = null;
        this.alive = true;
        this.stats = { videoPackets: 0, audioPackets: 0, droppedPackets: 0 };
    }

    async handleOffer(sdpOffer) {
        this.pc = createRelayPeerConnection(this.videoCodec);

        this.pc.onIceCandidate.subscribe(candidate => {
            if (candidate && this.ws && this.ws.readyState === 1) {
                try {
                    this.ws.send(JSON.stringify({ type: 'VIEWER_ICE_CANDIDATE', candidate }));
                } catch (e) { }
            }
        });

        this.pc.iceConnectionStateChange.subscribe(state => {
            logger.info(`[Viewer Relay ${this.streamKey}] ICE state: ${state}`);
            if (state === 'failed' || state === 'disconnected' || state === 'closed') {
                this.alive = false;
            }
        });

        await this.pc.setRemoteDescription({ type: 'offer', sdp: sdpOffer });

        const transceivers = this.pc.getTransceivers();
        for (const t of transceivers) {
            logger.info(`[Viewer Relay ${this.streamKey}] Transceiver: kind=${t.kind} mid=${t.mid} direction=${t.direction}`);
            if (t.kind === 'video') {
                t.direction = 'sendonly';
                this.videoSender = t.sender;
                logger.info(`[Viewer Relay ${this.streamKey}] Video sender codec: ${JSON.stringify(t.sender.codec?.name || 'none')}`);
            } else if (t.kind === 'audio') {
                t.direction = 'sendonly';
                this.audioSender = t.sender;
            }
        }

        const answer = await this.pc.createAnswer();
        await this.pc.setLocalDescription(answer);

        if (this.ws && this.ws.readyState === 1) {
            this.ws.send(JSON.stringify({ type: 'VIEWER_WEBRTC_ANSWER', sdp: answer.sdp }));
        }

        logger.info(`[Viewer Relay ${this.streamKey}] SDP answer sent. Codec=${this.videoCodec} VideoSender=${!!this.videoSender} AudioSender=${!!this.audioSender}`);
    }

    async handleIceCandidate(candidate) {
        if (this.pc && candidate) {
            try { await this.pc.addIceCandidate(candidate); } catch (e) {
                logger.warn(`[Viewer Relay ${this.streamKey}] Error adding ICE candidate: ${e.message}`);
            }
        }
    }

    sendVideoRtp(rtp) {
        if (!this.alive || !this.videoSender) return;
        try {
            const dtlsState = this.videoSender.dtlsTransport?.state;
            const codecSet = !!this.videoSender.codec;
            if (!codecSet || dtlsState !== 'connected') {
                this.stats.droppedPackets++;
                if (!this._loggedSilent) {
                    this._loggedSilent = true;
                    logger.warn(`[Viewer Relay ${this.streamKey}] sendRtp blocked: dtls=${dtlsState} codec=${codecSet}`);
                }
                return;
            }
            this.videoSender.sendRtp(cloneRtpPacket(rtp)).catch(e => {
                this.stats.droppedPackets++;
                logger.error(`[Viewer Relay ${this.streamKey}] async video sendRtp error: ${e.message}`);
            });
            this.stats.videoPackets++;
        } catch (e) {
            this.stats.droppedPackets++;
            logger.error(`[Viewer Relay ${this.streamKey}] sendRtp error: ${e.message}`);
        }
    }

    sendAudioRtp(rtp) {
        if (!this.alive || !this.audioSender) return;
        try {
            this.audioSender.sendRtp(cloneRtpPacket(rtp)).catch(() => {
                this.stats.droppedPackets++;
            });
            this.stats.audioPackets++;
        } catch (e) {
            this.stats.droppedPackets++;
        }
    }

    close() {
        this.alive = false;
        if (this.pc) { try { this.pc.close(); } catch (e) { } this.pc = null; }
    }
}
