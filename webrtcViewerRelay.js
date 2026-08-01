import { RTCPeerConnection } from 'werift';
import logger from './logger.js';

export class WebRtcViewerSession {
    constructor(streamKey, ws) {
        this.streamKey = streamKey;
        this.ws = ws;
        this.pc = null;
        this.videoSender = null;
        this.audioSender = null;
        this.alive = true;
    }

    async handleOffer(sdpOffer) {
        this.pc = new RTCPeerConnection({
            iceServers: [{ urls: 'stun:stun.l.google.com:19302' }]
        });

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

        logger.info(`[Viewer Relay ${this.streamKey}] SDP answer sent. VideoSender=${!!this.videoSender} AudioSender=${!!this.audioSender}`);
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
                if (!this._loggedSilent) {
                    this._loggedSilent = true;
                    logger.warn(`[Viewer Relay ${this.streamKey}] sendRtp blocked: dtls=${dtlsState} codec=${codecSet}`);
                }
                return;
            }
            this.videoSender.sendRtp(rtp);
        } catch (e) {
            logger.error(`[Viewer Relay ${this.streamKey}] sendRtp error: ${e.message}`);
        }
    }

    sendAudioRtp(rtp) {
        if (!this.alive || !this.audioSender) return;
        try {
            this.audioSender.sendRtp(rtp);
        } catch (e) { }
    }

    close() {
        this.alive = false;
        if (this.pc) { try { this.pc.close(); } catch (e) { } this.pc = null; }
    }
}
