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

    async initialize() {
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

        this.pc.onIceConnectionStateChange.subscribe(() => {
            const state = this.pc.iceConnectionState;
            if (state === 'failed' || state === 'disconnected' || state === 'closed') {
                this.alive = false;
            }
        });

        const videoTransceiver = this.pc.addTransceiver('video', { direction: 'sendonly' });
        this.videoSender = videoTransceiver.sender;

        const audioTransceiver = this.pc.addTransceiver('audio', { direction: 'sendonly' });
        this.audioSender = audioTransceiver.sender;
    }

    async handleOffer(sdpOffer) {
        if (!this.pc) await this.initialize();

        await this.pc.setRemoteDescription({ type: 'offer', sdp: sdpOffer });
        const answer = await this.pc.createAnswer();
        await this.pc.setLocalDescription(answer);

        if (this.ws && this.ws.readyState === 1) {
            this.ws.send(JSON.stringify({ type: 'VIEWER_WEBRTC_ANSWER', sdp: answer.sdp }));
        }

        logger.info(`[Viewer Relay ${this.streamKey}] SDP answer sent`);
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
        try { this.videoSender.sendRtp(rtp); } catch (e) { }
    }

    sendAudioRtp(rtp) {
        if (!this.alive || !this.audioSender) return;
        try { this.audioSender.sendRtp(rtp); } catch (e) { }
    }

    close() {
        this.alive = false;
        if (this.pc) { try { this.pc.close(); } catch (e) { } this.pc = null; }
    }
}
