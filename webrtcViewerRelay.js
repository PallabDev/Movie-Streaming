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

        this.pc.onIceConnectionStateChange.subscribe(() => {
            const state = this.pc.iceConnectionState;
            logger.info(`[Viewer Relay ${this.streamKey}] ICE state: ${state}`);
            if (state === 'failed' || state === 'disconnected' || state === 'closed') {
                this.alive = false;
            }
        });

        await this.pc.setRemoteDescription({ type: 'offer', sdp: sdpOffer });

        const transceivers = this.pc.getTransceivers();
        for (const t of transceivers) {
            if (t.kind === 'video') {
                t.direction = 'sendonly';
                this.videoSender = t.sender;
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
