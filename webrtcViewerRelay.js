import { RTCPeerConnection, RtpPacket, useH264, useOPUS, useVP8 } from 'werift';
import logger from './logger.js';

const WEBRTC_ICE_SERVERS = [{ urls: 'stun:stun.l.google.com:19302' }];

function createRelayPeerConnection(videoCodec = 'vp8') {
    const preferH264 = videoCodec === 'h264';
    return new RTCPeerConnection({
        iceServers: WEBRTC_ICE_SERVERS,
        codecs: {
            audio: [useOPUS()],
            video: preferH264 ? [useH264(), useVP8()] : [useVP8(), useH264()]
        }
    });
}

function extractNegotiatedPt(sdp, mediaType) {
    const mediaLineRegex = new RegExp(`m=${mediaType}.*SAVPF\\s+([\\d\\s]+)`);
    const match = sdp.match(mediaLineRegex);
    if (!match) return null;
    const pts = match[1].trim().split(/\s+/).map(Number);
    return pts[0];
}

export class WebRtcViewerSession {
    constructor(streamKey, ws, videoCodec = 'vp8', onKeyframeNeeded = null) {
        this.streamKey = streamKey;
        this.ws = ws;
        this.videoCodec = videoCodec;
        this.onKeyframeNeeded = onKeyframeNeeded;
        this.pc = null;
        this.videoSender = null;
        this.audioSender = null;
        this.alive = true;
        this.videoSsrc = 0;
        this.audioSsrc = 0;
        this.videoPt = 0;
        this.audioPt = 0;
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
            if (state === 'failed' || state === 'closed') {
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
                this.videoSsrc = t.sender.ssrc;
                this.videoSender.onPictureLossIndication.subscribe(() => this.requestKeyframe('viewer pli'));
                logger.info(`[Viewer Relay ${this.streamKey}] Video sender ssrc=${this.videoSsrc} track=${!!t.sender.track}`);
            } else if (t.kind === 'audio') {
                t.direction = 'sendonly';
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

        logger.info(`[Viewer Relay ${this.streamKey}] Negotiated: video PT=${this.videoPt} ssrc=${this.videoSsrc}, audio PT=${this.audioPt} ssrc=${this.audioSsrc} senderCodec=${JSON.stringify(this.videoSender?.codec)}`);

        if (this.ws && this.ws.readyState === 1) {
            this.ws.send(JSON.stringify({ type: 'VIEWER_WEBRTC_ANSWER', sdp: answer.sdp }));
        }

        logger.info(`[Viewer Relay ${this.streamKey}] SDP answer sent. Codec=${this.videoCodec}`);
        setTimeout(() => this.requestKeyframe('viewer joined'), 500);
        setTimeout(() => this.requestKeyframe('viewer warmup'), 2000);
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
            if (dtlsState !== 'connected') {
                this.stats.droppedPackets++;
                if (!this._loggedSilent) {
                    this._loggedSilent = true;
                    logger.warn(`[Viewer Relay ${this.streamKey}] sendRtp blocked: dtls=${dtlsState} codec=${!!this.videoSender.codec}`);
                }
                return;
            }

            const cloned = RtpPacket.deSerialize(rtp.serialize());
            cloned.header.ssrc = this.videoSsrc;
            cloned.header.payloadType = this.videoPt;

            this.videoSender.sendRtp(cloned).then(sent => {
                if (this.stats.videoPackets < 5) {
                    logger.info(`[Viewer Relay ${this.streamKey}] video packet sent OK: bytes=${sent}`);
                }
            }).catch(e => {
                this.stats.droppedPackets++;
                logger.error(`[Viewer Relay ${this.streamKey}] sendRtp error: ${e.message}`);
            });
            this.stats.videoPackets++;
            if (this.stats.videoPackets <= 5) {
                logger.info(`[Viewer Relay ${this.streamKey}] video packet queued: total=${this.stats.videoPackets} pt=${this.videoPt} ssrc=${this.videoSsrc} payloadLen=${cloned.payload?.length}`);
            }
        } catch (e) {
            this.stats.droppedPackets++;
            logger.error(`[Viewer Relay ${this.streamKey}] sendVideoRtp error: ${e.message}`);
        }
    }

    requestKeyframe(reason) {
        if (typeof this.onKeyframeNeeded !== 'function') return;
        try {
            this.onKeyframeNeeded(reason);
        } catch (e) { }
    }

    sendAudioRtp(rtp) {
        if (!this.alive || !this.audioSender) return;
        try {
            const cloned = RtpPacket.deSerialize(rtp.serialize());
            cloned.header.ssrc = this.audioSsrc;
            cloned.header.payloadType = this.audioPt;

            this.audioSender.sendRtp(cloned).catch(() => {
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
