import mediasoup from 'mediasoup';
import os from 'os';
import logger from './logger.js';

// Configuration
const ANNOUNCED_IP = process.env.ANNOUNCED_IP || '222.167.207.36';
const MIN_PORT = parseInt(process.env.MEDIASOUP_MIN_PORT || '40000', 10);
const MAX_PORT = parseInt(process.env.MEDIASOUP_MAX_PORT || '40100', 10);

const mediaCodecs = [
    {
        kind: 'audio',
        mimeType: 'audio/opus',
        clockRate: 48000,
        channels: 2,
        parameters: {
            'sprop-stereo': 1,
            'stereo': 1,
            'maxaveragebitrate': 510000,
            'useinbandfec': 1
        }
    },
    {
        kind: 'video',
        mimeType: 'video/VP8',
        clockRate: 90000,
        parameters: {
            'x-google-start-bitrate': 2000
        }
    },
    {
        kind: 'video',
        mimeType: 'video/H264',
        clockRate: 90000,
        parameters: {
            'packetization-mode': 1,
            'profile-level-id': '42e01f',
            'level-asymmetry-allowed': 1
        }
    }
];

class MediasoupManager {
    constructor() {
        this.workers = [];
        this.nextWorkerIdx = 0;
        this.routers = new Map(); // streamKey -> Router
        this.transports = new Map(); // transportId -> WebRtcTransport
        this.producers = new Map(); // producerId -> Producer
        this.consumers = new Map(); // consumerId -> Consumer
        this.sessionProducers = new Map(); // streamKey -> Set of producerIds
    }

    async init() {
        const numWorkers = Math.min(os.cpus().length || 1, 4);
        logger.info(`[Mediasoup] Initializing ${numWorkers} worker(s)...`);

        for (let i = 0; i < numWorkers; i++) {
            const worker = await mediasoup.createWorker({
                logLevel: 'warn',
                logTags: ['info', 'ice', 'dtls', 'rtp', 'srtp', 'rtcp'],
                rtcMinPort: MIN_PORT,
                rtcMaxPort: MAX_PORT
            });

            worker.on('died', (error) => {
                logger.error(`[Mediasoup] Worker died, exiting process...: ${error}`);
                setTimeout(() => process.exit(1), 2000);
            });

            this.workers.push(worker);
        }

        logger.info(`[Mediasoup] ${numWorkers} worker(s) successfully created. Ports=${MIN_PORT}-${MAX_PORT}, AnnouncedIP=${ANNOUNCED_IP}`);
    }

    getNextWorker() {
        const worker = this.workers[this.nextWorkerIdx];
        this.nextWorkerIdx = (this.nextWorkerIdx + 1) % this.workers.length;
        return worker;
    }

    async getOrCreateRouter(streamKey) {
        if (!streamKey) streamKey = 'default';
        if (this.routers.has(streamKey)) {
            return this.routers.get(streamKey);
        }

        const worker = this.getNextWorker();
        const router = await worker.createRouter({ mediaCodecs });

        logger.info(`[Mediasoup ${streamKey}] Created Router id=${router.id}`);
        this.routers.set(streamKey, router);
        this.sessionProducers.set(streamKey, new Set());
        return router;
    }

    async createWebRtcTransport(streamKey) {
        const router = await this.getOrCreateRouter(streamKey);

        const transport = await router.createWebRtcTransport({
            listenIps: [
                {
                    ip: '0.0.0.0',
                    announcedIp: ANNOUNCED_IP
                }
            ],
            enableUdp: true,
            enableTcp: true,
            preferUdp: true
        });

        logger.info(`[Mediasoup ${streamKey}] Created WebRtcTransport id=${transport.id}`);

        transport.on('dtlsstatechange', (dtlsState) => {
            logger.info(`[Mediasoup ${streamKey}] Transport ${transport.id} DTLS state: ${dtlsState}`);
            if (dtlsState === 'failed' || dtlsState === 'closed') {
                this.closeTransport(transport.id);
            }
        });

        transport.on('close', () => {
            logger.info(`[Mediasoup ${streamKey}] Transport ${transport.id} closed`);
        });

        this.transports.set(transport.id, transport);

        return {
            id: transport.id,
            iceParameters: transport.iceParameters,
            iceCandidates: transport.iceCandidates,
            dtlsParameters: transport.dtlsParameters
        };
    }

    async connectWebRtcTransport(transportId, dtlsParameters) {
        const transport = this.transports.get(transportId);
        if (!transport) {
            throw new Error(`Transport ${transportId} not found`);
        }
        await transport.connect({ dtlsParameters });
    }

    async produce(streamKey, transportId, kind, rtpParameters, appData = {}) {
        const transport = this.transports.get(transportId);
        if (!transport) {
            throw new Error(`Transport ${transportId} not found`);
        }

        const producer = await transport.produce({ kind, rtpParameters, appData });
        logger.info(`[Mediasoup ${streamKey}] Created Producer id=${producer.id} kind=${producer.kind} appData=${JSON.stringify(appData)}`);

        producer.on('transportclose', () => {
            logger.info(`[Mediasoup ${streamKey}] Producer ${producer.id} transport closed`);
            this.closeProducer(streamKey, producer.id);
        });

        this.producers.set(producer.id, producer);

        if (!this.sessionProducers.has(streamKey)) {
            this.sessionProducers.set(streamKey, new Set());
        }
        this.sessionProducers.get(streamKey).add(producer.id);

        return {
            id: producer.id,
            kind: producer.kind
        };
    }

    async consume(streamKey, transportId, producerId, rtpCapabilities) {
        const router = await this.getOrCreateRouter(streamKey);
        const transport = this.transports.get(transportId);

        if (!router) throw new Error(`Router not found for ${streamKey}`);
        if (!transport) throw new Error(`Transport ${transportId} not found`);
        if (!this.producers.has(producerId)) throw new Error(`Producer ${producerId} not found`);

        if (!router.canConsume({ producerId, rtpCapabilities })) {
            throw new Error(`Cannot consume producer ${producerId} with given RTP capabilities`);
        }

        const consumer = await transport.consume({
            producerId,
            rtpCapabilities,
            paused: true
        });

        logger.info(`[Mediasoup ${streamKey}] Created Consumer id=${consumer.id} for producer=${producerId} kind=${consumer.kind}`);

        consumer.on('transportclose', () => {
            this.consumers.delete(consumer.id);
        });

        consumer.on('producerclose', () => {
            logger.info(`[Mediasoup ${streamKey}] Consumer ${consumer.id} closed because Producer ${producerId} was closed`);
            this.consumers.delete(consumer.id);
        });

        this.consumers.set(consumer.id, consumer);

        return {
            id: consumer.id,
            producerId: consumer.producerId,
            kind: consumer.kind,
            rtpParameters: consumer.rtpParameters
        };
    }

    async resumeConsumer(consumerId) {
        const consumer = this.consumers.get(consumerId);
        if (consumer) {
            await consumer.resume();
        }
    }

    closeProducer(streamKey, producerId) {
        const producer = this.producers.get(producerId);
        if (producer) {
            producer.close();
            this.producers.delete(producerId);
        }
        if (this.sessionProducers.has(streamKey)) {
            this.sessionProducers.get(streamKey).delete(producerId);
        }
    }

    closeTransport(transportId) {
        const transport = this.transports.get(transportId);
        if (transport) {
            transport.close();
            this.transports.delete(transportId);
        }
    }

    getProducersForSession(streamKey) {
        const set = this.sessionProducers.get(streamKey);
        if (!set) return [];
        const result = [];
        for (const pId of set) {
            const p = this.producers.get(pId);
            if (p) {
                result.push({
                    id: p.id,
                    kind: p.kind,
                    appData: p.appData
                });
            }
        }
        return result;
    }
}

export const mediasoupManager = new MediasoupManager();
