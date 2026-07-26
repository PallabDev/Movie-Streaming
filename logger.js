import winston from 'winston';
import os from 'os';

const { combine, timestamp, printf, colorize, errors } = winston.format;

const logFormat = printf(({ level, message, timestamp, stack, ...meta }) => {
    let msg = `${timestamp} [${level}] ${message}`;
    if (stack) msg += `\n${stack}`;
    if (Object.keys(meta).length > 0) msg += ` ${JSON.stringify(meta)}`;
    return msg;
});

const logger = winston.createLogger({
    level: process.env.LOG_LEVEL || 'info',
    format: combine(
        timestamp({ format: 'YYYY-MM-DD HH:mm:ss.SSS' }),
        errors({ stack: true }),
        logFormat
    ),
    transports: [
        new winston.transports.Console({
            format: combine(
                colorize(),
                timestamp({ format: 'HH:mm:ss.SSS' }),
                logFormat
            )
        }),
        new winston.transports.File({
            filename: 'logs/error.log',
            level: 'error',
            maxsize: 10 * 1024 * 1024,
            maxFiles: 5,
            tailable: true
        }),
        new winston.transports.File({
            filename: 'logs/combined.log',
            maxsize: 10 * 1024 * 1024,
            maxFiles: 5,
            tailable: true
        }),
        new winston.transports.File({
            filename: 'logs/stream.log',
            level: 'info',
            maxsize: 10 * 1024 * 1024,
            maxFiles: 3,
            tailable: true
        })
    ]
});

// ─── CPU Usage Tracker ───────────────────────────────────────────────────────
let prevCpuInfo = os.cpus();

export function getCpuUsage() {
    const cpus = os.cpus();
    let totalIdle = 0, totalTick = 0;
    let prevTotalIdle = 0, prevTotalTick = 0;

    for (let i = 0; i < cpus.length; i++) {
        const cpu = cpus[i];
        const prev = prevCpuInfo[i] || cpu;
        for (const type in cpu.times) {
            totalTick += cpu.times[type];
            prevTotalTick += prev.times[type];
        }
        totalIdle += cpu.times.idle;
        prevTotalIdle += prev.times.idle;
    }

    prevCpuInfo = cpus;
    const idleDiff = totalIdle - prevTotalIdle;
    const totalDiff = totalTick - prevTotalTick;
    return totalDiff === 0 ? 0 : Math.round((1 - idleDiff / totalDiff) * 100);
}

export function getSystemInfo() {
    const totalMem = os.totalmem();
    const freeMem = os.freemem();
    const usedMem = totalMem - freeMem;
    return {
        cpuCores: os.cpus().length,
        cpuModel: os.cpus()[0]?.model || 'unknown',
        cpuUsage: getCpuUsage(),
        memTotal: (totalMem / 1024 / 1024 / 1024).toFixed(1) + ' GB',
        memUsed: (usedMem / 1024 / 1024 / 1024).toFixed(1) + ' GB',
        memPercent: Math.round(usedMem / totalMem * 100),
        loadAvg: os.loadavg().map(l => l.toFixed(2)).join(' '),
        uptime: Math.round(os.uptime() / 3600) + 'h'
    };
}

export default logger;
