/**
 * PRISM Agent Service entry point.
 *
 * Runs the agent gateway (/ws/agent), workstation admin API, and BullMQ
 * convert worker in a single process so the worker can dispatch jobs
 * directly to locally-held agent sockets (no cross-process hop needed
 * for the hot path).
 *
 * Cross-process dispatch FROM the core server (e.g. visualiser) reaches
 * agents via the Redis pub/sub channel each agent subscribes to on
 * connect (see agentProtocol.ts → redisRegistry.subscribeToDispatch).
 *
 * Port: process.env.PORT (default 8767)
 */
import 'dotenv/config';
import Fastify from 'fastify';
import fastifyWebsocket from '@fastify/websocket';
import cookie from '@fastify/cookie';
import cors from '@fastify/cors';
import multipart from '@fastify/multipart';
import { runBootstrap } from '@rebus-industries/prism-shared';
import { handleAgentSocket } from './ws/agentProtocol.js';

const PORT = Number(process.env.PORT ?? 8767);
const HOST = process.env.HOST ?? '0.0.0.0';
const LOG_LEVEL = process.env.LOG_LEVEL ?? 'info';

async function buildApp() {
  const app = Fastify({
    logger: {
      level: LOG_LEVEL,
      transport: process.env.NODE_ENV === 'production'
        ? undefined
        : { target: 'pino-pretty', options: { translateTime: 'SYS:HH:MM:ss.l', ignore: 'pid,hostname' } },
    },
    bodyLimit: 64 * 1024 * 1024,
    disableRequestLogging: false,
    trustProxy: true,
  });

  const sessionSecret = process.env.SESSION_SECRET;
  if (!sessionSecret) {
    app.log.warn('SESSION_SECRET is not set — admin login cookies will not be signable. Set this in production!');
  }
  await app.register(cookie, { secret: sessionSecret ?? 'unsafe-dev-only-do-not-use-in-prod' });
  await app.register(cors, {
    origin: (origin, cb) => {
      if (!origin) return cb(null, true);
      if (process.env.NODE_ENV !== 'production') return cb(null, true);
      const allowed = (process.env.CORS_ALLOWED_ORIGINS ?? '').split(',').map((s) => s.trim()).filter(Boolean);
      cb(null, allowed.includes(origin));
    },
    credentials: true,
  });
  await app.register(multipart, {
    limits: { fileSize: 1024 * 1024 * 1024, files: 1, fields: 32 },
  });

  app.get('/health', async () => ({ status: 'ok', service: 'prism-agent' }));

  await app.register(fastifyWebsocket, { options: { maxPayload: 16 * 1024 * 1024 } });
  app.get('/ws/agent', { websocket: true }, (socket, req) => {
    handleAgentSocket(socket, req.ip, req.log);
  });

  await app.register(import('./api/workstations.js'), { prefix: '/api/workstations' });
  await app.register(import('./api/workstationDownloads.js'), { prefix: '/api/admin/workstations/downloads' });

  return app;
}

async function main() {
  const app = await buildApp();
  try {
    await runBootstrap(app.log);
  } catch (err) {
    app.log.error({ err }, 'bootstrap failed');
    process.exit(1);
  }
  try {
    await app.listen({ host: HOST, port: PORT });
  } catch (err) {
    app.log.error(err);
    process.exit(1);
  }

  const { startConvertWorker } = await import('./jobs/worker.js');
  const worker = startConvertWorker(app.log);

  for (const sig of ['SIGINT', 'SIGTERM'] as const) {
    process.on(sig, async () => {
      app.log.info({ sig }, 'shutdown');
      try { await worker.close(); } catch (err) { app.log.warn({ err }, 'worker close failed'); }
      await app.close();
      process.exit(0);
    });
  }
}

main();
