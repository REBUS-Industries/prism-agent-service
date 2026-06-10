/**
 * BullMQ connection options.
 *
 * BullMQ v5 bundles its own ioredis to avoid version conflicts.
 * Passing a pre-created Redis instance from a different ioredis install
 * causes TypeScript errors. Use plain RedisOptions instead so BullMQ
 * creates its own managed connections internally.
 */

const raw = process.env.REDIS_URL ?? 'redis://localhost:6379';
const url = new URL(raw);

export const bullConnOpts = {
  host: url.hostname || 'localhost',
  port: parseInt(url.port || '6379', 10),
  ...(url.password ? { password: decodeURIComponent(url.password) } : {}),
  ...(url.username ? { username: decodeURIComponent(url.username) } : {}),
  maxRetriesPerRequest: null as null,
  enableReadyCheck: true,
} as const;
