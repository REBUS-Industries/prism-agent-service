/**
 * BullMQ job queue.
 *
 * Phase 1: queue declaration only. Phase 2 attaches a Worker that hands
 * jobs to the dispatcher; phase 3 the dispatcher pushes to a live agent.
 */
import { Queue, QueueEvents } from 'bullmq';
import { bullConnOpts } from './bullRedis.js';

/**
 * The DB row is the canonical source of truth — the queue payload only
 * needs to carry the jobId plus a few hints for logging. Everything the
 * worker/dispatcher actually uses is read from the `jobs` table.
 *
 * `noAgentAttempts` is how many times the dispatcher has requeued this
 * job because no eligible agent was available (capped server-side).
 */
export type ConvertJobPayload = {
  jobId: string;
  format?: string;
  fileName?: string;
  filePath?: string;
  orbitTarget?: 'prod' | 'dev';
  projectId?: string;
  modelId?: string;
  modelName?: string;
  callbackUrl?: string;
  submittedBy?: string;
  noAgentAttempts?: number;
};

export const CONVERT_QUEUE = 'prism-convert';

export const convertQueue = new Queue<ConvertJobPayload, unknown, string>(CONVERT_QUEUE, {
  connection: bullConnOpts,
  defaultJobOptions: {
    attempts: 1,                                  // PRISM handles retry policy explicitly
    removeOnComplete: { age: 60 * 60, count: 1000 },
    removeOnFail:     { age: 24 * 60 * 60 },
  },
});

export const convertQueueEvents = new QueueEvents(CONVERT_QUEUE, { connection: bullConnOpts });

export async function enqueueConvert(payload: ConvertJobPayload): Promise<string> {
  const job = await convertQueue.add('convert', payload, { jobId: payload.jobId });
  return job.id!;
}
