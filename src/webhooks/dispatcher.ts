/**
 * Webhook dispatcher.
 *
 * Fired from the WS layer when a job reaches a terminal state. We:
 *   1. Snapshot the job row.
 *   2. POST a small JSON payload to:
 *        a) the per-job `callbackUrl` (if set)
 *        b) every active webhook in the `webhooks` table whose `events`
 *           array includes the matching event id (`job.complete` / `job.failed`)
 *   3. Sign each request with HMAC-SHA256 when the webhook has a `secret`,
 *      using the same scheme ORBIT uses elsewhere:
 *        `x-prism-signature: sha256=<hex>`
 *        body is the canonical JSON payload.
 *   4. Fire-and-forget with a short timeout. Failures are logged, not
 *      retried (the consumer can poll if they care). A retry policy can be
 *      bolted on later by routing through a BullMQ delayed queue.
 */
import { createHmac } from 'node:crypto';
import { eq } from 'drizzle-orm';
import { db, jobs, webhooks } from '@rebus-industries/prism-shared';

export type WebhookEvent = 'job.complete' | 'job.failed';

interface JobPayload {
  id: string;
  status: string;
  jobType: string;
  fileName: string;
  fileSize: number;
  format: string;
  orbitTarget: string;
  projectId: string;
  modelId: string;
  modelName: string | null;
  nodeName: string | null;
  resultUrl: string | null;
  rootObjectId: string | null;
  versionId: string | null;
  outputs: Record<string, unknown> | null;
  error: string | null;
  createdAt: string;
  completedAt: string | null;
}

function toPayload(row: typeof jobs.$inferSelect): JobPayload {
  return {
    id: row.id,
    status: row.status,
    jobType: row.jobType,
    fileName: row.fileName,
    fileSize: row.fileSize,
    format: row.format,
    orbitTarget: row.orbitTarget,
    projectId: row.projectId,
    modelId: row.modelId,
    modelName: row.modelName,
    nodeName: row.nodeName,
    resultUrl: row.resultUrl,
    rootObjectId: row.rootObjectId,
    versionId: row.versionId,
    outputs: (row.outputs as Record<string, unknown>) ?? null,
    error: row.error,
    createdAt: row.createdAt.toISOString(),
    completedAt: row.completedAt?.toISOString() ?? null,
  };
}

async function postOnce(url: string, body: string, secret: string | null): Promise<void> {
  const headers: Record<string, string> = { 'content-type': 'application/json', 'user-agent': 'PRISM-webhook/1' };
  if (secret) {
    const sig = createHmac('sha256', secret).update(body).digest('hex');
    headers['x-prism-signature'] = `sha256=${sig}`;
  }
  const ctl = new AbortController();
  const t = setTimeout(() => ctl.abort(), 5_000);
  try {
    await fetch(url, { method: 'POST', headers, body, signal: ctl.signal });
  } finally {
    clearTimeout(t);
  }
}

/**
 * Dispatch a job-terminal webhook. Always invoked from the agent message
 * handler after the DB row has been updated.
 */
export async function dispatchJobEvent(event: WebhookEvent, jobId: string): Promise<void> {
  const job = await db.query.jobs.findFirst({ where: eq(jobs.id, jobId) });
  if (!job) return;

  const payload = { event, ts: new Date().toISOString(), job: toPayload(job) };
  const body = JSON.stringify(payload);

  const tasks: Promise<unknown>[] = [];

  // Per-job callback (no signature — synthesized at submit time, the
  // caller knows the URL they gave us).
  if (job.callbackUrl) {
    tasks.push(
      postOnce(job.callbackUrl, body, null).catch((err) => {
        console.warn('[webhook] per-job callback failed', { jobId, url: job.callbackUrl, err: String(err) });
      }),
    );
  }

  // Admin-configured webhooks
  const hooks = await db.query.webhooks.findMany({ where: eq(webhooks.isActive, true) });
  for (const h of hooks) {
    const events = (h.events as string[]) ?? [];
    if (!events.includes(event)) continue;
    tasks.push(
      postOnce(h.url, body, h.secret ?? null).catch((err) => {
        console.warn('[webhook] dispatch failed', { hookId: h.id, url: h.url, err: String(err) });
      }),
    );
  }

  await Promise.allSettled(tasks);
}
