/**
 * /api/workstations — admin CRUD over the persistent workstation pool.
 *
 * The live status (online/busy + slot count) is joined from
 * `agent_sessions`, which the WS gateway maintains.
 */
import type { FastifyPluginAsync } from 'fastify';
import { z } from 'zod';
import { eq, desc } from 'drizzle-orm';
import { db, agentSessions, workstations, type AgentSession, requireAdmin } from '@rebus-industries/prism-shared';
import { sendRestartToAgent, sendUpdateToAgent, sendPullTemplateToAgent } from '../ws/agentProtocol.js';

const updateBody = z.object({
  nodeName:     z.string().min(1).max(128).optional(),
  canConvert:   z.boolean().optional(),
  canLayer:     z.boolean().optional(),
  canReceive:   z.boolean().optional(),
  canVisualise: z.boolean().optional(),
  isEnabled:    z.boolean().optional(),
  notes:        z.string().nullable().optional(),
});

/**
 * Pick the most useful `remote_addr` out of a workstation's session
 * list. Heartbeat-then-connect ordering means a live agent (which has
 * been emitting heartbeats every 15s) wins over a stale row whose
 * socket never cleanly closed, and the per-machineId de-dup in the WS
 * gateway means we usually only have one row anyway.
 *
 * Returns null when no session exists at all — `agent_sessions` rows
 * are deleted on socket close so an offline workstation has no IP to
 * surface here. (The admin SPA falls back to the legacy
 * `nodeName.dnsSuffix` URL in that case via `workstationUrl.ts`.)
 */
function pickHost(sessions: AgentSession[] | undefined): string | null {
  if (!sessions || sessions.length === 0) return null;
  const sorted = [...sessions].sort((a, b) => {
    const ta = (a.lastHeartbeat ?? a.connectedAt ?? new Date(0)).valueOf();
    const tb = (b.lastHeartbeat ?? b.connectedAt ?? new Date(0)).valueOf();
    return tb - ta;
  });
  for (const s of sorted) {
    if (s.remoteAddr && s.remoteAddr.trim().length > 0) return s.remoteAddr;
  }
  return null;
}

/** Default template repo the version picker lists when no override is set. */
const DEFAULT_TEMPLATE_REPO = process.env['PRISM_UE_TEMPLATE_REPO']?.trim() || 'REBUS-ORBIT/orbit-ue-template';

interface TemplateRelease {
  tag: string;
  name: string | null;
  publishedAt: string | null;
  prerelease: boolean;
  hasArchive: boolean;
}

/**
 * In-memory cache for the GitHub release list (keyed by repo). 5-min TTL; on
 * expiry we re-validate with the stored ETag (`If-None-Match`) so a 304 keeps
 * the cached list WITHOUT counting against the GitHub rate limit.
 */
const releaseCache = new Map<string, { at: number; etag: string | null; releases: TemplateRelease[] }>();
const RELEASE_TTL_MS = 300_000;

/** Error thrown when GitHub reports the API rate limit is exhausted. */
class GitHubRateLimitError extends Error {
  constructor(message: string) { super(message); this.name = 'GitHubRateLimitError'; }
}

/** Build the actionable rate-limit message (with reset time) from a 403/429 response. */
function rateLimitMessage(res: Response): string {
  const token = process.env['PRISM_GITHUB_TOKEN'] || process.env['GITHUB_TOKEN'];
  const advice = token
    ? 'A PRISM_GITHUB_TOKEN is set server-side but the limit was still hit — the token may be invalid/expired.'
    : 'The server is making UNAUTHENTICATED GitHub requests (60/hour per IP). Set PRISM_GITHUB_TOKEN '
      + '(a GitHub PAT with public_repo scope; repo scope for private repos) in the server environment '
      + '(infra/.env → PRISM_GITHUB_TOKEN) to raise the limit to 5000/hour.';
  let reset = '';
  const resetHdr = res.headers.get('x-ratelimit-reset');
  if (resetHdr && /^\d+$/.test(resetHdr)) {
    const when = new Date(Number(resetHdr) * 1000);
    const mins = Math.max(0, Math.round((when.getTime() - Date.now()) / 60_000));
    reset = ` Limit resets at ${when.toISOString()} (~${mins} min).`;
  } else {
    const retry = res.headers.get('retry-after');
    if (retry) reset = ` Retry after ~${retry}s.`;
  }
  return `GitHub API rate limit exceeded (HTTP ${res.status}). ${advice}${reset}`;
}

/**
 * Fetch the published releases for `repo` (newest first) so the admin
 * Workstations page can offer a template version picker. Mirrors the agent's
 * `TemplatePuller.ListReleasesAsync` shape. Authenticated when a
 * PRISM_GITHUB_TOKEN / GITHUB_TOKEN is set server-side (also lifts the rate
 * limit 60→5000/hr). Throws {@link GitHubRateLimitError} on a 403/429 with
 * `x-ratelimit-remaining: 0`.
 */
async function fetchTemplateReleases(repo: string): Promise<TemplateRelease[]> {
  const cached = releaseCache.get(repo);
  if (cached && Date.now() - cached.at < RELEASE_TTL_MS) return cached.releases;

  const token = process.env['PRISM_GITHUB_TOKEN'] || process.env['GITHUB_TOKEN'];
  const headers: Record<string, string> = {
    accept: 'application/vnd.github+json',
    'user-agent': 'prism-server',
  };
  if (token) headers['authorization'] = `Bearer ${token}`;
  // Conditional request: a 304 doesn't count against the rate limit.
  if (cached?.etag) headers['if-none-match'] = cached.etag;

  const res = await fetch(`https://api.github.com/repos/${repo}/releases?per_page=50`, { headers });

  // Cache still valid — refresh the timestamp and serve the cached list.
  if (res.status === 304 && cached) {
    releaseCache.set(repo, { ...cached, at: Date.now() });
    return cached.releases;
  }
  if (res.status === 404) return [];
  if ((res.status === 403 || res.status === 429) && res.headers.get('x-ratelimit-remaining') === '0') {
    throw new GitHubRateLimitError(rateLimitMessage(res));
  }
  if (!res.ok) throw new Error(`GitHub API ${res.status} ${res.statusText}`);

  const body = (await res.json()) as Array<{
    tag_name?: string;
    name?: string | null;
    draft?: boolean;
    prerelease?: boolean;
    published_at?: string | null;
    zipball_url?: string | null;
    assets?: Array<{ name?: string }>;
  }>;

  const releases: TemplateRelease[] = [];
  for (const r of body) {
    if (r.draft) continue;
    const tag = r.tag_name?.trim();
    if (!tag) continue;
    const hasZipAsset = (r.assets ?? []).some((a) => a.name?.toLowerCase().endsWith('.zip'));
    releases.push({
      tag,
      name: r.name && r.name !== tag ? r.name : tag,
      publishedAt: r.published_at ?? null,
      prerelease: !!r.prerelease,
      hasArchive: hasZipAsset || !!r.zipball_url,
    });
  }
  releaseCache.set(repo, { at: Date.now(), etag: res.headers.get('etag'), releases });
  return releases;
}

const plugin: FastifyPluginAsync = async (app) => {
  app.addHook('preHandler', requireAdmin);

  app.get('/', async () => {
    const rows = await db.select().from(workstations).orderBy(desc(workstations.lastSeenAt));
    // Join sessions in code (small table). Returns live online state per machine.
    const sessions = await db.select().from(agentSessions);
    const sessByWs = new Map<string, typeof sessions[number][]>();
    for (const s of sessions) {
      const arr = sessByWs.get(s.workstationId) ?? [];
      arr.push(s);
      sessByWs.set(s.workstationId, arr);
    }
    return {
      workstations: rows.map((w) => ({
        ...w,
        online: (sessByWs.get(w.id) ?? []).length > 0,
        slotsBusy: (sessByWs.get(w.id) ?? []).reduce((acc, s) => acc + s.slotsBusy, 0),
        sessions: (sessByWs.get(w.id) ?? []).length,
        host: pickHost(sessByWs.get(w.id)),
      })),
    };
  });

  app.get<{ Params: { id: string } }>('/:id', async (req, reply) => {
    const row = await db.query.workstations.findFirst({ where: eq(workstations.id, req.params.id) });
    if (!row) return reply.code(404).send({ error: 'not found' });
    const sessions = await db
      .select()
      .from(agentSessions)
      .where(eq(agentSessions.workstationId, row.id));
    return { ...row, host: pickHost(sessions) };
  });

  app.patch<{ Params: { id: string }; Body: unknown }>('/:id', async (req, reply) => {
    const body = updateBody.safeParse(req.body);
    if (!body.success) return reply.code(400).send({ error: 'invalid body', issues: body.error.issues });
    const res = await db
      .update(workstations)
      .set({ ...body.data })
      .where(eq(workstations.id, req.params.id))
      .returning();
    if (res.length === 0) return reply.code(404).send({ error: 'not found' });
    return res[0];
  });

  // Workstation rows are otherwise only inserted by the WS gateway when an
  // agent calls `hello`. Admin can delete a stale row here.
  app.delete<{ Params: { id: string } }>('/:id', async (req, reply) => {
    const res = await db.delete(workstations).where(eq(workstations.id, req.params.id)).returning({ id: workstations.id });
    if (res.length === 0) return reply.code(404).send({ error: 'not found' });
    return { deleted: res[0]!.id };
  });

  /**
   * GET /template-releases — list the published versions of the UE template
   * repo so the admin can pick a specific version to pull onto a workstation.
   * Optional `?repo=owner/repo` overrides the default (to match a workstation
   * whose agent points at a fork). Cached 5 min + ETag-revalidated; 429 with an
   * actionable message when GitHub's rate limit is hit, 502 otherwise.
   */
  app.get<{ Querystring: { repo?: string } }>(
    '/template-releases',
    async (req, reply) => {
      const repo = (req.query.repo?.trim() || DEFAULT_TEMPLATE_REPO).replace(/^\/+|\/+$/g, '');
      if (!repo.includes('/')) return reply.code(400).send({ error: "invalid repo (expected 'owner/repo')" });
      try {
        const releases = await fetchTemplateReleases(repo);
        return { repo, releases };
      } catch (err) {
        if (err instanceof GitHubRateLimitError) {
          req.log.warn({ repo }, 'template releases: GitHub rate limit exceeded');
          return reply.code(429).send({ error: err.message, repo, releases: [] });
        }
        req.log.warn({ err, repo }, 'failed to list template releases');
        return reply.code(502).send({ error: 'could not list template releases', repo, releases: [] });
      }
    },
  );

  // ------------------------------------------------------------------ lifecycle
  // Both routes look up the workstation by id, confirm an active agent
  // session exists in the in-memory registry (keyed off machineId), and
  // dispatch the WS envelope. The agent acks the action by either
  // disconnecting (restart) or completing the download (update).

  /**
   * POST /:id/restart — ask the agent to cleanly exit. The Windows
   * Scheduled Task + a self-spawned PowerShell helper script
   * relaunch the agent within ~1 minute.
   */
  app.post<{ Params: { id: string }; Body: { reason?: string } | undefined }>(
    '/:id/restart',
    async (req, reply) => {
      const row = await db.query.workstations.findFirst({ where: eq(workstations.id, req.params.id) });
      if (!row) return reply.code(404).send({ error: 'workstation not found' });
      const reason = typeof req.body?.reason === 'string' ? req.body.reason : undefined;
      const sent = sendRestartToAgent(row.machineId, reason ? { reason } : {});
      if (!sent) return reply.code(503).send({ error: 'agent not connected' });
      req.log.info({ workstationId: row.id, nodeName: row.nodeName, machineId: row.machineId, reason }, 'restart dispatched to agent');
      return { queued: true };
    },
  );

  /**
   * POST /:id/update — ask the agent to check GitHub Releases and apply
   * a new build if one is available. Optional `{tag: "v0.1.33"}` pins a
   * specific release; default is the latest.
   *
   * Older agents (pre-v0.1.33) silently ignore unknown message types,
   * so this returns 503 only when no agent is connected at all.
   */
  app.post<{ Params: { id: string }; Body: { tag?: string } | undefined }>(
    '/:id/update',
    async (req, reply) => {
      const row = await db.query.workstations.findFirst({ where: eq(workstations.id, req.params.id) });
      if (!row) return reply.code(404).send({ error: 'workstation not found' });
      const tag = typeof req.body?.tag === 'string' && req.body.tag.trim().length > 0
        ? req.body.tag.trim()
        : undefined;
      const sent = sendUpdateToAgent(row.machineId, tag ? { tag } : {});
      if (!sent) return reply.code(503).send({ error: 'agent not connected' });
      req.log.info({ workstationId: row.id, nodeName: row.nodeName, machineId: row.machineId, tag }, 'update dispatched to agent');
      return { queued: true };
    },
  );

  /**
   * POST /:id/pull-template — ask the agent to download the latest (or a
   * pinned) orbit-ue-template GitHub release and install it into its
   * visualiser template root. Optional `{tag: "v1.0.0-ue5.7"}` pins a
   * specific release; default uses the agent's configured tag / latest.
   *
   * Fire-and-forget like /update: the agent runs the pull in the background
   * and surfaces progress on its local web UI. Older agents (pre-pullTemplate)
   * silently ignore the message, so this returns 503 only when no agent is
   * connected at all.
   */
  app.post<{ Params: { id: string }; Body: { tag?: string; force?: boolean } | undefined }>(
    '/:id/pull-template',
    async (req, reply) => {
      const row = await db.query.workstations.findFirst({ where: eq(workstations.id, req.params.id) });
      if (!row) return reply.code(404).send({ error: 'workstation not found' });
      const tag = typeof req.body?.tag === 'string' && req.body.tag.trim().length > 0
        ? req.body.tag.trim()
        : undefined;
      // The admin "Pull template" button clicks through its own confirmation
      // dialog, so it sends force=true: the agent force-closes a running
      // Unreal Editor (the field bug — UE locks the template folder) before
      // pulling. The agent's local web UI uses the two-step prompt instead.
      const force = req.body?.force === true;
      const payload: { tag?: string; force?: boolean } = {};
      if (tag) payload.tag = tag;
      if (force) payload.force = true;
      const sent = sendPullTemplateToAgent(row.machineId, payload);
      if (!sent) return reply.code(503).send({ error: 'agent not connected' });
      req.log.info({ workstationId: row.id, nodeName: row.nodeName, machineId: row.machineId, tag, force }, 'pull-template dispatched to agent');
      return { queued: true };
    },
  );
};

export default plugin;
