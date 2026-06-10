/**
 * Typed agent WS handler.
 *
 * One handler instance per agent connection. Lifecycle:
 *   1. open  -> wait for `hello`
 *   2. hello -> upsert workstations + agent_sessions row, send `welcome`
 *   3. heartbeat -> bump session, update slotsBusy
 *   4. progress/log/complete/fail/layers -> update job row, fan to admin
 *   5. close -> remove session row + registry entry
 */
import { randomUUID } from 'node:crypto';
import type { WebSocket } from 'ws';
import type { FastifyBaseLogger } from 'fastify';
import { and, eq, notInArray } from 'drizzle-orm';
import {
  db, agentSessions, jobLogs, jobs, visualiserRuns, workstations,
  sessionRegistry, type AgentConn,
  redisRegistry,
  envelope, PROTOCOL_VERSION,
  type AgentToServerMsg, type HelloData, type RestartData, type UpdateData, type PullTemplateData, type WelcomeData,
  type SignallingFrameData, type SignallingViewerCloseData, type SetViewerControlData,
  broadcastJobUpdate, broadcastWorkstationUpdate,
  visualiserRunRegistry,
  appendVisualiserRunLog,
  signallingProxyRegistry,
} from '@rebus-industries/prism-shared';
import { dispatchJobEvent } from '../webhooks/dispatcher.js';
import { releaseVisualiserSlot, tryDispatch } from '../jobs/dispatcher.js';
import { visualiserIdleReaper } from '../visualiser/idleReaper.js';

const HEARTBEAT_SECONDS = 15;

/**
 * Normalise the WS peer address before persisting it.
 *
 * Node represents an IPv4 socket whose peer happens to be reachable
 * over the IPv6 dual-stack listener as `::ffff:10.0.10.202` -- the
 * canonical IPv4-mapped IPv6 form (RFC 4291 §2.5.5.2). The bare IPv4
 * `10.0.10.202` is what every operator types into a browser address
 * bar, so strip the prefix at the boundary instead of forcing every
 * downstream consumer (`/api/workstations`, the URL helper, admin SPA
 * tooltips) to know about it.
 *
 * Also trims surrounding whitespace defensively and drops the empty
 * string so the column stays NULL when we have no useful value
 * (e.g. unit tests that never go through a real socket).
 */
function normaliseRemoteAddr(addr: string | undefined | null): string | null {
  if (addr == null) return null;
  let s = addr.trim();
  if (!s) return null;
  if (s.toLowerCase().startsWith('::ffff:')) s = s.slice('::ffff:'.length);
  return s || null;
}

export async function handleAgentSocket(socket: WebSocket, remoteAddrRaw: string | undefined, log: FastifyBaseLogger): Promise<void> {
  let conn: AgentConn | null = null;
  let helloProcessed = false;
  const childLog = log.child({ ws: 'agent' });
  const remoteAddr = normaliseRemoteAddr(remoteAddrRaw) ?? undefined;

  // Send a JSON-level server_ping every 30 s. The Websocket.Client library on
  // the agent side only resets its ReconnectTimeout (60 s) on application-
  // layer text frames — protocol-level WS pings are invisible to it. Without
  // this the agent re-connects every ~61 s even when the connection is healthy.
  const pingFrame = JSON.stringify(envelope('server_ping', {}));
  const pingInterval = setInterval(() => {
    if (socket.readyState === socket.OPEN) socket.send(pingFrame);
  }, 30_000);

  // Serialize per-connection message processing so the WS send order ==
  // the DB write order. Without this, the `message` event fires
  // fire-and-forget async handlers; back-to-back messages from the same
  // agent (e.g. PollLayersJob sends `progress("extracting-layers", "walking
  // layer table")` immediately followed by `layers(<tree>)`) race in the
  // DB and the loser-by-microseconds overwrites the winner. Observed
  // failure mode (2026-05-25 job c80c9a1d): the Progress UPDATE landed
  // *after* the Layers UPDATE, leaving the job stuck in
  // `processing/extracting-layers/walking layer table` with
  // `layers_json` populated and `agent_sessions.slots_busy` already
  // decremented — i.e. the Layers handler did its work but the Progress
  // handler's stale write clobbered the status/stage/lastMessage. The
  // SSE broadcast suffers the same race because it fires right after
  // each handler's `await db.update`, so the user's browser also
  // received `awaiting_selection` followed by `processing` and reverted
  // to the loading state.
  let pendingHandler: Promise<void> = Promise.resolve();
  socket.on('message', (raw) => {
    let msg: AgentToServerMsg;
    try {
      msg = JSON.parse(raw.toString());
    } catch (err) {
      childLog.warn({ err }, 'agent sent non-JSON; closing');
      socket.close(1003, 'invalid json');
      return;
    }
    if (msg.v !== PROTOCOL_VERSION) {
      childLog.warn({ got: msg.v }, 'protocol version mismatch; closing');
      socket.close(1002, 'protocol version mismatch');
      return;
    }
    pendingHandler = pendingHandler.then(() =>
      handleMessage(msg).catch((err) => {
        childLog.error({ err, type: msg.type }, 'agent handler failed');
      }),
    );
  });

  socket.on('close', async (code, reason) => {
    clearInterval(pingInterval);
    if (conn) {
      childLog.info({ sessionId: conn.sessionId, nodeName: conn.nodeName, code, reason: reason.toString() }, 'agent ws closed');
      await sessionRegistry.removeAgent(conn.sessionId);
      await redisRegistry.unsubscribeFromDispatch(conn.workstationId);
      try {
        await db.delete(agentSessions).where(eq(agentSessions.id, conn.sessionId));
        broadcastWorkstationUpdate({ id: conn.workstationId, online: false });
      } catch (err) {
        childLog.warn({ err }, 'failed to delete agent_sessions row on close');
      }
    }
  });

  socket.on('error', (err) => {
    childLog.warn({ err }, 'agent ws error');
  });

  async function handleMessage(msg: AgentToServerMsg) {
    switch (msg.type) {
      case 'hello':
        await onHello(msg.data);
        return;
      case 'heartbeat':
        if (!conn) return;
        conn.lastHeartbeat = new Date();
        conn.slotsBusy = msg.data.slotsBusy;
        void sessionRegistry.syncSlotsBusy(conn.workstationId, conn.slotsBusy);
        void sessionRegistry.refreshPresence(conn.workstationId);
        await db
          .update(agentSessions)
          .set({ lastHeartbeat: conn.lastHeartbeat, slotsBusy: conn.slotsBusy })
          .where(eq(agentSessions.id, conn.sessionId));
        return;
      case 'ack':
        if (!conn) return;
        childLog.debug({ jobId: msg.data.jobId, accepted: msg.data.accepted }, 'agent ack');
        return;
      case 'progress':
        if (!conn) return;
        // Refuse to downgrade a job that's already in a terminal state.
        // The connector reports ("Done", 100) at the end of SendAsync via
        // a `new Progress<T>(...)` callback, which is a fire-and-forget
        // thread-pool dispatch. The agent then immediately awaits
        // SendAsync(MessageType.Complete, ...). On a fast network the
        // Complete frame wins the race to the wire, the server transitions
        // the job to status='complete', and then the late "Done" progress
        // frame arrives and — without this guard — clobbers it back to
        // status='processing' / currentStage='Done' while result_url,
        // outputs and completed_at remain set. Symptom in admin UI: a
        // permanently "PROCESSING" row that already uploaded successfully.
        // Same defence covers cancelled and failed.
        const progressResult = await db
          .update(jobs)
          .set({
            status: 'processing',
            currentStage: msg.data.stage,
            progressPercent: msg.data.percent ?? null,
            lastMessage: msg.data.message ?? null,
            updatedAt: new Date(),
          })
          .where(and(
            eq(jobs.id, msg.data.jobId),
            notInArray(jobs.status, ['complete', 'failed', 'cancelled']),
          ))
          .returning({ id: jobs.id });
        if (progressResult.length === 0) {
          childLog.debug({ jobId: msg.data.jobId, stage: msg.data.stage }, 'ignoring late progress on terminal job');
          return;
        }
        broadcastJobUpdate(msg.data.jobId, {
          status: 'processing',
          currentStage: msg.data.stage,
          progressPercent: msg.data.percent,
          lastMessage: msg.data.message,
        });
        return;
      case 'log':
        if (!conn) return;
        await db.insert(jobLogs).values({
          jobId: msg.data.jobId,
          level: msg.data.level,
          source: 'agent',
          message: msg.data.message,
        });
        broadcastJobUpdate(msg.data.jobId, { logLine: { ts: new Date().toISOString(), ...msg.data } });
        return;
      case 'complete':
        if (!conn) return;
        await db
          .update(jobs)
          .set({
            status: 'complete',
            resultUrl: msg.data.versionUrl ?? null,
            rootObjectId: msg.data.rootObjectId ?? null,
            versionId: msg.data.versionId ?? null,
            outputs: msg.data.outputs ?? {},
            currentStage: 'complete',
            progressPercent: 100,
            completedAt: new Date(),
            updatedAt: new Date(),
          })
          .where(eq(jobs.id, msg.data.jobId));
        broadcastJobUpdate(msg.data.jobId, { status: 'complete', resultUrl: msg.data.versionUrl, outputs: msg.data.outputs });
        void dispatchJobEvent('job.complete', msg.data.jobId).catch((err) => childLog.warn({ err }, 'webhook dispatch failed'));
        return;
      case 'fail':
        if (!conn) return;
        await db
          .update(jobs)
          .set({
            status: 'failed',
            error: msg.data.error,
            currentStage: 'failed',
            updatedAt: new Date(),
            completedAt: new Date(),
          })
          .where(eq(jobs.id, msg.data.jobId));
        broadcastJobUpdate(msg.data.jobId, { status: 'failed', error: msg.data.error });
        void dispatchJobEvent('job.failed', msg.data.jobId).catch((err) => childLog.warn({ err }, 'webhook dispatch failed'));
        return;
      case 'visualisationReady':
        if (!conn) return;
        await onVisualisationReady(msg.data, conn, childLog);
        return;
      case 'visualisationFailed':
        if (!conn) return;
        await onVisualisationFailed(msg.data, conn, childLog);
        return;
      case 'visualisationEnded':
        if (!conn) return;
        await onVisualisationEnded(msg.data, conn, childLog);
        return;
      case 'signallingFrame':
        if (!conn) return;
        signallingProxyRegistry.forwardAgentToBrowser(msg.data);
        return;
      case 'layers':
        if (!conn) return;
        childLog.info({ jobId: msg.data.jobId, count: msg.data.layers.length }, 'received layer tree');
        // Two-phase flow: store the layer tree on the job row, transition
        // to `awaiting_selection`, and broadcast so SSE / admin UI clients
        // can render the picker. The job sits here until the caller POSTs
        // a selection to /api/jobs/:id/layers (see api/jobs.ts), at which
        // point the job is re-queued and the dispatcher dispatches it as
        // a regular convert.
        await db
          .update(jobs)
          .set({
            status: 'awaiting_selection',
            layersJson: msg.data.layers,
            currentStage: 'awaiting_selection',
            lastMessage: `received layer tree (${msg.data.layers.length} root layer${msg.data.layers.length === 1 ? '' : 's'})`,
            updatedAt: new Date(),
          })
          .where(eq(jobs.id, msg.data.jobId));
        // Free the agent's slot — the pollLayers job is done from the
        // agent's perspective. The follow-up convert dispatch will pick
        // up a fresh slot (possibly on a different workstation).
        await sessionRegistry.releaseConversionSlot(conn.workstationId);
        broadcastJobUpdate(msg.data.jobId, {
          status: 'awaiting_selection',
          currentStage: 'awaiting_selection',
          lastMessage: `received layer tree (${msg.data.layers.length} root layer${msg.data.layers.length === 1 ? '' : 's'})`,
          layers: msg.data.layers,
        });
        return;
    }
  }

  async function onHello(hello: HelloData) {
    if (helloProcessed) {
      // A live agent re-sends `hello` on the SAME socket whenever its
      // self-reported capabilities or installed versions change — e.g. after a
      // template pull (AgentControlPlane.PullTemplate), a config mutation
      // (SendHelloAsync), or a periodic version-change check. Previously we
      // dropped every hello after the first, so the agent's local web UI
      // (which reads the on-disk .prism-template.json marker live on each
      // request) showed the true installed UE template / connector version
      // while the admin Workstations row stayed frozen at whatever was true at
      // initial connect — only converging on a full reconnect. Treat a re-hello
      // as a REFRESH of the persisted self-reported fields instead. We do NOT
      // create a second agent_sessions row, re-run the queued-job dispatch
      // sweep, or touch the admin-managed canConvert/canLayer/canReceive flags.
      if (conn) {
        conn.hello = hello;
        try {
          await db
            .update(workstations)
            .set({
              nodeName: hello.nodeName,
              supportedFormats: hello.formats,
              slotsTotal: hello.slots,
              agentVersion: hello.agentVersion,
              rhinoVersion: hello.rhinoVersion ?? null,
              installedTemplateTag: hello.installedTemplateTag ?? null,
              installedConnectorTag: hello.installedConnectorTag ?? null,
              lastSeenAt: new Date(),
            })
            .where(eq(workstations.id, conn.workstationId));
          // The admin Workstations page re-fetches /api/workstations on any
          // `workstation` broadcast, so this is enough to surface the fresh
          // installed-version pills live.
          broadcastWorkstationUpdate({ id: conn.workstationId, nodeName: hello.nodeName, online: true });
          childLog.info(
            { machineId: hello.machineId, installedTemplateTag: hello.installedTemplateTag, installedConnectorTag: hello.installedConnectorTag },
            're-hello refreshed workstation self-reported fields',
          );
        } catch (err) {
          childLog.warn({ err, machineId: hello.machineId }, 'failed to refresh workstation on re-hello');
        }
      } else {
        childLog.warn({ machineId: hello.machineId }, 'duplicate hello before session established; ignoring');
      }
      return;
    }
    helloProcessed = true;
    childLog.info({ machineId: hello.machineId, nodeName: hello.nodeName, slots: hello.slots, roles: hello.roles }, 'agent hello');

    // Upsert workstation row by machineId.
    let workstation = (await db
      .select()
      .from(workstations)
      .where(eq(workstations.machineId, hello.machineId))
      .limit(1))[0];

    if (!workstation) {
      const inserted = await db
        .insert(workstations)
        .values({
          machineId: hello.machineId,
          nodeName: hello.nodeName,
          supportedFormats: hello.formats,
          slotsTotal: hello.slots,
          agentVersion: hello.agentVersion,
          rhinoVersion: hello.rhinoVersion ?? null,
          installedTemplateTag: hello.installedTemplateTag ?? null,
          installedConnectorTag: hello.installedConnectorTag ?? null,
          canConvert: hello.roles.includes('conversion'),
          canLayer:   hello.roles.includes('layering'),
          canReceive: hello.roles.includes('receive'),
          lastSeenAt: new Date(),
        })
        .returning();
      workstation = inserted[0]!;
      childLog.info({ workstationId: workstation.id }, 'registered new workstation');
    } else {
      // UPDATE — do NOT touch canConvert/canLayer/canReceive; those are admin-managed.
      // Only refresh the fields the agent self-reports on every connect.
      await db
        .update(workstations)
        .set({
          nodeName: hello.nodeName,
          supportedFormats: hello.formats,
          slotsTotal: hello.slots,
          agentVersion: hello.agentVersion,
          rhinoVersion: hello.rhinoVersion ?? null,
          installedTemplateTag: hello.installedTemplateTag ?? null,
          installedConnectorTag: hello.installedConnectorTag ?? null,
          lastSeenAt: new Date(),
        })
        .where(eq(workstations.id, workstation.id));
    }

    if (!workstation.isEnabled) {
      socket.send(JSON.stringify(envelope('cancel', { jobId: '00000000-0000-0000-0000-000000000000', reason: 'workstation disabled by admin' })));
      socket.close(1008, 'workstation disabled');
      return;
    }

    // Insert agent_sessions row (id = sessionId we'll hand back in welcome)
    const inserted = await db
      .insert(agentSessions)
      .values({
        workstationId: workstation.id,
        remoteAddr: remoteAddr ?? null,
        slotsBusy: 0,
      })
      .returning();
    const session = inserted[0]!;

    conn = {
      sessionId: session.id,
      workstationId: workstation.id,
      machineId: hello.machineId,
      nodeName: hello.nodeName,
      socket,
      hello,
      slotsBusy: 0,
      connectedAt: session.connectedAt!,
      lastHeartbeat: new Date(),
      remoteAddr,
    };
    await sessionRegistry.addAgent(conn);
    await redisRegistry.subscribeToDispatch(conn.workstationId, (payload) => {
      if (socket.readyState === socket.OPEN) socket.send(payload);
    });

    const welcome: WelcomeData = {
      sessionId: session.id,
      serverTime: new Date().toISOString(),
      heartbeatSeconds: HEARTBEAT_SECONDS,
    };
    socket.send(JSON.stringify(envelope('welcome', welcome, randomUUID())));

    broadcastWorkstationUpdate({
      id: workstation.id,
      nodeName: workstation.nodeName,
      online: true,
      slotsTotal: workstation.slotsTotal,
      slotsBusy: 0,
    });

    // Dispatch any jobs that were queued before this agent connected.
    try {
      const queuedJobs = await db
        .select({ id: jobs.id })
        .from(jobs)
        .where(eq(jobs.status, 'queued'));
      for (const { id } of queuedJobs) {
        const outcome = await tryDispatch(id, childLog);
        if (outcome.dispatched) {
          childLog.info({ jobId: id, nodeName: outcome.nodeName }, 'dispatched queued job to newly connected agent');
        }
      }
    } catch (err) {
      childLog.warn({ err }, 'post-hello dispatch sweep failed');
    }
  }
}

/* -------------------------------------------------------------------------- */
/* Outbound server -> agent dispatchers (lifecycle commands)                  */
/* -------------------------------------------------------------------------- */

/**
 * Dispatch a `restart` envelope to a live agent connection.
 * Returns true if the frame was written, false if the underlying socket
 * threw (the caller should map that to a 503).
 */
export function sendRestartToAgent(machineId: string, data: RestartData = {}): boolean {
  const conn = sessionRegistry.getAgentByMachine(machineId);
  if (!conn) return false;
  try {
    conn.socket.send(JSON.stringify(envelope('restart', data, randomUUID())));
    return true;
  } catch {
    return false;
  }
}

/**
 * Dispatch an `update` envelope to a live agent connection.
 * Tag is optional; when omitted the agent picks the latest release.
 */
export function sendUpdateToAgent(machineId: string, data: UpdateData = {}): boolean {
  const conn = sessionRegistry.getAgentByMachine(machineId);
  if (!conn) return false;
  try {
    conn.socket.send(JSON.stringify(envelope('update', data, randomUUID())));
    return true;
  } catch {
    return false;
  }
}

/**
 * Dispatch a `pullTemplate` envelope to a live agent connection, asking it
 * to download + install the latest (or pinned) orbit-ue-template release
 * into its visualiser template root. Fire-and-forget like `update`; the
 * agent runs the pull in the background and surfaces progress on its local
 * web UI. `tag` is optional; when omitted the agent uses its configured
 * template tag (or the repo's latest release).
 */
export function sendPullTemplateToAgent(machineId: string, data: PullTemplateData = {}): boolean {
  const conn = sessionRegistry.getAgentByMachine(machineId);
  if (!conn) return false;
  try {
    conn.socket.send(JSON.stringify(envelope('pullTemplate', data, randomUUID())));
    return true;
  } catch {
    return false;
  }
}

/**
 * Forward a `signallingFrame` envelope from PRISM server to the agent
 * that's hosting `runId`. Used by `signallingProxy.ts` for every
 * browser→agent frame. Returns true if the frame was written.
 */
export function sendSignallingFrameToAgent(agentSessionId: string, frame: SignallingFrameData): boolean {
  const conn = sessionRegistry.getAgent(agentSessionId);
  if (!conn || conn.socket.readyState !== conn.socket.OPEN) return false;
  try {
    conn.socket.send(JSON.stringify(envelope('signallingFrame', frame, randomUUID())));
    return true;
  } catch {
    return false;
  }
}

/**
 * Tell the agent a browser viewer disconnected so it can tear down that
 * viewer's dedicated local Cirrus/Wilbur player WS (otherwise stale
 * Wilbur players accumulate as tabs open/close). Best-effort.
 */
export function sendSignallingViewerCloseToAgent(agentSessionId: string, data: SignallingViewerCloseData): boolean {
  const conn = sessionRegistry.getAgent(agentSessionId);
  if (!conn || conn.socket.readyState !== conn.socket.OPEN) return false;
  try {
    conn.socket.send(JSON.stringify(envelope('signallingViewerClose', data, randomUUID())));
    return true;
  } catch {
    return false;
  }
}

/**
 * Push the authoritative single-controller lock state for one viewer to
 * the agent, which gates that viewer's browser→Cirrus input frames.
 * Best-effort — the agent also stores the last value so a bridge created
 * after this call still picks it up.
 */
export function sendSetViewerControlToAgent(agentSessionId: string, data: SetViewerControlData): boolean {
  const conn = sessionRegistry.getAgent(agentSessionId);
  if (!conn || conn.socket.readyState !== conn.socket.OPEN) return false;
  try {
    conn.socket.send(JSON.stringify(envelope('setViewerControl', data, randomUUID())));
    return true;
  } catch {
    return false;
  }
}

/* -------------------------------------------------------------------------- */
/* Visualiser inbound handlers                                                */
/* -------------------------------------------------------------------------- */

interface VisualisationReadyMsgData { runId: string; signallingUrl: string; streamerId?: string; expiresAt?: string; }
interface VisualisationFailedMsgData { runId: string; error: string; stack?: string; }
interface VisualisationEndedMsgData  { runId: string; reason?: string; }

async function onVisualisationReady(
  data: VisualisationReadyMsgData,
  conn: AgentConn,
  log: FastifyBaseLogger,
): Promise<void> {
  log.info({ runId: data.runId, signallingUrl: data.signallingUrl, sessionId: conn.sessionId }, 'visualisationReady');
  // Persist the agent-supplied streamer id + ready timestamp. The
  // route handler builds the public signallingUrl + playerUrl from
  // PUBLIC_BASE_URL when it commits the `streaming` transition (so
  // the agent's local URL never leaks to the portal). We don't
  // overwrite `signallingUrl` here for the same reason.
  await db
    .update(visualiserRuns)
    .set({
      streamerId: data.streamerId ?? null,
      readyAt: new Date(),
      updatedAt: new Date(),
    })
    .where(eq(visualiserRuns.id, data.runId));
  await appendVisualiserRunLog(data.runId, `agent reported ready (streamerId ${data.streamerId ?? 'unknown'})`, { source: 'agent', log });
  visualiserRunRegistry.ready({
    runId: data.runId,
    signallingUrl: data.signallingUrl,
    streamerId: data.streamerId,
    expiresAt: data.expiresAt,
  });
  broadcastWorkstationUpdate({ id: conn.workstationId, visualiserRunReady: data.runId });
}

async function onVisualisationFailed(
  data: VisualisationFailedMsgData,
  conn: AgentConn,
  log: FastifyBaseLogger,
): Promise<void> {
  log.warn({ runId: data.runId, error: data.error, sessionId: conn.sessionId }, 'visualisationFailed');
  await db
    .update(visualiserRuns)
    .set({
      status: 'failed',
      failureReason: 'agent_failed',
      error: data.error,
      endedAt: new Date(),
      updatedAt: new Date(),
    })
    .where(eq(visualiserRuns.id, data.runId));
  await appendVisualiserRunLog(data.runId, `agent reported failure: ${data.error}`, { level: 'error', source: 'agent', log });
  // Hand the failure back to whoever is `await`-ing the POST. If no
  // waiter exists the row is still updated for follow-up GETs.
  visualiserRunRegistry.fail({
    runId: data.runId,
    code: 'agent_failed',
    message: data.error,
    stack: data.stack,
  });
  // Run is terminal — drop any pending idle-reap countdown for it.
  visualiserIdleReaper.cancel(data.runId);
  await releaseVisualiserSlot(conn.workstationId).catch(() => null);
  // Drop any active signalling proxy connections for this runId so
  // the browser sees a clean close instead of a frozen socket.
  signallingProxyRegistry.closeRun(data.runId, 1011, `visualiser failed: ${data.error}`);
  broadcastWorkstationUpdate({ id: conn.workstationId, visualiserRunFailed: data.runId, error: data.error });
}

async function onVisualisationEnded(
  data: VisualisationEndedMsgData,
  conn: AgentConn,
  log: FastifyBaseLogger,
): Promise<void> {
  log.info({ runId: data.runId, reason: data.reason ?? '<none>', sessionId: conn.sessionId }, 'visualisationEnded');
  await db
    .update(visualiserRuns)
    .set({ status: 'ended', endedAt: new Date(), updatedAt: new Date() })
    .where(eq(visualiserRuns.id, data.runId));
  await appendVisualiserRunLog(data.runId, `agent reported ended${data.reason ? `: ${data.reason}` : ''}`, { source: 'agent', log });
  // Run is terminal — drop any pending idle-reap countdown for it.
  visualiserIdleReaper.cancel(data.runId);
  await releaseVisualiserSlot(conn.workstationId).catch(() => null);
  signallingProxyRegistry.closeRun(data.runId, 1000, data.reason ?? 'ended');
  broadcastWorkstationUpdate({ id: conn.workstationId, visualiserRunEnded: data.runId, reason: data.reason });
}
