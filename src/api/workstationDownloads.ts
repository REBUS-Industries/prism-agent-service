/**
 * /api/admin/workstations/downloads — node provisioning artifacts.
 *
 * Surfaces the prebuilt PRISM.Agent installer that admins download to
 * bring a new Rhino workstation online. As of agent v0.1.30 the
 * canonical artifact is the Inno Setup wizard installer
 * (`PRISM.Agent-Setup-vX.Y.Z.exe`), which embeds `install.ps1` /
 * `uninstall.ps1` and prompts for `prismUrl` / `nodeName` / `slots`
 * during setup -- so the legacy per-node `agent-config.json` template
 * and standalone PowerShell script downloads are no longer needed.
 *
 * Agent version resolution -- GitHub Releases API is always the primary source.
 * The DB settings `workstation_agent_version` / `workstation_agent_download_url`
 * act as admin overrides to pin a specific version; when they are absent the
 * latest release from REBUS-ORBIT/prism-agent is used automatically on every
 * page load (no server-side cache so the page always reflects the actual latest).
 */
import type { FastifyPluginAsync } from 'fastify';
import { requireAdmin, getSetting } from '@rebus-industries/prism-shared';

const GITHUB_RELEASE_REPO = 'REBUS-ORBIT/prism-agent';
// Preference order: the Inno Setup wizard .exe is the user-facing
// install artifact; the multi-file zip is kept as a fallback for older
// agents whose in-app self-update path still grabs the .zip directly.
const SETUP_EXE_PATTERN = /^PRISM\.Agent-Setup-.+\.exe$/;
const ZIP_ASSET_PATTERN = /^PRISM\.Agent-.+\.zip$/;

interface GitHubReleaseInfo {
  version: string;
  downloadUrl: string;
}

/** Always fetches the latest release directly from the GitHub Releases API.
 *  No server-side cache — the admin page should reflect the real latest on
 *  every load. GitHub's unauthenticated rate limit (60 req/h) is well above
 *  realistic admin page traffic. Picks the `.exe` setup wrapper first and
 *  falls back to the `.zip` payload if a release doesn't carry an .exe yet
 *  (e.g. tags from before v0.1.30). */
async function fetchLatestAgentRelease(): Promise<GitHubReleaseInfo | null> {
  try {
    const res = await fetch(
      `https://api.github.com/repos/${GITHUB_RELEASE_REPO}/releases/latest`,
      {
        headers: {
          Accept: 'application/vnd.github+json',
          'User-Agent': 'PRISM-Server/1.0',
          'X-GitHub-Api-Version': '2022-11-28',
        },
        signal: AbortSignal.timeout(8000),
      },
    );
    if (!res.ok) return null;
    const json = (await res.json()) as {
      tag_name: string;
      assets: { name: string; browser_download_url: string }[];
    };
    const assets = json.assets ?? [];
    const asset =
      assets.find((a) => SETUP_EXE_PATTERN.test(a.name)) ??
      assets.find((a) => ZIP_ASSET_PATTERN.test(a.name));
    if (!asset) return null;
    return { version: json.tag_name, downloadUrl: asset.browser_download_url };
  } catch {
    return null;
  }
}

/** Compute the WSS URL the agent should use for this server, honouring the
 *  admin override and falling back to the request's host. Caddy fronts the
 *  prod deployment and terminates TLS, so we must look at x-forwarded-proto
 *  (and x-forwarded-host) before falling back to Fastify's direct values. */
async function resolveAgentWsUrl(req: {
  hostname?: string;
  protocol?: string;
  headers: Record<string, string | string[] | undefined>;
}): Promise<string> {
  const override = (await getSetting('workstation_agent_ws_url'))?.trim();
  if (override) return override;
  const xfHost  = pickFirstHeader(req.headers['x-forwarded-host']);
  const xfProto = pickFirstHeader(req.headers['x-forwarded-proto']);
  const host = (xfHost ?? req.hostname ?? '').trim() || 'prism.rebus.industries';
  const proto = (xfProto ?? req.protocol ?? '').trim();
  const scheme = proto === 'http' ? 'ws' : 'wss';
  return `${scheme}://${host}/ws/agent`;
}

function pickFirstHeader(value: string | string[] | undefined): string | undefined {
  if (Array.isArray(value)) return value[0];
  if (typeof value === 'string') return value.split(',')[0]?.trim();
  return undefined;
}

const plugin: FastifyPluginAsync = async (app) => {
  app.addHook('preHandler', requireAdmin);

  /**
   * GET /agent — meta JSON describing the latest agent build.
   *
   * Resolution order for version + downloadUrl:
   *   1. GitHub Releases API for REBUS-ORBIT/prism-agent (live, no cache).
   *      The .exe setup wrapper is preferred; .zip is used only if no .exe
   *      asset is published for the latest tag.
   *   2. DB settings — admin override to pin a specific version. Only used
   *      when GitHub API is unreachable or returns no matching asset.
   *   3. null / available: false so the UI renders the "build pending" state.
   */
  app.get('/agent', async (req) => {
    // Primary: GitHub Releases API — always reflects the true latest build.
    const ghRelease = await fetchLatestAgentRelease();
    let downloadUrl = ghRelease?.downloadUrl ?? null;
    let version     = ghRelease?.version     ?? null;

    // Fallback: DB admin override (pinned version or when GitHub is unreachable).
    if (!downloadUrl || !version) {
      const dbUrl = (await getSetting('workstation_agent_download_url'))?.trim() || null;
      const dbVer = (await getSetting('workstation_agent_version'))?.trim()      || null;
      downloadUrl = downloadUrl ?? dbUrl;
      version     = version     ?? dbVer;
    }

    const wsUrl = await resolveAgentWsUrl(req);
    return {
      downloadUrl,
      version,
      wsUrl,
      available: !!downloadUrl,
      buildSource: {
        workflow: '.github/workflows/agent.yml',
        artifact: 'PRISM.Agent-Setup-<tag>.exe',
        howTo: 'Tag a release (vX.Y.Z) or trigger the agent-msi workflow manually.',
      },
    };
  });

  /**
   * GET /agent/download — 302 redirect to the latest installer URL.
   * Resolves via GitHub Releases API first, DB override as fallback.
   * Picks the `.exe` setup wrapper when present and falls back to `.zip`.
   */
  app.get('/agent/download', async (_req, reply) => {
    const ghRelease = await fetchLatestAgentRelease();
    const url = ghRelease?.downloadUrl
      ?? (await getSetting('workstation_agent_download_url'))?.trim()
      ?? null;
    if (!url) {
      return reply.code(404).send({
        error: 'no agent build available',
        hint: 'push a vX.Y.Z tag to trigger the agent-msi workflow',
      });
    }
    return reply.redirect(url, 302);
  });
};

export default plugin;
