/**
 * Prism MCP Server (draft v1 — for review, not yet run)
 *
 * Exposes Ghost Arcade's show-control surface to AI agents over MCP.
 * Architecture: this server is a WebSocket *client* of the existing
 * server/ws-server.js (:9001). It sends the same message types the mobile
 * companion sends — zero changes to ws-server.js required.
 *
 *   Agent --MCP--> mcp-server.js --WS--> ws-server.js (:9001) --broadcast--> desktop/Electron
 *
 * Reads: on WS connect the server pushes `sync` (project state),
 * `library_sync`, `vj_clips_sync`, `compositions_sync`, `output_freeze_state`
 * — the bridge caches all of them.
 *
 * Writes: send the command message, wait briefly, then `sync_request` to
 * confirm resulting state. Every mutating call is audit-logged (JSONL).
 *
 * Transports:
 *   node server/mcp-server.js            stdio (agents on this machine)
 *   node server/mcp-server.js --http     + Streamable HTTP on :9003 (LAN)
 *
 * Install: npm install @modelcontextprotocol/sdk zod
 *
 * NOTE (Phase 1b): `get_preview` needs a small desktop-side addition —
 * the desktop must answer `request_preview` with `output_preview` {dataUrl}.
 * Until then the tool returns a clear "unsupported" error (no fake data).
 */

import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { z } from 'zod';
import { WebSocket } from 'ws';
import http from 'http';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const WS_URL = process.env.PRISM_WS_URL || 'ws://127.0.0.1:9001';
const HTTP_PORT = parseInt(process.env.PRISM_MCP_PORT || '9003', 10); // :9002 is the companion HTTP server
const AUDIT_PATH = process.env.PRISM_AUDIT_PATH || path.join(__dirname, 'mcp-audit.jsonl');

// ─── WS bridge ──────────────────────────────────────────────────────────────

const bridge = {
  ws: null,
  connected: false,
  project: null,        // last `sync` projectState (mapping layers, warps, …)
  library: [],          // last `library_sync`
  vjClips: null,        // last `vj_clips_sync`
  compositions: [],     // last `compositions_sync`
  outputFrozen: false,  // last `output_freeze_state`
  pendingSync: null,    // resolver waiting for a `sync` reply
  pendingPreview: null, // resolver waiting for `output_preview`
  queue: [],
};

function audit(tool, args, ok, error) {
  const line = JSON.stringify({ t: new Date().toISOString(), tool, args, ok, error: error || undefined });
  fs.appendFile(AUDIT_PATH, line + '\n', () => {});
}

function connectBridge() {
  return new Promise((resolve) => {
    const ws = new WebSocket(WS_URL);
    bridge.ws = ws;
    ws.on('open', () => {
      bridge.connected = true;
      console.error(`[prism-mcp] WS connected → ${WS_URL}`);
      resolve(true);
    });
    ws.on('message', (raw) => {
      let msg;
      try { msg = JSON.parse(raw.toString()); } catch { return; }
      switch (msg.type) {
        case 'sync':
          bridge.project = msg.project || null;
          if (bridge.pendingSync) { const r = bridge.pendingSync; bridge.pendingSync = null; r(bridge.project); }
          break;
        case 'library_sync': bridge.library = msg.library || []; break;
        case 'vj_clips_sync': bridge.vjClips = msg.vjClips || null; break;
        case 'compositions_sync': bridge.compositions = msg.compositions || []; break;
        case 'output_freeze_state': bridge.outputFrozen = !!msg.frozen; break;
        case 'output_preview': // Phase 1b — desktop answers request_preview
          if (bridge.pendingPreview) { const r = bridge.pendingPreview; bridge.pendingPreview = null; r(msg); }
          break;
        default: break; // broadcasts we don't need to act on
      }
    });
    ws.on('close', () => {
      bridge.connected = false;
      console.error('[prism-mcp] WS closed — reconnecting in 2s…');
      setTimeout(() => connectBridge(), 2000);
    });
    ws.on('error', (e) => console.error('[prism-mcp] WS error:', e.message));
    setTimeout(() => resolve(false), 8000); // don't hang boot forever
  });
}

function requireBridge() {
  if (!bridge.connected || !bridge.ws || bridge.ws.readyState !== WebSocket.OPEN) {
    throw new Error('Not connected to Ghost Arcade WS server (:9001). Is the desktop app running?');
  }
}

function send(msg) {
  requireBridge();
  bridge.ws.send(JSON.stringify(msg));
}

/** Ask the server for fresh state; resolves with projectState (or null on timeout). */
function requestSync(timeoutMs = 2500) {
  requireBridge();
  return new Promise((resolve) => {
    bridge.pendingSync = resolve;
    send({ type: 'sync_request' });
    setTimeout(() => {
      if (bridge.pendingSync) { bridge.pendingSync = null; resolve(bridge.project); }
    }, timeoutMs);
  });
}

/** Fire a command, then confirm via fresh sync. Returns { ok, state }. */
async function command(type, payload = {}) {
  send({ type, ...payload });
  await new Promise((r) => setTimeout(r, 250)); // let desktop apply + push state
  const state = await requestSync();
  return { ok: true, state: summarize(state) };
}

function summarize(project) {
  if (!project) return { note: 'no project state yet' };
  const layers = (project.layers || []).map((l) => ({
    id: l.id, name: l.name, visible: l.visible, opacity: l.opacity,
    warpMode: l.warpMode, effects: (l.effects || []).map((e) => e.name || e.id),
  }));
  return { layerCount: layers.length, layers, selectedLayerId: project.selectedLayerId || null };
}

const ok = (text, extra) => ({ content: [{ type: 'text', text: extra ? `${text}\n${JSON.stringify(extra, null, 2)}` : text }] });
const fail = (tool, args, message) => {
  audit(tool, args, false, message);
  return { content: [{ type: 'text', text: `ERROR: ${message}` }], isError: true };
};

// ─── Validation (mirrors server-side whitelists so agents get good errors) ──

const CORNERS = ['topLeft', 'topRight', 'bottomLeft', 'bottomRight'];
const finiteNum = (v) => typeof v === 'number' && Number.isFinite(v);
const normPoint = z.object({ x: z.number().min(0).max(1), y: z.number().min(0).max(1) })
  .refine((p) => finiteNum(p.x) && finiteNum(p.y), { message: 'x/y must be finite numbers in [0,1]' });

// ─── MCP server ───────────────────────────────────────────────────────────────

const server = new McpServer({ name: 'prism', version: '0.1.0' },
  { capabilities: { tools: {} } });

// — status / discovery —
server.tool('get_status', 'Current show state: layers, warps, VJ clips, compositions, output freeze.',
  {}, async () => {
    try {
      const state = await requestSync();
      const clips = bridge.vjClips;
      const clipCount = clips?.blocks?.reduce((a, b) => a + b.clipGrid.flat().filter((c) => c !== null).length, 0) || 0;
      return ok('status', {
        ...summarize(state),
        vjClipCount: clipCount,
        compositions: bridge.compositions.map((c) => ({ id: c.id, name: c.name })),
        mediaItems: bridge.library.length,
        outputFrozen: bridge.outputFrozen,
      });
    } catch (e) { return fail('get_status', {}, e.message); }
  });

server.tool('list_compositions', 'Saved compositions/looks that can be loaded.',
  {}, async () => {
    const list = bridge.compositions.map((c) => ({ id: c.id ?? c.name, name: c.name }));
    return ok(list.length ? 'compositions' : 'No compositions cached yet — open the desktop app so it pushes its preset list.', list);
  });

server.tool('load_composition', 'Load a saved composition/look by id or name.',
  { id: z.string().describe('composition id or name from list_compositions') },
  async ({ id }) => {
    try {
      const match = bridge.compositions.find((c) => c.id === id || c.name === id);
      const r = await command('load_composition', { compositionId: match?.id ?? id });
      audit('load_composition', { id }, true);
      return ok(`Loaded composition "${id}".`, r.state);
    } catch (e) { return fail('load_composition', { id }, e.message); }
  });

// — VJ deck —
server.tool('launch_clip', 'Fire a VJ clip.',
  { layerIndex: z.number().int().min(0), columnIndex: z.number().int().min(0) },
  async (a) => {
    try { const r = await command('trigger_vj_clip', a); audit('launch_clip', a, true); return ok(`Launched clip [layer ${a.layerIndex}, col ${a.columnIndex}].`, r.state); }
    catch (e) { return fail('launch_clip', a, e.message); }
  });

server.tool('trigger_column', 'Fire a whole VJ column.',
  { columnIndex: z.number().int().min(0) },
  async (a) => {
    try { const r = await command('trigger_vj_column', a); audit('trigger_column', a, true); return ok(`Triggered column ${a.columnIndex}.`, r.state); }
    catch (e) { return fail('trigger_column', a, e.message); }
  });

server.tool('stop_all_vj', 'Stop all playing VJ clips.', {},
  async () => {
    try { const r = await command('stop_all_vj'); audit('stop_all_vj', {}, true); return ok('Stopped all VJ clips.', r.state); }
    catch (e) { return fail('stop_all_vj', {}, e.message); }
  });

server.tool('crossfade', 'Move the deck crossfader (0 = A, 1 = B).',
  { value: z.number().min(0).max(1) },
  async ({ value }) => {
    try { const r = await command('set_vj_crossfader_value', { value }); audit('crossfade', { value }, true); return ok(`Crossfader → ${value}.`, r.state); }
    catch (e) { return fail('crossfade', { value }, e.message); }
  });

server.tool('cut_to_deck', 'Hard cut to deck A or B.',
  { deck: z.enum(['a', 'b']) },
  async ({ deck }) => {
    try { const r = await command(deck === 'a' ? 'vj_cut_to_a' : 'vj_cut_to_b'); audit('cut_to_deck', { deck }, true); return ok(`Cut to deck ${deck.toUpperCase()}.`, r.state); }
    catch (e) { return fail('cut_to_deck', { deck }, e.message); }
  });

server.tool('set_macro', 'Set a VJ macro value.',
  { macroId: z.string(), value: z.number() },
  async (a) => {
    try { const r = await command('set_vj_macro_value', a); audit('set_macro', a, true); return ok(`Macro "${a.macroId}" → ${a.value}.`, r.state); }
    catch (e) { return fail('set_macro', a, e.message); }
  });

server.tool('recall_snapshot', 'Recall a saved VJ snapshot.',
  { name: z.string() },
  async (a) => {
    try { const r = await command('recall_vj_snapshot', a); audit('recall_snapshot', a, true); return ok(`Recalled snapshot "${a.name}".`, r.state); }
    catch (e) { return fail('recall_snapshot', a, e.message); }
  });

server.tool('tap_tempo', 'Tap tempo (also clears manual BPM override via vj_clear_manual_bpm if needed).', {},
  async () => {
    try { const r = await command('vj_tap_tempo'); audit('tap_tempo', {}, true); return ok('Tap registered.', r.state); }
    catch (e) { return fail('tap_tempo', {}, e.message); }
  });

// — mapping layers / warps —
server.tool('set_warp_corner', 'Move one warp corner of a mapping layer (normalized 0..1 coords).',
  { layerId: z.string(), corner: z.enum(CORNERS), position: normPoint },
  async (a) => {
    try {
      const r = await command('control_point', { layerId: a.layerId, corner: a.corner, position: a.position });
      audit('set_warp_corner', a, true);
      return ok(`Warp corner ${a.corner} → (${a.position.x.toFixed(3)}, ${a.position.y.toFixed(3)}).`, r.state);
    } catch (e) { return fail('set_warp_corner', a, e.message); }
  });

server.tool('set_warp_mesh_point', 'Move one mesh-grid point of a mapping layer.',
  { layerId: z.string(), row: z.number().int().min(0).max(31), col: z.number().int().min(0).max(31), position: normPoint },
  async (a) => {
    try {
      const r = await command('mesh_point', { layerId: a.layerId, row: a.row, col: a.col, position: a.position });
      audit('set_warp_mesh_point', a, true);
      return ok(`Mesh [${a.row},${a.col}] → (${a.position.x.toFixed(3)}, ${a.position.y.toFixed(3)}).`, r.state);
    } catch (e) { return fail('set_warp_mesh_point', a, e.message); }
  });

server.tool('set_warp_mode', 'Switch a layer between corner and mesh warp.',
  { layerId: z.string(), mode: z.enum(['corners', 'mesh']) },
  async (a) => {
    try { const r = await command('warp_mode', a); audit('set_warp_mode', a, true); return ok(`Warp mode → ${a.mode}.`, r.state); }
    catch (e) { return fail('set_warp_mode', a, e.message); }
  });

server.tool('set_layer_param', 'Set a mapping-layer parameter (opacity 0..1, blendMode, visible).',
  {
    layerId: z.string(),
    param: z.enum(['opacity', 'blendMode', 'visible']),
    value: z.union([z.number(), z.string(), z.boolean()]),
  },
  async (a) => {
    try {
      if (a.param === 'opacity' && (typeof a.value !== 'number' || a.value < 0 || a.value > 1))
        return fail('set_layer_param', a, 'opacity must be a number in [0,1] — rejected, not clamped.');
      const r = await command('parameter', a);
      audit('set_layer_param', a, true);
      return ok(`Layer param ${a.param} → ${a.value}.`, r.state);
    } catch (e) { return fail('set_layer_param', a, e.message); }
  });

server.tool('set_effect_params', 'Update effect parameters on a mapping layer or VJ target.',
  {
    scope: z.enum(['mapping_layer', 'vj_layer', 'vj_comp', 'vj_clip']),
    layerId: z.string().optional().describe('mapping layerId (mapping_layer scope)'),
    effectId: z.string(),
    params: z.record(z.string(), z.union([z.number(), z.string(), z.boolean()])),
    vjTarget: z.object({ layerIndex: z.number().int().min(0).optional(), clipId: z.string().optional() }).optional()
      .describe('for vj_layer / vj_clip scopes'),
  },
  async (a) => {
    const type = { mapping_layer: 'update_mapping_layer_effect_params', vj_layer: 'update_vj_layer_effect_params', vj_comp: 'update_vj_comp_effect_params', vj_clip: 'update_vj_clip_effect_params' }[a.scope];
    try {
      const r = await command(type, { layerId: a.layerId, effectId: a.effectId, params: a.params, ...a.vjTarget });
      audit('set_effect_params', a, true);
      return ok(`Effect params updated (${a.scope}).`, r.state);
    } catch (e) { return fail('set_effect_params', a, e.message); }
  });

server.tool('set_layer_shape', 'Set the mask/shape of a mapping layer.',
  {
    layerId: z.string(),
    shape: z.enum(['rectangle', 'circle', 'ellipse', 'triangle', 'polygon', 'star', 'line', 'polyline', 'custom']),
    params: z.record(z.string(), z.union([z.number(), z.string(), z.boolean()])).optional(),
  },
  async (a) => {
    try { const r = await command('set_mapping_layer_shape', a); audit('set_layer_shape', a, true); return ok(`Layer shape → ${a.shape}.`, r.state); }
    catch (e) { return fail('set_layer_shape', a, e.message); }
  });

// — output —
server.tool('freeze_output', 'Freeze/unfreeze the projector output.',
  { frozen: z.boolean() },
  async (a) => {
    try { const r = await command('set_output_freeze', a); audit('freeze_output', a, true); return ok(`Output ${a.frozen ? 'frozen' : 'live'}.`, r.state); }
    catch (e) { return fail('freeze_output', a, e.message); }
  });

server.tool('get_preview', 'Grab the current projector output frame (agent eyes). Phase 1b: needs desktop support.',
  { maxWidth: z.number().int().min(160).max(1920).default(640) },
  async (a) => {
    try {
      requireBridge();
      const frame = await new Promise((resolve) => {
        bridge.pendingPreview = resolve;
        send({ type: 'request_preview', maxWidth: a.maxWidth });
        setTimeout(() => { if (bridge.pendingPreview) { bridge.pendingPreview = null; resolve(null); } }, 3000);
      });
      if (!frame?.dataUrl) {
        return fail('get_preview', a, 'Desktop build does not answer request_preview yet (Phase 1b). Look at the projector / desktop window instead.');
      }
      const base64 = frame.dataUrl.split(',')[1] || '';
      audit('get_preview', a, true);
      return { content: [{ type: 'image', data: base64, mimeType: 'image/jpeg' }] };
    } catch (e) { return fail('get_preview', a, e.message); }
  });

// ─── Boot ─────────────────────────────────────────────────────────────────────

async function main() {
  // Don't block boot on the WS bridge: it retries forever in the background,
  // and tools fail with a clear "desktop app not running" error until it's up.
  connectBridge();
  const useHttp = process.argv.includes('--http');

  if (useHttp) {
    // Stateless Streamable HTTP on :9003 (no session persistence — LAN use)
    const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined });
    await server.connect(transport);
    const httpServer = http.createServer(async (req, res) => {
      if (req.url === '/mcp' && (req.method === 'POST' || req.method === 'GET' || req.method === 'DELETE')) {
        let body;
        if (req.method === 'POST') {
          body = await new Promise((resolve, reject) => {
            let data = '';
            req.on('data', (c) => { data += c; if (data.length > 4 * 1024 * 1024) req.destroy(); });
            req.on('end', () => { try { resolve(JSON.parse(data || '{}')); } catch (e) { reject(e); } });
            req.on('error', reject);
          }).catch(() => null);
          if (!body) { res.writeHead(400).end('bad json'); return; }
        }
        await transport.handleRequest(req, res, body);
      } else { res.writeHead(404).end('not found'); }
    });
    // NOTE: verify handleRequest signature against the installed @modelcontextprotocol/sdk version.
    httpServer.listen(HTTP_PORT, '0.0.0.0', () =>
      console.error(`[prism-mcp] HTTP listening on :${HTTP_PORT}/mcp (LAN only)`));
  } else {
    await server.connect(new StdioServerTransport());
    console.error('[prism-mcp] stdio transport ready');
  }
}

main().catch((e) => { console.error('[prism-mcp] fatal:', e); process.exit(1); });
