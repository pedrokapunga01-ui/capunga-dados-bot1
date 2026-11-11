'use strict';

/**
 * Bac Bo Signal Monitor — rebuilt core
 * Required environment: BOT_TOKEN, CHAT_ID, ADMIN_TOKEN, DATABASE_URL.
 * PORT may be supplied by the hosting platform.
 * This software reports historical patterns; it cannot predict or guarantee results.
 */
const crypto = require('node:crypto');
const express = require('express');
const cors = require('cors');
const axios = require('axios');
const { Pool } = require('pg');

const VERSION = '20.0.0';
const PORT = Number(process.env.PORT || 10000);
const { BOT_TOKEN = '', CHAT_ID = '', ADMIN_TOKEN = '', DATABASE_URL = '' } = process.env;
const TZ = 'Africa/Luanda';
const FEED = 'https://api-cs.casino.org/svc-evolution-game-events/api/bacbo';
const CFG = Object.freeze({
  pollMs: 2500, pageSize: 100, maxPages: 10, historyLimit: 5000,
  maxFeedAgeMs: 90000, settleDelayMs: 4500, minSignalIntervalMs: 180000,
  maxGale: 2, maxPending: 1, minHistory: 120, minStrategySamples: 100,
  minLowerBound: 0.55, // Conservative research gate, not an assurance of positive EV.
  roundGapMs: 7 * 60_000, alertCooldownMs: 10 * 60_000,
  requestTimeoutMs: 9000, telegramTimeoutMs: 10000
});
const OUTCOME = Object.freeze({ PLAYER: 'Jogador', BANKER: 'Banca', TIE: 'Empate' });
const CLOSED = new Set(['GREEN', 'G1', 'G2', 'EMPATE', 'EG1', 'EG2', 'LOSS']);
const app = express();
app.disable('x-powered-by');
app.use(cors({ origin: false }));
app.use(express.json({ limit: '32kb' }));
const pool = new Pool({ connectionString: DATABASE_URL, max: 8, connectionTimeoutMillis: 8000, idleTimeoutMillis: 30000, ssl: /localhost|127\.0\.0\.1/.test(DATABASE_URL) ? false : { rejectUnauthorized: false } });
const state = {
  rounds: [], signals: [], pending: [], nextSignalId: 1, lastSignalAt: 0,
  feedHealthy: false, feedReason: 'ainda não sincronizado', lastSyncAt: null, trustedRoundId: null,
  lastRoundAt: null, lastFeedError: null, lockClient: null, lockOk: false,
  lastAlertAt: 0, dailyStats: emptyStats(), dailyKey: luandaDate()
};
let running = false, shuttingDown = false, timer = null, httpServer = null;
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
function emptyStats() { return { total: 0, green: 0, g1: 0, g2: 0, empate: 0, eg1: 0, eg2: 0, loss: 0 }; }
function luandaDate(d = new Date()) { return new Intl.DateTimeFormat('en-CA', { timeZone: TZ, year: 'numeric', month: '2-digit', day: '2-digit' }).format(d); }
function luandaTime(d = new Date()) { return new Intl.DateTimeFormat('pt-PT', { timeZone: TZ, hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false }).format(d); }
function log(event, details = {}) { console.log(JSON.stringify({ at: new Date().toISOString(), event, ...details })); }
function normalizeOutcome(raw) {
  if (raw && typeof raw === 'object') raw = raw.outcome ?? raw.resultado ?? raw.winner ?? raw.name;
  const s = String(raw ?? '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().replace(/[\s_-]/g, '');
  if (['player', 'jogador', 'playerwon', 'playerwin', 'blue'].includes(s)) return OUTCOME.PLAYER;
  if (['banker', 'banca', 'bankerwon', 'bankerwin', 'red', 'vermelho'].includes(s)) return OUTCOME.BANKER;
  if (['tie', 'draw', 'empate', 'tied', 'push'].includes(s)) return OUTCOME.TIE;
  return null;
}
function toMs(v) {
  if (v == null || v === '') return null;
  let n;
  if (typeof v === 'number' || /^\d{10,13}$/.test(String(v))) { n = Number(v); if (n < 1e12) n *= 1000; }
  else n = Date.parse(v);
  return Number.isFinite(n) ? n : null;
}
function firstArray(obj) {
  if (Array.isArray(obj)) return obj;
  for (const x of [obj?.data, obj?.results, obj?.items, obj?.rounds, obj?.events, obj?.resultados, obj?.data?.results, obj?.data?.items, obj?.data?.rounds, obj?.data?.events, obj?.data?.data]) if (Array.isArray(x)) return x;
  return [];
}
function normalizeRound(item) {
  const d = item?.data && !Array.isArray(item.data) ? item.data : item?.round ?? item ?? {};
  const result = (d.result && typeof d.result === 'object') ? d.result : (item?.result && typeof item.result === 'object' ? item.result : {});
  const raw = result.outcome ?? result.resultado ?? result.winner ?? d.outcome ?? d.resultado ?? d.winner ?? item?.outcome ?? item?.resultado ?? item?.winner ?? (typeof d.result === 'string' ? d.result : null) ?? (typeof item?.result === 'string' ? item.result : null);
  const outcome = normalizeOutcome(raw);
  const id = String(d.id ?? d.roundId ?? d.round_id ?? d.eventId ?? item?.id ?? item?.roundId ?? item?.round_id ?? item?.eventId ?? '').trim();
  const at = toMs(d.settledAt ?? d.endTime ?? d.endedAt ?? d.finishedAt ?? d.fim ?? item?.settledAt ?? item?.fim);
  if (!id || !outcome || !at) return null;
  const score = v => v == null || v === '' || !Number.isFinite(Number(v)) ? null : Number(v);
  return { id, outcome, at, player: score(result.playerDice?.score ?? d.playerDice?.score ?? d.playerScore ?? d.player_score), banker: score(result.bankerDice?.score ?? d.bankerDice?.score ?? d.bankerScore ?? d.banker_score) };
}
function fmtRoundOrder(a, b) { return a.at - b.at || a.id.localeCompare(b.id); }
async function getPage(page) {
  const response = await axios.get(FEED, { timeout: CFG.requestTimeoutMs, headers: { Accept: 'application/json', 'User-Agent': 'Mozilla/5.0', Origin: 'https://www.casino.org', Referer: 'https://www.casino.org/casinoscores/bac-bo/' }, params: { page, size: CFG.pageSize, sort: 'data.settledAt,desc', duration: 120, wheelResults: 'PlayerWon,BankerWon,Tie' } });
  return firstArray(response.data);
}
async function fetchSnapshot() {
  const map = new Map(); let pagesRead = 0, metOverlap = false;
  const known = new Set(state.rounds.map(r => r.id));
  for (let page = 0; page < CFG.maxPages; page++) {
    const items = await getPage(page); pagesRead++;
    if (!items.length) break;
    let validCount = 0;
    for (const item of items) {
      const r = normalizeRound(item); if (!r) continue;
      validCount++; map.set(r.id, r); if (known.has(r.id)) metOverlap = true;
    }
    const foundAnchor = state.trustedRoundId ? map.has(state.trustedRoundId) : metOverlap;
    if (validCount === 0 || (foundAnchor && page >= 1) || items.length < CFG.pageSize) break;
  }
  const rounds = [...map.values()].sort(fmtRoundOrder);
  if (!rounds.length) throw new Error('O feed não trouxe rodadas válidas.');
  const includesTrusted = !state.trustedRoundId || map.has(state.trustedRoundId);
  return { rounds, pagesRead, metOverlap, includesTrusted };
}
async function initDb() {
  if (!BOT_TOKEN || !CHAT_ID || !ADMIN_TOKEN || !DATABASE_URL) throw new Error('Configure BOT_TOKEN, CHAT_ID, ADMIN_TOKEN e DATABASE_URL.');
  if (!/^postgres(?:ql)?:\/\//i.test(DATABASE_URL)) throw new Error('DATABASE_URL não parece uma URL PostgreSQL.');
  await pool.query(`
    CREATE TABLE IF NOT EXISTS bb_rounds(id TEXT PRIMARY KEY, outcome TEXT NOT NULL, at_ms BIGINT NOT NULL, player DOUBLE PRECISION, banker DOUBLE PRECISION, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW());
    CREATE INDEX IF NOT EXISTS bb_rounds_at_idx ON bb_rounds(at_ms);
    CREATE TABLE IF NOT EXISTS bb_state(k TEXT PRIMARY KEY, v TEXT NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW());
    CREATE TABLE IF NOT EXISTS bb_signals(id BIGINT PRIMARY KEY, round_id TEXT NOT NULL, color TEXT NOT NULL, strategy TEXT NOT NULL, status TEXT NOT NULL, gale SMALLINT, created_at TIMESTAMPTZ NOT NULL, closed_at TIMESTAMPTZ, settled_round_id TEXT, sample_n INTEGER, empirical_rate DOUBLE PRECISION);
    CREATE INDEX IF NOT EXISTS bb_signals_created_idx ON bb_signals(created_at DESC);
    CREATE INDEX IF NOT EXISTS bb_signals_status_idx ON bb_signals(status);
    CREATE TABLE IF NOT EXISTS bb_outbox(id BIGSERIAL PRIMARY KEY, dedupe_key TEXT UNIQUE NOT NULL, text TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), sent_at TIMESTAMPTZ, attempts INTEGER NOT NULL DEFAULT 0, last_error TEXT);
  `);
  // Preserve data from the v19 schema when upgrading an existing database.
  const legacy = await pool.query("SELECT to_regclass('public.capunga_rounds') rounds, to_regclass('public.capunga_signals') signals, to_regclass('public.capunga_state') state");
  const hasLegacy = Boolean(legacy.rows[0]?.rounds || legacy.rows[0]?.signals || legacy.rows[0]?.state);
  if (hasLegacy) {
    const client = await pool.connect(); let began = false;
    try {
      await client.query('BEGIN'); began = true;
      if (legacy.rows[0].rounds) await client.query(`INSERT INTO bb_rounds(id,outcome,at_ms,player,banker) SELECT id,resultado,t,pj,pb FROM capunga_rounds ON CONFLICT(id) DO NOTHING`);
      if (legacy.rows[0].signals) await client.query(`INSERT INTO bb_signals(id,round_id,color,strategy,status,gale,created_at,closed_at)
        SELECT s.id, COALESCE((SELECT r.id FROM bb_rounds r WHERE r.at_ms <= EXTRACT(EPOCH FROM s.created_at)*1000 ORDER BY r.at_ms DESC LIMIT 1), 'legacy-'||s.id),
          s.cor,s.nome,s.status,s.gale,s.created_at,s.closed_at FROM capunga_signals s ON CONFLICT(id) DO NOTHING`);
      if (legacy.rows[0].signals) await client.query(`INSERT INTO bb_outbox(dedupe_key,text,sent_at)
        SELECT 'result:'||id,'Resultado histórico importado; notificação original considerada entregue.',NOW()
        FROM capunga_signals WHERE status <> 'PENDENTE' ON CONFLICT(dedupe_key) DO NOTHING`);
      if (legacy.rows[0].state) await client.query(`INSERT INTO bb_state(k,v) SELECT k,v FROM capunga_state WHERE k IN ('nextId','lastSignalAt') ON CONFLICT(k) DO NOTHING`);
      await client.query('COMMIT');
    } catch (e) {
      if (began) await client.query('ROLLBACK').catch(()=>{});
      throw new Error(`Migração do banco legado falhou sem concluir parcialmente: ${e.message}`);
    } finally { client.release(); }
  }
  const lock = await pool.connect();
  const locked = await lock.query('SELECT pg_try_advisory_lock($1) AS ok', [727272]);
  if (!locked.rows[0].ok) { lock.release(); throw new Error('Outra instância do bot mantém o lock no banco.'); }
  state.lockClient = lock; state.lockOk = true;
}
async function loadState() {
  const [rounds, signals] = await Promise.all([
    pool.query('SELECT id,outcome,at_ms,player,banker FROM bb_rounds ORDER BY at_ms DESC LIMIT $1', [CFG.historyLimit]),
    pool.query('SELECT * FROM bb_signals ORDER BY created_at DESC LIMIT 1000')
  ]);
  state.rounds = rounds.rows.map(r => ({ id: r.id, outcome: r.outcome, at: Number(r.at_ms), player: r.player == null ? null : Number(r.player), banker: r.banker == null ? null : Number(r.banker) })).reverse();
  state.signals = signals.rows.map(rowToSignal).reverse();
  for (const s of state.signals.filter(x => CLOSED.has(x.status))) await enqueue(resultText(s), `result:${s.id}`);
  const idRow = await pool.query("SELECT v FROM bb_state WHERE k='nextId'");
  if (idRow.rows[0]) state.nextSignalId = Math.max(state.nextSignalId, Number.parseInt(idRow.rows[0].v, 10) || 1);
  const maxId = await pool.query('SELECT COALESCE(MAX(id),0)+1 AS next_id FROM bb_signals');
  state.nextSignalId = Math.max(state.nextSignalId, Number(maxId.rows[0].next_id) || 1);
  const timeRow = await pool.query("SELECT v FROM bb_state WHERE k='lastSignalAt'"); state.lastSignalAt = Number.parseInt(timeRow.rows[0]?.v || '0', 10) || 0;
  const cursorRow = await pool.query("SELECT v FROM bb_state WHERE k='trustedRoundId'");
  state.trustedRoundId = cursorRow.rows[0]?.v || state.rounds.at(-1)?.id || null;
  await refreshDailyStats();
}
function rowToSignal(r) { return { id: Number(r.id), roundId: r.round_id, color: r.color, strategy: r.strategy, status: r.status, gale: r.gale == null ? null : Number(r.gale), t: new Date(r.created_at).getTime(), closedAt: r.closed_at ? new Date(r.closed_at).getTime() : null, settledRoundId: r.settled_round_id, sampleN: r.sample_n, empiricalRate: r.empirical_rate }; }
async function refreshDailyStats() {
  const result = await pool.query(`SELECT status,COUNT(*)::int n FROM bb_signals WHERE status <> 'PENDENTE' AND (created_at AT TIME ZONE $1)::date=(NOW() AT TIME ZONE $1)::date GROUP BY status`, [TZ]);
  const s = emptyStats();
  for (const row of result.rows) { const n = row.n; s.total += n; if (row.status === 'GREEN') s.green += n; else if (row.status === 'G1') s.g1 += n; else if (row.status === 'G2') s.g2 += n; else if (row.status === 'EMPATE') s.empate += n; else if (row.status === 'EG1') s.eg1 += n; else if (row.status === 'EG2') s.eg2 += n; else if (row.status === 'LOSS') s.loss += n; }
  state.dailyStats = s; state.dailyKey = luandaDate();
}
function dailyStreak() {
  const key = luandaDate(); let n = 0;
  for (let i = state.signals.length - 1; i >= 0; i--) {
    const s = state.signals[i]; if (luandaDate(new Date(s.t)) !== key) continue;
    if (!CLOSED.has(s.status)) continue; if (s.status === 'LOSS') break; n++;
  }
  return n;
}
async function storeRounds(rounds) {
  const client = await pool.connect(); let inserted = 0, changed = 0;
  try {
    await client.query('BEGIN');
    for (const r of rounds) {
      const q = await client.query(`INSERT INTO bb_rounds(id,outcome,at_ms,player,banker) VALUES($1,$2,$3,$4,$5) ON CONFLICT(id) DO UPDATE SET outcome=EXCLUDED.outcome,at_ms=EXCLUDED.at_ms,player=EXCLUDED.player,banker=EXCLUDED.banker,updated_at=NOW() WHERE (bb_rounds.outcome,bb_rounds.at_ms,bb_rounds.player,bb_rounds.banker) IS DISTINCT FROM (EXCLUDED.outcome,EXCLUDED.at_ms,EXCLUDED.player,EXCLUDED.banker) RETURNING (xmax=0) AS was_inserted`, [r.id, r.outcome, r.at, r.player, r.banker]);
      if (q.rowCount) { if (q.rows[0].was_inserted) inserted++; else changed++; }
    }
    await client.query('COMMIT');
  } catch (e) { await client.query('ROLLBACK'); throw e; } finally { client.release(); }
  return { inserted, changed };
}
function assessFeed(rounds, fetchInfo, priorRounds = [], now = Date.now()) {
  const last = rounds.at(-1);
  const tooOld = !last || now - last.at > CFG.maxFeedAgeMs;
  const oldById = new Map(priorRounds.map(r => [r.id, r]));
  const newlySeen = rounds.filter(r => !oldById.has(r.id));
  let reason = null;
  if (tooOld) reason = 'última rodada fora do limite de atualização';
  else if (priorRounds.length && fetchInfo.includesTrusted === false) reason = 'o cursor confiável não foi reencontrado; reconciliação incompleta';
  else if (priorRounds.length && !fetchInfo.metOverlap) reason = 'não foi encontrado ponto de sobreposição; reconciliação incompleta';
  else if (priorRounds.length && newlySeen.length) {
    const earliestNew = newlySeen[0]; const oldNewest = priorRounds.at(-1);
    if (earliestNew.at < oldNewest.at && !oldById.has(earliestNew.id)) reason = 'novos resultados chegaram fora de ordem';
  }
  return { healthy: !reason, reason: reason || 'sincronização recente; integridade limitada ao conteúdo do provedor' };
}
async function syncFeed() {
  const info = await fetchSnapshot();
  const priorRounds = state.rounds;
  const dbChange = await storeRounds(info.rounds);
  const merged = new Map(state.rounds.map(r => [r.id, r])); for (const r of info.rounds) merged.set(r.id, r);
  state.rounds = [...merged.values()].sort(fmtRoundOrder).slice(-CFG.historyLimit);
  const last = state.rounds.at(-1); state.lastRoundAt = last?.at || null;
  const health = assessFeed(info.rounds, info, priorRounds); state.feedHealthy = health.healthy; state.feedReason = health.reason;
  if (state.feedHealthy) {
    state.trustedRoundId = info.rounds.at(-1).id;
    await pool.query("INSERT INTO bb_state(k,v) VALUES('trustedRoundId',$1) ON CONFLICT(k) DO UPDATE SET v=EXCLUDED.v,updated_at=NOW()", [state.trustedRoundId]);
  }
  state.lastSyncAt = new Date().toISOString(); state.lastFeedError = null;
  if (dbChange.inserted || dbChange.changed) log('feed_sync', { ...dbChange, rounds: state.rounds.length, pages: info.pagesRead, healthy: state.feedHealthy, reason: state.feedReason });
  await settleSignals();
  if (!state.feedHealthy) await maybeAlert(`Feed em pausa: ${state.feedReason}. Não serão emitidos novos sinais até a fonte ficar atualizada.`);
  return dbChange;
}
function transitionPattern(history) {
  const colors = history.filter(r => r.outcome !== OUTCOME.TIE).slice(0, 12).map(r => r.outcome);
  if (colors.length < 8) return null;
  // Pattern labels are descriptive only; they are not claims of predictive power.
  const latest = colors[0]; let run = 1;
  while (run < colors.length && colors[run] === latest) run++;
  if (run >= 4) return { color: latest, strategy: `sequência observada ${run}` };
  return null;
}
function wilsonLower(w, n, z = 1.96) {
  if (!n) return 0;
  const p = w / n, z2 = z * z;
  return (p + z2 / (2*n) - z * Math.sqrt((p*(1-p) + z2/(4*n))/n)) / (1 + z2/n);
}
function strategyEvidence(strategy, color) {
  // Evaluate prior examples only, never including the current decision point.
  const history = state.rounds; let n = 0, wins = 0;
  for (let i = 12; i < history.length - 1; i++) {
    const prefix = history.slice(0, i).reverse(); const candidate = transitionPattern(prefix);
    if (!candidate || candidate.strategy !== strategy || candidate.color !== color) continue;
    n++;
    const next = history[i]; if (next && (next.outcome === color || next.outcome === OUTCOME.TIE)) wins++;
  }
  return { n, rate: n ? wins / n : 0, lower: wilsonLower(wins, n) };
}
async function enqueue(text, key) { await pool.query('INSERT INTO bb_outbox(dedupe_key,text) VALUES($1,$2) ON CONFLICT(dedupe_key) DO NOTHING', [key, text]); }
async function telegram(text) {
  const result = await axios.post(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, { chat_id: CHAT_ID, text, parse_mode: 'HTML', disable_web_page_preview: true }, { timeout: CFG.telegramTimeoutMs });
  if (!result.data?.ok) throw new Error(`Telegram recusou mensagem: ${JSON.stringify(result.data)}`);
  return result.data.result.message_id;
}
async function drainOutbox() {
  const rows = await pool.query('SELECT id,dedupe_key,text FROM bb_outbox WHERE sent_at IS NULL AND attempts < 10 ORDER BY id LIMIT 20');
  for (const row of rows.rows) {
    try { await telegram(row.text); await pool.query('UPDATE bb_outbox SET sent_at=NOW(),attempts=attempts+1,last_error=NULL WHERE id=$1', [row.id]); }
    catch (e) { await pool.query('UPDATE bb_outbox SET attempts=attempts+1,last_error=$2 WHERE id=$1', [row.id, String(e.message).slice(0, 500)]); log('telegram_error', { id: row.id, error: e.message }); }
  }
}
function signalText(id, candidate, evidence) {
  return [`🎲 <b>BAC BO · SINAL #${id}</b>`, '━━━━━━━━━━━━━━━', `Entrada: <b>${candidate.color === OUTCOME.BANKER ? 'Banca 🔴' : 'Jogador 🔵'}</b>`, 'Proteção: empate (push)', `Leitura: ${candidate.strategy}`, `Amostra semelhante: ${evidence.n}`, `Frequência histórica observada: ${(evidence.rate*100).toFixed(1)}%`, `Hora Angola: ${luandaTime()}`, '━━━━━━━━━━━━━━━', '<i>Estatística passada não prevê nem garante o próximo resultado. Aposte apenas o que pode perder.</i>'].join('\n');
}
async function maybeCreateSignal() {
  if (!state.feedHealthy) return;
  if (state.rounds.length < CFG.minHistory) return;
  if (state.pending.length >= CFG.maxPending) return;
  if (Date.now() - (state.lastRoundAt || 0) < CFG.settleDelayMs) return;
  if (Date.now() - state.lastSignalAt < CFG.minSignalIntervalMs) return;
  const anchor = state.rounds.at(-1);
  const candidate = transitionPattern([...state.rounds].reverse()); if (!candidate) return;
  const evidence = strategyEvidence(candidate.strategy, candidate.color);
  if (evidence.n < CFG.minStrategySamples || evidence.lower < CFG.minLowerBound) return;
  const id = state.nextSignalId;
  const nextId = id + 1;
  const createdAt = Date.now();
  const text = signalText(id, candidate, evidence);
  const client = await pool.connect();
  let began = false;
  try {
    await client.query('BEGIN'); began = true;
    await client.query('INSERT INTO bb_signals(id,round_id,color,strategy,status,created_at,sample_n,empirical_rate) VALUES($1,$2,$3,$4,$5,NOW(),$6,$7)', [id, anchor.id, candidate.color, candidate.strategy, 'PENDENTE', evidence.n, evidence.rate]);
    await client.query("INSERT INTO bb_state(k,v) VALUES('nextId',$1),('lastSignalAt',$2) ON CONFLICT(k) DO UPDATE SET v=EXCLUDED.v,updated_at=NOW()", [String(nextId), String(createdAt)]);
    await client.query('INSERT INTO bb_outbox(dedupe_key,text) VALUES($1,$2) ON CONFLICT(dedupe_key) DO NOTHING', [`signal:${id}`, text]);
    await client.query('COMMIT'); began = false;
  } catch (e) { if (began) await client.query('ROLLBACK').catch(()=>{}); throw e; }
  finally { client.release(); }
  state.nextSignalId = nextId;
  state.lastSignalAt = createdAt;
  state.signals.push({ id, roundId: anchor.id, color: candidate.color, strategy: candidate.strategy, status: 'PENDENTE', gale: null, t: createdAt, closedAt: null, sampleN: evidence.n, empiricalRate: evidence.rate });
  state.pending = state.signals.filter(s => s.status === 'PENDENTE');
  log('signal_created', { id, anchor: anchor.id, strategy: candidate.strategy, n: evidence.n, rate: evidence.rate, lower: evidence.lower });
}
function settleStatus(round, color, attempt) {
  if (round.outcome === OUTCOME.TIE) return attempt === 0 ? 'EMPATE' : attempt === 1 ? 'EG1' : 'EG2';
  if (round.outcome === color) return attempt === 0 ? 'GREEN' : attempt === 1 ? 'G1' : 'G2';
  return null;
}
function resolveWindow(base, after, color) {
  const attempts = after.slice(0, CFG.maxGale + 1);
  for (let i = 0; i < attempts.length; i++) {
    const previous = i === 0 ? base : attempts[i - 1];
    if (attempts[i].at - previous.at > CFG.roundGapMs) return { final: null, uncertain: true, missed: attempts.slice(0, i) };
    const status = settleStatus(attempts[i], color, i);
    if (status) return { final: { status, attempt: i, round: attempts[i] }, uncertain: false, missed: attempts.slice(0, i) };
  }
  if (attempts.length >= CFG.maxGale + 1) return { final: { status: 'LOSS', attempt: CFG.maxGale, round: attempts[CFG.maxGale] }, uncertain: false, missed: attempts.slice(0, CFG.maxGale) };
  return { final: null, uncertain: false, missed: attempts };
}
async function settleSignals() {
  const positions = new Map(state.rounds.map((r, i) => [r.id, i]));
  for (const s of state.signals.filter(x => x.status === 'PENDENTE')) {
    const start = positions.get(s.roundId); if (start == null) continue;
    const after = state.rounds.slice(start + 1, start + 1 + CFG.maxGale + 1);
    const { final, missed } = resolveWindow(state.rounds[start], after, s.color);
    if (!final && !missed.length) continue;
    const closedAt = Date.now();
    const closed = final ? { ...s, status: final.status, gale: final.attempt, closedAt, settledRoundId: final.round.id } : null;
    const client = await pool.connect(); let began = false, changed = false;
    try {
      await client.query('BEGIN'); began = true;
      for (let i = 0; i < missed.length; i++) await client.query('INSERT INTO bb_outbox(dedupe_key,text) VALUES($1,$2) ON CONFLICT(dedupe_key) DO NOTHING', [`gale:${s.id}:${i+1}`, `↪️ <b>PROTEÇÃO GALE ${i+1}</b> · sinal #${s.id}\nManter entrada: ${s.color === OUTCOME.BANKER ? 'Banca 🔴' : 'Jogador 🔵'}\n<i>Não aumente a aposta para recuperar perdas.</i>`]);
      if (final) {
        const update = await client.query('UPDATE bb_signals SET status=$1,gale=$2,closed_at=NOW(),settled_round_id=$3 WHERE id=$4 AND status=$5', [final.status, final.attempt, final.round.id, s.id, 'PENDENTE']);
        changed = update.rowCount === 1;
        if (changed) await client.query('INSERT INTO bb_outbox(dedupe_key,text) VALUES($1,$2) ON CONFLICT(dedupe_key) DO NOTHING', [`result:${s.id}`, resultText(closed)]);
      }
      await client.query('COMMIT'); began = false;
    } catch (e) { if (began) await client.query('ROLLBACK').catch(()=>{}); throw e; }
    finally { client.release(); }
    if (!final || !changed) continue;
    Object.assign(s, closed);
    await refreshDailyStats();
    await enqueue(historyText(), `daily-history:${luandaDate()}:${s.id}`);
    log('signal_settled', { id: s.id, status: s.status, gale: s.gale, round: final.round.id });
  }
  state.pending = state.signals.filter(s => s.status === 'PENDENTE');
}
function resultText(s) { return `${s.status === 'LOSS' ? '❌' : '✅'} <b>${s.status}</b> · sinal #${s.id}\nRodada de liquidação: <code>${escapeHtml(s.settledRoundId || '')}</code>\nHora Angola: ${luandaTime()}\n<i>Resultado informativo; respeite seu limite de risco.</i>`; }
function escapeHtml(s) { return String(s).replace(/[&<>"']/g, c => ({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' }[c])); }
function historyText() { const s=state.dailyStats; return `📊 <b>RESUMO DO DIA · ANGOLA</b>\n${luandaDate()}\nTotal: ${s.total} | Greens: ${s.green} | Gale 1: ${s.g1} | Gale 2: ${s.g2} | Empates: ${s.empate+s.eg1+s.eg2} | Loss: ${s.loss}\nSequência atual: ${dailyStreak()}`; }
async function maybeAlert(message) { const now=Date.now(); if (now-state.lastAlertAt<CFG.alertCooldownMs) return; state.lastAlertAt=now; await enqueue(`⚠️ <b>ESTADO DO FEED</b>\n${escapeHtml(message)}\nHora Angola: ${luandaTime()}`, `feed-alert:${Math.floor(now/CFG.alertCooldownMs)}`); }
async function runCycle() {
  if (running || shuttingDown) return; running=true;
  try {
    if (luandaDate() !== state.dailyKey) await refreshDailyStats();
    await syncFeed(); await maybeCreateSignal(); await drainOutbox();
  } catch (e) {
    state.feedHealthy=false; state.feedReason='erro de sincronização'; state.lastFeedError=String(e.message); log('cycle_error',{error:e.message});
    await maybeAlert(`Falha ao sincronizar os resultados: ${e.message}`.slice(0,800)).catch(()=>{});
    await drainOutbox().catch(()=>{});
  } finally { running=false; }
}
function safeEqual(a,b) { const x=Buffer.from(String(a)), y=Buffer.from(String(b)); return x.length===y.length && crypto.timingSafeEqual(x,y); }
function admin(req,res,next) { const token=String(req.get('authorization')||'').replace(/^Bearer\s+/i,''); if(!safeEqual(token,ADMIN_TOKEN)) return res.status(401).json({error:'unauthorized'}); next(); }
app.get('/health',(_req,res)=>res.status(state.feedHealthy&&state.lockOk?200:503).json({ok:state.feedHealthy&&state.lockOk,version:VERSION,lock:state.lockOk,feedHealthy:state.feedHealthy,feedReason:state.feedReason,lastSyncAt:state.lastSyncAt,lastRoundAt:state.lastRoundAt?new Date(state.lastRoundAt).toISOString():null,feedAgeMs:state.lastRoundAt?Date.now()-state.lastRoundAt:null,lastFeedError:state.lastFeedError,uptimeSeconds:Math.floor(process.uptime())}));
app.get('/status',admin,(_req,res)=>res.json({version:VERSION,nowLuanda:luandaTime(),dateLuanda:luandaDate(),feedHealthy:state.feedHealthy,feedReason:state.feedReason,lastSyncAt:state.lastSyncAt,lastRoundAt:state.lastRoundAt?new Date(state.lastRoundAt).toISOString():null,roundsInMemory:state.rounds.length,pending:state.pending.length,lastFeedError:state.lastFeedError,stats:state.dailyStats,greenStreak:dailyStreak(),config:CFG}));
const signalsHandler=(req,res)=>{const n=Math.min(100,Math.max(1,Number.parseInt(req.query.n,10)||20));res.json(state.signals.slice(-n).reverse());};
app.get('/signals',admin,signalsHandler); app.get('/sinais',admin,signalsHandler);
app.get('/stats',admin,(_req,res)=>res.json({dateLuanda:luandaDate(),...state.dailyStats,greenStreak:dailyStreak()}));
const calibrationHandler=(_req,res)=>{const settled=state.signals.filter(s=>CLOSED.has(s.status));const n=settled.length,w=settled.filter(s=>s.status!=='LOSS').length;res.json({n,wins:w,observedRate:n?w/n:null,warning:'Historical hit rate is not expected value and is not a guarantee.'});};
app.get('/calibration',admin,calibrationHandler); app.get('/calibracao',admin,calibrationHandler);
async function start() {
  await initDb(); await loadState();
  httpServer=app.listen(PORT,()=>log('http_listen',{port:PORT,version:VERSION}));
  timer=setInterval(()=>runCycle().catch(e=>log('unhandled_cycle',{error:e.message})),CFG.pollMs);
  await runCycle();
}
async function shutdown() {
  if(shuttingDown)return; shuttingDown=true; clearInterval(timer);
  if(httpServer) await new Promise(resolve=>httpServer.close(resolve));
  if(state.lockClient){await state.lockClient.query('SELECT pg_advisory_unlock($1)',[727272]).catch(()=>{});state.lockClient.release();}
  await pool.end().catch(()=>{}); process.exit(0);
}
module.exports = { normalizeOutcome, toMs, firstArray, normalizeRound, transitionPattern, wilsonLower, settleStatus, resolveWindow, assessFeed, luandaDate, CFG };
if (require.main === module) {
  process.on('SIGTERM',shutdown); process.on('SIGINT',shutdown);
  start().catch(async e=>{console.error(e); if(state.lockClient)state.lockClient.release(); await pool.end().catch(()=>{}); process.exit(1);});
}
