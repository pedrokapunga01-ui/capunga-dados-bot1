'use strict';

/**
 * CAPUNGA DADOS — BAC BO · v18.1 (Gale 2) — fix arranque Render
 * Correções v18.1:
 *  - HTTP abre ANTES do advisory lock (Render precisa da porta para "live")
 *  - Lock timeout reduzido (3×5s) e não-bloqueante (modo degradado se falhar)
 *  - app.listen(PORT, '0.0.0.0') explícito
 *  - keepAlive/wakeCheck só arrancam depois do listen
 * Env: BOT_TOKEN, CHAT_ID, ADMIN_TOKEN, DATABASE_URL, PORT, API_BACBO (opcional)
 */

const fs = require('fs');
const crypto = require('crypto');
const express = require('express');
const cors = require('cors');
const axios = require('axios');
const { Pool } = require('pg');

const PORT = Number(process.env.PORT || 10000);
const BOT_TOKEN = process.env.BOT_TOKEN || '';
const CHAT_ID = process.env.CHAT_ID || '';
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || '';
const DATABASE_URL = process.env.DATABASE_URL || '';

const CFG = {
  ativos: true,
  maxGale: 2,
  maxPending: 1,
  scoreMin: 80,
  minAmostra: 60,
  minStratN: 5,
  sinalInicial: 1,
  syncMs: 3000,
  minGapMs: 30000,
  minSettledAgeMs: 5000,
  apuraMaxMs: 900000,
  maxHist: 4000,
  gapMs: 7 * 60 * 1000,
  maxSignalsMem: 500,
  signalsOnBoot: 500,
  suspendMs: 4 * 3600 * 1000,
  suspendLossStreak: 3,
  staleAlertMs: 3 * 60 * 1000,
  alertThrottleMs: 10 * 60 * 1000,
  keepAliveMs: 60000,
  lockTries: 3,
  lockDelayMs: 5000
};

const J = 'Jogador';
const B = 'Banca';
const E = 'Empate';
const isCol = (x) => x === J || x === B;
const opp = (c) => (c === J ? B : c === B ? J : null);
const FECHADOS = new Set(['GREEN', 'G1', 'G2', 'EMPATE', 'EG1', 'EG2', 'LOSS']);
const LOCK_KEY = 727272;

const API_BACBO = process.env.API_BACBO ||
  'https://api-cs.casino.org/svc-evolution-game-events/api/bacbo?page=0&size=50&sort=data.settledAt,desc&duration=120&wheelResults=PlayerWon,BankerWon,Tie';

const DEPLOY_ID = (() => {
  if (process.env.DEPLOY_ID) return String(process.env.DEPLOY_ID).trim();
  try { return crypto.createHash('sha256').update(fs.readFileSync(__filename)).digest('hex').slice(0, 16); }
  catch { return 'unknown'; }
})();

const S = {
  hist: [], seen: new Set(), signals: [],
  nextId: CFG.sinalInicial, lastSignalAt: 0,
  stats: null, statsAtLen: -1
};

const STATS = { total: 0, green: 0, g1: 0, g2: 0, empate: 0, eg1: 0, eg2: 0, loss: 0 };
const SUSPENDED = {};

const R = {
  iniciadoEm: new Date().toISOString(), deployId: DEPLOY_ID,
  ultimaSync: null, ultimaRodadaNova: null,
  erroAPI: null, erroTelegram: null, erroDB: null,
  ultimoMotivo: null, ultimoSinal: null, wakes: 0, ultimoWake: null,
  ultimoAlertaAPI: 0, ultimoAlertaDB: 0, lockOk: false
};

let pool = null, server = null, tickTimer = null, keepTimer = null;
let busy = false, stopping = false, lastTickMs = Date.now();

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const log = (tag, d) => console.log(`[${new Date().toISOString()}] [${tag}] ${typeof d === 'string' ? d : JSON.stringify(d)}`);

const fmtHora = new Intl.DateTimeFormat('pt-PT', {
  timeZone: 'Africa/Luanda', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false
});
const hora = (ms = Date.now()) => fmtHora.format(new Date(ms));

const nomeCor = (c) => (c === B ? 'Vermelho🔴' : c === J ? 'Azul🔵' : 'Empate🟡');
const emojiCor = (c) => (c === B ? '🔴' : c === J ? '🔵' : '🟡');
const barra = (p) => {
  const f = Math.max(0, Math.min(10, Math.round((p / 100) * 10)));
  return '▰'.repeat(f) + '▱'.repeat(10 - f);
};
const SEP = '━━━━━━━━━━━━━━━';

/* ============================ DB ============================ */

async function dbInit() {
  if (!DATABASE_URL) { log('DB', 'sem persistência'); return; }
  try {
    const local = /^postgres(?:ql)?:\/\/[^@]*@?(localhost|127\.0\.0\.1)(:|\/|$)/i.test(DATABASE_URL);
    pool = new Pool({
      connectionString: DATABASE_URL,
      ssl: local ? false : { rejectUnauthorized: false },
      max: 6
    });
    await pool.query('SELECT 1');
    await pool.query(`
      CREATE TABLE IF NOT EXISTS capunga_state (k TEXT PRIMARY KEY, v TEXT NOT NULL, updated_at TIMESTAMPTZ DEFAULT NOW());
      CREATE TABLE IF NOT EXISTS capunga_rounds (id TEXT PRIMARY KEY, resultado TEXT NOT NULL, t BIGINT NOT NULL, pj DOUBLE PRECISION, pb DOUBLE PRECISION);
      CREATE INDEX IF NOT EXISTS idx_cap_rounds_t ON capunga_rounds(t DESC);
      CREATE TABLE IF NOT EXISTS capunga_signals (
        id INTEGER PRIMARY KEY, cor TEXT NOT NULL, key TEXT NOT NULL, nome TEXT NOT NULL,
        tipo TEXT NOT NULL, conf INT NOT NULL, status TEXT NOT NULL DEFAULT 'PENDENTE',
        gale INT, agree INT, created_at TIMESTAMPTZ NOT NULL, closed_at TIMESTAMPTZ
      );
      CREATE INDEX IF NOT EXISTS idx_cap_sig_status ON capunga_signals(status);
      CREATE INDEX IF NOT EXISTS idx_cap_sig_created ON capunga_signals(created_at DESC);
    `);
    R.erroDB = null; log('DB', 'schema ok');
  } catch (e) { R.erroDB = e.message; log('DB ERRO', e.message); pool = null; }
}

async function acquireLock() {
  if (!pool) return false;
  for (let i = 1; i <= CFG.lockTries; i++) {
    try {
      const r = await pool.query('SELECT pg_try_advisory_lock($1) AS ok', [LOCK_KEY]);
      if (r.rows[0].ok) { R.lockOk = true; log('LOCK', 'adquirido'); return true; }
      log('LOCK', `tentativa ${i}/${CFG.lockTries} — concorrente ativa`);
    } catch (e) { R.erroDB = e.message; log('LOCK ERRO', e.message); }
    if (i < CFG.lockTries) await sleep(CFG.lockDelayMs);
  }
  return false;
}
async function releaseLock() {
  if (!pool || !R.lockOk) return;
  try { await pool.query('SELECT pg_advisory_unlock($1)', [LOCK_KEY]); } catch (_) {}
}

async function dbStateGet(k) {
  if (!pool) return null;
  try { const r = await pool.query('SELECT v FROM capunga_state WHERE k=$1', [k]); return r.rows[0]?.v ?? null; }
  catch (e) { R.erroDB = e.message; return null; }
}
async function dbStateSet(k, v) {
  if (!pool) return;
  try {
    await pool.query(`INSERT INTO capunga_state(k,v,updated_at) VALUES ($1,$2,NOW())
      ON CONFLICT (k) DO UPDATE SET v=EXCLUDED.v, updated_at=NOW()`, [k, String(v)]);
    if (k !== 'lastTick') R.erroDB = null;
  } catch (e) { R.erroDB = e.message; }
}
async function dbSignalInsert(s) {
  if (!pool) return;
  try {
    await pool.query(`INSERT INTO capunga_signals (id,cor,key,nome,tipo,conf,status,gale,agree,created_at,closed_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT (id) DO NOTHING`,
      [s.id, s.cor, s.key, s.nome, s.tipo, s.conf, s.status, s.gale ?? null, s.agree ?? 1,
       new Date(s.t), s.fechadoEm ? new Date(s.fechadoEm) : null]);
  } catch (e) { R.erroDB = e.message; }
}
async function dbSignalUpdate(s) {
  if (!pool) return;
  try {
    await pool.query('UPDATE capunga_signals SET status=$1, gale=$2, closed_at=$3 WHERE id=$4',
      [s.status, s.gale ?? null, s.fechadoEm ? new Date(s.fechadoEm) : null, s.id]);
  } catch (e) { R.erroDB = e.message; }
}
async function dbRoundUpsert(r) {
  if (!pool) return;
  try {
    await pool.query(`INSERT INTO capunga_rounds(id,resultado,t,pj,pb) VALUES($1,$2,$3,$4,$5)
      ON CONFLICT(id) DO UPDATE SET resultado=EXCLUDED.resultado,t=EXCLUDED.t,pj=EXCLUDED.pj,pb=EXCLUDED.pb`,
      [r.id, r.resultado, r.t, r.pj, r.pb]);
  } catch (e) { R.erroDB = e.message; }
}

async function dbLoadStats() {
  if (!pool) return;
  try {
    const r = await pool.query(`SELECT status, COUNT(*)::int AS n FROM capunga_signals
      WHERE status <> 'PENDENTE' GROUP BY status`);
    STATS.total = 0; STATS.green = 0; STATS.g1 = 0; STATS.g2 = 0;
    STATS.empate = 0; STATS.eg1 = 0; STATS.eg2 = 0; STATS.loss = 0;
    for (const row of r.rows) {
      STATS.total += row.n;
      if (row.status === 'GREEN') STATS.green = row.n;
      else if (row.status === 'G1') STATS.g1 = row.n;
      else if (row.status === 'G2') STATS.g2 = row.n;
      else if (row.status === 'EMPATE') STATS.empate = row.n;
      else if (row.status === 'EG1') STATS.eg1 = row.n;
      else if (row.status === 'EG2') STATS.eg2 = row.n;
      else if (row.status === 'LOSS') STATS.loss = row.n;
    }
  } catch (e) { R.erroDB = e.message; }
}

async function dbLoadState() {
  if (!pool) return;
  try {
    const nextId = await dbStateGet('nextId');
    const lastSignalAt = await dbStateGet('lastSignalAt');
    if (nextId) S.nextId = Math.max(CFG.sinalInicial, parseInt(nextId, 10));
    if (lastSignalAt) S.lastSignalAt = parseInt(lastSignalAt, 10);

    try {
      const raw = await dbStateGet('suspended');
      if (raw) {
        const arr = JSON.parse(raw);
        const now = Date.now();
        for (const [key, until] of arr) if (until > now) SUSPENDED[key] = until;
      }
    } catch (_) {}

    const rounds = await pool.query(`SELECT id,resultado,t,pj,pb FROM capunga_rounds ORDER BY t DESC LIMIT ${CFG.maxHist}`);
    S.hist = rounds.rows.map((r) => ({ id: r.id, resultado: r.resultado, t: Number(r.t), pj: r.pj === null ? null : Number(r.pj), pb: r.pb === null ? null : Number(r.pb) })).reverse();
    S.seen = new Set(S.hist.map((r) => r.id));
    const r = await pool.query(`SELECT * FROM capunga_signals ORDER BY created_at DESC LIMIT ${CFG.signalsOnBoot}`);
    S.signals = r.rows.reverse().map((row) => ({
      id: row.id, cor: row.cor, key: row.key, nome: row.nome, tipo: row.tipo,
      conf: row.conf, status: row.status, gale: row.gale, agree: row.agree,
      t: new Date(row.created_at).getTime(), criadoEm: new Date(row.created_at).getTime(),
      fechadoEm: row.closed_at ? new Date(row.closed_at).getTime() : null,
      gale1MsgId: null, gale2MsgId: null, galeLevel: row.status !== 'PENDENTE' ? 2 : 0
    }));
    await dbLoadStats();
    log('DB', `estado carregado: nextId=${S.nextId}, sinais=${S.signals.length}, stats=${STATS.total}`);
  } catch (e) { R.erroDB = e.message; log('DB ERRO load', e.message); }
}

/* ============================ TELEGRAM ============================ */

async function tgSendRaw(texto) {
  if (!BOT_TOKEN || !CHAT_ID) { R.erroTelegram = 'TOKEN/CHAT em falta'; return { ok: false }; }
  for (let t = 1; t <= 3; t++) {
    try {
      const r = await axios.post(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`,
        { chat_id: CHAT_ID, text: texto, parse_mode: 'HTML', disable_web_page_preview: true },
        { timeout: 11000 });
      if (r.data?.ok) { R.erroTelegram = null; return { ok: true, messageId: r.data.result.message_id }; }
      R.erroTelegram = JSON.stringify(r.data);
    } catch (e) {
      R.erroTelegram = e.response?.data ? JSON.stringify(e.response.data) : e.message;
      const retry = Number(e.response?.data?.parameters?.retry_after);
      if (retry > 0 && retry <= 20) await sleep(retry * 1000 + 200);
    }
    if (t < 3) await sleep(1100 * t);
  }
  return { ok: false };
}
async function tgDelete(messageId) {
  if (!messageId || !BOT_TOKEN || !CHAT_ID) return;
  try {
    await axios.post(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`,
      { chat_id: CHAT_ID, message_id: messageId }, { timeout: 8000 });
  } catch (_) {}
}
async function tgAlert(tag, detalhe) {
  const now = Date.now();
  if (tag === 'API') {
    if (now - R.ultimoAlertaAPI < CFG.alertThrottleMs) return;
    R.ultimoAlertaAPI = now;
  }
  if (tag === 'DB') {
    if (now - R.ultimoAlertaDB < CFG.alertThrottleMs) return;
    R.ultimoAlertaDB = now;
  }
  await tgSendRaw(`⚠️ <b>ALERTA ${tag}</b>\n\n${detalhe}\n\n⏱ ${hora()}\n<i>Capunga Dados v18</i>`);
}

/* ============================ TEMPLATES ============================ */

function fmtAnalisando() {
  return ['🔍 <b>Analisando o mercado tropa…</b>', '', `⏱ ${hora()}`].join('\n');
}

function fmtSinal(s) {
  const confEmoji = s.conf >= 93 ? '💎' : s.conf >= 86 ? '🔥' : '⚡';
  return [
    `╔═══ 🎲 CAPUNGA · SINAL #${s.id} 🎲 ═══╗`,
    '🎯 <b>ENTRADA CONFIRMADA</b>',
    `Cor: ${nomeCor(s.cor)}`,
    'Proteção: Empate🟡',
    `📊 Confiança: ${confEmoji} <b>${s.conf}%</b>`,
    barra(s.conf),
    `⏱ ${hora(s.t)}`,
    '╚════════════════════════════╝',
    '',
    '💎 <i>Capunga Premium</i>'
  ].join('\n');
}

function fmtGale(s, nivel) {
  const icons = '✊🏿'.repeat(nivel);
  const extra = nivel === 2 ? '\n⚠️ <i>2ª tentativa — dobra a aposta</i>' : '';
  return [
    `${icons} <b>FAZER GALE ${nivel}</b> ${icons}`,
    `Continuar na cor: ${emojiCor(s.cor)}${extra}`
  ].join('\n');
}

function fmtResult(s) {
  const msg = {
    GREEN:  '✅ <b>GREEN</b> ✅\n\nA mesa obedeceu tropa 💚\n\n<i>Capunga Dados</i>',
    G1:     '✅✅ <b>GREEN NO GALE 1</b> ✅✅\n\nA mesa obedeceu tropa 💚\n\n<i>Capunga Dados</i>',
    G2:     '✅✅✅ <b>GREEN NO GALE 2</b> ✅✅✅\n\nA mesa obedeceu tropa 💚\n\n<i>Capunga Dados</i>',
    EMPATE: '🟡 <b>GREEN NO EMPATE</b> 🟡\n\nA mesa obedeceu tropa 💚\n\n<i>Capunga Dados</i>',
    EG1:    '🟡🟡 <b>GREEN NO EMPATE GALE 1</b> 🟡🟡\n\nA mesa obedeceu tropa 💚\n\n<i>Capunga Dados</i>',
    EG2:    '🟡🟡🟡 <b>GREEN NO EMPATE GALE 2</b> 🟡🟡🟡\n\nA mesa obedeceu tropa 💚\n\n<i>Capunga Dados</i>',
    LOSS:   '❌ <b>LOSS</b> ❌\n\nTropa, a mesa não obedeceu 😤\n\n<i>Capunga Dados</i>'
  };
  return msg[s.status] || msg.LOSS;
}

function getGreenStreak() {
  let count = 0;
  for (let i = S.signals.length - 1; i >= 0; i--) {
    const s = S.signals[i];
    if (!FECHADOS.has(s.status)) continue;
    if (s.status === 'LOSS') break;
    count++;
  }
  return count;
}

function fmtHistorico() {
  const wins = STATS.green + STATS.g1 + STATS.g2 + STATS.empate + STATS.eg1 + STATS.eg2;
  const taxa = STATS.total > 0 ? (wins / STATS.total) * 100 : 0;
  const winsReais = STATS.green + STATS.g1 + STATS.g2;
  const decididos = winsReais + STATS.loss;
  const taxaReal = decididos > 0 ? (winsReais / decididos) * 100 : 0;
  const streak = getGreenStreak();
  const streakTxt = streak >= 3 ? `${streak}🔥` : `${streak}`;
  return [
    '📊 <b>HISTÓRICO</b>',
    SEP,
    `Total: ${STATS.total} sinais`,
    `✅ Green: ${STATS.green}`,
    `✅✅ G1: ${STATS.g1}`,
    `✅✅✅ G2: ${STATS.g2}`,
    `🟡 Empate: ${STATS.empate}`,
    `🟡🟡 Empate G1: ${STATS.eg1}`,
    `🟡🟡🟡 Empate G2: ${STATS.eg2}`,
    `❌ Loss: ${STATS.loss}`,
    `Sequência de greens: ${streakTxt}`,
    `🎯 Taxa: ${taxa.toFixed(1)}%`,
    `💡 Taxa real (sem empate): ${taxaReal.toFixed(1)}%`
  ].join('\n');
}

/* ============================ API ============================ */

function normalizeOutcome(v) {
  if (v && typeof v === 'object') v = v.outcome ?? v.resultado ?? v.winner ?? v.name;
  const s = String(v ?? '').normalize('NFD').replace(/[̀-ͯ]/g, '')
    .trim().toLowerCase().replace(/[\s_-]+/g, '');
  if (['player', 'jogador', 'playerwin', 'playerwins', 'playerwon', 'blue'].includes(s)) return J;
  if (['banker', 'banca', 'bankerwin', 'bankerwins', 'bankerwon', 'red', 'vermelho'].includes(s)) return B;
  if (['tie', 'draw', 'empate', 'equal', 'push', 'tied'].includes(s)) return E;
  return null;
}
const validMs = (v) => { if (!v) return null; const t = new Date(v).getTime(); return Number.isNaN(t) ? null : t; };
function extractItems(b) {
  if (Array.isArray(b)) return b;
  if (!b || typeof b !== 'object') return [];
  return [b.data, b.results, b.items, b.resultados, b.rounds, b.events,
    b.data?.results, b.data?.items, b.data?.resultados, b.data?.rounds,
    b.data?.events, b.data?.data].find(Array.isArray) || [];
}
const numOrNull = (v) => {
  if (v === null || v === undefined || v === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};
function normalizeRound(item) {
  const d = item?.data || item?.round || item || {};
  const r = (d.result && typeof d.result === 'object') ? d.result
    : (item?.result && typeof item.result === 'object') ? item.result : {};
  const t = validMs(d.settledAt ?? d.endTime ?? d.endedAt ?? d.finishedAt ?? d.fim ?? item?.settledAt ?? item?.fim);
  const raw = r.outcome ?? r.resultado ?? r.winner ?? d.outcome ?? d.resultado ?? d.winner ??
    item?.outcome ?? item?.resultado ?? item?.winner ??
    (typeof d.result === 'string' ? d.result : null) ??
    (typeof item?.result === 'string' ? item.result : null);
  const resultado = normalizeOutcome(raw);
  const id = String(d.id ?? d.roundId ?? d.round_id ?? d.eventId ?? item?.id ?? item?.roundId ?? item?.round_id ?? item?.eventId ?? '').trim();
  if (!id || !resultado || !t) return null;
  const pj = numOrNull(r.playerDice?.score ?? d.playerDice?.score ?? d.playerScore ?? d.player_score ?? d.pontos_jogador);
  const pb = numOrNull(r.bankerDice?.score ?? d.bankerDice?.score ?? d.bankerScore ?? d.banker_score ?? d.pontos_banca);
  return { id, resultado, t, pj, pb };
}
async function fetchRounds() {
  let lastErr = null;
  for (let i = 1; i <= 2; i++) {
    try {
      const resp = await axios.get(API_BACBO, {
        timeout: 10000, headers: { Accept: 'application/json', 'User-Agent': 'CapungaDados/18.1' }
      });
      const map = new Map();
      for (const it of extractItems(resp.data)) {
        const rd = normalizeRound(it);
        if (rd) map.set(rd.id, rd);
      }
      R.erroAPI = null;
      return [...map.values()].sort((a, b) => a.t - b.t || (a.id < b.id ? -1 : 1));
    } catch (e) {
      lastErr = e; R.erroAPI = e.message;
      if (i < 2) await sleep(500);
    }
  }
  throw lastErr || new Error('Falha API');
}

/* ============================ HELPERS ============================ */

const hasScore = (r) => r && Number.isFinite(r.pj) && Number.isFinite(r.pb);
const margem = (r) => Math.abs((r.pj || 0) - (r.pb || 0));
const scoreOf = (r) => (r.resultado === J ? r.pj : r.pb);
const scoreRange = (s) => {
  if (!Number.isFinite(s)) return null;
  if (s <= 2) return 'MB';
  if (s <= 4) return 'B';
  if (s <= 6) return 'M';
  if (s <= 8) return 'A';
  return 'MA';
};
function streak(seq) {
  if (!seq.length) return 0;
  let n = 0; const cor = seq[0].resultado;
  for (const r of seq) { if (r.resultado === cor) n++; else break; }
  return n;
}
function alternation(seq) {
  if (seq.length < 2) return 1;
  let L = 1;
  while (L < seq.length && seq[L].resultado !== seq[L - 1].resultado) L++;
  return L;
}
function detectRegime(seq) {
  if (seq.length < 6) return { trend: 0.5, chop: 0.5, label: 'neutro' };
  const n = Math.min(seq.length, 15);
  let changes = 0;
  for (let i = 1; i < n; i++) if (seq[i].resultado !== seq[i - 1].resultado) changes++;
  const chop = changes / (n - 1);
  const trend = 1 - chop;
  const label = chop >= 0.65 ? 'errático' : chop <= 0.35 ? 'tendência' : 'neutro';
  return { trend, chop, label };
}
function regimeFit(cand, regime) {
  const k = cand.key;
  if (/SEQ_(QUEBRA|SURF|PINGPONG|2x2|EXTEND|3_1|MOMENTUM|ALT)/.test(k)) return regime.chop >= 0.45;
  if (/SEQ_(REP|STRONG_STREAK)|BANKER_BIAS/.test(k)) return regime.trend >= 0.45;
  return true;
}

/* ============================ ESTRATÉGIAS ============================ */

const STRATS = [
  function seqRepeticao({ seq }) {
    const n = streak(seq);
    if (n < 3 || n > 7) return null;
    const strength = [0, 0, 0, 72, 78, 84, 86, 86][n] || 86;
    return { cor: seq[0].resultado, key: `SEQ_REP_${n}`, nome: `Sequência x${n}`, emoji: '🔁', strength, tipo: 'seq' };
  },
  function seqQuebra({ seq }) {
    const n = streak(seq);
    if (n < 4 || n > 8) return null;
    const strength = [0, 0, 0, 0, 74, 80, 84, 85, 85][n] || 85;
    return { cor: opp(seq[0].resultado), key: `SEQ_QUEBRA_${n}`, nome: `Quebra x${n}`, emoji: '💥', strength, tipo: 'seq' };
  },
  function seqSurf({ seq }) {
    const L = alternation(seq);
    if (L < 4) return null;
    const strength = L >= 6 ? 84 : L >= 5 ? 78 : 72;
    return { cor: opp(seq[0].resultado), key: `SEQ_SURF_${L}`, nome: `Surf ${L}`, emoji: '🌊', strength, tipo: 'seq' };
  },
  function seqDoisDois({ seq }) {
    if (seq.length < 4) return null;
    const [a, b, c, d] = seq.map((x) => x.resultado);
    if (a === b && c === d && a !== c) return { cor: c, key: 'SEQ_2x2', nome: 'Bloco 2+2', emoji: '♟️', strength: 76, tipo: 'seq' };
    return null;
  },
  function seqTresUm({ seq }) {
    if (seq.length < 4) return null;
    const [a, b, c, d] = seq.map((x) => x.resultado);
    