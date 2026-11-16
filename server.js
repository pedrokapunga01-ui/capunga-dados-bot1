
'use strict';

/**
 * BAC BO Telegram Signals — v5.2
 *
 * - Sinais automáticos 24h, intervalo mínimo ~2.5 min
 * - maxPending = 1 rigorosamente respeitado (ciclo único, sem race conditions)
 * - Histórico enviado em TODOS os resultados (green, gale, empate, loss)
 * - Empate (EMPATE/EG1/EG2) aumenta a sequência de greens seguidos
 * - Fila de reenvio: resultados nunca se perdem se o Telegram falhar
 *
 * Dependências: express, axios, pg
 * Variáveis: BOT_TOKEN, CHAT_ID, ADMIN_TOKEN, DATABASE_URL
 * Opcionais: API_BACBO, API_BACBO_2, PORT, HISTORY_TZ, SELF_URL/RENDER_EXTERNAL_URL
 */

const crypto = require('crypto');
const express = require('express');
const axios = require('axios');
const { Pool } = require('pg');

/* ============================ CONFIG ============================ */

const PORT = Number(process.env.PORT || 10000);
const BOT_TOKEN = process.env.BOT_TOKEN || '';
const CHAT_ID = process.env.CHAT_ID || '';
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || '';
const DATABASE_URL = process.env.DATABASE_URL || '';
const SELF_URL = process.env.SELF_URL || process.env.RENDER_EXTERNAL_URL || '';

const ALLOWED_TZ = new Set(['Africa/Luanda', 'America/New_York', 'America/Sao_Paulo', 'Europe/Lisbon', 'UTC']);
const HISTORY_TZ = ALLOWED_TZ.has(process.env.HISTORY_TZ) ? process.env.HISTORY_TZ : 'Africa/Luanda';

const CFG = {
  ativos: true,
  maxGale: 2,
  maxPending: 1,
  minAmostra: 50,
  syncMs: 2500,
  syncFastMs: 900,
  minGapMs: 150000,
  relaxMs: 2 * 60 * 1000,
  forceSignalMs: 4 * 60 * 1000,
  apuraMaxMs: 7 * 60 * 1000,
  maxHist: 4000,
  gapMs: 6 * 60 * 1000,
  maxSignalsMem: 600,
  signalsOnBoot: 400,
  suspendMs: 2.5 * 3600 * 1000,
  suspendLossStreak: 3,
  strategyCooldownMs: 90 * 1000,
  staleAlertMs: 3 * 60 * 1000,
  alertThrottleMs: 10 * 60 * 1000,
  keepAliveMs: 55000,
  backtestWindow: 1500,
  tiers: [
    { scoreMin: 82, minStratN: 5, liveMin: 0.78 },
    { scoreMin: 79, minStratN: 4, liveMin: 0.72 },
    { scoreMin: 76, minStratN: 2, liveMin: 0.60 }
  ]
};

const J = 'Jogador', B = 'Banca', E = 'Empate';
const isCol = (x) => x === J || x === B;
const opp = (c) => (c === J ? B : c === B ? J : null);
const LOCK_KEY = 727272;

const BUCKET = { GREEN: 'green', G1: 'g1', G2: 'g2', EMPATE: 'empate', EG1: 'eg1', EG2: 'eg2', LOSS: 'loss' };
const WIN_SET = new Set(['GREEN', 'G1', 'G2']);

const API_SOURCES = [
  process.env.API_BACBO ||
  'https://api-cs.casino.org/svc-evolution-game-events/api/bacbo?page=0&size=50&sort=data.settledAt,desc&duration=120&wheelResults=PlayerWon,BankerWon,Tie',
  process.env.API_BACBO_2 || ''
].filter(Boolean);

const DEPLOY_ID = (() => {
  if (process.env.DEPLOY_ID) return String(process.env.DEPLOY_ID).trim();
  return crypto.createHash('sha256').update(String(Date.now())).digest('hex').slice(0, 12);
})();

const S = {
  hist: [],
  seen: new Set(),
  signals: [],
  nextId: 1,
  lastSignalAt: 0,
  curStreak: 0,
  stats: null,
  statsAtLen: -1,
  outbox: []
};

const DAILY = { date: '', total: 0, green: 0, g1: 0, g2: 0, empate: 0, eg1: 0, eg2: 0, loss: 0, bestStreak: 0 };
const ALLTIME = { total: 0, green: 0, g1: 0, g2: 0, empate: 0, eg1: 0, eg2: 0, loss: 0 };
const SUSPENDED = {};
const STRATEGY_COOLDOWN = {};

const R = {
  iniciadoEm: new Date().toISOString(),
  deployId: DEPLOY_ID,
  versao: '5.2',
  ultimaSync: null,
  ultimaRodadaNova: null,
  fonteAPI: 0,
  erroAPI: null,
  erroTelegram: null,
  erroDB: null,
  ultimoMotivo: null,
  ultimoSinal: null,
  tier: 0,
  ultimoAlertaAPI: 0,
  ultimoAlertaDB: 0,
  lockOk: false
};

let pool = null, server = null, tickTimer = null, keepTimer = null;
let busy = false, stopping = false, lastNewMs = Date.now();

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const log = (tag, d) =>
  console.log(`[${new Date().toISOString()}] [${tag}] ${typeof d === 'string' ? d : JSON.stringify(d)}`);

const fmtHora = new Intl.DateTimeFormat('pt-PT', {
  timeZone: HISTORY_TZ, hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false
});
const hora = (ms = Date.now()) => fmtHora.format(new Date(ms));

const fmtDia = new Intl.DateTimeFormat('en-CA', {
  timeZone: HISTORY_TZ, year: 'numeric', month: '2-digit', day: '2-digit'
});
const dayKey = (ms = Date.now()) => fmtDia.format(new Date(ms));

const nomeCor = (c) => (c === B ? '🔴 BANCA' : c === J ? '🔵 JOGADOR' : '🟡 EMPATE');

/* ============================ DB ============================ */

async function dbInit() {
  if (!DATABASE_URL) throw new Error('DATABASE_URL em falta');
  const local = /^postgres(?:ql)?:\/\/[^@]*@?(localhost|127\.0\.0\.1)(:|\/|$)/i.test(DATABASE_URL);
  pool = new Pool({
    connectionString: DATABASE_URL,
    ssl: local ? false : { rejectUnauthorized: false },
    max: 6,
    connectionTimeoutMillis: 10000,
    idleTimeoutMillis: 30000
  });
  await pool.query('SELECT 1');
  await pool.query(`
    CREATE TABLE IF NOT EXISTS capunga_state (
      k TEXT PRIMARY KEY, v TEXT NOT NULL, updated_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS capunga_rounds (
      id TEXT PRIMARY KEY, resultado TEXT NOT NULL, t BIGINT NOT NULL,
      pj DOUBLE PRECISION, pb DOUBLE PRECISION
    );
    CREATE INDEX IF NOT EXISTS idx_cap_rounds_t ON capunga_rounds(t DESC);
    CREATE TABLE IF NOT EXISTS capunga_signals (
      id INTEGER PRIMARY KEY, cor TEXT NOT NULL, key TEXT NOT NULL, nome TEXT NOT NULL,
      tipo TEXT NOT NULL, conf INT NOT NULL, status TEXT NOT NULL DEFAULT 'PENDENTE',
      gale INT, agree INT, tier INT DEFAULT 0, base_round_id TEXT,
      created_at TIMESTAMPTZ NOT NULL, closed_at TIMESTAMPTZ
    );
    ALTER TABLE capunga_signals ADD COLUMN IF NOT EXISTS base_round_id TEXT;
    CREATE INDEX IF NOT EXISTS idx_cap_sig_status ON capunga_signals(status);
    CREATE INDEX IF NOT EXISTS idx_cap_sig_created ON capunga_signals(created_at DESC);
  `);

  try {
    const { rows: old } = await pool.query(`
      SELECT id, entry, strategy, attempts, result, created_at, settled_at
      FROM signals
      WHERE result IN ('green','g1','g2','empate','eg1','eg2','red')
    `);
    const statusMap = { green: 'GREEN', g1: 'G1', g2: 'G2', empate: 'EMPATE', eg1: 'EG1', eg2: 'EG2', red: 'LOSS' };
    let migrados = 0;
    for (const r of old) {
      const status = statusMap[r.result];
      if (!status) continue;
      const cor = r.entry === 'P' ? J : B;
      const res = await pool.query(
        `INSERT INTO capunga_signals (id,cor,key,nome,tipo,conf,status,gale,agree,tier,created_at,closed_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT (id) DO NOTHING`,
        [r.id, cor, 'MIGRATED', r.strategy || 'Migrado', 'seq', 85, status, r.attempts, 1, 0, r.created_at, r.settled_at]
      );
      if (res.rowCount) migrados++;
    }
    if (migrados) log('MIGRATE', `${migrados} sinais antigos migrados`);
  } catch (e) {
    if (!/relation .* does not exist/i.test(e.message)) log('MIGRATE', `aviso: ${e.message}`);
  }

  log('DB', 'schema ok');
}

async function acquireLock() {
  for (let i = 1; i <= 10; i++) {
    try {
      const r = await pool.query('SELECT pg_try_advisory_lock($1) AS ok', [LOCK_KEY]);
      if (r.rows[0].ok) { R.lockOk = true; return true; }
      log('LOCK', `instância concorrente — tentativa ${i}/10`);
    } catch (e) { log('LOCK ERRO', e.message); }
    await sleep(15000);
  }
  return false;
}

async function releaseLock() {
  if (!R.lockOk) return;
  try { await pool.query('SELECT pg_advisory_unlock($1)', [LOCK_KEY]); } catch (_) {}
}

async function dbStateGet(k) {
  try {
    const r = await pool.query('SELECT v FROM capunga_state WHERE k=$1', [k]);
    return r.rows[0]?.v ?? null;
  } catch (e) { R.erroDB = e.message; return null; }
}

async function dbStateSet(k, v) {
  try {
    await pool.query(
      `INSERT INTO capunga_state(k,v,updated_at) VALUES ($1,$2,NOW())
       ON CONFLICT (k) DO UPDATE SET v=EXCLUDED.v, updated_at=NOW()`,
      [k, String(v)]
    );
  } catch (e) { R.erroDB = e.message; }
}

async function dbSignalInsert(s) {
  try {
    await pool.query(
      `INSERT INTO capunga_signals (id,cor,key,nome,tipo,conf,status,gale,agree,tier,base_round_id,created_at,closed_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) ON CONFLICT (id) DO NOTHING`,
      [s.id, s.cor, s.key, s.nome, s.tipo, s.conf, s.status, s.gale ?? null, s.agree ?? 1, s.tier ?? 0, s.baseRoundId ?? null,
       new Date(s.criadoEm), s.fechadoEm ? new Date(s.fechadoEm) : null]
    );
  } catch (e) { R.erroDB = e.message; }
}

async function dbSignalUpdate(s) {
  try {
    await pool.query(
      'UPDATE capunga_signals SET status=$1, gale=$2, closed_at=$3 WHERE id=$4',
      [s.status, s.gale ?? null, s.fechadoEm ? new Date(s.fechadoEm) : null, s.id]
    );
  } catch (e) { R.erroDB = e.message; }
}

async function dbSignalDelete(id) {
  try { await pool.query('DELETE FROM capunga_signals WHERE id=$1', [id]); }
  catch (e) { R.erroDB = e.message; }
}

async function dbRoundUpsert(r) {
  try {
    await pool.query(
      `INSERT INTO capunga_rounds(id,resultado,t,pj,pb) VALUES($1,$2,$3,$4,$5)
       ON CONFLICT(id) DO UPDATE SET resultado=EXCLUDED.resultado,t=EXCLUDED.t,pj=EXCLUDED.pj,pb=EXCLUDED.pb`,
      [r.id, r.resultado, r.t, r.pj, r.pb]
    );
  } catch (e) { R.erroDB = e.message; }
}

async function dbLoadAlltime() {
  try {
    const r = await pool.query(
      `SELECT status, COUNT(*)::int AS n FROM capunga_signals WHERE status <> 'PENDENTE' GROUP BY status`
    );
    for (const k of Object.keys(ALLTIME)) ALLTIME[k] = 0;
    for (const row of r.rows) {
      const b = BUCKET[row.status];
      if (!b) continue;
      ALLTIME.total += row.n;
      ALLTIME[b] = row.n;
    }
  } catch (e) { R.erroDB = e.message; }
}

async function rebuildDailyFromDb() {
  const today = dayKey();
  for (const k of ['total', 'green', 'g1', 'g2', 'empate', 'eg1', 'eg2', 'loss']) DAILY[k] = 0;
  DAILY.bestStreak = 0;
  DAILY.date = today;

  try {
    const byStatus = await pool.query(
      `SELECT status, COUNT(*)::int AS n
       FROM capunga_signals
       WHERE status <> 'PENDENTE'
         AND (closed_at AT TIME ZONE $1)::date = (NOW() AT TIME ZONE $1)::date
       GROUP BY status`,
      [HISTORY_TZ]
    );
    for (const row of byStatus.rows) {
      const b = BUCKET[row.status];
      if (!b) continue;
      DAILY[b] = row.n;
      DAILY.total += row.n;
    }

    const ordered = await pool.query(
      `SELECT status FROM capunga_signals
       WHERE status <> 'PENDENTE'
         AND (closed_at AT TIME ZONE $1)::date = (NOW() AT TIME ZONE $1)::date
       ORDER BY closed_at ASC, id ASC`,
      [HISTORY_TZ]
    );
    // Green, G1, G2 E empates (EMPATE/EG1/EG2) somam na sequência; só LOSS zera.
    let cur = 0, best = 0;
    for (const row of ordered.rows) {
      if (row.status === 'LOSS') cur = 0;
      else { cur++; if (cur > best) best = cur; }
    }
    S.curStreak = cur;
    DAILY.bestStreak = best;

    await dbStateSet('daily_v4', JSON.stringify(DAILY));
    await dbStateSet('curStreak', String(S.curStreak));
    log('DIÁRIO', `reconstruído do DB: ${DAILY.total} resolvidos hoje, streak ${S.curStreak}/${DAILY.bestStreak}`);
  } catch (e) {
    log('DIÁRIO ERRO', e.message);
  }
}

async function dbLoadState() {
  const nextId = await dbStateGet('nextId');
  const lastSignalAt = await dbStateGet('lastSignalAt');
  if (nextId) S.nextId = Math.max(1, parseInt(nextId, 10));
  if (lastSignalAt) S.lastSignalAt = parseInt(lastSignalAt, 10);

  try {
    const raw = await dbStateGet('suspended');
    if (raw) {
      const arr = JSON.parse(raw);
      const now = Date.now();
      for (const [k, until] of arr) if (until > now) SUSPENDED[k] = until;
    }
  } catch (_) {}

  try { if ((await dbStateGet('ativos')) === 'false') CFG.ativos = false; } catch (_) {}
  try {
    const gap = await dbStateGet('minGapMs');
    if (gap) CFG.minGapMs = parseInt(gap, 10) || CFG.minGapMs;
  } catch (_) {}
  try {
    const gales = await dbStateGet('maxGale');
    if (gales !== null && gales !== undefined && gales !== '') {
      const v = parseInt(gales, 10);
      if (v >= 0 && v <= 2) CFG.maxGale = v;
    }
  } catch (_) {}

  const rounds = await pool.query(
    `SELECT id,resultado,t,pj,pb FROM capunga_rounds ORDER BY t DESC, id DESC LIMIT ${CFG.maxHist}`
  );
  S.hist = rounds.rows.map((r) => ({
    id: r.id, resultado: r.resultado, t: Number(r.t),
    pj: r.pj === null ? null : Number(r.pj),
    pb: r.pb === null ? null : Number(r.pb)
  })).reverse();
  S.seen = new Set(S.hist.map((r) => r.id));

  const rs = await pool.query(
    `SELECT * FROM capunga_signals ORDER BY created_at DESC LIMIT ${CFG.signalsOnBoot}`
  );
  S.signals = rs.rows.reverse().map((row) => ({
    id: row.id, cor: row.cor, key: row.key, nome: row.nome, tipo: row.tipo,
    conf: row.conf, status: row.status, gale: row.gale, agree: row.agree, tier: row.tier || 0,
    criadoEm: new Date(row.created_at).getTime(),
    fechadoEm: row.closed_at ? new Date(row.closed_at).getTime() : null,
    baseRoundId: row.base_round_id || null,
    galeLevel: 0,
    restored: row.status === 'PENDENTE'
  }));

  await dbLoadAlltime();
  await rebuildDailyFromDb();

  if (!(await dbStateGet('nextId'))) {
    const next = await pool.query('SELECT COALESCE(MAX(id),0)+1 AS n FROM capunga_signals');
    S.nextId = Number(next.rows[0].n);
    await dbStateSet('nextId', String(S.nextId));
  }

  log('DB', `estado carregado: nextId=${S.nextId}, sinais=${S.signals.length}, dia=${DAILY.date}`);
}

/* ============================ TELEGRAM ============================ */

async function tgSend(texto) {
  if (!BOT_TOKEN || !CHAT_ID) {
    R.erroTelegram = 'TOKEN/CHAT em falta';
    return { ok: false };
  }
  for (let t = 1; t <= 3; t++) {
    try {
      const r = await axios.post(
        `https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`,
        { chat_id: CHAT_ID, text: texto, disable_web_page_preview: true },
        { timeout: 8000 }
      );
      if (r.data?.ok) {
        R.erroTelegram = null;
        return { ok: true, messageId: r.data.result.message_id };
      }
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

// Envio fiável: se falhar, fica na fila e é reenviado nos próximos ciclos.
async function tgSendReliable(texto) {
  if (S.outbox.length) { S.outbox.push(texto); return; } // mantém a ordem
  const r = await tgSend(texto);
  if (!r.ok) {
    S.outbox.push(texto);
    if (S.outbox.length > 30) S.outbox.shift();
  }
}

async function flushOutbox() {
  while (S.outbox.length) {
    const r = await tgSend(S.outbox[0]);
    if (!r.ok) return;
    S.outbox.shift();
  }
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
  await tgSend(`⚠️ ALERTA ${tag}\n${detalhe}\n⏱ ${hora()}`);
}

/* ============================ MENSAGENS ============================ */

function fmtSinal(s, tentativas) {
  return [
    '🎲🔥 SINAL BAC BO 🔥🎲',
    '',
    '✅ ENTRADA CONFIRMADA ✅',
    `🎯 Jogar: ${nomeCor(s.cor)}`,
    '🛡️ Proteção: 🟡 EMPATE',
    `🔄 Até ${CFG.maxGale} gale(s) • ${tentativas} tentativa(s)`,
    `🧠 Estratégia: ${s.nome}`,
    '',
    `🔖 Sinal #${s.id}`
  ].join('\n');
}

function fmtGale(s, nivel) {
  return [
    `🔄⚠️ GALE ${nivel} — VAMOS À PRÓXIMA!`,
    `🎯 Manter: ${nomeCor(s.cor)}`,
    '🛡️ Empate continua protegido',
    `🔖 Sinal #${s.id}`
  ].join('\n');
}

const RESULT_LABEL = {
  GREEN: '🟢 GREEN XIRA — DE PRIMEIRA! 🎉🥳',
  G1: '🟢 GREEN XIRA — GALE 1! 🎊💚',
  G2: '🟢 GREEN XIRA — GALE 2! 🎉🛡️',
  EMPATE: '🟡 EMPATE! PROTEÇÃO ATIVADA — ENTRADA ENCERRADA 🛡️🤝',
  EG1: '🟡 EMPATE NO GALE 1 — ENTRADA ENCERRADA 🛡️🤝',
  EG2: '🟡 EMPATE NO GALE 2 — ENTRADA ENCERRADA 🛡️🤝',
  LOSS: '🔴 TÁ MALÊ — LOSS NESTA ENTRADA 😕💔'
};

function fmtHistoricoBloco() {
  return [
    `📊 HISTÓRICO DO DIA — ${DAILY.date}`,
    `🎯 Resolvidos: ${DAILY.total}`,
    `🟢 Green sem gale: ${DAILY.green} • G1: ${DAILY.g1} • G2: ${DAILY.g2}`,
    `🟡 Empate: ${DAILY.empate} • EG1: ${DAILY.eg1} • EG2: ${DAILY.eg2}`,
    `🔴 Loss: ${DAILY.loss} • 🔥 Greens seguidos: ${S.curStreak}`
  ].join('\n');
}

// Histórico SEMPRE incluído, em qualquer resultado.
function fmtResultado(s) {
  const label = RESULT_LABEL[s.status] || '📣 RESULTADO DA ENTRADA';
  return `${label}\n🎲 Sinal #${s.id}\n\n${fmtHistoricoBloco()}`;
}

/* ============================ RESET DIÁRIO ============================ */

async function checkDailyReset() {
  const hoje = dayKey();
  if (!DAILY.date) { DAILY.date = hoje; return; }
  if (DAILY.date !== hoje) {
    const ontem = DAILY.date;
    for (const k of ['total', 'green', 'g1', 'g2', 'empate', 'eg1', 'eg2', 'loss', 'bestStreak']) DAILY[k] = 0;
    DAILY.date = hoje;
    S.curStreak = 0;
    await dbStateSet('daily_v4', JSON.stringify(DAILY));
    await dbStateSet('curStreak', '0');
    log('DIÁRIO', `reset de ${ontem} -> ${hoje}`);
  }
}

/* ============================ API ============================ */

function stripAccents(str) {
  return String(str ?? '').normalize('NFD').split('').filter((c) => {
    const code = c.charCodeAt(0);
    return code < 0x0300 || code > 0x036f;
  }).join('');
}

function normalizeOutcome(v) {
  if (v && typeof v === 'object') v = v.outcome ?? v.resultado ?? v.winner ?? v.name;
  const s = stripAccents(v).trim().toLowerCase().replace(/[\s_-]+/g, '');
  if (['player', 'jogador', 'playerwin', 'playerwins', 'playerwon', 'blue'].includes(s)) return J;
  if (['banker', 'banca', 'bankerwin', 'bankerwins', 'bankerwon', 'red', 'vermelho'].includes(s)) return B;
  if (['tie', 'draw', 'empate', 'equal', 'push', 'tied'].includes(s)) return E;
  return null;
}

const validMs = (v) => {
  if (!v) return null;
  const t = new Date(v).getTime();
  return Number.isNaN(t) ? null : t;
};

function extractItems(b) {
  if (Array.isArray(b)) return b;
  if (!b || typeof b !== 'object') return [];
  return [
    b.data, b.results, b.items, b.resultados, b.rounds, b.events,
    b.data?.results, b.data?.items, b.data?.resultados, b.data?.rounds,
    b.data?.events, b.data?.data
  ].find(Array.isArray) || [];
}

const numOrNull = (v) => {
  if (v === null || v === undefined || v === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};

function normalizeRound(item) {
  const d = item?.data || item?.round || item || {};
  const r = (d.result && typeof d.result === 'object') ? d.result
    : (item?.result && typeof item.result === 'object') ? item.result
    : {};
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

async function fetchFrom(url) {
  const r = await axios.get(url, {
    timeout: 8000,
    headers: { 'User-Agent': 'Mozilla/5.0', Accept: 'application/json' }
  });
  return extractItems(r.data).map(normalizeRound).filter(Boolean);
}

async function syncOnce() {
  let got = null, src = -1;
  for (let i = 0; i < API_SOURCES.length; i++) {
    try {
      const rs = await fetchFrom(API_SOURCES[i]);
      if (rs.length) { got = rs; src = i; break; }
      R.erroAPI = `fonte ${i + 1}: sem dados`;
    } catch (e) {
      R.erroAPI = `fonte ${i + 1}: ${e.message}`;
    }
  }
  R.ultimaSync = new Date().toISOString();

  if (!got) {
    if (Date.now() - lastNewMs > CFG.staleAlertMs) await tgAlert('API', R.erroAPI || 'sem dados da API');
    return 0;
  }
  R.erroAPI = null;
  R.fonteAPI = src + 1;

  const novos = got.filter((r) => !S.seen.has(r.id)).sort((a, b) => a.t - b.t);
  for (const r of novos) {
    S.seen.add(r.id);
    S.hist.push(r);
    await dbRoundUpsert(r);
  }
  if (novos.length) {
    S.hist.sort((a, b) => (a.t - b.t) || (a.id < b.id ? -1 : 1));
    while (S.hist.length > CFG.maxHist) {
      const old = S.hist.shift();
      S.seen.delete(old.id);
    }
    R.ultimaRodadaNova = new Date().toISOString();
    lastNewMs = Date.now();
    S.statsAtLen = -1;
  } else if (Date.now() - lastNewMs > CFG.staleAlertMs) {
    await tgAlert('API', 'Sem rodadas novas há mais de 3 minutos');
  }
  if (R.erroDB) await tgAlert('DB', R.erroDB);
  return novos.length;
}

/* ============================ ESTRATÉGIAS ============================ */

function colOnly(res, n, k) {
  const out = [];
  for (let i = n - 1; i >= 0 && out.length < k; i--) if (isCol(res[i])) out.unshift(res[i]);
  return out;
}

function runInfo(res, n) {
  const c = colOnly(res, n, 12);
  if (!c.length) return { cor: null, len: 0 };
  const last = c[c.length - 1];
  let len = 0;
  for (let i = c.length - 1; i >= 0 && c[i] === last; i--) len++;
  return { cor: last, len };
}

const margem = (r) => (r && r.pj !== null && r.pb !== null ? Math.abs(r.pj - r.pb) : null);

// fn(h, res, n) => { cor, nome } | null   (n = nº de rodadas conhecidas)
const STRATS = [
  ...[3, 4, 5].map((k) => ({
    key: `seq${k}`, tipo: 'seq',
    fn: (h, res, n) => {
      const r = runInfo(res, n);
      return r.len >= k ? { cor: r.cor, nome: `Sequência x${r.len}` } : null;
    }
  })),
  ...[4, 5].map((k) => ({
    key: `quebra${k}`, tipo: 'quebra',
    fn: (h, res, n) => {
      const r = runInfo(res, n);
      return r.len >= k ? { cor: opp(r.cor), nome: `Quebra x${r.len}` } : null;
    }
  })),
  {
    key: 'vies', tipo: 'vies',
    fn: (h, res, n) => {
      const c = colOnly(res, n, 20);
      if (c.length < 20) return null;
      const cj = c.filter((x) => x === J).length;
      if (cj >= 13) return { cor: J, nome: 'Viés Jogador' };
      if (cj <= 7) return { cor: B, nome: 'Viés Banca' };
      return null;
    }
  },
  {
    key: 'alterna', tipo: 'alt',
    fn: (h, res, n) => {
      const c = colOnly(res, n, 4);
      if (c.length < 4) return null;
      if (c[0] !== c[1] && c[1] !== c[2] && c[2] !== c[3]) return { cor: opp(c[3]), nome: 'Alternância' };
      return null;
    }
  },
  {
    key: 'margem', tipo: 'margem',
    fn: (h, res, n) => {
      const r = h[n - 1];
      const m = margem(r);
      if (m !== null && m >= 6 && isCol(r.resultado)) {
        return { cor: r.resultado, nome: `Margem alta (${r.pj}×${r.pb})` };
      }
      return null;
    }
  },
  {
    key: 'dupla', tipo: 'margem',
    fn: (h, res, n) => {
      if (n < 2) return null;
      const a = h[n - 1], b = h[n - 2];
      const ma = margem(a), mb = margem(b);
      if (ma !== null && mb !== null && ma >= 5 && mb >= 5 && isCol(a.resultado) && a.resultado === b.resultado) {
        return { cor: a.resultado, nome: 'Dupla margem alta' };
      }
      return null;
    }
  },
  {
    key: 'posEmpate', tipo: 'empate',
    fn: (h, res, n) => {
      if (res[n - 1] !== E) return null;
      const c = colOnly(res, n, 1);
      return c.length ? { cor: c[0], nome: 'Pós-empate' } : null;
    }
  }
];

function evalBet(res, i, cor) {
  for (let j = 0; j <= CFG.maxGale; j++) {
    const r = res[i + j];
    if (r === undefined) return null;
    if (r === cor || r === E) return 1;
  }
  return 0;
}

function wilson(w, n) {
  if (!n) return 0;
  const z = 1.28, p = w / n;
  const num = p + (z * z) / (2 * n) - z * Math.sqrt((p * (1 - p) + (z * z) / (4 * n)) / n);
  return (num / (1 + (z * z) / n)) * 100;
}

function computeStats() {
  if (S.stats && S.statsAtLen === S.hist.length) return S.stats;
  const h = S.hist;
  const res = h.map((r) => r.resultado);
  const stats = {};
  for (const st of STRATS) stats[st.key] = { n: 0, w: 0, score: 0 };
  const start = Math.max(30, res.length - CFG.backtestWindow);
  for (let i = start; i < res.length; i++) {
    for (const st of STRATS) {
      const p = st.fn(h, res, i);
      if (!p || !p.cor) continue;
      const ev = evalBet(res, i, p.cor);
      if (ev === null) continue;
      stats[st.key].n++;
      stats[st.key].w += ev;
    }
  }
  for (const st of STRATS) stats[st.key].score = wilson(stats[st.key].w, stats[st.key].n);
  S.stats = stats;
  S.statsAtLen = S.hist.length;
  return stats;
}

function liveRate(key) {
  const rec = S.signals.filter((s) => s.key === key && s.status !== 'PENDENTE').slice(-20);
  if (rec.length < 3) return null;
  return rec.filter((s) => s.status !== 'LOSS').length / rec.length;
}

function pending() { return S.signals.filter((s) => s.status === 'PENDENTE'); }

/* ============================ GERAÇÃO DE SINAIS ============================ */

function pickSignal() {
  const h = S.hist, n = h.length;
  const res = h.map((r) => r.resultado);
  const stats = computeStats();
  const now = Date.now();
  const sinceLast = now - S.lastSignalAt;

  const cands = [];
  for (const st of STRATS) {
    if ((SUSPENDED[st.key] || 0) > now) continue;
    if ((STRATEGY_COOLDOWN[st.key] || 0) > now) continue;
    const stt = stats[st.key];
    if (!stt || stt.n < CFG.minAmostra) continue;
    const p = st.fn(h, res, n);
    if (!p || !p.cor) continue;
    cands.push({ key: st.key, tipo: st.tipo, nome: p.nome, cor: p.cor, score: stt.score, live: liveRate(st.key) });
  }
  if (!cands.length) return { motivo: 'sem estratégia ativa' };

  const groups = {};
  for (const c of cands) {
    const g = (groups[c.cor] ||= { cor: c.cor, agree: 0, sum: 0, best: null });
    g.agree++;
    g.sum += c.score;
    if (!g.best || c.score > g.best.score) g.best = c;
  }
  const ordered = Object.values(groups).sort((a, b) => (b.agree - a.agree) || (b.sum - a.sum));
  const g = ordered[0];
  const liveOk = (min) => g.best.live === null || g.best.live >= min;

  let tier = 0;
  for (let i = 0; i < CFG.tiers.length; i++) {
    const t = CFG.tiers[i];
    if (g.best.score >= t.scoreMin && g.agree >= t.minStratN && liveOk(t.liveMin)) { tier = i + 1; break; }
  }
  if (!tier && sinceLast >= CFG.relaxMs) {
    const t = CFG.tiers[CFG.tiers.length - 1];
    if (g.best.score >= t.scoreMin) tier = 3;
  }
  if (!tier && sinceLast >= CFG.forceSignalMs) tier = 4;
  if (!tier) return { motivo: 'aguardando confluência' };

  return { cand: g.best, agree: g.agree, tier };
}

async function maybeGenerate() {
  if (!CFG.ativos) { R.ultimoMotivo = 'desativado'; return; }
  if (pending().length >= CFG.maxPending) { R.ultimoMotivo = 'sinal pendente'; return; }
  if (S.hist.length < 40) { R.ultimoMotivo = 'histórico insuficiente'; return; }

  const now = Date.now();
  if (now - S.lastSignalAt < CFG.minGapMs) { R.ultimoMotivo = 'intervalo mínimo'; return; }
  const last = S.hist[S.hist.length - 1];
  if (now - last.t > CFG.gapMs) { R.ultimoMotivo = 'API desatualizada'; return; }

  const pk = pickSignal();
  if (!pk.cand) { R.ultimoMotivo = pk.motivo; return; }
  const c = pk.cand;

  const s = {
    id: S.nextId++, cor: c.cor, key: c.key, nome: c.nome, tipo: c.tipo,
    conf: Math.round(c.score), status: 'PENDENTE', gale: null, agree: pk.agree, tier: pk.tier,
    criadoEm: now, fechadoEm: null, baseRoundId: last.id, galeLevel: 0, restored: false
  };

  const sent = await tgSend(fmtSinal(s, CFG.maxGale + 1));
  if (!sent.ok) {
    S.nextId--; // nada foi publicado; liberta o id
    R.ultimoMotivo = 'falha Telegram';
    return;
  }

  S.signals.push(s);
  if (S.signals.length > CFG.maxSignalsMem) {
    const keep = S.signals.filter((x) => x.status === 'PENDENTE');
    const rest = S.signals.filter((x) => x.status !== 'PENDENTE').slice(-(CFG.maxSignalsMem - keep.length));
    S.signals = [...rest, ...keep].sort((a, b) => a.id - b.id);
  }
  S.lastSignalAt = now;
  STRATEGY_COOLDOWN[c.key] = now + CFG.strategyCooldownMs;
  R.tier = pk.tier;
  R.ultimoSinal = `#${s.id} ${nomeCor(s.cor)} (${s.nome})`;
  R.ultimoMotivo = 'sinal enviado';

  await dbSignalInsert(s);
  await dbStateSet('nextId', String(S.nextId));
  await dbStateSet('lastSignalAt', String(S.lastSignalAt));
  log('SINAL', `#${s.id} ${s.cor} ${s.nome} tier=${pk.tier} agree=${pk.agree}`);
}

/* ============================ RESULTADOS ============================ */

async function registerLoss(key) {
  const rec = S.signals.filter((x) => x.key === key && x.status !== 'PENDENTE').slice(-CFG.suspendLossStreak);
  if (rec.length >= CFG.suspendLossStreak && rec.every((x) => x.status === 'LOSS')) {
    SUSPENDED[key] = Date.now() + CFG.suspendMs;
    await dbStateSet('suspended', JSON.stringify(Object.entries(SUSPENDED)));
    log('SUSPENSA', `${key} suspensa`);
  }
}

async function closeSignal(s, status, lost) {
  await checkDailyReset();
  s.status = status;
  s.gale = lost;
  s.fechadoEm = Date.now();
  await dbSignalUpdate(s);

  const b = BUCKET[status];
  ALLTIME.total++; ALLTIME[b]++;
  DAILY.total++; DAILY[b]++;

  // Green, gales e empates somam na sequência. Só LOSS zera.
  if (status === 'LOSS') S.curStreak = 0;
  else {
    S.curStreak++;
    if (S.curStreak > DAILY.bestStreak) DAILY.bestStreak = S.curStreak;
  }
  await dbStateSet('daily_v4', JSON.stringify(DAILY));
  await dbStateSet('curStreak', String(S.curStreak));

  if (status === 'LOSS') await registerLoss(s.key);

  // Histórico já atualizado acima → mensagem sai sempre com os números certos.
  await tgSendReliable(fmtResultado(s));
  log('RESULTADO', `#${s.id} ${status}`);
}

async function settlePending() {
  for (const s of S.signals) {
    if (s.status !== 'PENDENTE') continue;

    let bi = -1;
    if (s.baseRoundId) {
      for (let i = S.hist.length - 1; i >= 0; i--) if (S.hist[i].id === s.baseRoundId) { bi = i; break; }
    }
    const atts = bi >= 0 ? S.hist.slice(bi + 1) : S.hist.filter((r) => r.t > s.criadoEm);

    let lost = 0, final = null;
    for (let i = 0; i <= CFG.maxGale; i++) {
      const r = atts[i];
      if (!r) break;
      if (r.resultado === s.cor) { final = ['GREEN', 'G1', 'G2'][i]; break; }
      if (r.resultado === E) { final = ['EMPATE', 'EG1', 'EG2'][i]; break; }
      lost++;
      if (i === CFG.maxGale) final = 'LOSS';
    }

    if (final) {
      await closeSignal(s, final, lost);
      continue;
    }

    if (s.restored) {
      s.galeLevel = lost;
      s.restored = false;
    } else if (lost > s.galeLevel) {
      s.galeLevel = lost;
      await tgSendReliable(fmtGale(s, lost));
    }

    if (Date.now() - s.criadoEm > CFG.apuraMaxMs) {
      s.status = 'ANULADO';
      await dbSignalDelete(s.id);
      S.signals = S.signals.filter((x) => x.id !== s.id);
      await tgSendReliable(`⚪ Sinal #${s.id} anulado (sem dados da mesa a tempo).`);
      log('ANULADO', `#${s.id}`);
    }
  }
}

/* ============================ LOOP ============================ */

async function tick() {
  if (busy || stopping) return;
  busy = true;
  try {
    await checkDailyReset();
    await flushOutbox();
    await syncOnce();
    await settlePending();
    await maybeGenerate();
  } catch (e) {
    log('TICK ERRO', e.message);
  } finally {
    busy = false;
  }
}

function schedule() {
  if (stopping) return;
  const ms = pending().length ? CFG.syncFastMs : CFG.syncMs;
  tickTimer = setTimeout(async () => {
    await tick();
    schedule();
  }, ms);
}

/* ============================ HTTP ============================ */

const app = express();

const auth = (req, res, next) => {
  const t = String(req.get('x-admin-token') || req.query.token || '');
  if (!ADMIN_TOKEN || t.length !== ADMIN_TOKEN.length ||
      !crypto.timingSafeEqual(Buffer.from(t), Buffer.from(ADMIN_TOKEN))) {
    return res.status(401).json({ erro: 'unauthorized' });
  }
  next();
};

app.get('/', (_req, res) => res.send(`BAC BO Signals v${R.versao} — ok`));
app.get('/health', (_req, res) => res.json({ ok: true, hora: hora() }));
app.get('/status', (_req, res) => res.json({
  R, CFG: { ativos: CFG.ativos, maxGale: CFG.maxGale, minGapMs: CFG.minGapMs },
  DAILY, ALLTIME, streak: S.curStreak, hist: S.hist.length,
  pendentes: pending().length, outbox: S.outbox.length,
  suspensas: Object.keys(SUSPENDED).filter((k) => SUSPENDED[k] > Date.now())
}));
app.get('/signals', (_req, res) => res.json(S.signals.slice(-50)));

app.get('/admin/on', auth, async (_req, res) => { CFG.ativos = true; await dbStateSet('ativos', 'true'); res.json({ ativos: true }); });
app.get('/admin/off', auth, async (_req, res) => { CFG.ativos = false; await dbStateSet('ativos', 'false'); res.json({ ativos: false }); });
app.get('/admin/gap', auth, async (req, res) => {
  const ms = parseInt(req.query.ms, 10);
  if (!(ms >= 30000 && ms <= 3600000)) return res.status(400).json({ erro: 'ms entre 30000 e 3600000' });
  CFG.minGapMs = ms; await dbStateSet('minGapMs', String(ms)); res.json({ minGapMs: ms });
});
app.get('/admin/gale', auth, async (req, res) => {
  const v = parseInt(req.query.n, 10);
  if (!(v >= 0 && v <= 2)) return res.status(400).json({ erro: 'n entre 0 e 2' });
  CFG.maxGale = v; S.statsAtLen = -1; await dbStateSet('maxGale', String(v)); res.json({ maxGale: v });
});
app.get('/admin/rebuild', auth, async (_req, res) => { await dbLoadAlltime(); await rebuildDailyFromDb(); res.json({ DAILY, streak: S.curStreak }); });
app.get('/admin/test', auth, async (_req, res) => {
  const r = await tgSend(`✅ Teste BAC BO v${R.versao} • ${hora()}`);
  res.json(r);
});

/* ============================ BOOT / SHUTDOWN ============================ */

async function shutdown(sig) {
  if (stopping) return;
  stopping = true;
  log('STOP', sig);
  clearTimeout(tickTimer);
  clearInterval(keepTimer);
  try { server?.close(); } catch (_) {}
  await releaseLock();
  try { await pool?.end(); } catch (_) {}
  process.exit(0);
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('unhandledRejection', (e) => log('UNHANDLED', String(e?.message || e)));
process.on('uncaughtException', (e) => log('UNCAUGHT', String(e?.message || e)));

(async () => {
  try {
    server = app.listen(PORT, () => log('HTTP', `porta ${PORT}`));
    await dbInit();
    if (!(await acquireLock())) { log('LOCK', 'não obtido, a sair'); process.exit(1); }
    await dbLoadState();
    await syncOnce();
    await settlePending();

    if (SELF_URL) {
      keepTimer = setInterval(() => {
        axios.get(`${SELF_URL.replace(/\/$/, '')}/health`, { timeout: 8000 }).catch(() => {});
      }, CFG.keepAliveMs);
    }

    log('BOOT', `v${R.versao} deploy=${DEPLOY_ID} tz=${HISTORY_TZ}`);
    schedule();
  } catch (e) {
    log('BOOT ERRO', e.message);
    process.exit(1);
  }
})();