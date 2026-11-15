'use strict';

/**
 * BAC BO Telegram Signals — v4
 * Lógica de processamento: CAPUNGA v19 (tick 1s, sync adaptativo, tiers, apuração com gap)
 * Estratégias e modelo de SMS: mantidos do bot original
 * Correção do histórico: streak em memória + reconstrução do DAILY a partir da BD no arranque
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

const ALLOWED_TZ = new Set(['Africa/Luanda', 'America/New_York', 'America/Sao_Paulo', 'Europe/Lisbon', 'UTC']);
const HISTORY_TZ = ALLOWED_TZ.has(process.env.HISTORY_TZ) ? process.env.HISTORY_TZ : 'Africa/Luanda';

const CFG = {
  ativos: true,
  maxGale: 2,
  maxPending: 1,
  minAmostra: 60,
  syncMs: 3000,
  syncFastMs: 1000,
  minGapMs: 20000,
  relaxMs: 3 * 60 * 1000,
  forceSignalMs: 5 * 60 * 1000,
  minSettledAgeMs: 5000,
  apuraMaxMs: 600000,
  maxHist: 4000,
  gapMs: 7 * 60 * 1000,
  maxSignalsMem: 500,
  signalsOnBoot: 500,
  suspendMs: 4 * 3600 * 1000,
  suspendLossStreak: 3,
  staleAlertMs: 3 * 60 * 1000,
  alertThrottleMs: 10 * 60 * 1000,
  keepAliveMs: 60000,
  tiers: [
    { scoreMin: 85, minStratN: 8, liveMin: 0.86 },
    { scoreMin: 81, minStratN: 5, liveMin: 0.80 },
    { scoreMin: 80, minStratN: 3, liveMin: 0 }
  ]
};

const J = 'Jogador', B = 'Banca', E = 'Empate';
const isCol = (x) => x === J || x === B;
const opp = (c) => (c === J ? B : c === B ? J : null);
const FECHADOS = new Set(['GREEN', 'G1', 'G2', 'EMPATE', 'EG1', 'EG2', 'LOSS']);
const LOCK_KEY = 727272;

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
  statsAtLen: -1
};

const DAILY = { date: '', total: 0, green: 0, g1: 0, g2: 0, empate: 0, eg1: 0, eg2: 0, loss: 0, bestStreak: 0 };
const ALLTIME = { total: 0, green: 0, g1: 0, g2: 0, empate: 0, eg1: 0, eg2: 0, loss: 0 };
const SUSPENDED = {};

const R = {
  iniciadoEm: new Date().toISOString(),
  deployId: DEPLOY_ID,
  versao: '4.0',
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
let busy = false, stopping = false, lastTickMs = Date.now();

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const log = (tag, d) => console.log(`[${new Date().toISOString()}] [${tag}] ${typeof d === 'string' ? d : JSON.stringify(d)}`);

const fmtHora = new Intl.DateTimeFormat('pt-PT', {
  timeZone: HISTORY_TZ, hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false
});
const hora = (ms = Date.now()) => fmtHora.format(new Date(ms));

function dayKey(ms = Date.now()) {
  const fmt = new Intl.DateTimeFormat('en-CA', {
    timeZone: HISTORY_TZ, year: 'numeric', month: '2-digit', day: '2-digit'
  });
  return fmt.format(new Date(ms));
}

const nomeCor = (c) => (c === B ? '🔴 BANCA' : c === J ? '🔵 JOGADOR' : '🟡 EMPATE');
const noHistory = (status) => ['EMPATE', 'EG1', 'EG2'].includes(status);

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

  // Migração opcional da tabela `signals` do bot anterior
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
      ALLTIME.total += row.n;
      if (row.status === 'GREEN') ALLTIME.green = row.n;
      else if (row.status === 'G1') ALLTIME.g1 = row.n;
      else if (row.status === 'G2') ALLTIME.g2 = row.n;
      else if (row.status === 'EMPATE') ALLTIME.empate = row.n;
      else if (row.status === 'EG1') ALLTIME.eg1 = row.n;
      else if (row.status === 'EG2') ALLTIME.eg2 = row.n;
      else if (row.status === 'LOSS') ALLTIME.loss = row.n;
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
      const bucket =
        row.status === 'GREEN' ? 'green' :
        row.status === 'G1' ? 'g1' :
        row.status === 'G2' ? 'g2' :
        row.status === 'EMPATE' ? 'empate' :
        row.status === 'EG1' ? 'eg1' :
        row.status === 'EG2' ? 'eg2' : 'loss';
      DAILY[bucket] = row.n;
      DAILY.total += row.n;
    }

    const ordered = await pool.query(
      `SELECT status FROM capunga_signals
       WHERE status <> 'PENDENTE'
         AND (closed_at AT TIME ZONE $1)::date = (NOW() AT TIME ZONE $1)::date
       ORDER BY closed_at ASC, id ASC`,
      [HISTORY_TZ]
    );
    let cur = 0, best = 0;
    for (const row of ordered.rows) {
      const isWin = row.status === 'GREEN' || row.status === 'G1' || row.status === 'G2';
      if (isWin) { cur++; if (cur > best) best = cur; }
      else { cur = 0; }
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

  try {
    const ativos = await dbStateGet('ativos');
    if (ativos === 'false') CFG.ativos = false;
  } catch (_) {}
  try {
    const gap = await dbStateGet('minGapMs');
    if (gap) CFG.minGapMs = parseInt(gap, 10) || CFG.minGapMs;
  } catch (_) {}
  try {
    const gales = await dbStateGet('maxGale');
    if (gales !== null && gales !== undefined && gales !== '') {
      const v = parseInt(gales, 10);
      if (v >= 0 && v <= 3) CFG.maxGale = v;
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
    gale1MsgId: null, gale2MsgId: null,
    galeLevel: row.status !== 'PENDENTE' ? 2 : 0
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

async function tgDelete(messageId) {
  if (!messageId) return;
  try {
    await axios.post(
      `https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`,
      { chat_id: CHAT_ID, message_id: messageId },
      { timeout: 6000 }
    );
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
  await tgSend(`⚠️ ALERTA ${tag}\n${detalhe}\n⏱ ${hora()}`);
}

/* ============================ SMS (formato do bot original) ============================ */

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

function fmtResultado(s) {
  let text = `${RESULT_LABEL[s.status] || '📣 RESULTADO DA ENTRADA'}\n🎲 Sinal #${s.id}`;
  if (!noHistory(s.status)) text += `\n\n${fmtHistoricoBloco()}`;
  return text;
}

/* ============================ RESET DIÁRIO ============================ */

async function checkDailyReset() {
  const hoje = dayKey();
  if (!DAILY.date) {
    DAILY.date = hoje;
    return;
  }
  if (DAILY.date !== hoje) {
    const ontem = DAILY.date;
    for (const k of ['total', 'green', 'g1', 'g2', 'empate', 'eg1', 'eg2', 'loss', 'bestStreak']) {
      DAILY[k] = 0;
    }
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
  const PAGE_SIZE = 50;
  const MAX_PAGES = 20; // Recupera até 1.000 rondas em caso de atraso do serviço.
  const base = new URL(url);
  const map = new Map();
  for (let page = 0; page < MAX_PAGES; page++) {
    const pageUrl = new URL(base);
    pageUrl.searchParams.set('page', String(page));
    pageUrl.searchParams.set('size', String(PAGE_SIZE));
    const resp = await axios.get(pageUrl.toString(), {
      timeout: 8000,
      headers: { Accept: 'application/json', 'User-Agent': 'BACBO/4.0' }
    });
    const items = extractItems(resp.data);
    for (const it of items) {
      const rd = normalizeRound(it);
      if (!rd) continue;
      const prev = map.get(rd.id);
      // Mantém a versão mais completa se a API repetir um evento entre páginas.
      if (!prev || ((!Number.isFinite(prev.pj) || !Number.isFinite(prev.pb)) && Number.isFinite(rd.pj) && Number.isFinite(rd.pb))) {
        map.set(rd.id, rd);
      }
    }
    if (items.length < PAGE_SIZE) break;
  }
  return [...map.values()].sort((a, b) => a.t - b.t || a.id.localeCompare(b.id));
}

async function fetchRounds() {
  const erros = [];
  for (let idx = 0; idx < API_SOURCES.length; idx++) {
    for (let tent = 1; tent <= 2; tent++) {
      try {
        const rounds = await fetchFrom(API_SOURCES[idx]);
        R.fonteAPI = idx;
        R.erroAPI = null;
        return rounds;
      } catch (e) {
        erros.push(`fonte${idx + 1}: ${e.message}`);
        if (tent < 2) await sleep(500);
      }
    }
  }
  R.erroAPI = erros.join(' | ');
  throw new Error(R.erroAPI);
}

/* ============================ HELPERS DE ANÁLISE ============================ */

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
  let n = 0;
  const cor = seq[0].resultado;
  for (const r of seq) {
    if (r.resultado === cor) n++;
    else break;
  }
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
  return { trend: 1 - chop, chop, label: chop >= 0.65 ? 'errático' : chop <= 0.35 ? 'tendência' : 'neutro' };
}

function regimeFit(cand, regime) {
  const k = cand.key;
  if (/SEQ_(QUEBRA|SURF|PINGPONG|2x2|EXTEND|3_1|MOMENTUM|ALT|BREAK_WEAK)/.test(k)) return regime.chop >= 0.45;
  if (/SEQ_(REP|STRONG_STREAK)|BANKER_BIAS/.test(k)) return regime.trend >= 0.45;
  return true;
}

/* ============================ ESTRATÉGIAS (mantidas do original) ============================ */

const STRATS = [
  ({ seq }) => { const n = streak(seq); if (n < 3 || n > 7) return null; return { cor: seq[0].resultado, key: `SEQ_REP_${n}`, nome: `Sequência x${n}`, tipo: 'seq', strength: [0,0,0,72,78,84,86,86][n] || 86 }; },
  ({ seq }) => { const n = streak(seq); if (n < 4 || n > 8) return null; return { cor: opp(seq[0].resultado), key: `SEQ_QUEBRA_${n}`, nome: `Quebra x${n}`, tipo: 'seq', strength: [0,0,0,0,74,80,84,85,85][n] || 85 }; },
  ({ seq }) => { const L = alternation(seq); if (L < 4) return null; return { cor: opp(seq[0].resultado), key: `SEQ_SURF_${L}`, nome: `Surf ${L}`, tipo: 'seq', strength: L >= 6 ? 84 : L >= 5 ? 78 : 72 }; },
  ({ seq }) => { if (seq.length < 4) return null; const [a,b,c,d] = seq.map(x => x.resultado); if (a === b && c === d && a !== c) return { cor: c, key: 'SEQ_2x2', nome: 'Bloco 2+2', tipo: 'seq', strength: 76 }; return null; },
  ({ seq }) => { if (seq.length < 4) return null; const [a,b,c,d] = seq.map(x => x.resultado); if (a !== b && b === c && c === d) return { cor: a, key: 'SEQ_3_1', nome: '3→1 confirmado', tipo: 'seq', strength: 78 }; return null; },
  ({ seq }) => { if (seq.length < 3) return null; const [a,b,c] = seq.map(x => x.resultado); if (a !== b && b === c) return { cor: c, key: 'SEQ_MOMENTUM_2', nome: 'Momentum 2', tipo: 'seq', strength: 77 }; return null; },
  ({ seq }) => { const n = streak(seq); if (n !== 4) return null; const s = seq.slice(0,4); if (!s.every(r => hasScore(r) && margem(r) <= 2)) return null; return { cor: opp(seq[0].resultado), key: 'SEQ_BREAK_WEAK_4', nome: 'Quebra fraca x4', tipo: 'seq', strength: 79 }; },
  ({ seq }) => { if (seq.length < 5) return null; const [a,b,c,d,e] = seq.map(x => x.resultado); if (a === b && b === c && c !== d && d === e) return { cor: a, key: 'SEQ_EXTEND', nome: 'Streak extend', tipo: 'seq', strength: 80 }; return null; },
  ({ seq }) => { const n = streak(seq); if (n < 4 || n > 6) return null; const s = seq.slice(0,n); if (!s.every(r => hasScore(r) && margem(r) >= 3)) return null; return { cor: seq[0].resultado, key: 'SEQ_STRONG_STREAK', nome: `Dragon ${n}`, tipo: 'seq', strength: 82 }; },
  ({ seq }) => { const L = alternation(seq); if (L < 4 || L > 10) return null; return { cor: opp(seq[0].resultado), key: 'SEQ_PINGPONG', nome: `Ping-pong ${L}`, tipo: 'seq', strength: 79 }; },
  ({ seq }) => { const L = alternation(seq); if (L < 3) return null; const s = seq.slice(0,L); if (!s.every(r => hasScore(r) && margem(r) >= 3)) return null; return { cor: opp(seq[0].resultado), key: 'SEQ_ALT_STRONG', nome: `Alt. forte ${L}`, tipo: 'seq', strength: 78 }; },
  ({ seq }) => { const s = seq.slice(0,20); if (s.length < 15) return null; const b = s.filter(r => r.resultado === B).length; const j = s.filter(r => r.resultado === J).length; if (b >= 12) return { cor: B, key: 'BANKER_BIAS_20', nome: 'Viés Banca', tipo: 'seq', strength: 80 }; if (j >= 12) return { cor: J, key: 'BANKER_BIAS_20', nome: 'Viés Jogador', tipo: 'seq', strength: 80 }; return null; },

  ({ seq }) => { if (seq.length < 3 || !seq.slice(0,3).every(hasScore)) return null; const [a,b,c] = seq; if (a.resultado !== b.resultado || b.resultado !== c.resultado) return null; const sa = scoreOf(a), sb = scoreOf(b), sc = scoreOf(c); if ((sb - sa === 1 && sc - sb === 1) || (sa - sb === 1 && sb - sc === 1)) return { cor: a.resultado, key: 'NUM_ESCADA', nome: `Escada ${sa}→${sb}→${sc}`, tipo: 'num', strength: 88 }; return null; },
  ({ seq }) => { if (seq.length < 2 || !seq.slice(0,2).every(hasScore)) return null; const [a,b] = seq; if (a.resultado !== b.resultado) return null; if (scoreOf(a) === scoreOf(b)) return { cor: a.resultado, key: 'NUM_REPETE', nome: `Score repetido ${scoreOf(a)}`, tipo: 'num', strength: 84 }; return null; },
  ({ seq }) => { if (seq.length < 3 || !seq.slice(0,3).every(hasScore)) return null; const [a,b,c] = seq; if (a.resultado !== b.resultado || b.resultado !== c.resultado) return null; if (scoreOf(a) <= 3 && scoreOf(b) <= 3 && scoreOf(c) <= 3) return { cor: opp(a.resultado), key: 'NUM_THREE_LOW', nome: 'Virada de baixos x3', tipo: 'num', strength: 81 }; return null; },
  ({ seq }) => { if (!seq.length || !hasScore(seq[0])) return null; if (scoreOf(seq[0]) === 0) return { cor: opp(seq[0].resultado), key: 'NUM_GATILHO_0', nome: 'Gatilho 0', tipo: 'num', strength: 86 }; return null; },
  ({ seq }) => { if (!seq.length || !hasScore(seq[0])) return null; if (scoreOf(seq[0]) === 1) return { cor: opp(seq[0].resultado), key: 'NUM_GATILHO_1', nome: 'Gatilho 1', tipo: 'num', strength: 84 }; return null; },
  ({ seq }) => { if (!seq.length || !hasScore(seq[0])) return null; if (scoreOf(seq[0]) === 9) return { cor: opp(seq[0].resultado), key: 'NUM_GATILHO_9', nome: 'Gatilho 9', tipo: 'num', strength: 88 }; return null; },
  ({ rows }) => { const r = rows[0]; if (!r || !isCol(r.resultado) || !hasScore(r) || margem(r) < 5) return null; return { cor: r.resultado, key: 'NUM_MARGEM5', nome: `Margem alta ${r.pj}×${r.pb}`, tipo: 'num', strength: 82 }; },
  ({ seq }) => { if (seq.length < 3) return null; const t = seq.slice(0,3); if (!t.every(r => hasScore(r) && margem(r) >= 4 && r.resultado === t[0].resultado)) return null; return { cor: t[0].resultado, key: 'NUM_DIFF4x3', nome: 'Diff ≥4 ×3', tipo: 'num', strength: 85 }; },
  ({ seq }) => { if (seq.length < 2) return null; const [a,b] = seq; if (!hasScore(a) || !hasScore(b) || a.resultado !== b.resultado) return null; if (scoreOf(a) > 4 || scoreOf(b) > 4) return null; return { cor: opp(a.resultado), key: 'NUM_VIRADA_BAIXA', nome: 'Virada de baixos', tipo: 'num', strength: 80 }; },
  ({ rows }) => { const s = rows.slice(0,8).filter(hasScore); if (s.length < 6) return null; const mj = s.reduce((a,r) => a + r.pj, 0) / s.length; const mb = s.reduce((a,r) => a + r.pb, 0) / s.length; const diff = mj - mb; if (Math.abs(diff) < 1.5) return null; return { cor: diff > 0 ? J : B, key: 'NUM_TENDENCIA', nome: `Tendência ${mj.toFixed(1)}×${mb.toFixed(1)}`, tipo: 'num', strength: Math.abs(diff) >= 2.2 ? 84 : 76 }; },
  ({ seq }) => { if (seq.length < 2 || !hasScore(seq[0]) || !hasScore(seq[1])) return null; const [a,b] = seq; const ra = scoreRange(scoreOf(a)), rb = scoreRange(scoreOf(b)); if (!ra || !rb) return null; if ((ra === 'MB' && rb === 'MA') || (ra === 'MA' && rb === 'MB')) return { cor: opp(a.resultado), key: 'NUM_SALTO_RANGE', nome: `Salto ${scoreOf(b)}→${scoreOf(a)}`, tipo: 'num', strength: 81 }; return null; },
  ({ seq }) => { if (seq.length < 3 || !seq.slice(0,3).every(hasScore)) return null; const [a,b,c] = seq; if (a.resultado !== b.resultado || b.resultado !== c.resultado) return null; const sa = scoreOf(a), sb = scoreOf(b), sc = scoreOf(c); if (sa < sb && sb < sc) return { cor: a.resultado, key: 'NUM_SCORE_TREND_3', nome: `Escala ascendente ${sa}→${sc}`, tipo: 'num', strength: 82 }; return null; },
  ({ seq }) => { if (seq.length < 2 || !seq.slice(0,2).every(hasScore)) return null; const [a,b] = seq; if (a.resultado !== b.resultado) return null; if (margem(a) >= 5 && margem(b) >= 5) return { cor: a.resultado, key: 'NUM_DOUBLE_HIGH', nome: 'Dupla margem alta', tipo: 'num', strength: 84 }; return null; },
  ({ seq }) => { if (seq.length < 4 || !seq.slice(0,4).every(hasScore)) return null; const [a,b,c,d] = seq; const sa = scoreOf(a), sb = scoreOf(b), sc = scoreOf(c), sd = scoreOf(d); if (sb - sa < 0 && sc - sb < 0 && sd - sc < 0) return { cor: opp(a.resultado), key: 'MOMENTUM_SHIFT', nome: 'Mudança de momentum', tipo: 'num', strength: 78 }; return null; },

  ({ rows, seq }) => { if (!rows[0] || rows[0].resultado !== E || !seq.length) return null; return { cor: seq[0].resultado, key: 'NUM_POS_EMPATE', nome: 'Pós-empate', tipo: 'num', strength: 74 }; },
  ({ rows }) => { const t = rows.slice(0,3).filter(r => r.resultado === E).length; if (t < 2) return null; const pre = rows.find(r => isCol(r.resultado)); if (!pre) return null; return { cor: pre.resultado, key: 'TIE_CASCADE', nome: 'Cascata de empates', tipo: 'num', strength: 76 }; },
  ({ rows, seq }) => { if (!rows[0] || rows[0].resultado !== E) return null; if (!rows[1] || rows[1].resultado === E) return null; if (!seq.length) return null; return { cor: opp(seq[0].resultado), key: 'TIE_SINGLE', nome: 'Empate isolado', tipo: 'num', strength: 77 }; },
  ({ rows }) => { if (rows.length < 3) return null; if (rows[0].resultado !== E || rows[1].resultado !== E) return null; if (!isCol(rows[2].resultado)) return null; return { cor: rows[2].resultado, key: 'TIE_DOUBLE_BACK', nome: 'Retorno pós-duplo empate', tipo: 'num', strength: 78 }; },

  ({ seq }) => { if (seq.length < 10) return null; const s = seq.slice(0,10); const p = s.filter(r => r.resultado === J).length; const b = s.length - p; if (p === b) return null; return { cor: p > b ? J : B, key: 'FREQ_10', nome: 'Frequência nas últimas 10', tipo: 'freq', strength: 70 + Math.min(10, Math.abs(p - b)) }; }
];

function runStrategies(rows) {
  const seq = rows.filter((r) => isCol(r.resultado));
  const ctx = { rows, seq };
  const out = [];
  for (const fn of STRATS) {
    try {
      const r = fn(ctx);
      if (r && isCol(r.cor)) out.push(r);
    } catch (_) {}
  }
  return out;
}

/* ============================ BACKTEST + CONFIANÇA ============================ */

function windowAt(H, i) {
  const rows = [];
  for (let j = i; j >= 0 && rows.length < 20; j--) {
    if (rows.length && rows[rows.length - 1].t - H[j].t > CFG.gapMs) break;
    rows.push(H[j]);
  }
  return rows;
}

function evalEntry(H, i, cor) {
  for (let k = 1; k <= CFG.maxGale + 1; k++) {
    const idx = i + k;
    if (idx >= H.length) return null;
    if (H[idx].t - H[idx - 1].t > CFG.gapMs) return false;
    if (H[idx].resultado === cor || H[idx].resultado === E) return true;
  }
  return false;
}

function backtest() {
  const H = S.hist, per = {};
  const ini = 10, fim = H.length - 1;
  if (fim - ini < 30) return { per, baseRate: 0.90 };
  let bw = 0, bn = 0;
  for (let i = ini; i < fim; i++) {
    const win = windowAt(H, i);
    if (win.length < 6) continue;
    for (const c of runStrategies(win)) {
      const v = evalEntry(H, i, c.cor);
      if (v === null) continue;
      const p = per[c.key] || (per[c.key] = { n: 0, w: 0 });
      p.n++;
      if (v) p.w++;
      bn++;
      if (v) bw++;
    }
  }
  return { per, baseRate: bn ? Math.min(0.95, Math.max(0.80, bw / bn)) : 0.90 };
}

function getStats(force = false) {
  if (force || !S.stats || S.hist.length - S.statsAtLen >= 30) {
    S.stats = backtest();
    S.statsAtLen = S.hist.length;
  }
  return S.stats;
}

function bayesianRate(w, n, priorRate = 0.90, priorWeight = 10) {
  return (w + priorRate * priorWeight) / (n + priorWeight);
}

function confianca(cand, ctx) {
  const stats = ctx.stats;
  const p = stats.per[cand.key] || { n: 0, w: 0 };
  const baseRate = stats.baseRate || 0.90;
  const btRate = bayesianRate(p.w, p.n, baseRate, 6);
  const live = S.signals.filter((x) => x.key === cand.key && FECHADOS.has(x.status));
  const liveW = live.filter((x) => x.status !== 'LOSS').length;
  const liveRate = live.length >= 3 ? bayesianRate(liveW, live.length, btRate, 4) : null;
  const btSamples = p.n + 6;
  const liveSamples = live.length >= 3 ? live.length + 4 : 0;
  const observed = ((btRate * btSamples) + ((liveRate ?? btRate) * liveSamples)) / (btSamples + liveSamples);

  let conf = Math.round(observed * 100);
  const agree = ctx.agreement?.confirmations || 0;
  conf += agree >= 3 ? 2 : agree >= 2 ? 1 : 0;
  conf += Math.round((cand.strength - 80) * 0.2);
  if (cand.tipo === 'seq' && ctx.regime) {
    const rm = (cand.key.includes('QUEBRA') || cand.key.includes('SURF') || cand.key.includes('PINGPONG'))
      ? ctx.regime.chop : ctx.regime.trend;
    conf += Math.round((rm - 0.5) * 4);
  }
  if (live.length >= 5 && liveW / live.length < 0.85) conf -= 4;
  const recentLoss = S.signals.find((x) =>
    x.key === cand.key && x.status === 'LOSS' && Date.now() - (x.fechadoEm || 0) < 5 * 60 * 1000);
  if (recentLoss) conf -= 3;

  return Math.max(80, Math.min(95, conf));
}

/* ============================ SUSPENSÃO + TIERS + GATE ============================ */

function isSuspended(key) {
  const u = SUSPENDED[key];
  if (!u) return false;
  if (Date.now() > u) {
    delete SUSPENDED[key];
    dbStateSet('suspended', JSON.stringify(Object.entries(SUSPENDED)));
    return false;
  }
  return true;
}

function maybeSuspend(key) {
  const recent = S.signals.filter((x) => x.key === key && FECHADOS.has(x.status)).slice(-CFG.suspendLossStreak);
  if (recent.length < CFG.suspendLossStreak) return;
  if (recent.every((x) => x.status === 'LOSS')) {
    SUSPENDED[key] = Date.now() + CFG.suspendMs;
    dbStateSet('suspended', JSON.stringify(Object.entries(SUSPENDED)));
    log('SUSPENSO', { key, n: recent.length });
  }
}

function idleMs() { return S.lastSignalAt ? Date.now() - S.lastSignalAt : Infinity; }
function tierAtual() {
  const i = idleMs();
  if (i >= CFG.forceSignalMs) return 2;
  if (i >= CFG.relaxMs) return 1;
  return 0;
}

function pendentes() { return S.signals.filter((s) => !FECHADOS.has(s.status)); }

function podeGerar() {
  if (!CFG.ativos) { R.ultimoMotivo = 'desativado'; return false; }
  if (pendentes().length >= CFG.maxPending) { R.ultimoMotivo = 'max pending'; return false; }
  const now = Date.now();
  if (S.lastSignalAt && now - S.lastSignalAt < CFG.minGapMs) {
    R.ultimoMotivo = `gap ${Math.ceil((CFG.minGapMs - (now - S.lastSignalAt)) / 1000)}s`;
    return false;
  }
  const last = S.hist[S.hist.length - 1];
  if (!last) { R.ultimoMotivo = 'sem hist'; return false; }
  if (now - last.t < CFG.minSettledAgeMs) {
    R.ultimoMotivo = `rodada recente ${Math.ceil((CFG.minSettledAgeMs - (now - last.t)) / 1000)}s`;
    return false;
  }
  if (S.hist.length < CFG.minAmostra) { R.ultimoMotivo = `amostra ${S.hist.length}`; return false; }
  return true;
}

function stratSamples(key, stats) {
  const p = stats.per[key];
  let liveN = 0;
  for (const x of S.signals) if (x.key === key && FECHADOS.has(x.status)) liveN++;
  return (p ? p.n : 0) + liveN;
}

function liveHitRate(key) {
  let n = 0, w = 0;
  for (const x of S.signals) {
    if (x.key !== key || !FECHADOS.has(x.status)) continue;
    n++;
    if (x.status !== 'LOSS') w++;
  }
  return n >= 5 ? w / n : null;
}

/* ============================ GERAR SINAL ============================ */

async function tentarSinal() {
  if (!podeGerar()) return;

  const tier = tierAtual();
  R.tier = tier;
  const T = CFG.tiers[tier];

  const rows = S.hist.slice(-30).reverse();
  const stats = getStats();
  const seq = rows.filter((r) => isCol(r.resultado));
  const regime = detectRegime(seq);

  let cands = runStrategies(rows);
  if (!cands.length && rows.length >= 6) {
    const s = rows.slice(0, 10);
    const p = s.filter((r) => r.resultado === J).length;
    const b = s.filter((r) => r.resultado === B).length;
    if (p || b) {
      cands = [{
        cor: p >= b ? J : B, key: 'FREQ_FALLBACK', nome: 'Frequência de reserva',
        tipo: 'freq', strength: 65
      }];
    }
  }
  if (!cands.length) { R.ultimoMotivo = 'sem estratégia'; return; }

  if (tier <= 1) cands = cands.filter((c) => regimeFit(c, regime));

  cands = cands.filter((c) => {
    if (isSuspended(c.key)) return false;
    if (stratSamples(c.key, stats) < T.minStratN) return false;
    const lr = liveHitRate(c.key);
    if (lr !== null && lr < T.liveMin) return false;
    return true;
  });
  if (!cands.length) { R.ultimoMotivo = `tier${tier}: filtros`; return; }

  const byColor = {};
  for (const c of cands) {
    if (!byColor[c.cor]) byColor[c.cor] = [];
    byColor[c.cor].push(c);
  }

  let melhor = null;
  for (const cor of Object.keys(byColor)) {
    const list = byColor[cor];
    const scored = list.map((c) => ({
      ...c,
      conf: confianca(c, { stats, regime, agreement: { confirmations: list.length } })
    }));
    scored.sort((a, b) => b.conf - a.conf);
    const top = scored[0];
    if (!melhor || top.conf > melhor.conf) melhor = { ...top, agree: scored.length };
  }

  if (!melhor) { R.ultimoMotivo = 'sem candidato'; return; }
  if (tier <= 1 && melhor.conf < T.scoreMin) {
    R.ultimoMotivo = `score ${melhor.conf}<${T.scoreMin}`;
    return;
  }

  const reservedId = S.nextId;
  const prelim = {
    id: reservedId, cor: melhor.cor, key: melhor.key, nome: melhor.nome,
    conf: melhor.conf, tipo: melhor.tipo, status: 'PENDENTE', tier,
    baseRoundId: S.hist[S.hist.length - 1]?.id || null,
    criadoEm: 0, agree: melhor.agree,
    gale1MsgId: null, gale2MsgId: null, galeLevel: 0
  };

  const envio = await tgSend(fmtSinal(prelim, CFG.maxGale + 1));
  if (!envio.ok) {
    R.ultimoMotivo = 'telegram falhou';
    return;
  }

  prelim.criadoEm = Date.now();
  S.nextId++;
  S.signals.push(prelim);
  if (S.signals.length > CFG.maxSignalsMem) S.signals = S.signals.slice(-CFG.maxSignalsMem + 50);
  S.lastSignalAt = prelim.criadoEm;
  R.ultimoSinal = prelim;
  R.ultimoMotivo = null;

  void dbSignalInsert(prelim);
  void dbStateSet('nextId', String(S.nextId));
  void dbStateSet('lastSignalAt', String(S.lastSignalAt));

  log('SINAL', {
    id: prelim.id, cor: prelim.cor, conf: prelim.conf,
    key: prelim.key, tier
  });
}

async function notificarGale(s, nivel) {
  if (s.galeLevel >= nivel) return;
  s.galeLevel = nivel;
  if (nivel === 2 && s.gale1MsgId) {
    void tgDelete(s.gale1MsgId);
    s.gale1MsgId = null;
  }
  const r = await tgSend(fmtGale(s, nivel));
  if (r.ok) {
    if (nivel === 1) s.gale1MsgId = r.messageId;
    else s.gale2MsgId = r.messageId;
  }
}

/* ============================ FECHAR SINAL ============================ */

async function fecharSinal(s, status, gale) {
  s.status = status;
  s.gale = gale;
  s.fechadoEm = Date.now();

  if (s.gale1MsgId) { void tgDelete(s.gale1MsgId); s.gale1MsgId = null; }
  if (s.gale2MsgId) { void tgDelete(s.gale2MsgId); s.gale2MsgId = null; }

  const bucket =
    status === 'GREEN' ? 'green' :
    status === 'G1' ? 'g1' :
    status === 'G2' ? 'g2' :
    status === 'EMPATE' ? 'empate' :
    status === 'EG1' ? 'eg1' :
    status === 'EG2' ? 'eg2' : 'loss';

  ALLTIME[bucket]++;
  ALLTIME.total++;
  DAILY[bucket]++;
  DAILY.total++;

  if (status === 'LOSS' || status === 'EMPATE' || status === 'EG1' || status === 'EG2') {
    S.curStreak = 0;
  } else {
    S.curStreak++;
    if (S.curStreak > DAILY.bestStreak) DAILY.bestStreak = S.curStreak;
  }

  void dbStateSet('curStreak', String(S.curStreak));
  void dbStateSet('daily_v4', JSON.stringify(DAILY));
  void dbSignalUpdate(s);

  void (async () => {
    try {
      for (let attempt = 1; attempt <= 3; attempt++) {
        const r = await tgSend(fmtResultado(s));
        if (r.ok) return;
        await sleep(2000 * attempt);
      }
      log('TG', `resultado #${s.id} não notificado após 3 tentativas`);
    } catch (e) {
      log('TG', `resultado #${s.id} erro: ${e.message}`);
    }
  })();

  maybeSuspend(s.key);
}

/* ============================ APURAÇÃO ============================ */

async function apurar() {
  const now = Date.now();
  for (const s of S.signals) {
    if (FECHADOS.has(s.status)) continue;
    const anchorIndex = s.baseRoundId ? S.hist.findIndex((r) => r.id === s.baseRoundId) : -1;
    const after = anchorIndex >= 0
      ? S.hist.slice(anchorIndex + 1)
      : S.hist.filter((r) => r.t > s.criadoEm).sort((a, b) => a.t - b.t || a.id.localeCompare(b.id));
    let decided = false;

    for (let k = 0; k <= CFG.maxGale && k < after.length; k++) {
      const r = after[k];
      const prevT = k === 0 ? s.criadoEm : after[k - 1].t;

      if (r.t - prevT > CFG.gapMs) {
        await fecharSinal(s, 'LOSS', k);
        decided = true;
        break;
      }

      if (r.resultado === s.cor || r.resultado === E) {
        const st = r.resultado === E
          ? (k === 0 ? 'EMPATE' : k === 1 ? 'EG1' : 'EG2')
          : (k === 0 ? 'GREEN' : k === 1 ? 'G1' : 'G2');
        await fecharSinal(s, st, k);
        decided = true;
        break;
      }

      if (k < CFG.maxGale && s.galeLevel < k + 1) await notificarGale(s, k + 1);
    }

    if (decided) continue;

    if (!after.length && now - s.criadoEm > CFG.gapMs) {
      await fecharSinal(s, 'LOSS', 0);
    } else if (after.length >= CFG.maxGale + 1) {
      await fecharSinal(s, 'LOSS', CFG.maxGale);
    } else if (now - s.criadoEm >= CFG.apuraMaxMs) {
      await fecharSinal(s, 'LOSS', s.galeLevel || 0);
    }
  }
}

/* ============================ SYNC ADAPTATIVO ============================ */

function currentSyncMs() {
  if (pendentes().length > 0) return CFG.syncFastMs;
  const last = S.hist[S.hist.length - 1];
  if (last && Date.now() - last.t < 45000) return CFG.syncFastMs;
  return CFG.syncMs;
}

async function sync() {
  try {
    const rounds = await fetchRounds();
    let novos = 0, atualizados = 0;
    const byId = new Map(S.hist.map((r) => [r.id, r]));
    for (const r of rounds) {
      const prev = byId.get(r.id);
      if (!prev) {
        novos++;
        byId.set(r.id, r);
        void dbRoundUpsert(r);
      } else if (prev.t !== r.t || prev.resultado !== r.resultado || prev.pj !== r.pj || prev.pb !== r.pb) {
        Object.assign(prev, r);
        atualizados++;
        void dbRoundUpsert(r);
      }
    }
    S.hist = [...byId.values()].sort((a, b) => a.t - b.t || a.id.localeCompare(b.id));
    if (S.hist.length > CFG.maxHist) S.hist = S.hist.slice(-CFG.maxHist);
    S.seen = new Set(S.hist.map((r) => r.id));
    R.ultimaSync = new Date().toISOString();
    const newest = S.hist[S.hist.length - 1];
    if (newest) R.ultimaRodadaNova = new Date(newest.t).toISOString();

    if (newest && Date.now() - newest.t > CFG.staleAlertMs) {
      const mins = Math.floor((Date.now() - newest.t) / 60000);
      await tgAlert('API', `API sem rodadas novas há ${mins} min.\nÚltima: ${R.ultimaRodadaNova}\nFonte: ${R.fonteAPI + 1}/${API_SOURCES.length}`);
    }
    if (novos || atualizados) log('SYNC', { novos, atualizados, total: S.hist.length, fonte: R.fonteAPI });
  } catch (e) {
    R.erroAPI = e.message;
    log('SYNC ERRO', e.message);
    await tgAlert('API', `Erro ao consultar API:\n${e.message}`);
  }
}

let lastSyncAt = 0;
async function tick() {
  if (busy || stopping) return;
  busy = true;
  lastTickMs = Date.now();
  try {
    await checkDailyReset();
    if (Date.now() - lastSyncAt >= currentSyncMs()) {
      await sync();
      lastSyncAt = Date.now();
    }
    await apurar();
    await tentarSinal();
  } catch (e) {
    log('TICK ERRO', e.message);
  } finally {
    busy = false;
  }
}

async function keepAlive() {
  const self = process.env.SELF_URL || process.env.RENDER_EXTERNAL_URL || '';
  if (self) {
    try {
      await axios.get(self.replace(/\/$/, '') + '/health', { timeout: 7000 });
    } catch (_) {}
  }
}

function wakeCheck() {
  if (Date.now() - lastTickMs > 10000) {
    log('WAKE', 'atraso detetado');
    tick().catch(() => {});
  }
}

/* ============================ EXPRESS ============================ */

const app = express();
app.disable('x-powered-by');
app.use(express.json({ limit: '100kb' }));

function requireAdmin(req, res, next) {
  if (!ADMIN_TOKEN) return res.status(503).json({ error: 'ADMIN_TOKEN não configurado' });
  const fromHeader = String(req.get('x-admin-token') || '');
  const fromAuth = String(req.get('authorization') || '').replace(/^Bearer\s+/i, '');
  const supplied = Buffer.from(fromHeader || fromAuth);
  const expected = Buffer.from(ADMIN_TOKEN);
  if (supplied.length !== expected.length || !crypto.timingSafeEqual(supplied, expected)) {
    return res.status(401).json({ error: 'não autorizado' });
  }
  next();
}

app.get('/ping', (_req, res) => res.status(200).type('text/plain').send('ok'));

app.get('/', (_req, res) => {
  res.json({
    name: 'BAC BO Telegram Signal Service',
    version: R.versao,
    running: CFG.ativos,
    feed: R.ultimaSync ? 'connected' : 'waiting'
  });
});

app.get('/health', (_req, res) => {
  const newest = S.hist[S.hist.length - 1];
  const feedFresh = R.ultimaSync
    ? Date.now() - new Date(R.ultimaSync).getTime() < 3 * 60 * 1000
    : false;
  const healthy = feedFresh && !stopping;
  res.status(healthy ? 200 : 503).json({
    ok: healthy,
    versao: R.versao,
    deployId: DEPLOY_ID,
    lock: R.lockOk,
    tier: R.tier,
    fonteAPI: R.fonteAPI + 1,
    fontes: API_SOURCES.length,
    hist: S.hist.length,
    sinais: S.signals.length,
    pendentes: pendentes().length,
    ultimaSync: R.ultimaSync,
    ultimaRodada: newest ? R.ultimaRodadaNova : null,
    apiStale: newest ? Date.now() - newest.t > CFG.staleAlertMs : true,
    erroAPI: R.erroAPI,
    erroDB: R.erroDB,
    uptime: Math.round(process.uptime())
  });
});

app.get('/admin/status', requireAdmin, (_req, res) => {
  res.json({
    running: CFG.ativos,
    maxGale: CFG.maxGale,
    minGapMs: CFG.minGapMs,
    maxPending: CFG.maxPending,
    tier: R.tier,
    motivo: R.ultimoMotivo,
    ultimaSync: R.ultimaSync,
    ultimaRodadaNova: R.ultimaRodadaNova,
    ultimoSinal: R.ultimoSinal ? {
      id: R.ultimoSinal.id,
      cor: R.ultimoSinal.cor,
      conf: R.ultimoSinal.conf,
      key: R.ultimoSinal.key,
      tier: R.ultimoSinal.tier
    } : null,
    sinaisPendentes: pendentes().length,
    diario: { ...DAILY, taxa: DAILY.total > 0 ? (DAILY.green + DAILY.g1 + DAILY.g2) / DAILY.total : null, streak: S.curStreak },
    geral: ALLTIME,
    suspensos: Object.keys(SUSPENDED)
  });
});

app.get('/admin/signals', requireAdmin, (req, res) => {
  const n = Math.min(200, Math.max(1, parseInt(req.query.n, 10) || 30));
  res.json(S.signals.slice(-n).reverse().map((s) => ({
    id: s.id, cor: s.cor, key: s.key, nome: s.nome, conf: s.conf,
    status: s.status, gale: s.gale, tier: s.tier,
    criadoEm: new Date(s.criadoEm).toISOString(),
    fechadoEm: s.fechadoEm ? new Date(s.fechadoEm).toISOString() : null
  })));
});

app.get('/admin/stats', requireAdmin, (_req, res) => {
  const winsD = DAILY.green + DAILY.g1 + DAILY.g2;
  const winsA = ALLTIME.green + ALLTIME.g1 + ALLTIME.g2 + ALLTIME.empate + ALLTIME.eg1 + ALLTIME.eg2;
  res.json({
    diario: { ...DAILY, wins: winsD, streak: S.curStreak },
    geral: { ...ALLTIME, wins: winsA }
  });
});

app.get('/admin/diagnostics', requireAdmin, (_req, res) => {
  const samples = S.hist.slice(-20);
  const now = Date.now();
  res.json({
    feed: {
      fontes: API_SOURCES.length,
      fonteAtual: R.fonteAPI + 1,
      erro: R.erroAPI,
      ultimaSync: R.ultimaSync,
      ultimaRodada: R.ultimaRodadaNova,
      syncMs: currentSyncMs(),
      rounds: samples.map((r) => ({
        id: r.id, resultado: r.resultado,
        t: new Date(r.t).toISOString(),
        idadeMs: now - r.t
      }))
    },
    sinais: {
      pendentes: pendentes().length,
      total: S.signals.length,
      tier: R.tier,
      motivo: R.ultimoMotivo
    },
    telegram: {
      erro: R.erroTelegram,
      alertaThrottleMs: CFG.alertThrottleMs
    },
    db: { erro: R.erroDB, lockOk: R.lockOk },
    env: {
      HISTORY_TZ,
      maxGale: CFG.maxGale,
      gapMs: CFG.gapMs,
      relaxMs: CFG.relaxMs,
      forceSignalMs: CFG.forceSignalMs
    }
  });
});

app.post('/admin/start', requireAdmin, async (_req, res) => {
  CFG.ativos = true;
  await dbStateSet('ativos', 'true');
  res.json({ ok: true, running: true });
});

app.post('/admin/stop', requireAdmin, async (_req, res) => {
  CFG.ativos = false;
  await dbStateSet('ativos', 'false');
  res.json({ ok: true, running: false });
});

app.post('/admin/cancel-pending', requireAdmin, async (_req, res) => {
  const list = [...pendentes()];
  for (const s of list) {
    s.status = 'LOSS';
    s.fechadoEm = Date.now();
    if (s.gale1MsgId) { void tgDelete(s.gale1MsgId); s.gale1MsgId = null; }
    if (s.gale2MsgId) { void tgDelete(s.gale2MsgId); s.gale2MsgId = null; }
    await dbSignalUpdate(s);
  }
  res.json({ ok: true, cancelados: list.length });
});

app.post('/admin/gales', requireAdmin, async (req, res) => {
  const value = Number(req.body?.gales);
  if (!Number.isInteger(value) || value < 0 || value > 3) {
    return res.status(400).json({ error: 'gales deve ser inteiro entre 0 e 3' });
  }
  CFG.maxGale = value;
  await dbStateSet('maxGale', String(value));
  res.json({ ok: true, maxGale: value });
});

app.post('/admin/interval', requireAdmin, async (req, res) => {
  const minutes = Number(req.body?.minutes);
  if (!Number.isFinite(minutes) || minutes < 0 || minutes > 10) {
    return res.status(400).json({ error: 'minutes deve estar entre 0 e 10 (0 = padrão v19 de 20s)' });
  }
  const gapMs = minutes === 0 ? 20000 : Math.round(minutes * 60000);
  CFG.minGapMs = gapMs;
  await dbStateSet('minGapMs', String(gapMs));
  res.json({ ok: true, minGapMs: gapMs, minutes: minutes === 0 ? 'default' : minutes });
});

app.use((err, _req, res, _next) => {
  console.error('[http] Erro:', err.message);
  if (res.headersSent) return;
  res.status(500).json({ error: 'Erro interno' });
});

/* ============================ STARTUP ============================ */

async function start() {
  for (const [name, value] of Object.entries({ BOT_TOKEN, CHAT_ID, ADMIN_TOKEN, DATABASE_URL })) {
    if (!value) throw new Error(`Variável obrigatória em falta: ${name}`);
  }
  if (!/^postgres(?:ql)?:\/\//i.test(DATABASE_URL)) throw new Error('DATABASE_URL inválido');
  if (ADMIN_TOKEN.length < 24) throw new Error('ADMIN_TOKEN deve ter >= 24 caracteres');
  if (!API_SOURCES.length) throw new Error('Nenhuma API configurada');

  log('BOOT', { versao: '4.0', deployId: DEPLOY_ID, fontes: API_SOURCES.length, tz: HISTORY_TZ });

  await dbInit();
  const locked = await acquireLock();
  if (!locked) throw new Error('Outra instância do bot está ativa (advisory lock). Encerrando.');

  await dbLoadState();
  await sync();
  getStats(true);

  // Recuperação de pendentes: reconstrói galeLevel
  for (const s of S.signals) {
    if (s.status !== 'PENDENTE') continue;
    const anchorIndex = s.baseRoundId ? S.hist.findIndex((r) => r.id === s.baseRoundId) : -1;
    const after = anchorIndex >= 0 ? S.hist.slice(anchorIndex + 1)
      : S.hist.filter((r) => r.t > s.criadoEm).sort((a, b) => a.t - b.t || a.id.localeCompare(b.id));
    let consecutivasFalhas = 0, prevT = s.criadoEm;
    for (const r of after) {
      if (r.t - prevT > CFG.gapMs || r.resultado === s.cor || r.resultado === E) break;
      consecutivasFalhas++;
      prevT = r.t;
      if (consecutivasFalhas >= CFG.maxGale) break;
    }
    s.galeLevel = Math.min(consecutivasFalhas, CFG.maxGale);
  }

  server = app.listen(PORT, '0.0.0.0', () => log('HTTP', `porta ${PORT}`));
  server.keepAliveTimeout = 65000;

  tickTimer = setInterval(() => tick().catch(() => {}), 1000);
  keepTimer = setInterval(() => { keepAlive().catch(() => {}); wakeCheck(); }, CFG.keepAliveMs);

  setTimeout(() => tick().catch(() => {}), 1500);

  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}

async function shutdown() {
  if (stopping) return;
  stopping = true;
  log('SHUTDOWN', 'a terminar');
  clearInterval(tickTimer);
  clearInterval(keepTimer);
  setTimeout(() => process.exit(0), 8000).unref();
  if (server) await new Promise((resolve) => { server.close(resolve); setTimeout(resolve, 3000).unref(); });
  await releaseLock();
  if (pool) await pool.end().catch(() => {});
  process.exit(0);
}

process.on('unhandledRejection', (e) => console.error('[process] Rejection:', e?.message || e));
process.on('uncaughtException', (e) => {
  console.error('[process] Exception:', e?.stack || e?.message || e);
  void shutdown();
});

start().catch((e) => {
  console.error('[start] Falha fatal:', e?.stack || e?.message || e);
  process.exit(1);
});