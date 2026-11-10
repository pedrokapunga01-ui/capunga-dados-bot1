'use strict';

/**
 * CAPUNGA DADOS — BAC BO · v18.2 (Produção Endurecida)
 * 100% completo · Anti-race · Fila Telegram · Apuração precisa · Sem delays críticos
 * Env: BOT_TOKEN, CHAT_ID, ADMIN_TOKEN, DATABASE_URL, PORT
 */

const fs = require('fs');
const crypto = require('crypto');
const express = require('express');
const cors = require('cors');
const axios = require('axios');
const { Pool } = require('pg');
const http = require('http');
const https = require('https');

// ====================== CONFIG ======================
const PORT = Number(process.env.PORT || 10000);
const BOT_TOKEN = process.env.BOT_TOKEN || '';
const CHAT_ID = process.env.CHAT_ID || '';
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || '';
const DATABASE_URL = process.env.DATABASE_URL || '';

const CFG = {
  ativos: true,
  maxGale: 2,
  maxPending: 3,
  minAmostra: 32,
  sinalInicial: 1,

  // Timing
  syncIntervalMin: 420,
  syncIntervalMax: 780,
  minGapMs: 21000,
  apuraMaxMs: 235000,
  forceCloseMs: 250000,

  maxHist: 3200,
  maxSignalsMem: 550,
  signalsOnBoot: 350,

  // Frequência
  maxPorHora: 30,
  minPorHora: 25,
  softUrgencyMs: 165000,
  ultraUrgencyMs: 225000,
  maxSemSinalMs: 255000,

  // Scores
  scoreNormal: 80,
  scoreUrgent: 70,
  scoreUltra: 60,
  scoreForced: 47,

  // Confiabilidade
  apiMaxRetries: 3,
  tgMaxRetries: 3,
  keepAliveMs: 50000,
  watchdogMs: 90000
};

const J = 'Jogador';
const B = 'Banca';
const E = 'Empate';
const isCol = (x) => x === J || x === B;
const opp = (c) => (c === J ? B : c === B ? J : null);
const FECHADOS = new Set(['GREEN', 'G1', 'G2', 'EMPATE', 'EG1', 'EG2', 'LOSS']);

const API_URL =
  'https://api-cs.casino.org/svc-evolution-game-events/api/bacbo?page=0&size=50&sort=data.settledAt,desc&duration=120&wheelResults=PlayerWon,BankerWon,Tie';

// ====================== AGENTS ======================
const httpAgent = new http.Agent({ keepAlive: true, maxSockets: 5, keepAliveMsecs: 20000 });
const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: 5, keepAliveMsecs: 20000 });

const apiClient = axios.create({
  timeout: 2800,
  httpAgent,
  httpsAgent,
  headers: { Accept: 'application/json', 'User-Agent': 'CapungaDados/18.2' }
});

const DEPLOY_ID = (() => {
  if (process.env.DEPLOY_ID) return String(process.env.DEPLOY_ID).trim();
  try {
    return crypto.createHash('sha256').update(fs.readFileSync(__filename)).digest('hex').slice(0, 12);
  } catch {
    return 'unknown';
  }
})();

// ====================== STATE ======================
const S = {
  hist: [],
  seen: new Set(),
  signals: [],
  nextId: CFG.sinalInicial,
  lastSignalAt: 0,
  lastRoundTs: 0
};

const STATS = {
  total: 0, green: 0, g1: 0, g2: 0,
  empate: 0, eg1: 0, eg2: 0, loss: 0
};

const ANALYSIS = { msgId: null, enviando: false };
const LOCKS = { sync: false, signal: false, apurar: false };
const TG_QUEUE = [];
let tgWorkerRunning = false;

const R = {
  iniciadoEm: new Date().toISOString(),
  deployId: DEPLOY_ID,
  ultimaSync: null,
  ultimaRodadaNova: null,
  erroAPI: null,
  erroTelegram: null,
  erroDB: null,
  ultimoMotivo: null,
  ultimoSinal: null,
  ultimoThreshold: null,
  apiFails: 0,
  apiBugs: 0,
  lastTickOk: Date.now()
};

let pool = null;
let server = null;
let stopping = false;
let tickTimer = null;
let keepTimer = null;
let watchdogTimer = null;

// ====================== UTILS ======================
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const log = (tag, d) => console.log(`[\( {new Date().toISOString()}] [ \){tag}] ${typeof d === 'string' ? d : JSON.stringify(d)}`);

const fmtHora = new Intl.DateTimeFormat('pt-PT', {
  timeZone: 'Africa/Luanda',
  hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false
});
const hora = (ms = Date.now()) => fmtHora.format(new Date(ms));

const emojiCor = (c) => (c === B ? '🔴' : c === J ? '🔵' : '🟡');
const nomeCor = (c) => (c === B ? 'VERMELHO' : c === J ? 'AZUL' : 'EMPATE');
const barra = (p) => {
  const f = Math.max(0, Math.min(10, Math.round((p / 100) * 10)));
  return '▰'.repeat(f) + '▱'.repeat(10 - f);
};

// ====================== TEMPLATES ======================
function fmtAnalisando() {
  return [
    '╭──────────────────────╮',
    '   🔍  <b>LENDO A MESA</b>',
    '╰──────────────────────╯',
    '',
    '  Padrões em análise…',
    '  Aguarde o próximo sinal.',
    '',
    `  ⏱  <code>${hora()}</code>`
  ].join('\n');
}

function fmtSinal(s) {
  const tipo = s.isForced ? '🆘 Forçado'
    : s.conf >= 91 ? '👑 Elite'
    : s.conf >= 83 ? '💠 Premium'
    : '⚡ Normal';

  return [
    '╭──────────────────────╮',
    `   🎯  <b>SINAL #${s.id}</b>`,
    '╰──────────────────────╯',
    '',
    '  ✅  <b>ENTRADA CONFIRMADA</b>',
    '',
    `  ▸ Cor          \( {emojiCor(s.cor)}  <b> \){nomeCor(s.cor)}</b>`,
    `  ▸ Cobertura    🟡  EMPATE`,
    `  ▸ Gale         🤜  Até 2`,
    `  ▸ Tipo         ${tipo}`,
    '',
    `  📊 Confiança   <b>${s.conf}%</b>`,
    `  ${barra(s.conf)}`,
    '',
    `  ⏱  <code>${hora(s.t)}</code>`,
    '────────────────────────',
    '  🏆  CAPUNGA PREMIUM',
    '────────────────────────'
  ].join('\n');
}

function fmtGale(s, nivel) {
  return [
    '╭──────────────────────╮',
    `   ${nivel === 1 ? '🤜' : '🤜🤜'}  <b>GALE ${nivel}</b>`,
    '╰──────────────────────╯',
    '',
    `  ▸ Manter       \( {emojiCor(s.cor)}  <b> \){nomeCor(s.cor)}</b>`,
    `  ▸ Ação         💰  Dobrar aposta`,
    nivel === 2 ? '  ⚠️  Última chance de gale' : '',
    '',
    `  ⏱  <code>${hora()}</code>`
  ].filter(Boolean).join('\n');
}

function fmtResult(s) {
  const map = {
    GREEN:  { emoji: '✅',     tag: 'GREEN',           txt: 'acertou na 1ª' },
    G1:     { emoji: '✅✅',   tag: 'GREEN NO GALE 1', txt: 'acertou no gale 1' },
    G2:     { emoji: '✅✅✅', tag: 'GREEN NO GALE 2', txt: 'acertou no gale 2' },
    EMPATE: { emoji: '🟡',     tag: 'GREEN NO EMPATE', txt: 'fechou no empate' },
    EG1:    { emoji: '🟡🟡',   tag: 'GREEN EMPATE G1', txt: 'fechou no empate (G1)' },
    EG2:    { emoji: '🟡🟡🟡', tag: 'GREEN EMPATE G2', txt: 'fechou no empate (G2)' },
    LOSS:   { emoji: '❌',     tag: 'LOSS',            txt: 'sem acerto em 3 rodadas' }
  };
  const m = map[s.status] || map.LOSS;
  const isLoss = s.status === 'LOSS';

  return [
    '╭──────────────────────╮',
    `   \( {m.emoji}  <b> \){m.tag}</b>`,
    '╰──────────────────────╯',
    '',
    isLoss ? '  😤  A mesa não obedeceu' : '  💚  A mesa obedeceu',
    `  ▸ ${emojiCor(s.cor)} ${nomeCor(s.cor)} · ${m.txt}`,
    '',
    `  ⏱  <code>${hora(s.fechadoEm || Date.now())}</code>`
  ].join('\n');
}

function fmtHistorico() {
  const wins = STATS.green + STATS.g1 + STATS.g2 + STATS.empate + STATS.eg1 + STATS.eg2;
  const taxa = STATS.total > 0 ? ((wins / STATS.total) * 100).toFixed(1) : '0.0';
  const winsReais = STATS.green + STATS.g1 + STATS.g2;
  const decididos = winsReais + STATS.loss;
  const taxaReal = decididos > 0 ? ((winsReais / decididos) * 100).toFixed(1) : '0.0';
  const streak = getGreenStreak();

  const ln = (l, v) => `  \( {l.padEnd(18)} <b> \){v}</b>`;

  return [
    '╭──────────────────────╮',
    '   📈  <b>HISTÓRICO</b>',
    '╰──────────────────────╯',
    '',
    ln('Total', STATS.total),
    '────────────────────────',
    ln('✅ Green', STATS.green),
    ln('✅✅ G1', STATS.g1),
    ln('✅✅✅ G2', STATS.g2),
    ln('🟡 Empate', STATS.empate),
    ln('🟡🟡 Empate G1', STATS.eg1),
    ln('🟡🟡🟡 Empate G2', STATS.eg2),
    ln('❌ Loss', STATS.loss),
    '────────────────────────',
    ln('🎯 Taxa', `${taxa}%`),
    ln('💡 Real', `${taxaReal}%`),
    ln('🔥 Streak', streak >= 3 ? `${streak} 🔥` : streak)
  ].join('\n');
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

// ====================== FREQUÊNCIA ======================
function sinaisUltimos60Min() {
  const agora = Date.now();
  return S.signals.filter((s) => s.t && agora - s.t < 3600000).length;
}

function calcularThreshold() {
  const agora = Date.now();
  const desde = S.lastSignalAt ? agora - S.lastSignalAt : Infinity;
  const sinaisHora = sinaisUltimos60Min();

  if (sinaisHora >= CFG.maxPorHora) {
    return { threshold: 999, forcar: false, motivo: `cap ${CFG.maxPorHora}/h` };
  }

  let threshold = CFG.scoreNormal;
  let forcar = false;

  if (desde >= CFG.maxSemSinalMs) {
    threshold = CFG.scoreForced;
    forcar = true;
  } else if (desde >= CFG.ultraUrgencyMs) {
    threshold = CFG.scoreUltra;
  } else if (desde >= CFG.softUrgencyMs) {
    threshold = CFG.scoreUrgent;
  } else if (desde >= 90000) {
    threshold = 75;
  }

  // Pace correction
  const inicioHora = agora - (agora % 3600000);
  const minPassados = (agora - inicioHora) / 60000;
  if (minPassados > 10) {
    const sinaisHoraAtual = S.signals.filter((s) => s.t >= inicioHora).length;
    const esperado = (minPassados / 60) * CFG.minPorHora;
    const deficit = esperado - sinaisHoraAtual;

    if (deficit > 3.2) threshold -= 8;
    else if (deficit > 1.6) threshold -= 4;
    else if (sinaisHoraAtual >= 28) threshold += 4;
  }

  threshold = Math.max(45, Math.min(93, threshold));
  return { threshold, forcar, motivo: `≥\( {threshold} ( \){Math.round(desde / 1000)}s)` };
}

// ====================== DATABASE ======================
async function dbInit() {
  if (!DATABASE_URL) {
    log('DB', 'sem persistência (ok)');
    return;
  }
  try {
    pool = new Pool({
      connectionString: DATABASE_URL,
      ssl: /localhost|127\.0\.0\.1/i.test(DATABASE_URL) ? false : { rejectUnauthorized: false },
      max: 3,
      idleTimeoutMillis: 30000
    });
    await pool.query('SELECT 1');
    await pool.query(`
      CREATE TABLE IF NOT EXISTS capunga_state (
        k TEXT PRIMARY KEY,
        v TEXT NOT NULL,
        updated_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS capunga_signals (
        id INTEGER PRIMARY KEY,
        cor TEXT NOT NULL,
        key TEXT NOT NULL,
        nome TEXT NOT NULL,
        tipo TEXT NOT NULL,
        conf INT NOT NULL,
        status TEXT NOT NULL DEFAULT 'PENDENTE',
        gale INT,
        agree INT,
        created_at TIMESTAMPTZ NOT NULL,
        closed_at TIMESTAMPTZ
      );
      CREATE INDEX IF NOT EXISTS idx_cap_status ON capunga_signals(status);
      CREATE INDEX IF NOT EXISTS idx_cap_created ON capunga_signals(created_at DESC);
    `);
    R.erroDB = null;
    log('DB', 'ok');
  } catch (e) {
    R.erroDB = e.message;
    log('DB ERRO', e.message);
    pool = null;
  }
}

async function dbStateGet(k) {
  if (!pool) return null;
  try {
    const r = await pool.query('SELECT v FROM capunga_state WHERE k = $1', [k]);
    return r.rows[0]?.v ?? null;
  } catch (e) {
    R.erroDB = e.message;
    return null;
  }
}

async function dbStateSet(k, v) {
  if (!pool) return;
  try {
    await pool.query(
      `INSERT INTO capunga_state (k, v, updated_at) VALUES ($1, $2, NOW())
       ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v, updated_at = NOW()`,
      [k, String(v)]
    );
  } catch (e) {
    R.erroDB = e.message;
  }
}

async function dbSignalInsert(s) {
  if (!pool) return;
  try {
    await pool.query(
      `INSERT INTO capunga_signals
       (id, cor, key, nome, tipo, conf, status, gale, agree, created_at, closed_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
       ON CONFLICT (id) DO NOTHING`,
      [
        s.id, s.cor, s.key, s.nome, s.tipo, s.conf, s.status,
        s.gale ?? null, s.agree ?? 1,
        new Date(s.t),
        s.fechadoEm ? new Date(s.fechadoEm) : null
      ]
    );
  } catch (e) {
    R.erroDB = e.message;
  }
}

async function dbSignalUpdate(s) {
  if (!pool) return;
  try {
    await pool.query(
      'UPDATE capunga_signals SET status = $1, gale = $2, closed_at = $3 WHERE id = $4',
      [s.status, s.gale ?? null, s.fechadoEm ? new Date(s.fechadoEm) : null, s.id]
    );
  } catch (e) {
    R.erroDB = e.message;
  }
}

async function dbLoadState() {
  if (!pool) return;
  try {
    const nextId = await dbStateGet('nextId');
    const lastSignalAt = await dbStateGet('lastSignalAt');
    if (nextId) S.nextId = Math.max(CFG.sinalInicial, parseInt(nextId, 10) || CFG.sinalInicial);
    if (lastSignalAt) S.lastSignalAt = parseInt(lastSignalAt, 10) || 0;

    const r = await pool.query(
      `SELECT * FROM capunga_signals ORDER BY created_at DESC LIMIT $1`,
      [CFG.signalsOnBoot]
    );

    S.signals = r.rows.reverse().map((row) => ({
      id: row.id,
      cor: row.cor,
      key: row.key,
      nome: row.nome,
      tipo: row.tipo,
      conf: row.conf,
      status: row.status,
      gale: row.gale,
      agree: row.agree,
      t: new Date(row.created_at).getTime(),
      criadoEm: new Date(row.created_at).getTime(),
      fechadoEm: row.closed_at ? new Date(row.closed_at).getTime() : null,
      galeLevel: 0,
      seenRounds: new Set(),
      msgId: null,
      gale1MsgId: null,
      gale2MsgId: null
    }));

    rebuildStats();
    log('DB', `carregado → nextId=\( {S.nextId} | sinais= \){S.signals.length}`);
  } catch (e) {
    R.erroDB = e.message;
    log('DB LOAD ERRO', e.message);
  }
}

function rebuildStats() {
  Object.keys(STATS).forEach((k) => (STATS[k] = 0));
  for (const s of S.signals) {
    if (!FECHADOS.has(s.status)) continue;
    STATS.total++;
    if (s.status === 'GREEN') STATS.green++;
    else if (s.status === 'G1') STATS.g1++;
    else if (s.status === 'G2') STATS.g2++;
    else if (s.status === 'EMPATE') STATS.empate++;
    else if (s.status === 'EG1') STATS.eg1++;
    else if (s.status === 'EG2') STATS.eg2++;
    else if (s.status === 'LOSS') STATS.loss++;
  }
}

// ====================== TELEGRAM (COM FILA) ======================
async function tgSendRaw(texto) {
  if (!BOT_TOKEN || !CHAT_ID) {
    R.erroTelegram = 'TOKEN/CHAT em falta';
    return { ok: false };
  }

  for (let t = 1; t <= CFG.tgMaxRetries; t++) {
    try {
      const r = await axios.post(
        `https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`,
        {
          chat_id: CHAT_ID,
          text: texto,
          parse_mode: 'HTML',
          disable_web_page_preview: true
        },
        { timeout: 5000, httpAgent, httpsAgent }
      );

      if (r.data?.ok) {
        R.erroTelegram = null;
        return { ok: true, messageId: r.data.result.message_id };
      }
      R.erroTelegram = JSON.stringify(r.data);
    } catch (e) {
      R.erroTelegram = e.response?.data ? JSON.stringify(e.response.data) : e.message;
      const retryAfter = Number(e.response?.data?.parameters?.retry_after);
      if (retryAfter > 0 && retryAfter <= 30) {
        await sleep(retryAfter * 1000 + 300);
      } else if (t < CFG.tgMaxRetries) {
        await sleep(400 * t);
      }
    }
  }
  return { ok: false };
}

function tgEnqueue(texto, meta = {}) {
  return new Promise((resolve) => {
    TG_QUEUE.push({ texto, meta, resolve });
    processTgQueue();
  });
}

async function processTgQueue() {
  if (tgWorkerRunning || TG_QUEUE.length === 0) return;
  tgWorkerRunning = true;

  while (TG_QUEUE.length > 0) {
    const item = TG_QUEUE.shift();
    try {
      const result = await tgSendRaw(item.texto);
      item.resolve(result);
    } catch (e) {
      item.resolve({ ok: false });
    }
    // pequena pausa para respeitar rate limit
    if (TG_QUEUE.length > 0) await sleep(320);
  }

  tgWorkerRunning = false;
}

async function tgDelete(messageId) {
  if (!messageId || !BOT_TOKEN || !CHAT_ID) return;
  try {
    await axios.post(
      `https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`,
      { chat_id: CHAT_ID, message_id: messageId },
      { timeout: 3500, httpAgent, httpsAgent }
    );
  } catch (_) {}
}

// ====================== ANALISANDO ======================
async function ensureAnalisando() {
  if (ANALYSIS.msgId || ANALYSIS.enviando) return;
  ANALYSIS.enviando = true;
  try {
    const r = await tgEnqueue(fmtAnalisando());
    if (r.ok) ANALYSIS.msgId = r.messageId;
  } finally {
    ANALYSIS.enviando = false;
  }
}

function hideAnalisando() {
  const id = ANALYSIS.msgId;
  ANALYSIS.msgId = null;
  if (id) tgDelete(id);
}

function manageAnalisando() {
  if (stopping) return;
  const pode = CFG.ativos && pendentes().length < CFG.maxPending;
  const desde = S.lastSignalAt ? Date.now() - S.lastSignalAt : Infinity;
  const mostrar = pode && desde >= CFG.minGapMs;

  if (mostrar) {
    if (!ANALYSIS.msgId && !ANALYSIS.enviando) ensureAnalisando();
  } else if (ANALYSIS.msgId) {
    hideAnalisando();
  }
}

// ====================== API ======================
function normalizeOutcome(v) {
  if (v && typeof v === 'object') v = v.outcome ?? v.resultado ?? v.winner ?? v.name;
  const s = String(v ?? '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .trim()
    .toLowerCase()
    .replace(/[\s_-]+/g, '');

  if (['player', 'jogador', 'playerwin', 'playerwins', 'playerwon', 'blue'].includes(s)) return J;
  if (['banker', 'banca', 'bankerwin', 'bankerwins', 'bankerwon', 'red', 'vermelho'].includes(s)) return B;
  if (['tie', 'draw', 'empate', 'equal', 'push', 'tied'].includes(s)) return E;
  return null;
}

function validMs(v) {
  if (!v) return null;
  const t = new Date(v).getTime();
  return Number.isNaN(t) ? null : t;
}

function extractItems(body) {
  if (Array.isArray(body)) return body;
  if (!body || typeof body !== 'object') return [];
  const candidates = [
    body.data, body.results, body.items, body.resultados, body.rounds, body.events,
    body.data?.results, body.data?.items, body.data?.resultados,
    body.data?.rounds, body.data?.events, body.data?.data
  ];
  return candidates.find(Array.isArray) || [];
}

function normalizeRound(item) {
  const d = item?.data || item?.round || item || {};
  const r = (d.result && typeof d.result === 'object') ? d.result
    : (item?.result && typeof item.result === 'object') ? item.result : {};

  const t = validMs(
    d.settledAt ?? d.endTime ?? d.endedAt ?? d.finishedAt ?? d.fim ??
    item?.settledAt ?? item?.fim
  );

  const raw =
    r.outcome ?? r.resultado ?? r.winner ??
    d.outcome ?? d.resultado ?? d.winner ??
    item?.outcome ?? item?.resultado ?? item?.winner ??
    (typeof d.result === 'string' ? d.result : null) ??
    (typeof item?.result === 'string' ? item.result : null);

  let resultado = normalizeOutcome(raw);
  const id = String(
    d.id ?? d.roundId ?? d.round_id ?? d.eventId ??
    item?.id ?? item?.roundId ?? item?.round_id ?? item?.eventId ?? ''
  ).trim();

  if (!id || !resultado || !t) return null;

  const pj = Number(r.playerDice?.score ?? d.playerDice?.score ?? d.playerScore ?? d.player_score ?? d.pontos_jogador);
  const pb = Number(r.bankerDice?.score ?? d.bankerDice?.score ?? d.bankerScore ?? d.banker_score ?? d.pontos_banca);
  const pjOk = Number.isFinite(pj) ? pj : null;
  const pbOk = Number.isFinite(pb) ? pb : null;

  if (pjOk !== null && pbOk !== null && pjOk === pbOk && resultado !== E) {
    R.apiBugs++;
    resultado = E;
  }

  return { id, resultado, t, pj: pjOk, pb: pbOk };
}

async function fetchRounds() {
  let lastErr = null;

  for (let attempt = 1; attempt <= CFG.apiMaxRetries; attempt++) {
    try {
      const resp = await apiClient.get(API_URL);
      const map = new Map();

      for (const it of extractItems(resp.data)) {
        const rd = normalizeRound(it);
        if (rd) map.set(rd.id, rd);
      }

      R.erroAPI = null;
      R.apiFails = 0;
      return [...map.values()].sort((a, b) => a.t - b.t || (a.id < b.id ? -1 : 1));
    } catch (e) {
      lastErr = e;
      R.erroAPI = e.message;
      R.apiFails++;
      if (attempt < CFG.apiMaxRetries) await sleep(180 * attempt);
    }
  }
  throw lastErr || new Error('API falhou');
}

// ====================== ANÁLISE ======================
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
  if (seq.length < 2) return 0;
  let alt = 0;
  for (let i = 1; i < seq.length; i++) {
    if (isCol(seq[i].resultado) && isCol(seq[i - 1].resultado) && seq[i].resultado !== seq[i - 1].resultado) {
      alt++;
    }
  }
  return alt;
}

function countLast(seq, cor, n = 14) {
  return seq.slice(0, n).filter((r) => r.resultado === cor).length;
}

function analisar(hist) {
  if (hist.length < CFG.minAmostra) return null;

  const recent = hist.slice(-45).reverse();
  const last = recent[0];
  if (!last || !isCol(last.resultado)) return null;

  const st = streak(recent);
  const alt = alternation(recent.slice(0, 12));
  const jCount = countLast(recent, J, 16);
  const bCount = countLast(recent, B, 16);

  let best = null;
  let bestScore = 0;

  // Quebra de streak
  if (st >= 4 && st <= 8) {
    const cor = opp(last.resultado);
    const conf = 67 + Math.min(st * 3.8, 24);
    if (conf > bestScore) {
      bestScore = conf;
      best = { cor, conf: Math.round(conf), key: `break_${st}`, nome: `Quebra ${st}`, tipo: 'break' };
    }
  }

  // Continuação
  if (st >= 2 && st <= 4) {
    const cor = last.resultado;
    const conf = 63 + st * 5.8;
    if (conf > bestScore) {
      bestScore = conf;
      best = { cor, conf: Math.round(conf), key: `cont_${st}`, nome: `Continua ${st}`, tipo: 'cont' };
    }
  }

  // Alternância
  if (alt >= 5) {
    const cor = opp(last.resultado);
    const conf = 69 + Math.min(alt * 2.4, 17);
    if (conf > bestScore) {
      bestScore = conf;
      best = { cor, conf: Math.round(conf), key: `alt_${alt}`, nome: `Alternância ${alt}`, tipo: 'alt' };
    }
  }

  // Imbalance
  const diff = Math.abs(jCount - bCount);
  if (diff >= 5) {
    const cor = jCount > bCount ? B : J;
    const conf = 65 + Math.min(diff * 3.6, 19);
    if (conf > bestScore) {
      bestScore = conf;
      best = { cor, conf: Math.round(conf), key: `imb_${diff}`, nome: `Imbalance ${diff}`, tipo: 'imb' };
    }
  }

  if (!best || best.conf < 47) return null;
  best.conf = Math.max(47, Math.min(95, best.conf));
  return best;
}

// ====================== SINAIS ======================
function pendentes() {
  return S.signals.filter((s) => !FECHADOS.has(s.status));
}

async function emitirSinal(analise, forcar = false) {
  if (LOCKS.signal || !CFG.ativos) return;
  LOCKS.signal = true;

  try {
    const agora = Date.now();
    if (!forcar && agora - S.lastSignalAt < CFG.minGapMs) return;
    if (pendentes().length >= CFG.maxPending) return;

    const id = S.nextId++;
    const s = {
      id,
      cor: analise.cor,
      key: analise.key,
      nome: analise.nome,
      tipo: analise.tipo,
      conf: analise.conf,
      status: 'PENDENTE',
      gale: 0,
      agree: 1,
      t: agora,
      criadoEm: agora,
      fechadoEm: null,
      galeLevel: 0,
      seenRounds: new Set(),
      msgId: null,
      gale1MsgId: null,
      gale2MsgId: null,
      isForced: forcar
    };

    S.signals.push(s);
    if (S.signals.length > CFG.maxSignalsMem) {
      const removed = S.signals.shift();
      // não precisa limpar seenRounds antigo
    }

    S.lastSignalAt = agora;
    await dbStateSet('nextId', S.nextId);
    await dbStateSet('lastSignalAt', S.lastSignalAt);
    await dbSignalInsert(s);

    hideAnalisando();
    const r = await tgEnqueue(fmtSinal(s));
    if (r.ok) s.msgId = r.messageId;

    R.ultimoSinal = { id: s.id, cor: s.cor, conf: s.conf, t: agora };
    R.ultimoMotivo = analise.key;
    log('SINAL', { id: s.id, cor: s.cor, conf: s.conf, key: s.key, forced: forcar });
  } finally {
    LOCKS.signal = false;
  }
}

// ====================== APURAÇÃO ======================
async function apurar() {
  if (LOCKS.apurar) return;
  LOCKS.apurar = true;

  try {
    const agora = Date.now();
    const pend = pendentes();

    for (const s of pend) {
      // timeout forçado
      if (agora - s.t > CFG.forceCloseMs) {
        s.status = 'LOSS';
        s.fechadoEm = agora;
        await fecharSinal(s);
        continue;
      }

      // pegar rodadas depois do sinal que ainda não foram vistas por este sinal
      const candidatas = S.hist.filter((r) => r.t > s.t && !s.seenRounds.has(r.id));
      if (!candidatas.length) continue;

      // processar em ordem
      for (const r of candidatas) {
        s.seenRounds.add(r.id);

        if (r.resultado === s.cor) {
          if (s.galeLevel === 0) s.status = 'GREEN';
          else if (s.galeLevel === 1) s.status = 'G1';
          else s.status = 'G2';
          s.gale = s.galeLevel;
          s.fechadoEm = r.t;
          await fecharSinal(s);
          break;
        }

        if (r.resultado === E) {
          if (s.galeLevel === 0) s.status = 'EMPATE';
          else if (s.galeLevel === 1) s.status = 'EG1';
          else s.status = 'EG2';
          s.gale = s.galeLevel;
          s.fechadoEm = r.t;
          await fecharSinal(s);
          break;
        }

        // errou → gale
        if (s.galeLevel === 0) {
          s.galeLevel = 1;
          s.status = 'GALE1';
          const gr = await tgEnqueue(fmtGale(s, 1));
          if (gr.ok) s.gale1MsgId = gr.messageId;
        } else if (s.galeLevel === 1) {
          s.galeLevel = 2;
          s.status = 'GALE2';
          const gr = await tgEnqueue(fmtGale(s, 2));
          if (gr.ok) s.gale2MsgId = gr.messageId;
        } else {
          // já estava no gale 2 e errou de novo
          s.status = 'LOSS';
          s.fechadoEm = r.t;
          await fecharSinal(s);
          break;
        }
      }
    }
  } finally {
    LOCKS.apurar = false;
  }
}

async function fecharSinal(s) {
  await dbSignalUpdate(s);
  rebuildStats();

  if (s.gale1MsgId) tgDelete(s.gale1MsgId);
  if (s.gale2MsgId) tgDelete(s.gale2MsgId);

  await tgEnqueue(fmtResult(s));
  log('FECHOU', { id: s.id, status: s.status, gale: s.gale });
}

// ====================== TICK PRINCIPAL ======================
async function tick() {
  if (stopping || LOCKS.sync) return;
  LOCKS.sync = true;

  try {
    const rounds = await fetchRounds();
    R.ultimaSync = new Date().toISOString();
    R.lastTickOk = Date.now();

    let novas = 0;
    for (const r of rounds) {
      if (S.seen.has(r.id)) continue;
      S.seen.add(r.id);
      S.hist.push(r);
      novas++;
      if (r.t > S.lastRoundTs) S.lastRoundTs = r.t;
    }

    if (S.hist.length > CFG.maxHist) {
      const excesso = S.hist.length - CFG.maxHist;
      const removed = S.hist.splice(0, excesso);
      for (const r of removed) S.seen.delete(r.id);
    }

    if (novas > 0) {
      R.ultimaRodadaNova = new Date().toISOString();
      log('SYNC', `+\( {novas} | hist= \){S.hist.length}`);
    }

    // Apurar primeiro (prioridade)
    await apurar();

    // Depois tentar novo sinal
    if (CFG.ativos && pendentes().length < CFG.maxPending) {
      const { threshold, forcar, motivo } = calcularThreshold();
      R.ultimoThreshold = threshold;

      const analise = analisar(S.hist);
      if (analise && (analise.conf >= threshold || forcar)) {
        await emitirSinal(analise, forcar);
      } else {
        R.ultimoMotivo = analise ? `baixo \( {analise.conf}< \){threshold}` : 'sem padrão';
      }
    }

    manageAnalisando();
  } catch (e) {
    R.erroAPI = e.message;
    log('TICK ERRO', e.message);
  } finally {
    LOCKS.sync = false;
  }
}

// ====================== EXPRESS ======================
const app = express();
app.use(cors());
app.use(express.json({ limit: '32kb' }));

function authAdmin(req, res, next) {
  const token = req.headers['x-admin-token'] || req.query.token;
  if (!ADMIN_TOKEN || token !== ADMIN_TOKEN) {
    return res.status(401).json({ ok: false, error: 'unauthorized' });
  }
  next();
}

app.get('/', (req, res) => {
  res.json({
    service: 'Capunga Dados Bac Bo',
    version: '18.2',
    deployId: DEPLOY_ID,
    uptime: Math.floor(process.uptime()),
    ativos: CFG.ativos
  });
});

app.get('/status', (req, res) => {
  res.json({
    ok: true,
    version: '18.2',
    deployId: DEPLOY_ID,
    iniciadoEm: R.iniciadoEm,
    ultimaSync: R.ultimaSync,
    ultimaRodadaNova: R.ultimaRodadaNova,
    hist: S.hist.length,
    signals: S.signals.length,
    pendentes: pendentes().length,
    nextId: S.nextId,
    lastSignalAt: S.lastSignalAt,
    stats: STATS,
    erros: { api: R.erroAPI, telegram: R.erroTelegram, db: R.erroDB },
    ultimo: {
      sinal: R.ultimoSinal,
      motivo: R.ultimoMotivo,
      threshold: R.ultimoThreshold
    },
    apiFails: R.apiFails,
    apiBugs: R.apiBugs,
    queueTg: TG_QUEUE.length
  });
});

app.get('/historico', (req, res) => {
  const txt = fmtHistorico()
    .replace(/<\/?b>/g, '')
    .replace(/<\/?code>/g, '');
  res.type('text').send(txt);
});

app.post('/admin/toggle', authAdmin, (req, res) => {
  CFG.ativos = !CFG.ativos;
  log('ADMIN', `ativos = ${CFG.ativos}`);
  res.json({ ok: true, ativos: CFG.ativos });
});

app.post('/admin/force', authAdmin, async (req, res) => {
  const a = analisar(S.hist);
  const th = calcularThreshold();
  res.json({ ok: true, analise: a, threshold: th });
});

app.get('/admin/signals', authAdmin, (req, res) => {
  res.json({
    ok: true,
    signals: S.signals.slice(-40).reverse().map((s) => ({
      id: s.id, cor: s.cor, conf: s.conf, status: s.status,
      gale: s.gale, t: s.t, key: s.key
    }))
  });
});

// ====================== BOOT + WATCHDOG ======================
function scheduleNextTick() {
  if (stopping) return;
  // intervalo adaptativo: mais rápido quando há atividade recente
  const base = Date.now() - S.lastRoundTs < 25000 ? CFG.syncIntervalMin : CFG.syncIntervalMax;
  const jitter = Math.floor(Math.random() * 80);
  tickTimer = setTimeout(async () => {
    await tick();
    scheduleNextTick();
  }, base + jitter);
}

async function start() {
  log('BOOT', `v18.2 | ${DEPLOY_ID}`);

  await dbInit();
  await dbLoadState();

  server = app.listen(PORT, () => log('HTTP', `:${PORT}`));

  // primeiro tick imediato
  await tick();
  scheduleNextTick();

  keepTimer = setInterval(() => {
    if (!stopping) {
      log('KEEP', `hist=\( {S.hist.length} pend= \){pendentes().length} next=\( {S.nextId} q= \){TG_QUEUE.length}`);
    }
  }, CFG.keepAliveMs);

  // Watchdog: se o tick parar de funcionar, força recovery
  watchdogTimer = setInterval(() => {
    if (stopping) return;
    const diff = Date.now() - R.lastTickOk;
    if (diff > CFG.watchdogMs) {
      log('WATCHDOG', `tick parado há ${Math.round(diff / 1000)}s — forçando recovery`);
      LOCKS.sync = false;
      LOCKS.apurar = false;
      LOCKS.signal = false;
      tick().catch(() => {});
    }
  }, 30000);

  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}

async function shutdown() {
  if (stopping) return;
  stopping = true;
  log('SHUTDOWN', 'iniciando…');

  clearTimeout(tickTimer);
  clearInterval(keepTimer);
  clearInterval(watchdogTimer);
  hideAnalisando();

  // espera a fila de telegram esvaziar (máx 8s)
  const deadline = Date.now() + 8000;
  while (TG_QUEUE.length > 0 && Date.now() < deadline) {
    await sleep(200);
  }

  if (server) await new Promise((r) => server.close(r));
  if (pool) await pool.end().catch(() => {});
  log('SHUTDOWN', 'concluído');
  process.exit(0);
}

start().catch((e) => {
  console.error('BOOT FATAL:', e);
  process.exit(1);
});