'use strict';

/**
 * CAPUNGA DADOS — BAC BO · v20 — "NO GALE"
 * Acertou na 1ª rodada = GREEN. EMPATE = proteção (green). Falhou = RED. Fim.
 * Lógica dominante: só dispara com CONFLUÊNCIA — >= 2 estratégias fortes na
 * mesma cor, ou 1 estratégia ELITE com backtest consolidado (n>=15, >=90%).
 * Gate único e duro: conf >= 90%, regime compatível, amostra mínima,
 * suspensão persistente após 2 REDs seguidos da mesma estratégia.
 * Sync adaptativo 1s com sinal pendente. Placar zera 00h00 Angola (UTC+1).
 * Env: BOT_TOKEN, CHAT_ID, ADMIN_TOKEN, DATABASE_URL, PORT, API_BACBO (opc.), API_BACBO_2 (opc.)
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
  maxPending: 1,
  minAmostra: 80,          // exige histórico maior que v19 — recusa mesa fria
  syncMs: 3000,
  syncFastMs: 1000,
  minGapMs: 15000,
  minSettledAgeMs: 5000,
  apuraMaxMs: 600000,
  maxHist: 4000,
  gapMs: 7 * 60 * 1000,
  maxSignalsMem: 500,
  signalsOnBoot: 500,
  suspendMs: 4 * 3600 * 1000,
  suspendLossStreak: 2,    // sem gale, 2 REDs seguidos já congelam a estratégia
  staleAlertMs: 3 * 60 * 1000,
  alertThrottleMs: 10 * 60 * 1000,
  keepAliveMs: 60000,
  // Gate único (sem escada de relaxamento — ou passa no rigor total, ou não sai sinal)
  scoreMin: 90,
  minAgree: 2,             // mínimo de estratégias DISTINTAS concordando na cor
  minStratN: 10,           // amostras (backtest+live) mínimas por estratégia
  liveMin: 0.88,
  btMinN: 8,               // backtest mínimo por estratégia
  btMinRate: 0.88,
  // Caminho ELITE: 1 estratégia sozinha pode disparar, mas exige histórico pesado
  eliteN: 15,
  eliteRate: 0.90
};

const ELITE = new Set(['NUM_ESCADA', 'NUM_DIFF4x3', 'NUM_DOUBLE_HIGH', 'NUM_GATILHO_9']);

const J = 'Jogador';
const B = 'Banca';
const E = 'Empate';
const isCol = (x) => x === J || x === B;
const FECHADOS = new Set(['GREEN', 'EMPATE', 'RED']);
const LOCK_KEY = 727273; // chave nova: v20 nunca disputa lock com v19

const API_SOURCES = [
  process.env.API_BACBO ||
    'https://api-cs.casino.org/svc-evolution-game-events/api/bacbo?page=0&size=50&sort=data.settledAt,desc&duration=120&wheelResults=PlayerWon,BankerWon,Tie',
  process.env.API_BACBO_2 || ''
].filter(Boolean);

const DEPLOY_ID = (() => {
  if (process.env.DEPLOY_ID) return String(process.env.DEPLOY_ID).trim();
  try { return crypto.createHash('sha256').update(fs.readFileSync(__filename)).digest('hex').slice(0, 16); }
  catch { return 'unknown'; }
})();

const S = {
  hist: [], seen: new Set(), signals: [],
  nextId: 1, lastSignalAt: 0, curStreak: 0,
  stats: null, statsAtLen: -1
};

// Placar DIÁRIO (zera 00h00 Angola = UTC+1)
const DAILY = { date: '', total: 0, green: 0, empate: 0, red: 0, bestStreak: 0 };
// Placar GERAL (persistente)
const ALLTIME = { total: 0, green: 0, empate: 0, red: 0 };
const SUSPENDED = {};

const R = {
  iniciadoEm: new Date().toISOString(), deployId: DEPLOY_ID, versao: '20.0',
  ultimaSync: null, ultimaRodadaNova: null, fonteAPI: 0,
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
const luandaDayKey = (ms = Date.now()) => new Date(ms + 3600000).toISOString().slice(0, 10);

const nomeCor = (c) => (c === B ? 'VERMELHO 🔴' : c === J ? 'AZUL 🔵' : 'EMPATE 🟡');
const barra = (p) => {
  const f = Math.max(0, Math.min(10, Math.round((p / 100) * 10)));
  return '▰'.repeat(f) + '▱'.repeat(10 - f);
};

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
        agree INT, created_at TIMESTAMPTZ NOT NULL, closed_at TIMESTAMPTZ
      );
      CREATE INDEX IF NOT EXISTS idx_cap_sig_status ON capunga_signals(status);
      CREATE INDEX IF NOT EXISTS idx_cap_sig_created ON capunga_signals(created_at DESC);
    `);
    R.erroDB = null; log('DB', 'schema v20 ok');
  } catch (e) { R.erroDB = e.message; log('DB ERRO', e.message); pool = null; }
}

async function acquireLock() {
  if (!pool) return false;
  for (let i = 1; i <= 10; i++) {
    try {
      const r = await pool.query('SELECT pg_try_advisory_lock($1) AS ok', [LOCK_KEY]);
      if (r.rows[0].ok) { R.lockOk = true; return true; }
      log('LOCK', `instância concorrente detectada — tentativa ${i}/10`);
    } catch (e) { R.erroDB = e.message; log('LOCK ERRO', e.message); }
    await sleep(15000);
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
  } catch (e) { R.erroDB = e.message; }
}
async function dbSignalInsert(s) {
  if (!pool) return;
  try {
    await pool.query(`INSERT INTO capunga_signals (id,cor,key,nome,tipo,conf,status,agree,created_at,closed_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT (id) DO NOTHING`,
      [s.id, s.cor, s.key, s.nome, s.tipo, s.conf, s.status, s.agree ?? 1,
       new Date(s.t), s.fechadoEm ? new Date(s.fechadoEm) : null]);
  } catch (e) { R.erroDB = e.message; }
}
async function dbSignalUpdate(s) {
  if (!pool) return;
  try {
    await pool.query('UPDATE capunga_signals SET status=$1, closed_at=$2 WHERE id=$3',
      [s.status, s.fechadoEm ? new Date(s.fechadoEm) : null, s.id]);
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

async function dbLoadAlltime() {
  if (!pool) return;
  try {
    const r = await pool.query(`SELECT status, COUNT(*)::int AS n FROM capunga_signals
      WHERE status <> 'PENDENTE' GROUP BY status`);
    for (const k of Object.keys(ALLTIME)) ALLTIME[k] = 0;
    for (const row of r.rows) {
      ALLTIME.total += row.n;
      if (row.status === 'GREEN') ALLTIME.green = row.n;
      else if (row.status === 'EMPATE') ALLTIME.empate = row.n;
      else if (row.status === 'RED') ALLTIME.red = row.n;
    }
  } catch (e) { R.erroDB = e.message; }
}

async function dbLoadState() {
  if (!pool) return;
  try {
    const nextId = await dbStateGet('nextId_v20');
    const lastSignalAt = await dbStateGet('lastSignalAt_v20');
    const curStreak = await dbStateGet('curStreak_v20');
    if (nextId) S.nextId = Math.max(1, parseInt(nextId, 10));
    if (lastSignalAt) S.lastSignalAt = parseInt(lastSignalAt, 10);
    if (curStreak) S.curStreak = parseInt(curStreak, 10);

    try {
      const raw = await dbStateGet('suspended_v20');
      if (raw) {
        const arr = JSON.parse(raw);
        const now = Date.now();
        for (const [key, until] of arr) if (until > now) SUSPENDED[key] = until;
      }
    } catch (_) {}

    try {
      const rawD = await dbStateGet('daily_v20');
      if (rawD) Object.assign(DAILY, JSON.parse(rawD));
    } catch (_) {}
    if (!DAILY.date) DAILY.date = luandaDayKey();

    const rounds = await pool.query(`SELECT id,resultado,t,pj,pb FROM capunga_rounds ORDER BY t DESC LIMIT ${CFG.maxHist}`);
    S.hist = rounds.rows.map((r) => ({ id: r.id, resultado: r.resultado, t: Number(r.t), pj: r.pj === null ? null : Number(r.pj), pb: r.pb === null ? null : Number(r.pb) })).reverse();
    S.seen = new Set(S.hist.map((r) => r.id));

    // Recupera apenas sinais v20 (agree existe; sinais antigos v19 têm coluna extra ignorada)
    const r = await pool.query(`SELECT id,cor,key,nome,tipo,conf,status,agree,created_at,closed_at
      FROM capunga_signals WHERE agree IS NOT NULL ORDER BY created_at DESC LIMIT ${CFG.signalsOnBoot}`);
    S.signals = r.rows.reverse().map((row) => ({
      id: row.id, cor: row.cor, key: row.key, nome: row.nome, tipo: row.tipo,
      conf: row.conf, status: row.status, agree: row.agree || 1,
      t: new Date(row.created_at).getTime(), criadoEm: new Date(row.created_at).getTime(),
      fechadoEm: row.closed_at ? new Date(row.closed_at).getTime() : null
    }));
    await dbLoadAlltime();
    log('DB', `estado v20 carregado: nextId=${S.nextId}, sinais=${S.signals.length}, diário=${DAILY.date}`);
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
  if (tag === 'API' && now - R.ultimoAlertaAPI < CFG.alertThrottleMs) return;
  if (tag === 'DB' && now - R.ultimoAlertaDB < CFG.alertThrottleMs) return;
  if (tag === 'API') R.ultimoAlertaAPI = now;
  if (tag === 'DB') R.ultimoAlertaDB = now;
  await tgSendRaw(`⚠️ <b>ALERTA ${tag}</b>\n${detalhe}\n⏱ ${hora()}`);
}

/* ============================ RESET DIÁRIO (00h00 ANGOLA) ============================ */

function dailyWins() { return DAILY.green + DAILY.empate; }
function dailyTaxa() { return DAILY.total > 0 ? (dailyWins() / DAILY.total) * 100 : 0; }
// Taxa REAL: empate devolve a aposta — conta separado
function dailyTaxaReal() {
  const decididos = DAILY.green + DAILY.red;
  return decididos > 0 ? (DAILY.green / decididos) * 100 : 0;
}

async function sendDailySummary(ontem) {
  await tgSendRaw([
    `🕛 <b>FIM DO DIA ${ontem}</b> 🕛`,
    '',
    `📊 Sinais: <b>${DAILY.total}</b>`,
    `✅ Greens de 1ª: ${DAILY.green}`,
    `🟡 Empates (proteção): ${DAILY.empate}`,
    `❌ Reds: ${DAILY.red}`,
    `🔥 Maior sequência: ${DAILY.bestStreak}`,
    `🎯 Taxa: ${dailyTaxa().toFixed(1)}% (real: ${dailyTaxaReal().toFixed(1)}%)`,
    '',
    `🌅 <b>PLACAR ZERADO — BOM DIA!</b>`
  ].join('\n'));
}

async function checkDailyReset() {
  const hoje = luandaDayKey();
  if (DAILY.date && DAILY.date !== hoje) {
    const ontem = DAILY.date;
    await sendDailySummary(ontem);
    for (const k of ['total', 'green', 'empate', 'red', 'bestStreak']) DAILY[k] = 0;
    DAILY.date = hoje;
    await dbStateSet('daily_v20', JSON.stringify(DAILY));
    log('DIÁRIO', `reset de ${ontem} -> ${hoje}`);
  } else if (!DAILY.date) {
    DAILY.date = hoje;
  }
}

/* ============================ TEMPLATES v20 ============================ */

function fmtAnalisando() {
  return [`⚙️ <b>CAPUNGA v20 — Caçando confluência…</b>`, `⏱ ${hora()}`].join('\n');
}

function fmtSinal(s) {
  const confEmoji = s.conf >= 95 ? '💎' : s.conf >= 92 ? '🔥' : '⚡';
  return [
    `🎲 ═══ <b>CAPUNGA PRIME v20</b> ═══ 🎲`,
    '',
    `🚨 <b>ENTRADA CONFIRMADA #${s.id}</b>`,
    '',
    `🎯 Entrar: <b>${nomeCor(s.cor)}</b>`,
    `🛡 Proteção: EMPATE 🟡`,
    `📊 Força: ${confEmoji} <b>${s.conf}%</b> ${barra(s.conf)}`,
    `🧠 ${s.emoji || '🧩'} ${s.nome}`,
    s.agree > 1 ? `🤝 Confluência: <b>${s.agree} leituras</b> na mesma cor` : `👑 Estratégia elite (backtest pesado)`,
    `⏰ ${hora(s.t)}`,
    '',
    `<i>Entrada única, sem gale. Não bateu = RED. Gestão de banca sempre.</i>`
  ].join('\n');
}

function fmtResult(s) {
  const placar = `📅 Dia: ${DAILY.green}G • ${DAILY.empate}E • ${DAILY.red}R • ${dailyTaxa().toFixed(0)}%`;
  const msg = {
    GREEN:  `✅ <b>GREEN #${s.id}</b> ✅\n\nPagou de primeira tropa 💚\n${placar}`,
    EMPATE: `🟡 <b>EMPATE #${s.id}</b> 🟡\n\nProteção segurou a entrada — stake devolvida 🛡\n${placar}`,
    RED:    `❌ <b>RED #${s.id}</b> ❌\n\nNão foi dessa. Stop limpo, sem martingale 🧠\n${placar}`
  };
  return msg[s.status] || msg.RED;
}

function fmtHistorico() {
  const streak = S.curStreak;
  const streakTxt = streak >= 3 ? `${streak} 🔥` : `${streak}`;
  return [
    `📊 <b>PLACAR DO DIA — ${DAILY.date}</b>`,
    '━━━━━━━━━━━━━━━',
    `Sinais: ${DAILY.total}`,
    `✅ Greens: ${DAILY.green} | 🟡 Empates: ${DAILY.empate} | ❌ Reds: ${DAILY.red}`,
    `🔥 Sequência atual: ${streakTxt} | Recorde: ${DAILY.bestStreak}`,
    `🎯 Taxa do dia: ${dailyTaxa().toFixed(1)}% (real: ${dailyTaxaReal().toFixed(1)}%)`
  ].join('\n');
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

async function fetchFrom(url) {
  const resp = await axios.get(url, {
    timeout: 10000,
    headers: { Accept: 'application/json', 'User-Agent': 'CapungaDados/20.0' }
  });
  const map = new Map();
  for (const it of extractItems(resp.data)) {
    const rd = normalizeRound(it);
    if (rd) map.set(rd.id, rd);
  }
  return [...map.values()].sort((a, b) => a.t - b.t || (a.id < b.id ? -1 : 1));
}

async function fetchRounds() {
  const erros = [];
  for (let idx = 0; idx < API_SOURCES.length; idx++) {
    for (let tentativa = 1; tentativa <= 2; tentativa++) {
      try {
        const rounds = await fetchFrom(API_SOURCES[idx]);
        R.fonteAPI = idx;
        R.erroAPI = null;
        return rounds;
      } catch (e) {
        erros.push(`fonte${idx + 1}: ${e.message}`);
        if (tentativa < 2) await sleep(500);
      }
    }
  }
  R.erroAPI = erros.join(' | ');
  throw new Error(R.erroAPI);
}

/* ============================ HELPERS ============================ */

const hasScore = (r) => r && Number.isFinite(r.pj) && Number.isFinite(r.pb);
const margem = (r) => Math.abs((r.pj || 0) - (r.pb || 0));
const scoreOf = (r) => (r.resultado === J ? r.pj : r.pb);
function streak(seq) {
  if (!seq.length) return 0;
  let n = 0; const cor = seq[0].resultado;
  for (const r of seq) { if (r.resultado === cor) n++; else break; }
  return n;
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
  if (cand.key === 'SEQ_STRONG_STREAK' || cand.key === 'BANKER_BIAS_20') return regime.trend >= 0.5;
  return true;
}

/* ============================ ESTRATÉGIAS v20 — só 1ª batida ============================ */
/* Cortadas TODAS as estratégias de "esperança no gale" (quebras, surfs, ping-pong,
   viradas). O que sobrou: padrões de CONTINUAÇÃO/FORÇA que tendem a pagar na
   rodada seguinte, e o pós-empate. */

const STRATS = [
  // Sequência forte: só segue dragão com margens reais (nunca aposta contra)
  function seqStrongStreak({ seq }) {
    const n = streak(seq);
    if (n < 4 || n > 6) return null;
    const same = seq.slice(0, n);
    if (!same.every((r) => hasScore(r) && margem(r) >= 3)) return null;
    return { cor: seq[0].resultado, key: 'SEQ_STRONG_STREAK', nome: `Dragon ${n} (margem forte)`, emoji: '🐉', strength: 90, tipo: 'seq' };
  },
  // Viés estatístico do bloco recente
  function bias20({ seq }) {
    const s = seq.slice(0, 20);
    if (s.length < 15) return null;
    const b = s.filter((r) => r.resultado === B).length;
    const j = s.filter((r) => r.resultado === J).length;
    if (b >= 13) return { cor: B, key: 'BIAS_20', nome: `Viés Banca ${b}/20`, emoji: '📉', strength: 86, tipo: 'seq' };
    if (j >= 13) return { cor: J, key: 'BIAS_20', nome: `Viés Jogador ${j}/20`, emoji: '📈', strength: 86, tipo: 'seq' };
    return null;
  },
  // Numéricas de força
  function numEscada({ seq }) {
    if (seq.length < 3 || !seq.slice(0, 3).every(hasScore)) return null;
    const [a, b, c] = seq;
    if (a.resultado !== b.resultado || b.resultado !== c.resultado) return null;
    const sa = scoreOf(a), sb = scoreOf(b), sc = scoreOf(c);
    if ((sb - sa === 1 && sc - sb === 1) || (sa - sb === 1 && sb - sc === 1)) {
      return { cor: a.resultado, key: 'NUM_ESCADA', nome: `Escada ${sa}→${sb}→${sc}`, emoji: '🪜', strength: 92, tipo: 'num' };
    }
    return null;
  },
  function numRepeteScore({ seq }) {
    if (seq.length < 2 || !hasScore(seq[0]) || !hasScore(seq[1])) return null;
    const [a, b] = seq;
    if (a.resultado !== b.resultado) return null;
    if (scoreOf(a) === scoreOf(b)) return { cor: a.resultado, key: 'NUM_REPETE', nome: `Score repetido ${scoreOf(a)}`, emoji: '🔂', strength: 88, tipo: 'num' };
    return null;
  },
  function numGatilho9({ seq }) {
    if (!seq.length || !hasScore(seq[0])) return null;
    if (scoreOf(seq[0]) === 9) return { cor: seq[0].resultado, key: 'NUM_GATILHO_9', nome: 'Gatilho 9 (dominante)', emoji: '🎯', strength: 93, tipo: 'num' };
    return null;
  },
  function numDiff4x3({ seq }) {
    if (seq.length < 3) return null;
    const t = seq.slice(0, 3);
    if (!t.every((r) => hasScore(r) && margem(r) >= 4 && r.resultado === t[0].resultado)) return null;
    return { cor: t[0].resultado, key: 'NUM_DIFF4x3', nome: 'Diff ≥4 ×3 seguidas', emoji: '🔢', strength: 91, tipo: 'num' };
  },
  function numDoubleHigh({ seq }) {
    if (seq.length < 2 || !seq.slice(0, 2).every(hasScore)) return null;
    const [a, b] = seq;
    if (a.resultado !== b.resultado) return null;
    if (margem(a) >= 5 && margem(b) >= 5) {
      return { cor: a.resultado, key: 'NUM_DOUBLE_HIGH', nome: 'Dupla margem alta', emoji: '💪', strength: 90, tipo: 'num' };
    }
    return null;
  },
  function numScoreTrend3({ seq }) {
    if (seq.length < 3 || !seq.slice(0, 3).every(hasScore)) return null;
    const [a, b, c] = seq;
    if (a.resultado !== b.resultado || b.resultado !== c.resultado) return null;
    const sa = scoreOf(a), sb = scoreOf(b), sc = scoreOf(c);
    if (sa < sb && sb < sc) {
      return { cor: a.resultado, key: 'NUM_SCORE_TREND_3', nome: `Escala ascendente ${sa}→${sc}`, emoji: '📊', strength: 87, tipo: 'num' };
    }
    return null;
  },
  function numTendencia({ rows }) {
    const s = rows.slice(0, 8).filter(hasScore);
    if (s.length < 6) return null;
    const mj = s.reduce((a, r) => a + r.pj, 0) / s.length;
    const mb = s.reduce((a, r) => a + r.pb, 0) / s.length;
    const diff = mj - mb;
    if (Math.abs(diff) < 2.2) return null;
    return {
      cor: diff > 0 ? J : B, key: 'NUM_TENDENCIA',
      nome: `Tendência ${mj.toFixed(1)}×${mb.toFixed(1)}`, emoji: '📈',
      strength: 85, tipo: 'num'
    };
  },
  // Pós-empate: a cor que vinha antes do empate tende a retomar
  function numPosEmpate({ rows, seq }) {
    if (!rows[0] || rows[0].resultado !== E || !seq.length) return null;
    return { cor: seq[0].resultado, key: 'NUM_POS_EMPATE', nome: 'Retomada pós-empate', emoji: '🎯', strength: 84, tipo: 'num' };
  }
];

function runStrategies(rows) {
  const seq = rows.filter((r) => isCol(r.resultado));
  const ctx = { rows, seq };
  const out = [];
  for (const fn of STRATS) {
    try { const r = fn(ctx); if (r && isCol(r.cor)) out.push(r); } catch (_) {}
  }
  return out;
}

/* ============================ BACKTEST (1 rodada apenas) ============================ */

function windowAt(H, i) {
  const rows = [];
  for (let j = i; j >= 0 && rows.length < 20; j--) {
    if (rows.length && rows[rows.length - 1].t - H[j].t > CFG.gapMs) break;
    rows.push(H[j]);
  }
  return rows;
}
function evalEntry(H, i, cor) {
  const idx = i + 1;
  if (idx >= H.length) return null;
  if (H[idx].t - H[i].t > CFG.gapMs) return false; // gap = entrada expirou
  return H[idx].resultado === cor || H[idx].resultado === E;
}
function backtest() {
  const H = S.hist;
  const per = {};
  const ini = 10, fim = H.length - 1;
  if (fim - ini < 30) return { per, baseRate: 0.92 };
  let bw = 0, bn = 0;
  for (let i = ini; i < fim; i++) {
    const win = windowAt(H, i);
    if (win.length < 6) continue;
    for (const c of runStrategies(win)) {
      const v = evalEntry(H, i, c.cor);
      if (v === null) continue;
      const p = per[c.key] || (per[c.key] = { n: 0, w: 0 });
      p.n++; if (v) p.w++;
      bn++; if (v) bw++;
    }
  }
  const baseRate = bn ? bw / bn : 0.92;
  return { per, baseRate: Math.min(0.96, Math.max(0.85, baseRate)) };
}
function getStats(force = false) {
  if (force || !S.stats || S.hist.length - S.statsAtLen >= 30) {
    S.stats = backtest();
    S.statsAtLen = S.hist.length;
  }
  return S.stats;
}

/* ============================ CONFIANÇA ============================ */

function bayesianRate(w, n, priorRate = 0.92, priorWeight = 10) {
  return (w + priorRate * priorWeight) / (n + priorWeight);
}
function confianca(cand, agree, regime) {
  const stats = S.stats;
  const p = stats.per[cand.key] || { n: 0, w: 0 };
  const baseRate = stats.baseRate || 0.92;
  const btRate = bayesianRate(p.w, p.n, baseRate, 6);
  const live = S.signals.filter((x) => x.key === cand.key && FECHADOS.has(x.status));
  const liveW = live.filter((x) => x.status !== 'RED').length;
  const liveRate = live.length >= 3 ? bayesianRate(liveW, live.length, btRate, 4) : null;
  const btSamples = p.n + 6;
  const liveSamples = live.length >= 3 ? live.length + 4 : 0;
  const observed = ((btRate * btSamples) + ((liveRate ?? btRate) * liveSamples)) / (btSamples + liveSamples);

  let conf = Math.round(observed * 100);

  // Confluência é o multiplicador principal (lógica dominante)
  if (agree >= 4) conf += 4;
  else if (agree === 3) conf += 3;
  else if (agree === 2) conf += 1;

  conf += Math.round((cand.strength - 86) * 0.25);
  if (cand.tipo === 'seq' && regime && regimeFit(cand, regime)) conf += 1;

  // Penalidades: estratégia esfriando
  if (live.length >= 5 && liveW / live.length < 0.85) conf -= 4;
  const recentRed = S.signals.find((x) =>
    x.key === cand.key && x.status === 'RED' && Date.now() - (x.fechadoEm || 0) < 10 * 60 * 1000);
  if (recentRed) conf -= 3;

  return Math.max(80, Math.min(97, conf));
}

/* ============================ SUSPENSÃO (persistente) ============================ */

function isSuspended(key) {
  const u = SUSPENDED[key];
  if (!u) return false;
  if (Date.now() > u) { delete SUSPENDED[key]; dbStateSet('suspended_v20', JSON.stringify(Object.entries(SUSPENDED))); return false; }
  return true;
}
function maybeSuspend(key) {
  const recent = S.signals.filter((x) => x.key === key && FECHADOS.has(x.status)).slice(-CFG.suspendLossStreak);
  if (recent.length < CFG.suspendLossStreak) return;
  if (recent.every((x) => x.status === 'RED')) {
    SUSPENDED[key] = Date.now() + CFG.suspendMs;
    dbStateSet('suspended_v20', JSON.stringify(Object.entries(SUSPENDED)));
    log('SUSPENSO', { key, consecutiveReds: recent.length });
  }
}

/* ============================ GATE ============================ */

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
  const bn = p ? p.n : 0;
  let liveN = 0;
  for (const x of S.signals) if (x.key === key && FECHADOS.has(x.status)) liveN++;
  return bn + liveN;
}
function liveHitRate(key) {
  let n = 0, w = 0;
  for (const x of S.signals) {
    if (x.key !== key || !FECHADOS.has(x.status)) continue;
    n++; if (x.status !== 'RED') w++;
  }
  return n >= 5 ? w / n : null;
}

/* ============================ GERAR SINAL (confluência ou elite) ============================ */

async function tentarSinal() {
  if (!podeGerar()) return;

  const rows = S.hist.slice(-30).reverse();
  const stats = getStats();
  const seq = rows.filter((r) => isCol(r.resultado));
  const regime = detectRegime(seq);

  let cands = runStrategies(rows);
  if (!cands.length) { R.ultimoMotivo = 'sem estratégia'; return; }

  // Filtros duros por estratégia
  cands = cands.filter((c) => {
    if (isSuspended(c.key)) return false;
    if (!regimeFit(c, regime)) return false;
    const p = stats.per[c.key];
    const btN = p ? p.n : 0;
    const elite = ELITE.has(c.key) && btN >= CFG.eliteN && (p.w / p.n) >= CFG.eliteRate;
    if (!elite) {
      if (btN < CFG.btMinN) return false;
      if (p.w / p.n < CFG.btMinRate) return false;
      if (stratSamples(c.key, stats) < CFG.minStratN) return false;
      const lr = liveHitRate(c.key);
      if (lr !== null && lr < CFG.liveMin) return false;
    }
    return true;
  });
  if (!cands.length) { R.ultimoMotivo = 'filtros duros'; return; }

  // Agrupa por cor: confluência = nº de estratégias DISTINTAS concordando
  const byColor = {};
  for (const c of cands) {
    (byColor[c.cor] = byColor[c.cor] || []).push(c);
  }

  let melhor = null;
  for (const cor of Object.keys(byColor)) {
    const list = byColor[cor];
    const distinctKeys = new Set(list.map((x) => x.key)).size;
    const hasElite = list.some((x) => ELITE.has(x.key));
    // Gate de confluência: >= minAgree leituras distintas, OU elite validada no backtest
    if (distinctKeys < CFG.minAgree && !hasElite) continue;
    const scored = list.map((c) => ({ ...c, conf: confianca(c, distinctKeys, regime) }));
    scored.sort((a, b) => b.conf - a.conf);
    const top = scored[0];
    if (!melhor || top.conf > melhor.conf) melhor = { ...top, agree: distinctKeys };
  }

  if (!melhor) { R.ultimoMotivo = `sem confluência (mín. ${CFG.minAgree})`; return; }
  if (melhor.conf < CFG.scoreMin) { R.ultimoMotivo = `score ${melhor.conf} < ${CFG.scoreMin}`; return; }

  const reservedId = S.nextId;
  const sinalPreliminar = {
    id: reservedId, cor: melhor.cor, key: melhor.key, nome: melhor.nome, emoji: melhor.emoji,
    conf: melhor.conf, tipo: melhor.tipo, status: 'PENDENTE',
    t: 0, criadoEm: 0, agree: melhor.agree,
    msgAnalisandoId: null, fechadoEm: null
  };

  const analisando = await tgSendRaw(fmtAnalisando());
  if (analisando.ok) sinalPreliminar.msgAnalisandoId = analisando.messageId;
  try {
    const envio = await tgSendRaw(fmtSinal(sinalPreliminar));
    if (!envio.ok) {
      log('SINAL', { motivo: 'telegram rejeitou sinal', id: reservedId });
      R.ultimoMotivo = 'telegram falhou';
      return;
    }
    const agora = Date.now();
    sinalPreliminar.t = agora;
    sinalPreliminar.criadoEm = agora;
    S.nextId++;
    S.signals.push(sinalPreliminar);
    if (S.signals.length > CFG.maxSignalsMem) S.signals = S.signals.slice(-CFG.maxSignalsMem + 50);
    S.lastSignalAt = agora;
    R.ultimoSinal = sinalPreliminar;
    R.ultimoMotivo = null;

    await dbSignalInsert(sinalPreliminar);
    await dbStateSet('nextId_v20', String(S.nextId));
    await dbStateSet('lastSignalAt_v20', String(agora));

    log('SINAL', { id: sinalPreliminar.id, cor: sinalPreliminar.cor, conf: sinalPreliminar.conf, key: sinalPreliminar.key, agree: sinalPreliminar.agree });
  } finally {
    if (sinalPreliminar.msgAnalisandoId) await tgDelete(sinalPreliminar.msgAnalisandoId);
  }
}

/* ============================ FECHAR ============================ */

async function fecharSinal(s, status) {
  if (FECHADOS.has(s.status)) return;
  s.status = status;
  s.fechadoEm = Date.now();

  const bucket = status === 'GREEN' ? 'green' : status === 'EMPATE' ? 'empate' : 'red';
  ALLTIME[bucket]++; ALLTIME.total++;
  DAILY[bucket]++; DAILY.total++;
  if (status === 'RED') {
    S.curStreak = 0;
  } else {
    S.curStreak++;
    if (S.curStreak > DAILY.bestStreak) DAILY.bestStreak = S.curStreak;
  }
  await dbStateSet('curStreak_v20', String(S.curStreak));
  await dbStateSet('daily_v20', JSON.stringify(DAILY));

  await dbSignalUpdate(s);
  await tgSendRaw(fmtResult(s));
  await tgSendRaw(fmtHistorico());
  maybeSuspend(s.key);
}

/* ============================ APURAÇÃO (1 rodada, sem gale) ============================ */

async function apurar() {
  const now = Date.now();
  for (const s of S.signals) {
    if (FECHADOS.has(s.status)) continue;
    const next = S.hist.find((r) => r.t > s.t);
    if (next) {
      if (next.t - s.t > CFG.gapMs) {
        await fecharSinal(s, 'RED'); // janela expirou
      } else if (next.resultado === s.cor) {
        await fecharSinal(s, 'GREEN');
      } else if (next.resultado === E) {
        await fecharSinal(s, 'EMPATE'); // proteção
      } else {
        await fecharSinal(s, 'RED');
      }
    } else if (now - s.t > CFG.apuraMaxMs) {
      await fecharSinal(s, 'RED');
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
      const previous = byId.get(r.id);
      if (!previous) {
        novos++; byId.set(r.id, r);
        await dbRoundUpsert(r);
      } else if (previous.t !== r.t || previous.resultado !== r.resultado || previous.pj !== r.pj || previous.pb !== r.pb) {
        Object.assign(previous, r); atualizados++;
        await dbRoundUpsert(r);
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
      await tgAlert('API', `API BACBO sem rodadas novas há <b>${mins} min</b>.\nÚltima: ${R.ultimaRodadaNova}\nFonte: ${R.fonteAPI + 1}/${API_SOURCES.length}`);
    }
    if (novos || atualizados) log('SYNC', { novos, atualizados, total: S.hist.length, fonte: R.fonteAPI });
  } catch (e) {
    R.erroAPI = e.message;
    log('SYNC ERRO', e.message);
    await tgAlert('API', `Erro ao consultar API BACBO:\n<code>${e.message}</code>`);
  }
}

let lastSyncAt = 0;
async function tick() {
  if (busy || stopping) return;
  busy = true; lastTickMs = Date.now();
  try {
    await checkDailyReset();
    const now = Date.now();
    if (now - lastSyncAt >= currentSyncMs()) {
      await sync();
      lastSyncAt = Date.now();
    }
    await apurar();
    await tentarSinal();
  } catch (e) { log('TICK ERRO', e.message); }
  finally { busy = false; }
}

async function keepAlive() {
  const self = process.env.SELF_URL || process.env.RENDER_EXTERNAL_URL || '';
  if (self) { try { await axios.get(self.replace(/\/$/, '') + '/health', { timeout: 7000 }); } catch (_) {} }
}
function wakeCheck() {
  if (Date.now() - lastTickMs > 10000) {
    R.wakes++; R.ultimoWake = new Date().toISOString();
    log('WAKE', 'atraso'); tick().catch(() => {});
  }
}

/* ============================ EXPRESS ============================ */

const app = express();
app.disable('x-powered-by');
const corsOrigins = (process.env.CORS_ORIGINS || '').split(',').map((x) => x.trim()).filter(Boolean);
app.use(cors(corsOrigins.length ? { origin: corsOrigins } : { origin: false }));
app.use(express.json({ limit: '200kb' }));

function requireAdmin(req, res, next) {
  if (!ADMIN_TOKEN) return res.status(503).json({ erro: 'ADMIN_TOKEN não configurado' });
  const supplied = String(req.get('authorization') || '').replace(/^Bearer\s+/i, '');
  const a = Buffer.from(supplied);
  const b = Buffer.from(ADMIN_TOKEN);
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) return res.status(401).json({ erro: 'não autorizado' });
  next();
}

app.get('/health', (req, res) => {
  const newest = S.hist[S.hist.length - 1];
  res.json({
    ok: !R.erroAPI && Boolean(R.ultimaSync) && R.lockOk,
    versao: '20.0', deployId: DEPLOY_ID, modo: 'SEM GALE', lock: R.lockOk,
    fonteAPI: R.fonteAPI + 1, fontes: API_SOURCES.length,
    hist: S.hist.length, sinais: S.signals.length, pendentes: pendentes().length,
    diario: { ...DAILY, taxa: dailyTaxa(), taxaReal: dailyTaxaReal() }, geral: ALLTIME,
    ultimaSync: R.ultimaSync, ultimaRodada: newest ? R.ultimaRodadaNova : null,
    apiStale: newest ? Date.now() - newest.t > CFG.staleAlertMs : true,
    erroAPI: R.erroAPI, erroDB: R.erroDB,
    uptime: Math.round(process.uptime())
  });
});
app.get('/status', requireAdmin, (req, res) => {
  res.json({ motivo: R.ultimoMotivo, R, DAILY, ALLTIME, CFG, elite: [...ELITE], suspenso: Object.keys(SUSPENDED) });
});
app.get('/sinais', requireAdmin, (req, res) => {
  const n = Math.min(100, Math.max(1, parseInt(req.query.n, 10) || 20));
  res.json(S.signals.slice(-n).reverse());
});
app.get('/stats', requireAdmin, (req, res) => {
  const gw = ALLTIME.green + ALLTIME.empate;
  res.json({
    diario: {
      ...DAILY, wins: gw, taxa: DAILY.total > 0 ? gw / DAILY.total : null,
      taxaReal: DAILY.green + DAILY.red > 0 ? DAILY.green / (DAILY.green + DAILY.red) : null,
      sequenciaAtual: S.curStreak
    },
    geral: { ...ALLTIME, wins: gw, taxa: ALLTIME.total > 0 ? gw / ALLTIME.total : null }
  });
});
app.get('/calibracao', requireAdmin, (req, res) => {
  const buckets = { '90-92': { n: 0, w: 0 }, '93-95': { n: 0, w: 0 }, '96-97': { n: 0, w: 0 } };
  for (const s of S.signals) {
    if (!FECHADOS.has(s.status)) continue;
    const b = s.conf >= 96 ? '96-97' : s.conf >= 93 ? '93-95' : '90-92';
    buckets[b].n++;
    if (s.status !== 'RED') buckets[b].w++;
  }
  res.json(buckets);
});
app.get('/estrategias', requireAdmin, (req, res) => {
  // Ranking vivo: desempenho real por estratégia (backtest + live)
  const out = [];
  const stats = S.stats || { per: {} };
  const keys = new Set(Object.keys(stats.per));
  for (const s of S.signals) if (FECHADOS.has(s.status)) keys.add(s.key);
  for (const k of keys) {
    const p = stats.per[k] || { n: 0, w: 0 };
    const live = S.signals.filter((x) => x.key === k && FECHADOS.has(x.status));
    const liveW = live.filter((x) => x.status !== 'RED').length;
    out.push({
      key: k, elite: ELITE.has(k), suspenso: isSuspended(k),
      bt: { n: p.n, w: p.w, rate: p.n ? p.w / p.n : null },
      live: { n: live.length, w: liveW, rate: live.length ? liveW / live.length : null }
    });
  }
  out.sort((a, b) => (b.bt.rate || 0) - (a.bt.rate || 0));
  res.json(out);
});

/* ============================ START ============================ */

async function start() {
  for (const [name, value] of Object.entries({ BOT_TOKEN, CHAT_ID, ADMIN_TOKEN, DATABASE_URL })) {
    if (!value) throw new Error(`Variável obrigatória em falta: ${name}`);
  }
  if (!/^postgres(?:ql)?:\/\//i.test(DATABASE_URL)) throw new Error('DATABASE_URL inválido');
  log('BOOT', { versao: '20.0', deployId: DEPLOY_ID, fontes: API_SOURCES.length, modo: 'SEM GALE' });
  await dbInit();
  if (!pool) throw new Error('Não foi possível inicializar PostgreSQL; arranque cancelado');

  const locked = await acquireLock();
  if (!locked) throw new Error('Outra instância do bot está ativa (advisory lock). Encerrando.');

  await dbLoadState();
  await sync();
  getStats(true);

  // Sinais PENDENTES são apurados pelo loop normal (apurar() decide na 1ª rodada nova)
  server = app.listen(PORT, () => log('HTTP', `porta ${PORT}`));
  tickTimer = setInterval(() => tick().catch(() => {}), 1000);
  keepTimer = setInterval(() => { keepAlive().catch(() => {}); wakeCheck(); }, CFG.keepAliveMs);
  setTimeout(() => tick().catch(() => {}), 1500);

  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}
async function shutdown() {
  if (stopping) return;
  stopping = true; log('SHUTDOWN', 'ok');
  clearInterval(tickTimer); clearInterval(keepTimer);
  if (server) await new Promise((resolve) => server.close(resolve));
  await releaseLock();
  if (pool) await pool.end().catch(() => {});
  process.exit(0);
}

start().catch((e) => { console.error(e); process.exit(1); });
