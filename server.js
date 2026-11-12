'use strict';

/**
 * CAPUNGA DADOS — BAC BO · v24 · INTERVALO MÁXIMO 4 MIN
 * Política: uma entrada por sinal, sem Gale e sem proteção. Só a próxima rodada
 * pode fechar GREEN; qualquer outro resultado (incluindo empate) fecha RED.
 * Sem tiers relaxados/forçados. Placar diário zera às 00h00 de Angola.
 * Env: BOT_TOKEN, CHAT_ID, ADMIN_TOKEN, DATABASE_URL, PORT, API_BACBO (opcional), API_BACBO_2 (opcional)
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
  maxGale: 0,
  maxPending: 1,
  minAmostra: 1, // usa todo o histórico disponível; sem corte por amostra
  syncMs: 3000,          // polling normal
  syncFastMs: 1000,      // polling com sinal pendente / mesa quente
  minGapMs: 60000, // em operação saudável, nova avaliação pelo menos a cada minuto       // no máximo um sinal por minuto
  minSettledAgeMs: 5000,
  maxDataAgeMs: 45000,
  apuraMaxMs: 210000, // aos 3m30s invalida pendente e abre espaço para sinal seguinte antes dos 4 min
  maxHist: 4000,
  gapMs: 7 * 60 * 1000,
  maxSignalsMem: 500,
  signalsOnBoot: 500,
  suspendMs: 4 * 3600 * 1000,
  suspendLossStreak: 3,
  staleAlertMs: 3 * 60 * 1000,
  alertThrottleMs: 10 * 60 * 1000,
  keepAliveMs: 60000,
  // Seleção por ensemble: sem filtros mínimos de taxa, amostra ou consenso.
  validationFraction: 0.25
};

const J = 'Jogador';
const B = 'Banca';
const E = 'Empate';
const isCol = (x) => x === J || x === B;
const opp = (c) => (c === J ? B : c === B ? J : null);
const FECHADOS = new Set(['GREEN', 'G1', 'G2', 'EMPATE', 'EG1', 'EG2', 'LOSS', 'VOID']);
const RESULTADOS_APURADOS = new Set(['GREEN', 'G1', 'G2', 'EMPATE', 'EG1', 'EG2', 'LOSS']); // compatibilidade com histórico legado
const LOCK_KEY = 727272;

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
const DAILY = { date: '', total: 0, green: 0, g1: 0, g2: 0, empate: 0, eg1: 0, eg2: 0, loss: 0, void: 0, bestStreak: 0 };
// Placar GERAL (persistente)
const ALLTIME = { total: 0, green: 0, g1: 0, g2: 0, empate: 0, eg1: 0, eg2: 0, loss: 0, void: 0 };
const SUSPENDED = {};

const R = {
  iniciadoEm: new Date().toISOString(), deployId: DEPLOY_ID, versao: '24.0',
  ultimaSync: null, ultimaRodadaNova: null, fonteAPI: 0,
  erroAPI: null, erroTelegram: null, erroDB: null,
  ultimoMotivo: null, ultimoSinal: null, tier: 0, wakes: 0, ultimoWake: null,
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
// Meia-noite em Angola (UTC+1, sem horário de verão)
const luandaDayKey = (ms = Date.now()) => new Date(ms + 3600000).toISOString().slice(0, 10);

const nomeCor = (c) => (c === B ? 'VERMELHO 🔴' : c === J ? 'AZUL 🔵' : 'EMPATE 🟡');
const emojiCor = (c) => (c === B ? '🔴' : c === J ? '🔵' : '🟡');
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
      CREATE TABLE IF NOT EXISTS capunga_v24_state (k TEXT PRIMARY KEY, v TEXT NOT NULL, updated_at TIMESTAMPTZ DEFAULT NOW());
      CREATE TABLE IF NOT EXISTS capunga_rounds (id TEXT PRIMARY KEY, resultado TEXT NOT NULL, t BIGINT NOT NULL, pj DOUBLE PRECISION, pb DOUBLE PRECISION);
      CREATE INDEX IF NOT EXISTS idx_cap_rounds_t ON capunga_rounds(t DESC);
      CREATE TABLE IF NOT EXISTS capunga_v24_signals (
        id INTEGER PRIMARY KEY, cor TEXT NOT NULL, key TEXT NOT NULL, nome TEXT NOT NULL,
        tipo TEXT NOT NULL, conf INT NOT NULL, status TEXT NOT NULL DEFAULT 'PENDENTE',
        gale INT, agree INT, tier INT DEFAULT 0, created_at TIMESTAMPTZ NOT NULL, closed_at TIMESTAMPTZ
      );
      CREATE INDEX IF NOT EXISTS idx_cap_v24_sig_status ON capunga_v24_signals(status);
      CREATE INDEX IF NOT EXISTS idx_cap_v24_sig_created ON capunga_v24_signals(created_at DESC);
    `);
    R.erroDB = null; log('DB', 'schema ok');
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
  try { const r = await pool.query('SELECT v FROM capunga_v24_state WHERE k=$1', [k]); return r.rows[0]?.v ?? null; }
  catch (e) { R.erroDB = e.message; return null; }
}
async function dbStateSet(k, v) {
  if (!pool) return;
  try {
    await pool.query(`INSERT INTO capunga_v24_state(k,v,updated_at) VALUES ($1,$2,NOW())
      ON CONFLICT (k) DO UPDATE SET v=EXCLUDED.v, updated_at=NOW()`, [k, String(v)]);
  } catch (e) { R.erroDB = e.message; }
}
async function dbSignalInsert(s) {
  if (!pool) return;
  try {
    await pool.query(`INSERT INTO capunga_v24_signals (id,cor,key,nome,tipo,conf,status,gale,agree,tier,created_at,closed_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT (id) DO NOTHING`,
      [s.id, s.cor, s.key, s.nome, s.tipo, s.conf, s.status, s.gale ?? null, s.agree ?? 1, s.tier ?? 0,
       new Date(s.t), s.fechadoEm ? new Date(s.fechadoEm) : null]);
  } catch (e) { R.erroDB = e.message; }
}
async function dbSignalUpdate(s) {
  if (!pool) return;
  try {
    await pool.query('UPDATE capunga_v24_signals SET status=$1, gale=$2, closed_at=$3 WHERE id=$4',
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

async function dbLoadAlltime() {
  if (!pool) return;
  try {
    const r = await pool.query(`SELECT status, COUNT(*)::int AS n FROM capunga_v24_signals
      WHERE status IN ('GREEN', 'LOSS', 'VOID') GROUP BY status`);
    for (const k of Object.keys(ALLTIME)) ALLTIME[k] = 0;
    for (const row of r.rows) {
      if (row.status !== 'VOID') ALLTIME.total += row.n;
      if (row.status === 'GREEN') ALLTIME.green = row.n;
      else if (row.status === 'G1') ALLTIME.g1 = row.n;
      else if (row.status === 'G2') ALLTIME.g2 = row.n;
      else if (row.status === 'EMPATE') ALLTIME.empate = row.n;
      else if (row.status === 'EG1') ALLTIME.eg1 = row.n;
      else if (row.status === 'EG2') ALLTIME.eg2 = row.n;
      else if (row.status === 'LOSS') ALLTIME.loss = row.n;
      else if (row.status === 'VOID') ALLTIME.void = row.n;
    }
  } catch (e) { R.erroDB = e.message; }
}

async function dbLoadState() {
  if (!pool) return;
  try {
    const nextId = await dbStateGet('nextId_v24');
    const lastSignalAt = await dbStateGet('lastSignalAt_v24');
    const curStreak = await dbStateGet('curStreak_v24');
    if (nextId) S.nextId = Math.max(1, parseInt(nextId, 10));
    if (lastSignalAt) S.lastSignalAt = parseInt(lastSignalAt, 10);
    if (curStreak) S.curStreak = parseInt(curStreak, 10);

    try {
      const raw = await dbStateGet('suspended_v24');
      if (raw) {
        const arr = JSON.parse(raw);
        const now = Date.now();
        for (const [key, until] of arr) if (until > now) SUSPENDED[key] = until;
      }
    } catch (_) {}

    // Placar diário persistido
    try {
      const rawD = await dbStateGet('daily_v24');
      if (rawD) Object.assign(DAILY, JSON.parse(rawD));
    } catch (_) {}
    if (!DAILY.date) DAILY.date = luandaDayKey();

    const rounds = await pool.query(`SELECT id,resultado,t,pj,pb FROM capunga_rounds ORDER BY t DESC LIMIT ${CFG.maxHist}`);
    S.hist = rounds.rows.map((r) => ({ id: r.id, resultado: r.resultado, t: Number(r.t), pj: r.pj === null ? null : Number(r.pj), pb: r.pb === null ? null : Number(r.pb) })).reverse();
    S.seen = new Set(S.hist.map((r) => r.id));
    const r = await pool.query(`SELECT * FROM capunga_v24_signals ORDER BY created_at DESC LIMIT ${CFG.signalsOnBoot}`);
    S.signals = r.rows.reverse().map((row) => ({
      id: row.id, cor: row.cor, key: row.key, nome: row.nome, tipo: row.tipo,
      conf: row.conf, status: row.status, gale: row.gale, agree: row.agree, tier: row.tier || 0,
      t: new Date(row.created_at).getTime(), criadoEm: new Date(row.created_at).getTime(),
      fechadoEm: row.closed_at ? new Date(row.closed_at).getTime() : null,
      gale1MsgId: null, gale2MsgId: null, galeLevel: row.status !== 'PENDENTE' ? 2 : 0
    }));
    await dbLoadAlltime();
    log('DB', `v24 isolado carregado: nextId=${S.nextId}, sinais_v24=${S.signals.length}, histórico_rondas=${S.hist.length}, diário=${DAILY.date}`);
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
  await tgSendRaw(`⚠️ <b>ALERTA ${tag}</b>\n${detalhe}\n⏱ ${hora()}`);
}

/* ============================ RESET DIÁRIO (00h00 ANGOLA) ============================ */

function dailyWins() { return DAILY.green; }
function dailyTaxa() { return DAILY.total > 0 ? (dailyWins() / DAILY.total) * 100 : 0; }

async function sendDailySummary(ontem) {
  const wins = DAILY.green;
  const taxa = DAILY.total > 0 ? (wins / DAILY.total) * 100 : 0;
  await tgSendRaw([
    `🕛 <b>FIM DO DIA ${ontem}</b> 🕛`,
    '',
    `📊 Resultados apurados: <b>${DAILY.total}</b>`,
    `✅ Green direto: ${wins}`,
    `⚪ Invalidado (API): ${DAILY.void}`, 
    `🟡 Empates: contam como RED (sem proteção)`,
    `🔴 RED: ${DAILY.loss}`, 
    `🔥 Maior sequência: ${DAILY.bestStreak}`,
    `🎯 Taxa de green direto: ${taxa.toFixed(1)}%`,
    '',
    `🌅 <b>PLACAR ZERADO — BOM DIA!</b>`
  ].join('\n'));
}

async function checkDailyReset() {
  const hoje = luandaDayKey();
  if (DAILY.date && DAILY.date !== hoje) {
    const ontem = DAILY.date;
    await sendDailySummary(ontem);
    for (const k of ['total', 'green', 'g1', 'g2', 'empate', 'eg1', 'eg2', 'loss', 'void', 'bestStreak']) DAILY[k] = 0;
    DAILY.date = hoje;
    await dbStateSet('daily_v24', JSON.stringify(DAILY));
    log('DIÁRIO', `reset de ${ontem} -> ${hoje}`);
  } else if (!DAILY.date) {
    DAILY.date = hoje;
  }
}

/* ============================ TEMPLATES SMS (novo estilo v24) ============================ */

function fmtAnalisando() {
  return [`⚙️ <b>CAPUNGA v24 — Lendo a mesa…</b>`, `⏱ ${hora()}`].join('\n');
}

function fmtSinal(s) {
  const confEmoji = s.conf >= 93 ? '💎' : s.conf >= 90 ? '✅' : '📊';
  return [
    `🎯 ═══ <b>CAPUNGA · SINAL SEM GALE</b> ═══ 🎯`,
    '',
    `📍 <b>ENTRADA ÚNICA #${s.id}</b>`,
    `➡️ Cor: <b>${nomeCor(s.cor)}</b>`,
    `${confEmoji} Estimativa histórica 1ª rodada: <b>${s.conf}%</b>`,
    `🧠 Estratégia: <b>${s.nome}</b>`,
    `🤝 Previsões no sentido escolhido: <b>${s.agree}</b>`,
    `🕒 ${hora(s.t || Date.now())}`,
    '',
    `⛔ <b>Sem Gale e sem proteção.</b>`,
    `Se a próxima rodada não for ${nomeCor(s.cor)}, o resultado é <b>RED</b>.`,
    `⚠️ Score histórico não garante resultado futuro.`
  ].join('\n');
}

function fmtResult(s) {
  const placar = `📅 Dia: ${DAILY.green} GREEN • ${DAILY.loss} RED • ${dailyTaxa().toFixed(0)}% GREEN direto`;
  if (s.status === 'GREEN') return `✅ <b>GREEN DIRETO #${s.id}</b> ✅\n\n🎯 Acertou na primeira rodada.\n${placar}`;
  if (s.status === 'VOID') return `⚪ <b>SINAL INVALIDADO #${s.id}</b> ⚪\n\nA API não forneceu uma rodada válida dentro da janela; resultado não apurado e não contado como GREEN/RED.`;
  return `🔴 <b>RED #${s.id}</b> 🔴\n\nA entrada única não acertou na primeira rodada. Sem Gale.\n${placar}`;
}

function fmtHistorico() {
  const streak = S.curStreak;
  const streakTxt = streak >= 3 ? `${streak} 🔥` : `${streak}`;
  return [
    `📊 <b>PLACAR DO DIA — ${DAILY.date}</b>`,
    '━━━━━━━━━━━━━━━',
    `Sinais: ${DAILY.total}`,
    `✅ Green direto: ${DAILY.green}`,
    `⚪ Invalidado (API): ${DAILY.void}`, 
    `🟡 Empates: contam como RED (sem proteção)`,
    `🔴 RED: ${DAILY.loss}`, 
    `🔥 Sequência atual: ${streakTxt} | Recorde: ${DAILY.bestStreak}`,
    `🎯 Green direto: ${DAILY.total ? (DAILY.green / DAILY.total * 100).toFixed(1) : '0.0'}%`
  ].join('\n');
}

/* ============================ API ============================ */

// Remove acentos SEM regex de caracteres combinantes (evita corrupção de encoding)
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
// null-safe: Number(null) === 0 criaria scores falsos (ex.: Gatilho 0 falso)
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
    headers: { Accept: 'application/json', 'User-Agent': 'CapungaDados/24.0' }
  });
  const map = new Map();
  for (const it of extractItems(resp.data)) {
    const rd = normalizeRound(it);
    if (rd) map.set(rd.id, rd);
  }
  return [...map.values()].sort((a, b) => a.t - b.t || (a.id < b.id ? -1 : 1));
}

// Fallback automático entre fontes: se a primária falhar, tenta a secundária
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
// Estratégias de quebra só em mesa errática; de sequência só em tendência
function regimeFit(cand, regime) {
  const k = cand.key;
  if (/SEQ_(QUEBRA|SURF|PINGPONG|2x2|EXTEND|3_1|MOMENTUM|ALT|BREAK_WEAK)/.test(k)) return regime.chop >= 0.45;
  if (/SEQ_(REP|STRONG_STREAK)|BANKER_BIAS/.test(k)) return regime.trend >= 0.45;
  return true;
}

/* ============================ ESTRATÉGIAS (30 — só as com lógica real) ============================ */

const STRATS = [
  // ---- SEQUÊNCIA (padrões reais de repetição/alternância) ----
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
    if (a !== b && b === c && c === d) return { cor: a, key: 'SEQ_3_1', nome: '3→1 confirmado', emoji: '🧩', strength: 78, tipo: 'seq' };
    return null;
  },
  function seqMomentum2({ seq }) {
    if (seq.length < 3) return null;
    const [a, b, c] = seq.map((x) => x.resultado);
    if (a !== b && b === c) return { cor: c, key: 'SEQ_MOMENTUM_2', nome: 'Momentum 2', emoji: '⚡', strength: 77, tipo: 'seq' };
    return null;
  },
  function seqBreakWeak4({ seq }) {
    const n = streak(seq);
    if (n !== 4) return null;
    const s = seq.slice(0, 4);
    if (!s.every((r) => hasScore(r) && margem(r) <= 2)) return null;
    return { cor: opp(seq[0].resultado), key: 'SEQ_BREAK_WEAK_4', nome: 'Quebra fraca x4', emoji: '🩹', strength: 79, tipo: 'seq' };
  },
  function seqExtend({ seq }) {
    if (seq.length < 5) return null;
    const [a, b, c, d, e] = seq.map((x) => x.resultado);
    if (a === b && b === c && c !== d && d === e) {
      return { cor: a, key: 'SEQ_EXTEND', nome: 'Streak extend', emoji: '⏭️', strength: 80, tipo: 'seq' };
    }
    return null;
  },
  function seqStrongStreak({ seq }) {
    const n = streak(seq);
    if (n < 4 || n > 6) return null;
    const same = seq.slice(0, n);
    if (!same.every((r) => hasScore(r) && margem(r) >= 3)) return null;
    return { cor: seq[0].resultado, key: 'SEQ_STRONG_STREAK', nome: `Dragon ${n}`, emoji: '🐉', strength: 82, tipo: 'seq' };
  },
  function seqPingPong({ seq }) {
    const L = alternation(seq);
    if (L < 4 || L > 10) return null;
    return { cor: opp(seq[0].resultado), key: 'SEQ_PINGPONG', nome: `Ping-pong ${L}`, emoji: '🏓', strength: 79, tipo: 'seq' };
  },
  function seqAlternatingStrong({ seq }) {
    const L = alternation(seq);
    if (L < 3) return null;
    const s = seq.slice(0, L);
    if (!s.every((r) => hasScore(r) && margem(r) >= 3)) return null;
    return { cor: opp(seq[0].resultado), key: 'SEQ_ALT_STRONG', nome: `Alt. forte ${L}`, emoji: '⚔️', strength: 78, tipo: 'seq' };
  },
  function bankerBias20({ seq }) {
    const s = seq.slice(0, 20);
    if (s.length < 15) return null;
    const b = s.filter((r) => r.resultado === B).length;
    const j = s.filter((r) => r.resultado === J).length;
    if (b >= 12) return { cor: B, key: 'BANKER_BIAS_20', nome: 'Viés Banca', emoji: '📉', strength: 80, tipo: 'seq' };
    if (j >= 12) return { cor: J, key: 'BANKER_BIAS_20', nome: 'Viés Jogador', emoji: '📈', strength: 80, tipo: 'seq' };
    return null;
  },
  // ---- NUMÉRICA (força real dos dados: margens, escores, sequências de pontos) ----
  function numEscada({ seq }) {
    if (seq.length < 3 || !seq.slice(0, 3).every(hasScore)) return null;
    const [a, b, c] = seq;
    if (a.resultado !== b.resultado || b.resultado !== c.resultado) return null;
    const sa = scoreOf(a), sb = scoreOf(b), sc = scoreOf(c);
    if ((sb - sa === 1 && sc - sb === 1) || (sa - sb === 1 && sb - sc === 1)) {
      return { cor: a.resultado, key: 'NUM_ESCADA', nome: `Escada ${sa}→${sb}→${sc}`, emoji: '🪜', strength: 88, tipo: 'num' };
    }
    return null;
  },
  function numRepeteScore({ seq }) {
    if (seq.length < 2 || !hasScore(seq[0]) || !hasScore(seq[1])) return null;
    const [a, b] = seq;
    if (a.resultado !== b.resultado) return null;
    if (scoreOf(a) === scoreOf(b)) return { cor: a.resultado, key: 'NUM_REPETE', nome: `Score repetido ${scoreOf(a)}`, emoji: '🔂', strength: 84, tipo: 'num' };
    return null;
  },
  function numThreeLow({ seq }) {
    if (seq.length < 3 || !seq.slice(0, 3).every(hasScore)) return null;
    const [a, b, c] = seq;
    if (a.resultado !== b.resultado || b.resultado !== c.resultado) return null;
    if (scoreOf(a) <= 3 && scoreOf(b) <= 3 && scoreOf(c) <= 3) {
      return { cor: opp(a.resultado), key: 'NUM_THREE_LOW', nome: 'Virada de baixos x3', emoji: '📉', strength: 81, tipo: 'num' };
    }
    return null;
  },
  function numGatilho_0({ seq }) {
    if (!seq.length || !hasScore(seq[0])) return null;
    if (scoreOf(seq[0]) === 0) return { cor: opp(seq[0].resultado), key: 'NUM_GATILHO_0', nome: 'Gatilho 0', emoji: '🎯', strength: 86, tipo: 'num' };
    return null;
  },
  function numGatilho_1({ seq }) {
    if (!seq.length || !hasScore(seq[0])) return null;
    if (scoreOf(seq[0]) === 1) return { cor: opp(seq[0].resultado), key: 'NUM_GATILHO_1', nome: 'Gatilho 1', emoji: '🎯', strength: 84, tipo: 'num' };
    return null;
  },
  function numGatilho_9({ seq }) {
    if (!seq.length || !hasScore(seq[0])) return null;
    if (scoreOf(seq[0]) === 9) return { cor: opp(seq[0].resultado), key: 'NUM_GATILHO_9', nome: 'Gatilho 9', emoji: '🎯', strength: 88, tipo: 'num' };
    return null;
  },
  function numMargemAlta({ rows }) {
    const r = rows[0];
    if (!r || !isCol(r.resultado) || !hasScore(r) || margem(r) < 5) return null;
    return { cor: r.resultado, key: 'NUM_MARGEM5', nome: `Margem alta ${r.pj}×${r.pb}`, emoji: '📊', strength: 82, tipo: 'num' };
  },
  function numDiff4x3({ seq }) {
    if (seq.length < 3) return null;
    const t = seq.slice(0, 3);
    if (!t.every((r) => hasScore(r) && margem(r) >= 4 && r.resultado === t[0].resultado)) return null;
    return { cor: t[0].resultado, key: 'NUM_DIFF4x3', nome: 'Diff ≥4 ×3', emoji: '🔢', strength: 85, tipo: 'num' };
  },
  function numViradaBaixa({ seq }) {
    if (seq.length < 2) return null;
    const [a, b] = seq;
    if (!hasScore(a) || !hasScore(b) || a.resultado !== b.resultado) return null;
    if (scoreOf(a) > 4 || scoreOf(b) > 4) return null;
    return { cor: opp(a.resultado), key: 'NUM_VIRADA_BAIXA', nome: 'Virada de baixos', emoji: '🔄', strength: 80, tipo: 'num' };
  },
  function numTendencia({ rows }) {
    const s = rows.slice(0, 8).filter(hasScore);
    if (s.length < 6) return null;
    const mj = s.reduce((a, r) => a + r.pj, 0) / s.length;
    const mb = s.reduce((a, r) => a + r.pb, 0) / s.length;
    const diff = mj - mb;
    if (Math.abs(diff) < 1.5) return null;
    return {
      cor: diff > 0 ? J : B, key: 'NUM_TENDENCIA',
      nome: `Tendência ${mj.toFixed(1)}×${mb.toFixed(1)}`, emoji: '📈',
      strength: Math.abs(diff) >= 2.2 ? 84 : 76, tipo: 'num'
    };
  },
  function numRangeSalto({ seq }) {
    if (seq.length < 2 || !hasScore(seq[0]) || !hasScore(seq[1])) return null;
    const [a, b] = seq;
    const ra = scoreRange(scoreOf(a)), rb = scoreRange(scoreOf(b));
    if (!ra || !rb) return null;
    if ((ra === 'MB' && rb === 'MA') || (ra === 'MA' && rb === 'MB')) {
      return { cor: opp(a.resultado), key: 'NUM_SALTO_RANGE', nome: `Salto ${scoreOf(b)}→${scoreOf(a)}`, emoji: '📐', strength: 81, tipo: 'num' };
    }
    return null;
  },
  function numScoreTrend3({ seq }) {
    if (seq.length < 3 || !seq.slice(0, 3).every(hasScore)) return null;
    const [a, b, c] = seq;
    if (a.resultado !== b.resultado || b.resultado !== c.resultado) return null;
    const sa = scoreOf(a), sb = scoreOf(b), sc = scoreOf(c);
    if (sa < sb && sb < sc) {
      return { cor: a.resultado, key: 'NUM_SCORE_TREND_3', nome: `Escala ascendente ${sa}→${sc}`, emoji: '📊', strength: 82, tipo: 'num' };
    }
    return null;
  },
  function numDoubleHigh({ seq }) {
    if (seq.length < 2 || !seq.slice(0, 2).every(hasScore)) return null;
    const [a, b] = seq;
    if (a.resultado !== b.resultado) return null;
    if (margem(a) >= 5 && margem(b) >= 5) {
      return { cor: a.resultado, key: 'NUM_DOUBLE_HIGH', nome: 'Dupla margem alta', emoji: '💪', strength: 84, tipo: 'num' };
    }
    return null;
  },
  function numMomentumShift({ seq }) {
    if (seq.length < 4 || !seq.slice(0, 4).every(hasScore)) return null;
    const [a, b, c, d] = seq;
    const sa = scoreOf(a), sb = scoreOf(b), sc = scoreOf(c), sd = scoreOf(d);
    if (sb - sa < 0 && sc - sb < 0 && sd - sc < 0) {
      return { cor: opp(a.resultado), key: 'MOMENTUM_SHIFT', nome: 'Mudança de momentum', emoji: '🔀', strength: 78, tipo: 'num' };
    }
    return null;
  },
  // ---- EMPATE (padrões pós-empate reais) ----
  function numPosEmpate({ rows, seq }) {
    if (!rows[0] || rows[0].resultado !== E || !seq.length) return null;
    return { cor: seq[0].resultado, key: 'NUM_POS_EMPATE', nome: 'Pós-empate', emoji: '🎯', strength: 74, tipo: 'num' };
  },
  function tieCascade({ rows }) {
    const ties = rows.slice(0, 3).filter((r) => r.resultado === E).length;
    if (ties < 2) return null;
    const pre = rows.find((r) => isCol(r.resultado));
    if (!pre) return null;
    return { cor: pre.resultado, key: 'TIE_CASCADE', nome: 'Cascata de empates', emoji: '🟡', strength: 76, tipo: 'num' };
  },
  function tieSingle({ rows, seq }) {
    if (!rows[0] || rows[0].resultado !== E) return null;
    if (!rows[1] || rows[1].resultado === E) return null;
    if (!seq.length) return null;
    return { cor: opp(seq[0].resultado), key: 'TIE_SINGLE', nome: 'Empate isolado', emoji: '🔀', strength: 77, tipo: 'num' };
  },
  function tieDoubleBack({ rows }) {
    if (rows.length < 3) return null;
    if (rows[0].resultado !== E || rows[1].resultado !== E) return null;
    if (!isCol(rows[2].resultado)) return null;
    return { cor: rows[2].resultado, key: 'TIE_DOUBLE_BACK', nome: 'Retorno pós-duplo empate', emoji: '🔙', strength: 78, tipo: 'num' };
  }
];

function estrategiaAdaptativa(rows) {
  const seq = rows.filter((r) => isCol(r.resultado));
  if (!seq.length) return null;
  // A) frequência recente com decaimento exponencial (mais peso às rondas recentes).
  let freqJ = 0, freqB = 0, weight = 1;
  for (const r of seq.slice(0, 12)) {
    if (r.resultado === J) freqJ += weight; else freqB += weight;
    weight *= 0.88;
  }
  // B) transições históricas para a cor atual (só passado, sem olhar para o futuro).
  const current = seq[0].resultado;
  let transJ = 1, transB = 1, transN = 0;
  for (let i = 2; i < seq.length; i++) {
    if (seq[i - 1].resultado !== current) continue;
    if (seq[i - 2].resultado === J) transJ++; else transB++;
    transN++;
  }
  // Suavização 50/50: Markov se há transições, senão frequência ponderada.
  const pJ = transN >= 2
    ? 0.5 * (transJ / (transJ + transB)) + 0.5 * (freqJ / (freqJ + freqB))
    : freqJ / (freqJ + freqB);
  const cor = pJ >= 0.5 ? J : B;
  return { cor, key: 'COMBINADA_ADAPTATIVA', nome: `Combinada adaptativa (${transN} transições)`, emoji: '🧠', strength: Math.round(Math.max(pJ, 1 - pJ) * 100), tipo: 'ensemble' };
}
function estrategiaMarkov2(rows) {
  const seq = rows.filter((r) => isCol(r.resultado));
  if (!seq.length) return null;
  const base = estrategiaAdaptativa(rows);
  let winJ = 0, winB = 0, n = 0;
  // Nas janelas históricas, procura o mesmo par ordenado e observa a cor seguinte.
  for (let i = 2; i < seq.length; i++) {
    if (seq[i - 1].resultado !== seq[0].resultado || seq[i].resultado !== seq[1]?.resultado) continue;
    if (seq[i - 2].resultado === J) winJ++; else winB++;
    n++;
  }
  const priorJ = base?.cor === J ? 0.56 : 0.44;
  const pJ = (winJ + 6 * priorJ) / (n + 6);
  return { cor: pJ >= 0.5 ? J : B, key: 'COMBINED_MARKOV2', nome: `Combinada Markov-2 (${n} padrões)`, emoji: '🔗', strength: Math.round(Math.max(pJ, 1 - pJ) * 100), tipo: 'ensemble' };
}
function estrategiaStreakRegime(rows) {
  const seq = rows.filter((r) => isCol(r.resultado));
  if (!seq.length) return null;
  const base = estrategiaAdaptativa(rows);
  const currentColor = seq[0].resultado;
  let currentLen = 1;
  while (currentLen < seq.length && seq[currentLen].resultado === currentColor) currentLen++;
  const bucket = Math.min(currentLen, 5);
  let winJ = 0, winB = 0, n = 0;
  // Encontra episódios antigos com a mesma cor e comprimento de streak; testa a ronda seguinte.
  for (let i = 1; i < seq.length; i++) {
    let len = 1;
    while (i + len < seq.length && seq[i + len].resultado === seq[i].resultado) len++;
    if (seq[i].resultado !== currentColor || Math.min(len, 5) !== bucket) continue;
    if (seq[i - 1].resultado === J) winJ++; else winB++;
    n++;
  }
  const priorJ = base?.cor === J ? 0.56 : 0.44;
  const pJ = n ? (winJ + 5 * priorJ) / (n + 5) : priorJ;
  return { cor: pJ >= 0.5 ? J : B, key: 'COMBINED_STREAK_REGIME', nome: `Combinada streak ${bucket} + contexto`, emoji: '📚', strength: Math.round(Math.max(pJ, 1 - pJ) * 100), tipo: 'ensemble' };
}
function estrategiaScoreRecencia(rows) {
  const seq = rows.filter((r) => isCol(r.resultado));
  if (!seq.length) return null;
  let j = 0, b = 0, weight = 1, scoreDiff = 0, scoreWeight = 0;
  for (const r of seq.slice(0, 10)) {
    if (r.resultado === J) j += weight; else b += weight;
    if (hasScore(r)) { scoreDiff += (r.pj - r.pb) * weight; scoreWeight += weight; }
    weight *= 0.86;
  }
  const pRecentJ = j / (j + b);
  const pScoreJ = scoreWeight ? 0.5 + 0.22 * Math.tanh((scoreDiff / scoreWeight) / 2) : pRecentJ;
  const pJ = 0.65 * pRecentJ + 0.35 * pScoreJ;
  return { cor: pJ >= 0.5 ? J : B, key: 'COMBINED_SCORE_RECENCY', nome: 'Combinada score + recência', emoji: '🧮', strength: Math.round(Math.max(pJ, 1 - pJ) * 100), tipo: 'ensemble' };
}
function runStrategies(rows) {
  const seq = rows.filter((r) => isCol(r.resultado));
  const ctx = { rows, seq };
  const out = [];
  for (const fn of STRATS) {
    try { const r = fn(ctx); if (r && isCol(r.cor)) out.push(r); } catch (_) {}
  }
  // Quatro modelos combinados são avaliados no mesmo backtest temporal das restantes estratégias.
  for (const fn of [estrategiaAdaptativa, estrategiaMarkov2, estrategiaStreakRegime, estrategiaScoreRecencia]) {
    try { const r = fn(rows); if (r && isCol(r.cor)) out.push(r); } catch (_) {}
  }
  return out;
}

/* ============================ BACKTEST ============================ */

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
  if (H[idx].t - H[i].t > CFG.gapMs) return null; // sem dado fiável: exclui da amostra
  return H[idx].resultado === cor; // empate e cor oposta são RED; sem recuperação
}

function backtest() {
  const H = S.hist;
  const per = {};
  const ini = 10, fim = H.length - 1;
  if (fim - ini < 30) return { per, baseRate: 0.50, validationN: 0 };
  // Últimos 25% do histórico são reservados como validação temporal.
  const validationStart = Math.floor(H.length * (1 - CFG.validationFraction));
  let validationWins = 0, validationN = 0;
  for (let i = ini; i < fim; i++) {
    const win = windowAt(H, i);
    if (win.length < 6) continue;
    for (const c of runStrategies(win)) {
      const v = evalEntry(H, i, c.cor);
      if (v === null) continue;
      const p = per[c.key] || (per[c.key] = { n: 0, w: 0, validationN: 0, validationW: 0 });
      p.n++; if (v) p.w++;
      if (i >= validationStart) {
        p.validationN++; if (v) p.validationW++;
        validationN++; if (v) validationWins++;
      }
    }
  }
  const baseRate = validationN ? validationWins / validationN : 0.50;
  return { per, baseRate, validationN };
}

function getStats(force = false) {
  if (force || !S.stats || S.hist.length - S.statsAtLen >= 30) {
    S.stats = backtest();
    S.statsAtLen = S.hist.length;
  }
  return S.stats;
}

/* ============================ CONFIANÇA ============================ */

function bayesianRate(w, n, priorRate = 0.50, priorWeight = 10) {
  return (w + priorRate * priorWeight) / (n + priorWeight);
}
function confianca(cand, ctx) {
  const stats = ctx.stats;
  const p = stats.per[cand.key] || { validationN: 0, validationW: 0 };
  const baseRate = Number.isFinite(stats.baseRate) ? stats.baseRate : 0.50;
  const btRate = bayesianRate(p.validationW, p.validationN, baseRate, 6);
  const live = S.signals.filter((x) => x.key === cand.key && RESULTADOS_APURADOS.has(x.status));
  const liveW = live.filter((x) => x.status === 'GREEN').length;
  const liveRate = live.length >= 3 ? bayesianRate(liveW, live.length, btRate, 4) : null;
  const btSamples = p.validationN + 6;
  const liveSamples = live.length >= 3 ? live.length + 4 : 0;
  const observed = ((btRate * btSamples) + ((liveRate ?? btRate) * liveSamples)) / (btSamples + liveSamples);

  let conf = Math.round(observed * 100);

  return Math.max(0, Math.min(100, conf));
}

/* ============================ SUSPENSÃO (persistente) ============================ */

function isSuspended(key) {
  const u = SUSPENDED[key];
  if (!u) return false;
  if (Date.now() > u) { delete SUSPENDED[key]; dbStateSet('suspended_v24', JSON.stringify(Object.entries(SUSPENDED))); return false; }
  return true;
}
function maybeSuspend(key) {
  const recent = S.signals.filter((x) => x.key === key && RESULTADOS_APURADOS.has(x.status)).slice(-CFG.suspendLossStreak);
  if (recent.length < CFG.suspendLossStreak) return;
  if (recent.every((x) => x.status !== 'GREEN')) {
    SUSPENDED[key] = Date.now() + CFG.suspendMs;
    dbStateSet('suspended_v24', JSON.stringify(Object.entries(SUSPENDED)));
    log('SUSPENSO', { key, consecutiveLosses: recent.length });
  }
}

/* ============================ POLÍTICA CONSERVADORA ============================ */
/* ============================ GATE ============================ */

function pendentes() { return S.signals.filter((s) => !FECHADOS.has(s.status)); }

function podeGerar() {
  if (!CFG.ativos) { R.ultimoMotivo = 'desativado'; return false; }
  if (R.erroAPI) { R.ultimoMotivo = 'API com erro; aguardando sincronização válida'; return false; }
  if (pendentes().length >= CFG.maxPending) { R.ultimoMotivo = 'max pending'; return false; }
  const now = Date.now();
  if (S.lastSignalAt && now - S.lastSignalAt < CFG.minGapMs) {
    R.ultimoMotivo = `gap ${Math.ceil((CFG.minGapMs - (now - S.lastSignalAt)) / 1000)}s`;
    return false;
  }
  const last = S.hist[S.hist.length - 1];
  if (!last) { R.ultimoMotivo = 'sem hist'; return false; }
  if (now - last.t > CFG.maxDataAgeMs) { R.ultimoMotivo = 'dados API desatualizados'; return false; }
  if (now - last.t < CFG.minSettledAgeMs) {
    R.ultimoMotivo = `rodada recente ${Math.ceil((CFG.minSettledAgeMs - (now - last.t)) / 1000)}s`;
    return false;
  }
  return true;
}

function liveHitRate(key) {
  let n = 0, w = 0;
  for (const x of S.signals) {
    if (x.key !== key || !RESULTADOS_APURADOS.has(x.status)) continue;
    n++; if (x.status === 'GREEN') w++;
  }
  return n >= 5 ? w / n : null;
}

/* ============================ GERAR SINAL ============================ */

async function tentarSinal() {
  if (!podeGerar()) return;

  const tier = 0;
  R.tier = tier;
  const rows = S.hist.slice(-30).reverse();
  const stats = getStats();
  const seq = rows.filter((r) => isCol(r.resultado));
  const regime = detectRegime(seq);
  const cands = runStrategies(rows); // nenhuma regra é eliminada por taxa ou tamanho da amostra
  if (!cands.length) { R.ultimoMotivo = 'sem resultados Jogador/Banca no histórico recente'; return; }

  // Votos ponderados por validação temporal, com prior neutro para estratégias sem amostra.
  // A combinação não presume independência perfeita; serve para ordenar as previsões.
  const votes = { [J]: 0, [B]: 0 };
  const scored = cands.map((c) => {
    const conf = confianca(c, { stats, regime });
    const p = stats.per[c.key];
    const n = p?.validationN || 0;
    const reliability = Math.max(0.25, Math.min(0.85, 0.50 + (conf / 100 - 0.50) * (n ? 1 : 0.35)));
    const weight = reliability * Math.sqrt(Math.max(1, Math.min(100, n)));
    votes[c.cor] += weight;
    return { ...c, conf, voteWeight: weight, validationN: n };
  });
  let winningColor = votes[J] >= votes[B] ? J : B;
  // Sem historial para diferenciar votos, prefere a combinada adaptativa.
  const winning = scored.filter((c) => c.cor === winningColor)
    .sort((a, b) => b.voteWeight - a.voteWeight || b.conf - a.conf || b.strength - a.strength);
  const selected = winning.find((c) => c.key === 'COMBINADA_ADAPTATIVA') || winning[0];
  const support = winning.length;
  const winningWeight = winning.reduce((sum, c) => sum + c.voteWeight, 0);
  // Score é média das estimativas históricas dos modelos do lado escolhido;
  // não é a percentagem de votos nem uma promessa de acerto.
  const combinedConf = winningWeight
    ? Math.round(winning.reduce((sum, c) => sum + c.conf * c.voteWeight, 0) / winningWeight)
    : selected.conf;
  const melhor = {
    ...selected,
    conf: Math.max(0, Math.min(100, combinedConf)),
    agree: support,
    nome: support > 1 ? `Ensemble ${support} estratégias · ${selected.nome}` : selected.nome
  };

  const reservedId = S.nextId;
  const sinalPreliminar = {
    id: reservedId, cor: melhor.cor, key: melhor.key, nome: melhor.nome, emoji: melhor.emoji,
    conf: melhor.conf, tipo: melhor.tipo, status: 'PENDENTE', tier,
    t: 0, criadoEm: 0, agree: melhor.agree,
    gale1MsgId: null, gale2MsgId: null, galeLevel: 0
  };

  const analisando = await tgSendRaw(fmtAnalisando());
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
    await dbStateSet('nextId_v24', String(S.nextId));
    await dbStateSet('lastSignalAt_v24', String(agora));

    log('SINAL', { id: sinalPreliminar.id, cor: sinalPreliminar.cor, conf: sinalPreliminar.conf, key: sinalPreliminar.key, tier });
  } finally {
    if (analisando.ok) await tgDelete(analisando.messageId);
  }
}

/* ============================ FECHAR ============================ */

async function fecharSinal(s, status, gale) {
  s.status = status;
  s.gale = gale;
  s.fechadoEm = Date.now();

  if (s.gale1MsgId) { await tgDelete(s.gale1MsgId); s.gale1MsgId = null; }
  if (s.gale2MsgId) { await tgDelete(s.gale2MsgId); s.gale2MsgId = null; }

  if (status === 'VOID') {
    ALLTIME.void++; DAILY.void++;
    await dbStateSet('daily_v24', JSON.stringify(DAILY));
    await dbSignalUpdate(s);
    await tgSendRaw(fmtResult(s));
    await tgSendRaw(fmtHistorico());
    return; // sem rodada observável: não é GREEN nem RED
  }
  const bucket = status === 'GREEN' ? 'green' : status === 'G1' ? 'g1' : status === 'G2' ? 'g2'
    : status === 'EMPATE' ? 'empate' : status === 'EG1' ? 'eg1' : status === 'EG2' ? 'eg2' : 'loss';
  ALLTIME[bucket]++; ALLTIME.total++;
  DAILY[bucket]++; DAILY.total++;
  if (status === 'LOSS') {
    S.curStreak = 0;
  } else {
    S.curStreak++;
    if (S.curStreak > DAILY.bestStreak) DAILY.bestStreak = S.curStreak;
  }
  await dbStateSet('curStreak_v24', String(S.curStreak));
  await dbStateSet('daily_v24', JSON.stringify(DAILY));

  await dbSignalUpdate(s);
  await tgSendRaw(fmtResult(s));
  await tgSendRaw(fmtHistorico());
  maybeSuspend(s.key);
}

/* ============================ APURAÇÃO — UMA RODADA, SEM GALE ============================ */
async function apurar() {
  const now = Date.now();
  for (const s of S.signals) {
    if (FECHADOS.has(s.status)) continue;
    const after = S.hist.filter((r) => r.t > s.t).sort((a, b) => a.t - b.t);
    if (after.length) {
      const first = after[0];
      // Se a rodada seguinte não chegou dentro da janela, sinal expira em RED.
      if (first.t - s.t > CFG.gapMs) { await fecharSinal(s, 'VOID', 0); continue; }
      await fecharSinal(s, first.resultado === s.cor ? 'GREEN' : 'LOSS', 0);
    } else if (now - s.t >= CFG.apuraMaxMs) {
      await fecharSinal(s, 'VOID', 0);
    }
  }
}

/* ============================ SYNC ADAPTATIVO ============================ */
/* FIX do bug "passa uma rodada e não atualiza": o tick roda a CADA 1 SEGUNDO.
   Quando há sinal pendente ou a mesa está quente, a API é consultada a cada 1s
   (anteriormente: só a cada 3s). */

function currentSyncMs() {
  if (pendentes().length > 0) return CFG.syncFastMs;
  const last = S.hist[S.hist.length - 1];
  if (last && Date.now() - last.t < 45000) return CFG.syncFastMs; // mesa quente
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
    await apurar();    // apura a cada 1s — só a primeira rodada conta
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
    versao: '24.0', deployId: DEPLOY_ID, maxGale: CFG.maxGale, lock: R.lockOk,
    tier: R.tier, fonteAPI: R.fonteAPI + 1, fontes: API_SOURCES.length,
    hist: S.hist.length, sinais: S.signals.length, pendentes: pendentes().length,
    diario: { ...DAILY, taxa: dailyTaxa() }, geral: ALLTIME,
    ultimaSync: R.ultimaSync, ultimaRodada: newest ? R.ultimaRodadaNova : null,
    apiStale: newest ? Date.now() - newest.t > CFG.maxDataAgeMs : true,
    erroAPI: R.erroAPI, erroDB: R.erroDB,
    uptime: Math.round(process.uptime())
  });
});
app.get('/status', requireAdmin, (req, res) => {
  res.json({ motivo: R.ultimoMotivo, R, DAILY, ALLTIME, CFG, suspenso: Object.keys(SUSPENDED) });
});
app.get('/sinais', requireAdmin, (req, res) => {
  const n = Math.min(100, Math.max(1, parseInt(req.query.n, 10) || 20));
  res.json(S.signals.slice(-n).reverse());
});
app.get('/stats', requireAdmin, (req, res) => {
  const wins = dailyWins();
  const gw = ALLTIME.green;
  res.json({
    diario: {
      ...DAILY, wins, taxa: DAILY.total > 0 ? wins / DAILY.total : null,
      taxaReal: DAILY.total > 0 ? DAILY.green / DAILY.total : null, sequenciaAtual: S.curStreak
    },
    geral: { ...ALLTIME, wins: gw, taxa: ALLTIME.total > 0 ? gw / ALLTIME.total : null }
  });
});
app.get('/calibracao', requireAdmin, (req, res) => {
  const buckets = { '0-49': { n: 0, w: 0 }, '50-59': { n: 0, w: 0 }, '60-69': { n: 0, w: 0 }, '70-79': { n: 0, w: 0 }, '80-100': { n: 0, w: 0 } };
  for (const s of S.signals) {
    if (!RESULTADOS_APURADOS.has(s.status)) continue;
    const b = s.conf >= 80 ? '80-100' : s.conf >= 70 ? '70-79' : s.conf >= 60 ? '60-69' : s.conf >= 50 ? '50-59' : '0-49';
    buckets[b].n++;
    if (s.status === 'GREEN') buckets[b].w++;
  }
  res.json(buckets);
});

/* ============================ START ============================ */

async function start() {
  for (const [name, value] of Object.entries({ BOT_TOKEN, CHAT_ID, ADMIN_TOKEN, DATABASE_URL })) {
    if (!value) throw new Error(`Variável obrigatória em falta: ${name}`);
  }
  if (!/^postgres(?:ql)?:\/\//i.test(DATABASE_URL)) throw new Error('DATABASE_URL inválido');
  log('BOOT', { versao: '24.0', deployId: DEPLOY_ID, fontes: API_SOURCES.length });
  await dbInit();
  if (!pool) throw new Error('Não foi possível inicializar PostgreSQL; arranque cancelado');

  const locked = await acquireLock();
  if (!locked) throw new Error('Outra instância do bot está ativa (advisory lock). Encerrando.');

  await dbLoadState();
  await sync();
  getStats(true);

  // Sinais PENDENTES são apurados em apurar() apenas contra a primeira rodada posterior.

  server = app.listen(PORT, () => log('HTTP', `porta ${PORT}`));
  // tick a CADA 1 SEGUNDO — sync interno é adaptativo (1s quente / 3s normal)
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
