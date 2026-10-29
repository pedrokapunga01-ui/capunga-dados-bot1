'use strict';

/**
 * CAPUNGA DADOS — BAC BO · v7
 *
 * Mudanças face à v6:
 *  - API substituída por scraping direto do Tipminer (Puppeteer). Lê TODAS as rodadas.
 *  - Confiança com Wilson Lower Bound + amostra mínima (não inflaciona com poucas amostras).
 *  - "Alta confiança" agora é estatisticamente real: 90% exibido = taxa real ≥90% (IC 95%).
 *  - BOT_INSTANCE: cada deploy novo (novo Render service) começa com histórico limpo.
 *  - Sinal expirado REMOVIDO: sinal só fecha quando uma rodada decide.
 *  - Mensagens mais simples e curtas.
 */

const express = require('express');
const axios = require('axios');
const puppeteer = require('puppeteer');
const { Pool } = require('pg');

/* ============================ CONFIG ============================ */

const num = (name, def, min, max) => {
  const n = Number.parseInt(process.env[name], 10);
  return Number.isFinite(n) ? Math.max(min, Math.min(max, n)) : def;
};
const bool = (v, def) =>
  v === undefined || v === '' ? def : ['true', '1', 'yes', 'sim', 'on'].includes(String(v).trim().toLowerCase());

const PORT = Number(process.env.PORT || 10000);
const BOT_TOKEN = process.env.BOT_TOKEN || '';
const CHAT_ID = process.env.CHAT_ID || '';
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || '';
const DATABASE_URL = process.env.DATABASE_URL || '';

// Identidade do bot: muda isto (ou cria novo serviço no Render) para começar com histórico limpo
const BOT_INSTANCE = (process.env.BOT_INSTANCE || process.env.RENDER_INSTANCE_ID || 'default').trim();
const TIPMINER_URL = process.env.TIPMINER_URL || 'https://www.tipminer.com/br/historico/evolution/bac-bo';

const MAX_GALE = 2;
const CFG = {
  ativos: bool(process.env.SINAIS_ATIVOS, true),
  intervaloSinalMs: num('INTERVALO_SINAL_MS', 165000, 0, 3600000),
  minConf: num('MIN_CONFIANCA', 90, 0, 99),       // agora 90 (era 88)
  minAmostra: num('MIN_AMOSTRA', 30, 5, 500),     // amostras mínimas para emitir sinal
  syncMs: num('SYNC_MS', 3000, 1500, 30000),      // 3s = tempo real
  sinalInicial: num('SINAL_INICIAL', 1704, 1, 100000000),
  maxHist: 4000,
  gapMs: 6 * 60 * 1000,
  frescoMs: 5 * 60 * 1000
};

const J = 'Jogador';
const B = 'Banca';
const E = 'Empate';
const isCol = (x) => x === J || x === B;
const opp = (c) => (c === J ? B : c === B ? J : null);

/* ============================ ESTADO ============================ */

const S = {
  hist: [],
  seen: new Set(),
  signals: [],
  nextId: CFG.sinalInicial,
  lastSignalAt: 0,
  stats: null,
  statsAtLen: -1
};

const R = {
  iniciadoEm: new Date().toISOString(),
  botInstance: BOT_INSTANCE,
  ultimaSync: null,
  ultimaRodadaNova: null,
  novosUltimoCiclo: 0,
  ciclosSemNovos: 0,
  erroScraper: null,
  erroDB: null,
  erroTelegram: null,
  ultimoMotivo: null,
  ultimoSinal: null,
  telegramPendentes: 0
};

let pool = null;
let server = null;
let timer = null;
let busy = false;
let stopping = false;

// Scraper state
let scraperBrowser = null;
let scraperPage = null;
let lastSeq = [];
let roundCounter = 0;
let lastRoundSeenAt = 0;

/* ============================ UTILS ============================ */

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function log(tag, data) {
  const p = typeof data === 'string' ? data : JSON.stringify(data);
  console.log(`[${new Date().toISOString()}] [${tag}] ${p}`);
}

const fmtHora = new Intl.DateTimeFormat('pt-PT', {
  timeZone: 'Africa/Luanda',
  hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false
});
const fmtDia = new Intl.DateTimeFormat('en-CA', {
  timeZone: 'Africa/Luanda',
  year: 'numeric', month: '2-digit', day: '2-digit'
});
const hora = (ms = Date.now()) => fmtHora.format(new Date(ms));
const dia = (ms = Date.now()) => fmtDia.format(new Date(ms));
const esc = (s) => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

const corTxt = (c) => (c === B ? '🔴 <b>BANCA</b>' : c === J ? '🔵 <b>JOGADOR</b>' : '🟡 <b>EMPATE</b>');
const corCurta = (c) => (c === B ? '🔴 Banca' : c === J ? '🔵 Jogador' : '🟡 Empate');

/* ============================ SCRAPER ============================ */

async function scraperInit() {
  scraperBrowser = await puppeteer.launch({
    headless: 'new',
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-gpu',
      '--disable-blink-features=AutomationControlled'
    ]
  });
  scraperPage = await scraperBrowser.newPage();
  await scraperPage.setUserAgent(
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ' +
    '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
  );
  await scraperPage.setViewport({ width: 1280, height: 800 });
  await scraperPage.setRequestInterception(true);
  scraperPage.on('request', (req) => {
    const type = req.resourceType();
    if (['image', 'font', 'media'].includes(type)) req.abort();
    else req.continue();
  });
  log('SCRAPER_READY', TIPMINER_URL);
}

async function scraperClose() {
  if (scraperBrowser) {
    try { await scraperBrowser.close(); } catch (_) {}
    scraperBrowser = null;
    scraperPage = null;
  }
}

async function ensureScraper() {
  if (scraperPage) return true;
  try { await scraperInit(); return true; }
  catch (e) {
    R.erroScraper = e.message;
    log('SCRAPER_LAUNCH_ERRO', e.message);
    return false;
  }
}

async function scraperFetchSeq() {
  if (!scraperPage) return [];
  try {
    await scraperPage.goto(TIPMINER_URL, { waitUntil: 'networkidle2', timeout: 30000 });
    await scraperPage.waitForSelector('.cell__result', { timeout: 15000 });
    const seq = await scraperPage.evaluate(() => {
      const out = [];
      for (const cell of document.querySelectorAll('.cell__result')) {
        const t = cell.textContent.trim().toUpperCase();
        let v = null;
        if (t === 'P' || t === 'PLAYER' || t === 'JOGADOR') v = 'J';
        else if (t === 'B' || t === 'BANKER' || t === 'BANCA') v = 'B';
        else if (t === 'T' || t === 'TIE' || t === 'EMPATE') v = 'E';
        if (v) out.push(v);
      }
      return out;
    });
    R.erroScraper = null;
    return seq;
  } catch (e) {
    R.erroScraper = e.message;
    log('SCRAPER_FETCH_ERRO', e.message);
    if (/Target closed|Session closed|detached/i.test(e.message)) await scraperClose();
    return [];
  }
}

/** Descobre quantas rodadas novas há no topo da lista (newest-first) */
function diffSeq(newSeq, oldSeq) {
  if (!newSeq.length) return 0;
  if (!oldSeq.length) return newSeq.length;
  for (let overlap = Math.min(8, oldSeq.length); overlap >= 3; overlap--) {
    const maxOff = Math.min(newSeq.length - overlap, 15);
    for (let off = 1; off <= maxOff; off++) {
      let ok = true;
      for (let i = 0; i < overlap; i++) {
        if (newSeq[off + i] !== oldSeq[i]) { ok = false; break; }
      }
      if (ok) return off;
    }
  }
  return newSeq[0] === oldSeq[0] ? 0 : 1;
}

async function fetchRounds() {
  if (!(await ensureScraper())) return [];
  const newSeq = await scraperFetchSeq();
  if (!newSeq.length) return [];

  const nNew = diffSeq(newSeq, lastSeq);
  const now = Date.now();
  const gapDetected = lastRoundSeenAt > 0 && (now - lastRoundSeenAt) > CFG.gapMs;

  const newRounds = [];
  for (let i = nNew - 1; i >= 0; i--) {
    roundCounter++;
    newRounds.push({
      id: `r${roundCounter}`,
      resultado: newSeq[i] === 'J' ? J : newSeq[i] === 'B' ? B : E,
      t: now - i * 20000,
      pj: null, pb: null, mult: null,
      gapBefore: i === nNew - 1 && gapDetected
    });
  }

  lastSeq = newSeq;
  if (nNew > 0) lastRoundSeenAt = now;
  return newRounds;
}

/* ============================ ESTRATÉGIAS ============================ */

function streak(seq) {
  if (!seq.length) return 0;
  let n = 0;
  for (const r of seq) { if (r.resultado === seq[0].resultado) n++; else break; }
  return n;
}

const hasScore = (r) => r && Number.isFinite(r.pj) && Number.isFinite(r.pb);
const margem = (r) => Math.abs(r.pj - r.pb);
const scoreVencedor = (r) => (r.resultado === J ? r.pj : r.pb);

const STRATS = [
  function repeticao({ seq }) {
    const n = streak(seq);
    if (n < 3 || n > 8) return null;
    return { cor: seq[0].resultado, key: `REPETICAO_${Math.min(n, 6)}`, nome: `Repetição x${n}`, emoji: '🔁' };
  },
  function quebra({ seq }) {
    const n = streak(seq);
    if (n < 4 || n > 9) return null;
    return { cor: opp(seq[0].resultado), key: `QUEBRA_${Math.min(n, 7)}`, nome: `Quebra x${n}`, emoji: '💥' };
  },
  function surf({ seq }) {
    if (seq.length < 4) return null;
    let L = 1;
    while (L < seq.length && seq[L].resultado !== seq[L - 1].resultado) L++;
    if (L < 4) return null;
    return { cor: opp(seq[0].resultado), key: `SURF_${Math.min(L, 6)}`, nome: `Surf ${L}`, emoji: '🌊' };
  },
  function doisDois({ seq }) {
    if (seq.length < 4) return null;
    const [a, b, c, d] = seq.map((x) => x.resultado);
    if (a === b && c === d && a !== c) return { cor: c, key: 'DOIS_DOIS', nome: '2+2', emoji: '♟️' };
    return null;
  },
  function tresUm({ seq }) {
    if (seq.length < 4) return null;
    const [a, b, c, d] = seq.map((x) => x.resultado);
    if (a !== b && b === c && c === d) return { cor: a, key: 'TRES_UM', nome: 'Quebra 3→1', emoji: '🧩' };
    return null;
  },
  function forca({ seq }) {
    const s = seq.slice(0, 12);
    if (s.length < 10) return null;
    const j = s.filter((x) => x.resultado === J).length;
    const pj = j / s.length;
    const dom = pj >= 0.5 ? J : B;
    const pct = Math.max(pj, 1 - pj);
    if (pct < 0.67) return null;
    const bucket = pct >= 0.75 ? 75 : pct >= 0.7 ? 70 : 67;
    return { cor: dom, key: `FORCA_${bucket}`, nome: `Força ${Math.round(pct * 100)}%`, emoji: '💪' };
  },
  function reversao({ seq }) {
    const s = seq.slice(0, 20);
    if (s.length < 16) return null;
    const j = s.filter((x) => x.resultado === J).length;
    const pj = j / s.length;
    const pct = Math.max(pj, 1 - pj);
    if (pct < 0.65) return null;
    const dom = pj >= 0.5 ? J : B;
    return { cor: opp(dom), key: 'REVERSAO_20', nome: `Retorno ${Math.round(pct * 100)}%`, emoji: '⚖️' };
  },
  // Numéricas — precisam de scores; com Tipminer auto-desativam (sem pj/pb)
  function numMargem({ rows }) {
    const r = rows[0];
    if (!r || !isCol(r.resultado) || !hasScore(r)) return null;
    if (margem(r) < 5) return null;
    return { cor: r.resultado, key: 'NUM_MARGEM_5', nome: `Margem ${r.pj}×${r.pb}`, emoji: '🔢' };
  },
  function numMargem3({ seq }) {
    if (seq.length < 3) return null;
    const t = seq.slice(0, 3);
    if (!t.every((r) => hasScore(r) && margem(r) >= 3 && r.resultado === t[0].resultado)) return null;
    return { cor: t[0].resultado, key: 'NUM_MARGEM_3X', nome: '3 vit. margem ≥3', emoji: '📐' };
  },
  function numFraca({ seq }) {
    if (seq.length < 2) return null;
    const [a, b] = seq;
    if (!hasScore(a) || !hasScore(b)) return null;
    if (a.resultado !== b.resultado) return null;
    if (scoreVencedor(a) > 6 || scoreVencedor(b) > 6) return null;
    return { cor: opp(a.resultado), key: 'NUM_FRACA', nome: '2 fracas', emoji: '📉' };
  },
  function numAlta({ rows }) {
    const r = rows[0];
    if (!r || !isCol(r.resultado) || !hasScore(r)) return null;
    if (scoreVencedor(r) < 11 || margem(r) < 3) return null;
    return { cor: r.resultado, key: 'NUM_ALTA_11', nome: `Forte ${r.pj}×${r.pb}`, emoji: '🚀' };
  },
  function numTendencia({ rows }) {
    const s = rows.slice(0, 6).filter(hasScore);
    if (s.length < 6) return null;
    const mj = s.reduce((a, r) => a + r.pj, 0) / s.length;
    const mb = s.reduce((a, r) => a + r.pb, 0) / s.length;
    const diff = mj - mb;
    if (Math.abs(diff) < 1.3) return null;
    return { cor: diff > 0 ? J : B, key: `NUM_TEND_${Math.abs(diff) >= 2 ? 2 : 1}`, nome: `Média ${mj.toFixed(1)}×${mb.toFixed(1)}`, emoji: '📊' };
  },
  function numPosEmpate({ rows, seq }) {
    if (!rows[0] || rows[0].resultado !== E || !seq.length) return null;
    return { cor: seq[0].resultado, key: 'NUM_POS_EMPATE', nome: 'Pós-empate', emoji: '🎯' };
  }
];

function runStrategies(rows) {
  const ctx = { rows, seq: rows.filter((r) => isCol(r.resultado)) };
  const out = [];
  for (const fn of STRATS) {
    try {
      const c = fn(ctx);
      if (c && isCol(c.cor)) out.push(c);
    } catch (_) {}
  }
  return out;
}

/* ============================ BACKTEST / CONFIANÇA ============================ */

function windowAt(H, i) {
  const rows = [];
  for (let j = i; j >= 0 && rows.length < 24; j--) {
    if (j < i && H[j + 1].gapBefore) break;
    rows.push(H[j]);
  }
  return rows;
}

function evalEntry(H, i, cor) {
  for (let k = 0; k <= MAX_GALE; k++) {
    const idx = i + 1 + k;
    if (idx >= H.length) return null;
    if (H[idx].gapBefore) return null;
    const r = H[idx].resultado;
    if (r === E || r === cor) return true;
  }
  return false;
}

/**
 * Wilson Lower Bound a 95%.
 * Reporta "quantos % de certeza o histórico nos dá de que a taxa real é ≥ X".
 * Com poucas amostras, o valor cai agressivamente (ao contrário da média simples).
 */
function wilsonLower(w, n, z = 1.96) {
  if (n === 0) return 0;
  const p = w / n;
  const denom = 1 + (z * z) / n;
  const centre = (p + (z * z) / (2 * n)) / denom;
  const margin = (z * Math.sqrt((p * (1 - p)) / n + (z * z) / (4 * n * n))) / denom;
  return Math.max(0, centre - margin);
}

function backtest() {
  const H = S.hist;
  const per = {};
  let bw = 0, bn = 0;
  for (let i = 8; i < H.length - 1; i++) {
    const win = windowAt(H, i);
    if (win.length < 6) continue;
    for (const cor of [J, B]) {
      const v = evalEntry(H, i, cor);
      if (v === null) continue;
      bn++;
      if (v) bw++;
    }
    for (const c of runStrategies(win)) {
      const v = evalEntry(H, i, c.cor);
      if (v === null) continue;
      const p = per[c.key] || (per[c.key] = { n: 0, w: 0 });
      p.n++;
      if (v) p.w++;
    }
  }
  const REF = 0.9;
  const prior = (bw + 200 * REF) / (bn + 200);
  return { per, prior, baseN: bn };
}

function getStats() {
  if (!S.stats || S.hist.length - S.statsAtLen >= 20) {
    S.stats = backtest();
    S.statsAtLen = S.hist.length;
  }
  return S.stats;
}

/**
 * Confiança = Wilson lower bound da taxa histórica do setup
 * (green em G0, G1, G2 ou empate, dentro do gale).
 *
 * Regra dura: se amostra < minAmostra → confiança = 0 (não emite).
 *
 * Efeito: "confiança 91%" significa, com 95% de confiança estatística,
 * que a taxa real de green (em qualquer gale/empate) é ≥ 91%.
 */
function confOf(st, key) {
  const e = st.per[key] || { n: 0, w: 0 };
  if (e.n < CFG.minAmostra) return { conf: 0, n: e.n, insuficiente: true };
  const conf = Math.round(wilsonLower(e.w, e.n, 1.96) * 100);
  return { conf: Math.min(99, conf), n: e.n };
}

function pickEntry(rows) {
  const st = getStats();
  const cands = runStrategies(rows)
    .map((c) => {
      const e = confOf(st, c.key);
      return { ...c, confianca: e.conf, amostra: e.n, base: Math.round(st.prior * 100) };
    })
    .filter((c) => c.confianca >= CFG.minConf);

  if (!cands.length) return null;

  const grupos = { [J]: [], [B]: [] };
  for (const c of cands) grupos[c.cor].push(c);

  const best = (arr) => (arr.length ? Math.max(...arr.map((c) => c.confianca)) + 0.5 * (arr.length - 1) : -1);
  const sJ = best(grupos[J]);
  const sB = best(grupos[B]);

  if (sJ >= 0 && sB >= 0 && Math.abs(sJ - sB) < 1) return { conflito: true };

  const arr = sJ > sB ? grupos[J] : grupos[B];
  arr.sort((a, b) => b.confianca - a.confianca || b.amostra - a.amostra);
  return { ...arr[0], consenso: arr.length };
}

/* ============================ BASE DE DADOS ============================ */

let dbChain = Promise.resolve();
function dbq(fn) {
  if (!pool) return;
  dbChain = dbChain
    .then(fn)
    .catch((e) => { R.erroDB = e.message; log('DB_ERRO', e.message); });
}

async function ensureSchema() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS bb_rodadas (
      id TEXT PRIMARY KEY,
      resultado TEXT NOT NULL,
      fim TIMESTAMPTZ NOT NULL,
      pj INTEGER, pb INTEGER, mult TEXT,
      gap_before BOOLEAN NOT NULL DEFAULT FALSE
    )`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_bb_rodadas_fim ON bb_rodadas (fim DESC)`);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS bb_sinais (
      id BIGINT PRIMARY KEY,
      criado_em TIMESTAMPTZ NOT NULL,
      cor TEXT NOT NULL,
      estrategia TEXT, nome TEXT,
      confianca INTEGER,
      estado TEXT NOT NULL,
      gale INTEGER NOT NULL DEFAULT 0,
      empate BOOLEAN NOT NULL DEFAULT FALSE,
      resultado_final TEXT,
      resolvido_em TIMESTAMPTZ,
      base_id TEXT
    )`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_bb_sinais_criado ON bb_sinais (criado_em DESC)`);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS bb_meta (
      key TEXT PRIMARY KEY,
      value TEXT
    )`);
}

/** Se o BOT_INSTANCE mudou (novo deploy/serviço), limpa o histórico */
async function checkBotInstance() {
  if (!pool) return;
  const r = await pool.query(`SELECT value FROM bb_meta WHERE key = 'instance'`);
  const stored = r.rows[0]?.value || '';
  if (stored && stored !== BOT_INSTANCE) {
    log('BOT_INSTANCE_MUDOU_LIMPANDO', { anterior: stored, atual: BOT_INSTANCE });
    await pool.query('TRUNCATE bb_rodadas');
    await pool.query('TRUNCATE bb_sinais');
  }
  await pool.query(
    `INSERT INTO bb_meta (key, value) VALUES ('instance', $1)
     ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`,
    [BOT_INSTANCE]
  );
}

async function loadFromDB() {
  const rr = await pool.query(
    `SELECT id, resultado, fim, pj, pb, mult, gap_before FROM bb_rodadas ORDER BY fim DESC LIMIT $1`,
    [CFG.maxHist]
  );
  S.hist = rr.rows.map((x) => ({
    id: x.id,
    resultado: x.resultado,
    t: new Date(x.fim).getTime(),
    pj: x.pj, pb: x.pb, mult: x.mult,
    gapBefore: Boolean(x.gap_before)
  })).sort((a, b) => a.t - b.t);
  S.seen = new Set(S.hist.map((x) => x.id));

  const sr = await pool.query(
    `SELECT * FROM bb_sinais WHERE criado_em >= NOW() - INTERVAL '36 hours' ORDER BY id ASC`
  );
  S.signals = sr.rows.map((x) => ({
    id: Number(x.id),
    criadoMs: new Date(x.criado_em).getTime(),
    cor: x.cor, key: x.estrategia, nome: x.nome, confianca: x.confianca,
    estado: x.estado, gale: x.gale, empate: x.empate,
    resultadoFinal: x.resultado_final,
    resolvidoMs: x.resolvido_em ? new Date(x.resolvido_em).getTime() : null,
    baseId: x.base_id
  }));

  const mx = await pool.query(`SELECT COALESCE(MAX(id), 0)::bigint AS m FROM bb_sinais`);
  const m = Number(mx.rows[0].m);
  S.nextId = m > 0 ? m + 1 : CFG.sinalInicial;

  const lastS = S.signals[S.signals.length - 1];
  if (lastS) S.lastSignalAt = lastS.criadoMs;
}

function dbSaveRound(r) {
  dbq(() =>
    pool.query(
      `INSERT INTO bb_rodadas (id, resultado, fim, pj, pb, mult, gap_before)
       VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (id) DO NOTHING`,
      [r.id, r.resultado, new Date(r.t), r.pj, r.pb, r.mult, Boolean(r.gapBefore)]
    )
  );
}

function dbSaveSignal(s) {
  dbq(() =>
    pool.query(
      `INSERT INTO bb_sinais (id, criado_em, cor, estrategia, nome, confianca, estado, gale, empate, resultado_final, resolvido_em, base_id)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
       ON CONFLICT (id) DO UPDATE SET
         estado = EXCLUDED.estado, gale = EXCLUDED.gale, empate = EXCLUDED.empate,
         resultado_final = EXCLUDED.resultado_final, resolvido_em = EXCLUDED.resolvido_em`,
      [
        s.id, new Date(s.criadoMs), s.cor, s.key, s.nome, s.confianca, s.estado, s.gale,
        Boolean(s.empate), s.resultadoFinal || null, s.resolvidoMs ? new Date(s.resolvidoMs) : null, s.baseId || null
      ]
    )
  );
}

/* ============================ TELEGRAM ============================ */

const tgQueue = [];
let tgRunning = false;

const stripHtml = (t) => t.replace(/<[^>]+>/g, '').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&amp;/g, '&');

async function tgPost(text, plain = false) {
  if (!BOT_TOKEN || !CHAT_ID) {
    R.erroTelegram = 'BOT_TOKEN ou CHAT_ID não configurado';
    return { ok: true, skipped: true };
  }
  try {
    const payload = { chat_id: CHAT_ID, text: plain ? stripHtml(text) : text, disable_web_page_preview: true };
    if (!plain) payload.parse_mode = 'HTML';
    const resp = await axios.post(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, payload, { timeout: 15000 });
    if (resp.data?.ok === true) { R.erroTelegram = null; return { ok: true }; }
    R.erroTelegram = JSON.stringify(resp.data);
    return { ok: false };
  } catch (e) {
    const data = e.response?.data;
    R.erroTelegram = data ? JSON.stringify(data) : e.message;
    if (e.response?.status === 429) return { ok: false, retryAfter: Number(data?.parameters?.retry_after) || 5 };
    if (e.response?.status === 400 && /parse entities/i.test(String(data?.description))) return { ok: false, parseError: true };
    return { ok: false };
  }
}

async function tgRun() {
  tgRunning = true;
  while (tgQueue.length) {
    const m = tgQueue[0];
    const r = await tgPost(m.text, m.plain);
    if (r.ok) { tgQueue.shift(); R.telegramPendentes = tgQueue.length; await sleep(450); continue; }
    if (r.retryAfter) { await sleep(r.retryAfter * 1000 + 300); continue; }
    if (r.parseError) { m.plain = true; continue; }
    m.tries = (m.tries || 0) + 1;
    if (m.tries >= 6) { log('TELEGRAM_DESCARTADA', { erro: R.erroTelegram }); tgQueue.shift(); }
    else await sleep(1500 * m.tries);
    R.telegramPendentes = tgQueue.length;
  }
  tgRunning = false;
}

function tgSend(text) {
  tgQueue.push({ text, tries: 0, plain: false });
  R.telegramPendentes = tgQueue.length;
  if (!tgRunning) tgRun().catch((e) => { tgRunning = false; log('TG_FILA_ERRO', e.message); });
}

/* ============================ PLACAR / MENSAGENS ============================ */

function placar() {
  const hoje = dia();
  const L = S.signals.filter((s) => dia(s.criadoMs) === hoje);
  const c = (st) => L.filter((s) => s.estado === st).length;
  const green = c('GREEN'), g1 = c('G1'), g2 = c('G2'), emp = c('EMPATE'), loss = c('LOSS');
  const wins = green + g1 + g2 + emp;
  const fechados = wins + loss;
  const taxa = fechados ? (wins / fechados) * 100 : 0;
  return { green, g1, g2, emp, loss, wins, fechados, taxa, pendentes: c('PENDENTE') };
}

function placarLinha() {
  const p = placar();
  if (!p.fechados) return '📊 Hoje: —';
  return `📊 Hoje: <b>${p.wins}G</b> · ${p.loss}R · <b>${p.taxa.toFixed(0)}%</b>`;
}

function msgEntrada(s) {
  return [
    `🎯 <b>SINAL #${s.id}</b>`,
    '',
    `🎲 <b>Apostar:</b> ${corTxt(s.cor)}`,
    `📊 <b>Confiança:</b> ${s.confianca}%`,
    `🧠 ${esc(s.nome)}`,
    '',
    `⏱ ${hora(s.criadoMs)} · Gale ${MAX_GALE} · 🟡 = Green`
  ].join('\n');
}

function msgGale(s, r) {
  return [
    `🔁 <b>GALE ${s.gale}</b> · #${s.id}`,
    `Saiu ${corCurta(r.resultado)}`,
    `➡️ Continua ${corCurta(s.cor)}`
  ].join('\n');
}

function msgGreen(s, r) {
  const titulo =
    s.estado === 'EMPATE' ? '🟡 <b>GREEN (EMPATE)</b>'
      : s.estado === 'GREEN' ? '✅ <b>GREEN</b>'
        : s.estado === 'G1' ? '⚡ <b>GREEN G1</b>'
          : '🔥 <b>GREEN G2</b>';
  return [
    `${titulo} · #${s.id}`,
    `${corCurta(s.cor)} → ${corCurta(r.resultado)}`,
    '',
    placarLinha()
  ].join('\n');
}

function msgRed(s, r) {
  return [
    `🟥 <b>RED</b> · #${s.id}`,
    `${corCurta(s.cor)} → ${corCurta(r.resultado)}`,
    '',
    placarLinha()
  ].join('\n');
}

/* ============================ MOTOR DE SINAIS ============================ */

const pendente = () => S.signals.find((s) => s.estado === 'PENDENTE') || null;

function resolver(r) {
  const s = pendente();
  if (!s) return;
  if (r.t <= s.baseT || r.id === s.baseId) return;

  if (r.resultado === E) return fechar(s, r, 'EMPATE', true);
  if (r.resultado === s.cor) return fechar(s, r, s.gale === 0 ? 'GREEN' : s.gale === 1 ? 'G1' : 'G2', false);
  if (s.gale < MAX_GALE) {
    s.gale += 1;
    dbSaveSignal(s);
    tgSend(msgGale(s, r));
    log('GALE', { id: s.id, gale: s.gale });
    return;
  }
  s.estado = 'LOSS';
  s.resultadoFinal = r.resultado;
  s.resolvidoMs = Date.now();
  dbSaveSignal(s);
  tgSend(msgRed(s, r));
  log('RED', { id: s.id });
}

function fechar(s, r, estado, empate) {
  s.estado = estado;
  s.empate = empate;
  s.resultadoFinal = r.resultado;
  s.resolvidoMs = Date.now();
  dbSaveSignal(s);
  tgSend(msgGreen(s, r));
  log('GREEN', { id: s.id, estado });
}

function criarSinal(ultima) {
  if (!CFG.ativos) return (R.ultimoMotivo = 'sinais_desativados');
  if (pendente()) return (R.ultimoMotivo = 'sinal_em_andamento');
  if (Date.now() - S.lastSignalAt < CFG.intervaloSinalMs) return (R.ultimoMotivo = 'aguardando_cadencia');
  if (Date.now() - ultima.t > CFG.frescoMs) return (R.ultimoMotivo = 'rodada_desatualizada');

  const rows = windowAt(S.hist, S.hist.length - 1);
  if (rows.length < 6) return (R.ultimoMotivo = 'historico_curto');

  const e = pickEntry(rows);
  if (!e) return (R.ultimoMotivo = 'sem_setup_confiavel');
  if (e.conflito) return (R.ultimoMotivo = 'estrategias_em_conflito');

  const s = {
    id: S.nextId++,
    criadoMs: Date.now(),
    cor: e.cor, key: e.key, nome: e.nome, emoji: e.emoji,
    confianca: e.confianca, base: e.base, consenso: e.consenso,
    estado: 'PENDENTE', gale: 0, empate: false,
    baseId: ultima.id, baseT: ultima.t
  };
  S.signals.push(s);
  if (S.signals.length > 1500) S.signals.splice(0, S.signals.length - 1500);
  S.lastSignalAt = s.criadoMs;
  R.ultimoSinal = { id: s.id, cor: s.cor, nome: s.nome, confianca: s.confianca, hora: hora(s.criadoMs) };
  R.ultimoMotivo = null;
  dbSaveSignal(s);
  tgSend(msgEntrada(s));
  log('SINAL', { id: s.id, cor: s.cor, key: s.key, confianca: s.confianca });
}

/* ============================ SINCRONIZAÇÃO ============================ */

async function syncOnce() {
  if (busy || stopping) return { ignorada: true };
  busy = true;
  try {
    const rounds = await fetchRounds();
    const warm = S.hist.length === 0;
    let lastT = S.hist.length ? S.hist[S.hist.length - 1].t : 0;

    const fresh = rounds.filter((r) => !S.seen.has(r.id));
    let live = 0, ultimaViva = null, desordem = false;

    for (const r of fresh) {
      S.seen.add(r.id);
      const eAoVivo = !warm && r.t > lastT;
      if (r.t < lastT) desordem = true;
      S.hist.push(r);
      dbSaveRound(r);
      if (r.t > lastT) lastT = r.t;
      if (eAoVivo) {
        live++;
        ultimaViva = r;
        resolver(r);
      }
    }

    if (desordem) S.hist.sort((a, b) => a.t - b.t);
    if (S.hist.length > CFG.maxHist) S.hist.splice(0, S.hist.length - CFG.maxHist);

    if (fresh.length) { R.ultimaRodadaNova = new Date().toISOString(); R.ciclosSemNovos = 0; }
    else R.ciclosSemNovos++;
    R.novosUltimoCiclo = fresh.length;

    if (warm && fresh.length) {
      R.ultimoMotivo = 'base_historica_inicializada';
      log('BASE_INICIAL', { rodadas: fresh.length });
    } else if (live > 0 && ultimaViva) {
      criarSinal(ultimaViva);
    }

    R.ultimaSync = new Date().toISOString();
    return { sucesso: true, novas: fresh.length, aoVivo: live, motivo: R.ultimoMotivo };
  } catch (e) {
    log('SYNC_ERRO', e.message);
    return { sucesso: false, erro: e.message };
  } finally {
    busy = false;
  }
}

function agendar() {
  if (stopping) return;
  timer = setTimeout(async () => { await syncOnce(); agendar(); }, CFG.syncMs);
}

/* ============================ ROTAS ============================ */

const app = express();
app.disable('x-powered-by');
app.use(express.json({ limit: '100kb' }));

function admin(req, res, next) {
  if (!ADMIN_TOKEN) return res.status(503).json({ sucesso: false, erro: 'ADMIN_TOKEN não configurado' });
  if (req.get('x-admin-token') !== ADMIN_TOKEN) return res.status(401).json({ sucesso: false, erro: 'Token inválido' });
  next();
}

app.get('/', (_req, res) => res.json({
  nome: 'Capunga Dados — Bac Bo v7',
  estado: 'online',
  horaAngola: hora(),
  botInstance: BOT_INSTANCE,
  estrategias: STRATS.length,
  fonte: 'tipminer (scraper)'
}));

app.get('/health', (_req, res) => res.json({
  sucesso: true,
  horaAngola: hora(),
  botInstance: BOT_INSTANCE,
  iniciadoEm: R.iniciadoEm,
  ultimaSync: R.ultimaSync,
  ultimaRodadaNova: R.ultimaRodadaNova,
  ciclosSemNovos: R.ciclosSemNovos,
  motivoSemSinal: R.ultimoMotivo,
  erroScraper: R.erroScraper,
  erroTelegram: R.erroTelegram,
  erroDB: R.erroDB,
  telegramPendentes: R.telegramPendentes
}));

app.get('/diagnostico', (_req, res) => {
  const st = S.stats || { per: {}, prior: null, baseN: 0 };
  res.json({
    sucesso: true,
    horaAngola: hora(),
    botInstance: BOT_INSTANCE,
    telegramConfigurado: Boolean(BOT_TOKEN && CHAT_ID),
    dbConfigurada: Boolean(pool),
    rodadasEmMemoria: S.hist.length,
    ultimasRodadas: S.hist.slice(-12).reverse().map((r) => ({ r: r.resultado, hora: hora(r.t), gap: r.gapBefore })),
    sinalPendente: pendente(),
    ultimoSinal: R.ultimoSinal,
    motivoSemSinal: R.ultimoMotivo,
    proximoSinalEmSeg: Math.max(0, Math.round((S.lastSignalAt + CFG.intervaloSinalMs - Date.now()) / 1000)),
    taxaBaseHistorica: st.prior ? Number((st.prior * 100).toFixed(1)) : null,
    amostrasBacktest: st.baseN,
    configuracao: { ...CFG, maxGale: MAX_GALE },
    placar: placar()
  });
});

app.get('/sinais', (req, res) => {
  const n = Math.min(Math.max(Number.parseInt(req.query.limit, 10) || 50, 1), 500);
  res.json({ sucesso: true, sinais: S.signals.slice(-n).reverse() });
});

app.get('/estatisticas', (_req, res) => {
  const st = getStats();
  const porEstrategia = {};
  for (const s of S.signals) {
    const k = s.key || 'N/D';
    const e = porEstrategia[k] || (porEstrategia[k] = { sinais: 0, greens: 0, reds: 0 });
    if (s.estado === 'LOSS') { e.reds++; e.sinais++; }
    else if (['GREEN', 'G1', 'G2', 'EMPATE'].includes(s.estado)) { e.greens++; e.sinais++; }
  }
  const backtest = Object.entries(st.per).map(([key, v]) => ({
    key, amostras: v.n,
    acertoPercent: Number(((v.w / v.n) * 100).toFixed(1)),
    confiancaExibida: confOf(st, key).conf
  })).sort((a, b) => b.confiancaExibida - a.confiancaExibida);
  res.json({ sucesso: true, placarDia: placar(), taxaBasePercent: Number((st.prior * 100).toFixed(1)), backtest, ao_vivo_por_estrategia: porEstrategia });
});

app.post('/admin/sincronizar', admin, async (_req, res) => res.json(await syncOnce()));

app.post('/admin/telegram-teste', admin, async (_req, res) => {
  const r = await tgPost(`🎰 <b>CAPUNGA DADOS</b>\n✅ Ligação OK\n⏱ ${hora()}`);
  res.status(r.ok ? 200 : 502).json({ sucesso: r.ok, erro: R.erroTelegram });
});

app.post('/admin/reset', admin, async (req, res) => {
  if (req.body?.confirmar !== 'APAGAR') return res.status(400).json({ sucesso: false, erro: 'Envie {"confirmar":"APAGAR"}' });
  try {
    S.hist = []; S.seen = new Set(); S.signals = []; S.stats = null; S.statsAtLen = -1; S.lastSignalAt = 0;
    S.nextId = CFG.sinalInicial;
    lastSeq = []; roundCounter = 0; lastRoundSeenAt = 0;
    if (pool) { await dbChain; await pool.query('TRUNCATE bb_rodadas'); await pool.query('TRUNCATE bb_sinais'); }
    res.json({ sucesso: true, mensagem: 'Base apagada.' });
  } catch (e) { res.status(500).json({ sucesso: false, erro: e.message }); }
});

app.use((req, res) => res.status(404).json({ sucesso: false, erro: 'Rota não encontrada', caminho: req.path }));

/* ============================ ARRANQUE ============================ */

async function start() {
  if (DATABASE_URL) {
    pool = new Pool({
      connectionString: DATABASE_URL,
      ssl: /localhost|127\.0\.0\.1/i.test(DATABASE_URL) ? false : { rejectUnauthorized: false },
      max: 4, idleTimeoutMillis: 20000, connectionTimeoutMillis: 12000
    });
    pool.on('error', (e) => { R.erroDB = e.message; log('POOL_ERRO', e.message); });
    try {
      await ensureSchema();
      await checkBotInstance();
      await loadFromDB();
      log('DB', { botInstance: BOT_INSTANCE, rodadas: S.hist.length, sinaisRecentes: S.signals.length, proximoId: S.nextId });
    } catch (e) {
      R.erroDB = e.message;
      log('DB_INDISPONIVEL_SEGUE_EM_MEMORIA', e.message);
      pool = null;
    }
  } else {
    log('DB', 'DATABASE_URL ausente: a funcionar apenas em memória');
  }

  await ensureScraper();

  server = app.listen(PORT, '0.0.0.0', () => log('START', {
    porta: PORT, botInstance: BOT_INSTANCE, estrategias: STRATS.length, maxGale: MAX_GALE,
    cadenciaMs: CFG.intervaloSinalMs, minConfianca: CFG.minConf, minAmostra: CFG.minAmostra,
    syncMs: CFG.syncMs, fonte: 'tipminer-scraper'
  }));

  const first = await syncOnce();
  log('PRIMEIRA_SYNC', first);
  agendar();
}

async function shutdown() {
  if (stopping) return;
  stopping = true;
  if (timer) clearTimeout(timer);
  try { await dbChain; } catch (_) {}
  await scraperClose();
  if (server) await new Promise((r) => server.close(r));
  if (pool) await pool.end().catch(() => {});
  process.exit(0);
}

process.on('unhandledRejection', (e) => log('UNHANDLED_REJECTION', String(e)));
process.on('uncaughtException', (e) => log('UNCAUGHT_EXCEPTION', e.message || String(e)));
process.on('SIGTERM', () => shutdown().catch(() => process.exit(1)));
process.on('SIGINT', () => shutdown().catch(() => process.exit(1)));

start().catch((e) => { console.error('[FATAL]', e); process.exit(1); });