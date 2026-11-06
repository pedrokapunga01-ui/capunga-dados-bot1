'use strict';

/**
 * CAPUNGA DADOS — BAC BO
 * Atualização incremental resiliente; não executa apostas.
 * Resultados são aleatórios: estratégias são heurísticas, sem garantia de acerto.
 */
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const axios = require('axios');
const crypto = require('node:crypto');
const { Pool } = require('pg');

const app = express();
app.disable('x-powered-by');
app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Referrer-Policy', 'no-referrer');
  res.setHeader('X-Frame-Options', 'DENY');
  next();
});
const allowedOrigins = (process.env.CORS_ORIGINS || '*').split(',').map(x => x.trim()).filter(Boolean);
app.use(cors({
  origin(origin, callback) {
    if (!origin || allowedOrigins.includes('*') || allowedOrigins.includes(origin)) return callback(null, true);
    return callback(null, false);
  },
  allowedHeaders: ['Content-Type', 'x-admin-token'],
  methods: ['GET', 'POST', 'OPTIONS']
}));
app.use(express.json({ limit: '1mb' }));

const PORT = Number(process.env.PORT || 10000);
const DATABASE_URL = process.env.DATABASE_URL || '';
const BOT_TOKEN = process.env.BOT_TOKEN || '';
const CHAT_ID = process.env.CHAT_ID || '';
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || '';
const DEFAULT_API_BACBO = 'https://api-cs.casino.org/svc-evolution-game-events/api/bacbo';
const configuredApiUrls = (process.env.BACBO_API_URLS || '').split(',').map(x => x.trim()).filter(Boolean);
const primaryApiUrl = String(process.env.BACBO_API_URL || '').trim() || DEFAULT_API_BACBO;
const API_URLS = [...new Set([primaryApiUrl, ...configuredApiUrls])];
const API_TOKEN = process.env.BACBO_API_TOKEN || '';
const LOCAL_DAY_START_SQL = `(date_trunc('day', NOW() AT TIME ZONE 'Africa/Luanda') AT TIME ZONE 'Africa/Luanda')`;

function boolEnv(value, fallback = false) {
  if (value === undefined || value === '') return fallback;
  return ['true', '1', 'yes', 'sim', 'on'].includes(String(value).trim().toLowerCase());
}
function intEnv(name, fallback, min, max) {
  const n = Number.parseInt(process.env[name], 10);
  return Number.isFinite(n) ? Math.max(min, Math.min(max, n)) : fallback;
}
function floatEnv(name, fallback, min, max) {
  const n = Number.parseFloat(process.env[name]);
  return Number.isFinite(n) ? Math.max(min, Math.min(max, n)) : fallback;
}

const FILTER_LEVELS = {
  conservador: { minScore: 72, minIntervalMs: 180000 },
  medio: { minScore: 60, minIntervalMs: 120000 },
  frequente: { minScore: 52, minIntervalMs: 60000 }
};
const requestedFilterLevel = String(process.env.NIVEL_FILTRO_SINAIS || 'medio').trim().toLowerCase();
const filterLevel = FILTER_LEVELS[requestedFilterLevel] ? requestedFilterLevel : 'medio';

const CONFIG = {
  nivelFiltroSinais: filterLevel,
  pontuacaoMinimaSinal: intEnv('PONTUACAO_MINIMA_SINAL', FILTER_LEVELS[filterLevel].minScore, 50, 90),
  sinaisAtivos: boolEnv(process.env.SINAIS_ATIVOS, true),
  streakMin: intEnv('STREAK_MIN', 3, 2, 12),
  maxGale: intEnv('MAX_GALE', 2, 0, 2),
  maxRedsDia: intEnv('MAX_REDS_DIA', 10, 1, 50),
  intervaloMinimoMs: intEnv('INTERVALO_MINIMO_SINAL_MS', FILTER_LEVELS[filterLevel].minIntervalMs, 30000, 3600000),
  intervaloMaximoSinalMs: intEnv('INTERVALO_MAXIMO_SINAL_MS', 180000, 120000, 600000),
  intervaloSyncMs: intEnv('INTERVALO_SYNC_MS', 15000, 5000, 60000),
  apiTimeoutMs: intEnv('API_TIMEOUT_MS', 30000, 5000, 90000),
  apiMaxPages: intEnv('API_MAX_PAGES', 4, 1, 10),
  apiPageSize: intEnv('API_PAGE_SIZE', 10, 1, 50),
  modeloAmostraMinima: intEnv('MODELO_AMOSTRA_MINIMA', 12, 4, 100),
  modeloConfiancaMinima: floatEnv('MODELO_CONFIANCA_MINIMA', 0.57, 0.5, 0.9),
  modeloContextoMaximo: intEnv('MODELO_CONTEXTO_MAXIMO', 3, 1, 5),
  confiancaMinimaPct: intEnv('CONFIANCA_MINIMA_PCT', 80, 80, 99),
  confiancaAmostraMinima: intEnv('CONFIANCA_AMOSTRA_MINIMA', 60, 20, 500),
  confiancaMinRondas: intEnv('CONFIANCA_MIN_RONDAS', 300, 100, 2000),
  confiancaHistoricoRondas: intEnv('CONFIANCA_HISTORICO_RONDAS', 1200, 300, 5000),
  staleAfterMs: intEnv('STALE_AFTER_MS', 120000, 30000, 3600000)
};

const pool = DATABASE_URL ? new Pool({
  connectionString: DATABASE_URL,
  ssl: /localhost|127\.0\.0\.1/i.test(DATABASE_URL)
    ? false
    : { rejectUnauthorized: boolEnv(process.env.PGSSL_REJECT_UNAUTHORIZED, true) },
  max: 5,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 15000
}) : null;

const runtime = {
  iniciadoEm: new Date().toISOString(), sincronizacaoEmCurso: false,
  ultimaSincronizacao: null, ultimaRodadaNova: null, ultimoIdNovo: null,
  ultimaHoraDaAPI: null, quantidadeAPI: 0, itensRecebidosAPI: 0,
  duplicadosUltimoCiclo: 0, novosUltimoCiclo: 0, erroSincronizacao: null,
  ultimoSinal: null, ultimaMensagemTelegram: null, ultimoErroTelegram: null,
  ultimoHTTPAPI: null, ultimoErroAPI: null, ultimaRodadaDB: null,
  ultimoIdDB: null, ciclosSemNovos: 0, motivoSemSinal: null,
  ultimaEstrategia: null, fonteAtual: null, latenciaAPIMs: null,
  falhasAPIConsecutivas: 0, ultimoErroFonte: null, ultimaAvaliacaoConfianca: null
};

let schemaPromise = null;
let server = null;
let syncTimer = null;

function log(tag, data) {
  console.log(`[${tag}]`, typeof data === 'string' ? data : JSON.stringify(data));
}
function horaAngola() {
  return new Intl.DateTimeFormat('pt-PT', {
    timeZone: 'Africa/Luanda', dateStyle: 'short', timeStyle: 'medium', hour12: false
  }).format(new Date());
}
function safeSource(url) {
  try { return new URL(url).host; } catch (_) { return 'fonte-configurada'; }
}
function corDisplay(cor) {
  if (cor === 'Banca') return '🔴 VERMELHO 🔴';
  if (cor === 'Jogador') return '🔵 AZUL 🔵';
  return String(cor || '');
}
function oposta(cor) {
  return cor === 'Banca' ? 'Jogador' : cor === 'Jogador' ? 'Banca' : null;
}
function normalizeOutcome(value) {
  if (value && typeof value === 'object') value = value.outcome ?? value.resultado ?? value.winner ?? value.name;
  const s = String(value ?? '').normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .trim().toLowerCase().replace(/[\s_-]+/g, '');
  if (['player', 'jogador', 'playerwin', 'playerwins', 'playerwon', 'playerwinner', 'blue'].includes(s)) return 'Jogador';
  if (['banker', 'banca', 'bankerwin', 'bankerwins', 'bankerwon', 'bankerwinner', 'red', 'vermelho'].includes(s)) return 'Banca';
  if (['tie', 'draw', 'empate', 'equal', 'push', 'tied', 'yellow'].includes(s)) return 'Empate';
  return null;
}
function validDate(value) {
  if (!value) return null;
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
}
function extractItems(body) {
  if (Array.isArray(body)) return body;
  if (!body || typeof body !== 'object') return [];
  const candidates = [
    body.data, body.results, body.items, body.resultados, body.rounds, body.events,
    body.history, body.records, body.content, body.data?.results, body.data?.items,
    body.data?.resultados, body.data?.rounds, body.data?.events, body.data?.history,
    body.data?.records, body.data?.content, body.data?.data
  ];
  return candidates.find(Array.isArray) || [];
}
function normalizeRound(item) {
  const d = item?.data || item?.round || item || {};
  const r = d.result && typeof d.result === 'object' ? d.result
    : item?.result && typeof item.result === 'object' ? item.result : {};
  const status = String(d.status ?? d.estado ?? item?.status ?? item?.estado ?? '').toLowerCase();
  const inicio = validDate(d.startedAt ?? d.startTime ?? d.started_at ?? d.inicio ?? item?.startedAt ?? item?.inicio);
  const fim = validDate(d.settledAt ?? d.endTime ?? d.endedAt ?? d.finishedAt ?? d.fim ?? item?.settledAt ?? item?.fim);
  const rawOutcome = r.outcome ?? r.resultado ?? r.winner ?? d.outcome ?? d.resultado ?? d.winner ??
    item?.outcome ?? item?.resultado ?? item?.winner ??
    (typeof d.result === 'string' ? d.result : null) ?? (typeof item?.result === 'string' ? item.result : null);
  const resultado = normalizeOutcome(rawOutcome);
  const resolvida = ['resolved', 'settled', 'finished', 'complete', 'completed', 'resolvido', 'concluido', 'finalizado']
    .some(w => status.includes(w)) || Boolean(fim);
  const id = String(d.id ?? d.roundId ?? d.round_id ?? d.eventId ?? item?.id ?? item?.roundId ?? item?.round_id ?? item?.eventId ?? '').trim();
  if (!id || !resultado || !resolvida || !fim) return null;
  const scoreJogadorRaw = r.playerDice?.score ?? d.playerDice?.score ?? d.playerScore ?? d.player_score ?? d.pontos_jogador;
  const scoreBancaRaw = r.bankerDice?.score ?? d.bankerDice?.score ?? d.bankerScore ?? d.banker_score ?? d.pontos_banca;
  const scoreJogador = scoreJogadorRaw == null ? null : Number(scoreJogadorRaw);
  const scoreBanca = scoreBancaRaw == null ? null : Number(scoreBancaRaw);
  const multiplicador = r.multiplier ?? d.multiplier ?? d.multiplicador ?? null;
  return {
    id, resultado, estado: 'Resolved', inicio: inicio || fim, fim,
    pontos_jogador: Number.isFinite(scoreJogador) ? scoreJogador : null,
    pontos_banca: Number.isFinite(scoreBanca) ? scoreBanca : null,
    multiplicador: multiplicador == null ? null : String(multiplicador)
  };
}

async function telegramSend(texto) {
  if (!BOT_TOKEN || !CHAT_ID) {
    runtime.ultimoErroTelegram = 'BOT_TOKEN ou CHAT_ID não configurado';
    return false;
  }
  try {
    const response = await axios.post(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`,
      { chat_id: CHAT_ID, text: texto, disable_web_page_preview: true }, { timeout: 15000 });
    if (response.data?.ok === true) {
      runtime.ultimaMensagemTelegram = new Date().toISOString();
      runtime.ultimoErroTelegram = null;
      return true;
    }
    runtime.ultimoErroTelegram = JSON.stringify(response.data);
    return false;
  } catch (error) {
    runtime.ultimoErroTelegram = error.response?.data ? JSON.stringify(error.response.data) : error.message;
    console.error('[TELEGRAM]', runtime.ultimoErroTelegram);
    return false;
  }
}

async function ensureSchema() {
  if (!pool) throw new Error('DATABASE_URL não configurada');
  if (schemaPromise) return schemaPromise;
  schemaPromise = (async () => {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS bacbo_resultados (
        id TEXT PRIMARY KEY,
        resultado TEXT NOT NULL,
        estado TEXT NOT NULL DEFAULT 'Resolved',
        inicio TIMESTAMPTZ,
        fim TIMESTAMPTZ,
        pontos_jogador INTEGER,
        pontos_banca INTEGER,
        multiplicador TEXT,
        guardado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        seq BIGSERIAL
      )
    `);
    await pool.query(`ALTER TABLE bacbo_resultados ADD COLUMN IF NOT EXISTS guardado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_bacbo_fim ON bacbo_resultados (fim DESC)`);

    await pool.query(`
      CREATE TABLE IF NOT EXISTS sinais (
        id BIGSERIAL PRIMARY KEY,
        criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        cor TEXT NOT NULL,
        estado TEXT NOT NULL DEFAULT 'PENDENTE',
        gale INTEGER NOT NULL DEFAULT 0,
        resultado_id TEXT,
        resultado_final TEXT,
        empates INTEGER NOT NULL DEFAULT 0,
        estrategia TEXT
      )
    `);
    await pool.query(`ALTER TABLE sinais ADD COLUMN IF NOT EXISTS estrategia TEXT`);
    await pool.query(`ALTER TABLE sinais ADD COLUMN IF NOT EXISTS resultado_final TEXT`);
    await pool.query(`ALTER TABLE sinais ADD COLUMN IF NOT EXISTS empates INTEGER NOT NULL DEFAULT 0`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_sinais_estado ON sinais (estado)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_sinais_criado ON sinais (criado_em DESC)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_sinais_loss_dia ON sinais (criado_em) WHERE estado = 'LOSS'`);
    log('DB', { tabelasPreparadas: true });
  })();
  try { await schemaPromise; } catch (e) { schemaPromise = null; throw e; }
}

function parseNextLink(link, currentUrl) {
  const match = String(link || '').match(/<([^>]+)>\s*;\s*rel="?next"?/i);
  if (!match) return null;
  try {
    const next = new URL(match[1], currentUrl);
    if (next.origin !== new URL(currentUrl).origin || !/^https?:$/.test(next.protocol)) return null;
    return next.toString();
  } catch (_) { return null; }
}
function withPage(url, page) {
  const next = new URL(url);
  next.searchParams.set('page', String(page));
  next.searchParams.set('size', String(CONFIG.apiPageSize));
  return next.toString();
}
function coversWatermark(rounds, latestDB) {
  if (!latestDB) return true;
  const watermark = latestDB.fim ? new Date(latestDB.fim).getTime() : null;
  return rounds.some(round => String(round.id) === String(latestDB.id) ||
    (watermark != null && new Date(round.fim).getTime() <= watermark));
}
async function getApiPage(url) {
  let attempt = 0;
  while (true) {
    try {
      const started = Date.now();
      const response = await axios.get(url, {
        timeout: CONFIG.apiTimeoutMs,
        maxContentLength: 5 * 1024 * 1024,
        headers: {
          Accept: 'application/json',
          'User-Agent': 'CapungaDadosBot/2.0 (+history-sync)',
          ...(API_TOKEN ? { Authorization: `Bearer ${API_TOKEN}` } : {})
        }
      });
      runtime.ultimoHTTPAPI = response.status;
      runtime.latenciaAPIMs = Date.now() - started;
      return response;
    } catch (error) {
      const status = error.response?.status;
      const transient = !status || status === 408 || status === 425 || status === 429 || status >= 500;
      if (attempt >= 1 || !transient) throw error;
      attempt++;
      await new Promise(resolve => setTimeout(resolve, 800));
    }
  }
}
async function fetchRounds(latestDB = null) {
  const errors = [];
  for (const baseUrl of API_URLS) {
    try {
      let url = baseUrl;
      let page = 0;
      const byId = new Map();
      let received = 0;
      let partialError = null;
      for (let n = 0; n < CONFIG.apiMaxPages; n++) {
        let response;
        try { response = await getApiPage(url); }
        catch (error) {
          if (n > 0 && byId.size) { partialError = `${safeSource(url)}: ${error.message || String(error)}`; break; }
          throw error;
        }
        const items = extractItems(response.data);
        received += items.length;
        for (const item of items) {
          const round = normalizeRound(item);
          if (round) byId.set(round.id, round);
        }
        if (coversWatermark([...byId.values()], latestDB) || !items.length) break;
        const next = parseNextLink(response.headers?.link, url);
        if (!next) break;
        page++;
        url = next;
      }
      const rounds = [...byId.values()].sort((a, b) => new Date(a.fim) - new Date(b.fim));
      if (latestDB && !coversWatermark(rounds, latestDB)) {
        const suffix = partialError ? `; página seguinte falhou: ${partialError}` : '';
        throw new Error(`Histórico incompleto: a fonte não alcançou o último resultado já guardado${suffix}`);
      }
      if (!rounds.length) throw new Error(`A fonte ${safeSource(baseUrl)} não devolveu rondas Bac Bo válidas`);
      runtime.fonteAtual = safeSource(baseUrl);
      runtime.itensRecebidosAPI = received;
      runtime.quantidadeAPI = rounds.length;
      runtime.ultimaHoraDaAPI = rounds.at(-1)?.fim || null;
      runtime.ultimoErroAPI = partialError;
      runtime.ultimoErroFonte = partialError;
      runtime.falhasAPIConsecutivas = 0;
      return rounds;
    } catch (error) {
      const status = error.response?.status ? ` HTTP ${error.response.status}` : '';
      errors.push(`\( {safeSource(baseUrl)} \){status}: ${error.message || String(error)}`);
      runtime.ultimoErroFonte = errors.at(-1);
      log('FONTE API FALHOU', errors.at(-1));
    }
  }
  runtime.ultimoErroAPI = errors.join(' | ');
  runtime.falhasAPIConsecutivas++;
  throw new Error(runtime.ultimoErroAPI || 'Nenhuma fonte Bac Bo configurada');
}

async function insertRound(round) {
  const result = await pool.query(`INSERT INTO bacbo_resultados
    (id, resultado, estado, inicio, fim, pontos_jogador, pontos_banca, multiplicador)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (id) DO NOTHING RETURNING id`,
  [round.id, round.resultado, round.estado, round.inicio, round.fim,
    round.pontos_jogador, round.pontos_banca, round.multiplicador]);
  return result.rows.length > 0;
}
async function getLatestFromDB() {
  const result = await pool.query(`SELECT id, resultado, fim FROM bacbo_resultados
    ORDER BY fim DESC NULLS LAST, seq DESC LIMIT 1`);
  return result.rows[0] || null;
}
async function getRecentOutcomes(limit = 500) {
  const result = await pool.query(`SELECT id, resultado, fim, pontos_jogador, pontos_banca, multiplicador
    FROM bacbo_resultados WHERE resultado IN ('Jogador','Banca')
    ORDER BY fim DESC NULLS LAST, seq DESC LIMIT $1`, [limit]);
  return result.rows;
}
async function getRecentRounds(limit = CONFIG.confiancaHistoricoRondas) {
  const result = await pool.query(`SELECT id, resultado, fim, pontos_jogador, pontos_banca, multiplicador
    FROM bacbo_resultados ORDER BY fim DESC NULLS LAST, seq DESC LIMIT $1`, [limit]);
  return result.rows;
}

/* Heurísticas transparentes; o modo contextual usa só observações anteriores. */
function streakLen(rows) {
  if (!rows.length || !['Jogador', 'Banca'].includes(rows[0].resultado)) return 0;
  let n = 0;
  for (const r of rows) { if (r.resultado !== rows[0].resultado) break; n++; }
  return n;
}
function stratRepeticao(rows) {
  const n = streakLen(rows);
  if (n < CONFIG.streakMin) return null;
  return { cor: rows[0].resultado, estrategia: 'REPETICAO', nome: `Repetição x${n}`, emoji: '🔁', prioridade: Math.min(90, 50 + n * 10) };
}
function stratQuebra(rows) {
  const n = streakLen(rows);
  if (n < CONFIG.streakMin + 1) return null;
  const cor = oposta(rows[0].resultado);
  return cor ? { cor, estrategia: 'QUEBRA', nome: `Quebra x${n}`, emoji: '💥', prioridade: Math.min(88, 55 + n * 8) } : null;
}
function stratSurf(rows) {
  if (rows.length < 4) return null;
  const a = rows[0].resultado, b = rows[1].resultado;
  if (!['Jogador', 'Banca'].includes(a) || !['Jogador', 'Banca'].includes(b) || a === b) return null;
  for (let i = 0; i < 4; i++) if (rows[i]?.resultado !== (i % 2 === 0 ? a : b)) return null;
  return { cor: b, estrategia: 'SURF', nome: 'Surf (alternado)', emoji: '🌊', prioridade: 78 };
}
function stratDoisDois(rows) {
  if (rows.length < 4) return null;
  const r = rows.map(x => x.resultado);
  if (r[0] === r[1] && r[2] === r[3] && r[0] !== r[2])
    return { cor: r[0], estrategia: 'DOIS_DOIS', nome: '2+2 (bloco)', emoji: '♟️', prioridade: 82 };
  if (r[0] !== r[1] && r[1] === r[2] && ['Jogador', 'Banca'].includes(r[0]))
    return { cor: r[0], estrategia: 'DOIS_DOIS', nome: '2+2 (completa par)', emoji: '♟️', prioridade: 75 };
  return null;
}
function stratForca(rows) {
  const sample = rows.slice(0, 12).filter(r => ['Jogador', 'Banca'].includes(r.resultado));
  if (sample.length < 8) return null;
  const j = sample.filter(r => r.resultado === 'Jogador').length;
  const b = sample.length - j;
  const pctJ = j / sample.length, pctB = b / sample.length;
  if (pctB >= 0.65) return { cor: 'Banca', estrategia: 'FORCA', nome: `Força Mesa Banca ${Math.round(pctB * 100)}%`, emoji: '💪', prioridade: 72 };
  if (pctJ >= 0.65) return { cor: 'Jogador', estrategia: 'FORCA', nome: `Força Mesa Jogador ${Math.round(pctJ * 100)}%`, emoji: '💪', prioridade: 72 };
  return null;
}
function stratNumerica(rows) {
  if (rows.length < 3) return null;
  const last = rows[0];
  const pj = last.pontos_jogador == null ? NaN : Number(last.pontos_jogador);
  const pb = last.pontos_banca == null ? NaN : Number(last.pontos_banca);
  if (Number.isFinite(pj) && Number.isFinite(pb)) {
    if (last.resultado === 'Jogador' && pj >= 10 && pj > pb)
      return { cor: 'Jogador', estrategia: 'NUMERICA', nome: `Numérica Alta J(${pj})`, emoji: '🔢', prioridade: 68 };
    if (last.resultado === 'Banca' && pb >= 10 && pb > pj)
      return { cor: 'Banca', estrategia: 'NUMERICA', nome: `Numérica Alta B(${pb})`, emoji: '🔢', prioridade: 68 };
  }
  let corDom = null, diffsOk = true;
  for (let i = 0; i < 3; i++) {
    const r = rows[i], a = r.pontos_jogador == null ? NaN : Number(r.pontos_jogador);
    const b = r.pontos_banca == null ? NaN : Number(r.pontos_banca);
    if (!Number.isFinite(a) || !Number.isFinite(b) || Math.abs(a - b) < 4) { diffsOk = false; break; }
    if (!corDom) corDom = r.resultado; else if (r.resultado !== corDom) diffsOk = false;
  }
  if (diffsOk && corDom) return { cor: corDom, estrategia: 'NUMERICA', nome: 'Numérica Diff≥4 x3', emoji: '🔢', prioridade: 70 };
  if (rows.length >= 2 && rows[0].resultado === rows[1].resultado && ['Jogador', 'Banca'].includes(rows[0].resultado)) {
    const r0 = rows[0], r1 = rows[1];
    const s0 = r0.resultado === 'Jogador' ? r0.pontos_jogador : r0.pontos_banca;
    const s1 = r1.resultado === 'Jogador' ? r1.pontos_jogador : r1.pontos_banca;
    if (s0 != null && s1 != null && Number(s0) <= 5 && Number(s1) <= 5)
      return { cor: oposta(r0.resultado), estrategia: 'NUMERICA', nome: 'Numérica Virada Baixa', emoji: '🔢', prioridade: 65 };
  }
  return null;
}
function stratModelo(rows) {
  const chronological = rows.slice().reverse().map(r => r.resultado).filter(x => ['Jogador', 'Banca'].includes(x));
  if (chronological.length < CONFIG.modeloAmostraMinima + 2) return null;
  const maxContext = Math.min(CONFIG.modeloContextoMaximo, chronological.length - 1);
  for (let k = maxContext; k >= 1; k--) {
    const currentStart = chronological.length - k;
    const context = chronological.slice(currentStart);
    let jogador = 0, banca = 0;
    for (let i = 0; i + k < currentStart; i++) {
      if (!context.every((x, j) => chronological[i + j] === x)) continue;
      const next = chronological[i + k];
      if (next === 'Jogador') jogador++; else if (next === 'Banca') banca++;
    }
    const samples = jogador + banca;
    if (samples < CONFIG.modeloAmostraMinima) continue;
    const cor = jogador > banca ? 'Jogador' : banca > jogador ? 'Banca' : null;
    if (!cor) continue;
    const observed = Math.max(jogador, banca) / samples;
    const smoothed = (Math.max(jogador, banca) + 1) / (samples + 2);
    if (smoothed < CONFIG.modeloConfiancaMinima) continue;
    return {
      cor, estrategia: 'MODELO_CONTEXTUAL', nome: `Contexto histórico (${k} resultados)`, emoji: '📊',
      prioridade: 60 + Math.min(12, Math.round((smoothed - 0.5) * 60)),
      amostras: samples, frequenciaObservada: Number((observed * 100).toFixed(1))
    };
  }
  return null;
}
function detectBestEntry(rows) {
  if (!rows || rows.length < 2) return null;
  const candidates = [stratRepeticao(rows), stratQuebra(rows), stratDoisDois(rows),
    stratSurf(rows), stratForca(rows), stratNumerica(rows), stratModelo(rows)].filter(Boolean);
  if (!candidates.length) return null;
  const order = { REPETICAO: 7, QUEBRA: 6, DOIS_DOIS: 5, SURF: 4, FORCA: 3, NUMERICA: 2, MODELO_CONTEXTUAL: 1 };
  candidates.sort((a, b) => b.prioridade - a.prioridade || (order[b.estrategia] || 0) - (order[a.estrategia] || 0));
  return candidates[0];
}

function wilsonLowerBound(wins, total, z = 1.96) {
  if (!Number.isFinite(total) || total <= 0) return 0;
  const n = total, p = Math.max(0, Math.min(1, wins / total)), z2 = z * z;
  const denominator = 1 + z2 / n;
  const center = p + z2 / (2 * n);
  const margin = z * Math.sqrt((p * (1 - p) + z2 / (4 * n)) / n);
  return Math.max(0, (center - margin) / denominator);
}
function luandaDayKey(value) {
  if (!value) return '';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return '';
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Africa/Luanda', year: 'numeric', month: '2-digit', day: '2-digit'
  }).formatToParts(date);
  const p = Object.fromEntries(parts.map(x => [x.type, x.value]));
  return `\( {p.year}- \){p.month}-${p.day}`;
}
function backtestConfidence(roundsNewestFirst, strategyFilter) {
  const rounds = roundsNewestFirst.slice().reverse().filter(r =>
    ['Jogador', 'Banca', 'Empate'].includes(r.resultado) && Number.isFinite(new Date(r.fim).getTime()));
  if (rounds.length < CONFIG.confiancaMinRondas) {
    return { eligible: false, confidencePct: null, wins: 0, losses: 0, samples: 0, motivo: 'historico_curto' };
  }
  let wins = 0, losses = 0, pending = null, lastSignalAt = null;
  const redsByDay = new Map();
  for (let i = Math.max(30, CONFIG.confiancaMinRondas); i < rounds.length; i++) {
    const round = rounds[i];
    if (pending) {
      const decision = settlementForRound(pending, round);
      if (decision.tipo === 'green' || decision.tipo === 'empate') {
        wins++;
        pending = null;
      } else if (decision.tipo === 'gale') {
        pending.gale = decision.gale;
        continue;
      } else {
        losses++;
        const day = luandaDayKey(round.fim);
        redsByDay.set(day, (redsByDay.get(day) || 0) + 1);
        pending = null;
      }
    }
    if (pending || i === rounds.length - 1 || !['Jogador', 'Banca'].includes(round.resultado)) continue;
    const at = new Date(round.fim).getTime();
    if (lastSignalAt != null && at - lastSignalAt < CONFIG.intervaloMinimoMs) continue;
    const day = luandaDayKey(round.fim);
    if ((redsByDay.get(day) || 0) >= CONFIG.maxRedsDia) continue;
    const history = rounds.slice(0, i + 1).filter(r => ['Jogador', 'Banca'].includes(r.resultado))
      .slice(-500).reverse();
    const entry = detectBestEntry(history);
    if (!entry || entry.estrategia !== strategyFilter) continue;
    pending = { cor: entry.cor, gale: 0 };
    lastSignalAt = at;
  }
  const samples = wins + losses;
  const confidencePct = samples ? Number((wilsonLowerBound(wins, samples) * 100).toFixed(1)) : null;
  return {
    eligible: samples >= CONFIG.confiancaAmostraMinima && confidencePct >= CONFIG.confiancaMinimaPct,
    confidencePct, wins, losses, samples,
    motivo: samples < CONFIG.confiancaAmostraMinima ? 'amostra_insuficiente' :
      confidencePct < CONFIG.confiancaMinimaPct ? 'limite_inferior_abaixo_do_minimo' : null
  };
}
function scoreSinal(entry, report) {
  const priority = Math.max(50, Math.min(90, Number(entry.prioridade) || 50));
  const strategyScore = 50 + (priority - 50) * 0.625;
  const samples = Number(report?.samples || 0);
  const historyScore = Number.isFinite(report?.confidencePct) ? report.confidencePct : 50;
  const historyWeight = samples >= CONFIG.confiancaAmostraMinima ? 0.65 : samples >= 20 ? 0.4 : samples >= 10 ? 0.2 : 0;
  return Math.round(strategyScore * (1 - historyWeight) + historyScore * historyWeight);
}
function fallbackCadencia(rows) {
  const sample = rows.filter(r => ['Jogador', 'Banca'].includes(r.resultado)).slice(0, 12);
  if (!sample.length) return null;
  const jogador = sample.filter(r => r.resultado === 'Jogador').length;
  const banca = sample.length - jogador;
  const cor = jogador === banca ? sample[0].resultado : jogador > banca ? 'Jogador' : 'Banca';
  return { cor, estrategia: 'CADENCIA_FALLBACK', nome: 'Fallback de cadência (baixa evidência)', emoji: '⏱️', prioridade: 50 };
}

async function redsToday() {
  const result = await pool.query(`SELECT COUNT(*)::int AS total FROM sinais
    WHERE estado = 'LOSS' AND criado_em >= ${LOCAL_DAY_START_SQL}`);
  return result.rows[0]?.total || 0;
}
async function getPlacarDia() {
  const result = await pool.query(`SELECT
    COUNT(*) FILTER (WHERE estado = 'GREEN')::int AS green,
    COUNT(*) FILTER (WHERE estado = 'G1')::int AS g1,
    COUNT(*) FILTER (WHERE estado = 'G2')::int AS g2,
    COUNT(*) FILTER (WHERE estado = 'LOSS')::int AS loss,
    COALESCE(SUM(empates), 0)::int AS empates
    FROM sinais WHERE criado_em >= ${LOCAL_DAY_START_SQL}
    AND estado IN ('GREEN','G1','G2','LOSS')`);
  const s = result.rows[0] || { green: 0, g1: 0, g2: 0, loss: 0, empates: 0 };
  const acertos = (s.green || 0) + (s.g1 || 0) + (s.g2 || 0);
  const finalizados = acertos + (s.loss || 0);
  const percent = finalizados ? Number((acertos / finalizados * 100).toFixed(1)) : 100;
  const recent = await pool.query(`SELECT estado FROM sinais WHERE criado_em >= ${LOCAL_DAY_START_SQL}
    AND estado IN ('GREEN','G1','G2','LOSS') ORDER BY criado_em DESC, id DESC LIMIT 50`);
  let greensSeguidos = 0;
  for (const row of recent.rows) { if (['GREEN', 'G1', 'G2'].includes(row.estado)) greensSeguidos++; else break; }
  return { green: s.green || 0, g1: s.g1 || 0, g2: s.g2 || 0, empates: s.empates || 0,
    loss: s.loss || 0, percent, greensSeguidos };
}
function formatPlacar(p) {
  const greens = (p.gs || 0) + (p.g1 || 0) + (p.g2 || 0) + (p.ge || 0);
  const percentagem = p.total ? `${(greens / p.total * 100).toFixed(1)}%` : '0.0%';
  return ['✨🎲 CAPUNGA DADOS 🎲✨', '📊 HISTÓRICO', `🔢 Total de Sinais: ${p.total || 0}`,
    `✅ Greens: ${greens}`, `🟢 GS — Sem Gale: ${p.gs || 0}`, `⚡ G1 — Gale 1: ${p.g1 || 0}`,
    `🔥 G2 — Gale 2: ${p.g2 || 0}`, `🟡 Empates: ${p.ge || 0}`, `❌ Loss: ${p.loss || 0}`,
    `📈 Percentagem: ${percentagem}`, `🔥 Estamos com ${p.greensSeguidos || 0} Greens seguidos`].join('\n');
}
async function getHistoricoSinais() {
  const result = await pool.query(`SELECT
    COUNT(*) FILTER (WHERE estado IN ('GREEN','G1','G2','LOSS'))::int AS total,
    COUNT(*) FILTER (WHERE estado='GREEN' AND COALESCE(empates,0)=0)::int AS gs,
    COUNT(*) FILTER (WHERE estado='G1')::int AS g1,
    COUNT(*) FILTER (WHERE estado='G2')::int AS g2,
    COUNT(*) FILTER (WHERE estado='GREEN' AND COALESCE(empates,0)>0)::int AS ge,
    COUNT(*) FILTER (WHERE estado='LOSS')::int AS loss
    FROM sinais`);
  const streak = await pool.query(`WITH resultados AS (
    SELECT estado, SUM(CASE WHEN estado='LOSS' THEN 1 ELSE 0 END) OVER
      (ORDER BY criado_em DESC, id DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS losses_seen
    FROM sinais WHERE estado IN ('GREEN','G1','G2','LOSS')
  ) SELECT COUNT(*) FILTER (WHERE losses_seen=0)::int AS total FROM resultados`);
  return { ...(result.rows[0] || { total: 0, gs: 0, g1: 0, g2: 0, ge: 0, loss: 0 }),
    greensSeguidos: streak.rows[0]?.total || 0 };
}
async function getPendingSignal() {
  const result = await pool.query(`SELECT * FROM sinais WHERE estado = 'PENDENTE' ORDER BY criado_em ASC, id ASC LIMIT 1`);
  return result.rows[0] || null;
}

function msgNovoSinal(entry) {
  const confidence = Number.isFinite(entry.confidenceScore) ? `📊 Confiança: ${entry.confidenceScore}%` : null;
  const estrategia = entry.nome || entry.estrategia || null;
  return [
    '✨🎲 CAPUNGA DADOS 🎲✨',
    '🚨✅ ENTRADA CONFIRMADA ✅🚨',
    '',
    `🎯 Cor: ${corDisplay(entry.cor)}`,
    '🛡️ Proteção: 🟡 EMPATE',
    `🐓 Gale: ${CONFIG.maxGale}`,
    estrategia ? `🧠 Estratégia: ${estrategia}` : null,
    confidence
  ].filter(line => line !== null).join('\n');
}

function msgResultadoGreen(signal, state, empate = false) {
  if (empate) return '✅ GREEN (Empate protegido)';
  if (state === 'G1') return '✅ GREEN — G1';
  if (state === 'G2') return '✅ GREEN — G2';
  return '✅ GREEN';
}

function msgResultadoLoss(signal) {
  return ['❌ LOSS', '😔 Infelizmente a mesa não obedeceu, tropa.'].join('\n');
}

function msgGale(signal, nextGale) {
  return [`🐓 GALE ${nextGale}`, `🎯 Mantém ${corDisplay(signal.cor)}`].join('\n');
}

function settlementForRound(signal, round, maxGale = CONFIG.maxGale) {
  if (round.resultado === 'Empate') return { estado: 'GREEN', resultadoFinal: 'Empate', empatesDelta: 1, tipo: 'empate' };
  if (round.resultado === signal.cor) {
    const gale = Number(signal.gale || 0);
    return { estado: gale === 1 ? 'G1' : gale >= 2 ? 'G2' : 'GREEN', resultadoFinal: round.resultado, empatesDelta: 0, tipo: 'green' };
  }
  const nextGale = Number(signal.gale || 0) + 1;
  if (nextGale <= maxGale) return { estado: 'PENDENTE', gale: nextGale, empatesDelta: 0, tipo: 'gale' };
  return { estado: 'LOSS', resultadoFinal: round.resultado, empatesDelta: 0, tipo: 'loss' };
}

async function maybeCreateSignal(round) {
  if (!CONFIG.sinaisAtivos) { runtime.motivoSemSinal = 'sinais_desativados'; return; }
  if (!['Jogador', 'Banca'].includes(round.resultado)) { runtime.motivoSemSinal = 'rodada_empate'; return; }
  const pending = await getPendingSignal();
  if (pending) { runtime.motivoSemSinal = 'sinal_pendente'; return; }
  const reds = await redsToday();
  if (reds >= CONFIG.maxRedsDia) { runtime.motivoSemSinal = `limite_reds_${reds}`; return; }
  const lastResult = await pool.query(`SELECT criado_em FROM sinais ORDER BY criado_em DESC, id DESC LIMIT 1`);
  const last = lastResult.rows[0];
  if (last?.criado_em && Date.now() - new Date(last.criado_em).getTime() < CONFIG.intervaloMinimoMs) {
    runtime.motivoSemSinal = 'intervalo_minimo'; return;
  }
  const rows = await getRecentOutcomes(500);
  const lastSignalAt = last?.criado_em ? new Date(last.criado_em).getTime() : new Date(runtime.iniciadoEm).getTime();
  const idleMs = Date.now() - lastSignalAt;
  const cadenceFallback = idleMs != null && idleMs >= CONFIG.intervaloMaximoSinalMs;
  let entry = detectBestEntry(rows);
  if (!entry && cadenceFallback) entry = fallbackCadencia(rows);
  if (!entry) { runtime.motivoSemSinal = 'sem_setup'; return; }
  const confidence = backtestConfidence(await getRecentRounds(), entry.estrategia);
  runtime.ultimaAvaliacaoConfianca = { estrategia: entry.estrategia, ...confidence };
  const score = scoreSinal(entry, confidence);
  if (score < CONFIG.pontuacaoMinimaSinal && !cadenceFallback) {
    runtime.motivoSemSinal = `filtro_\( {CONFIG.nivelFiltroSinais}_score_ \){score}_abaixo_de_${CONFIG.pontuacaoMinimaSinal}`;
    return;
  }
  entry.confidenceScore = score;
  entry.backtestWins = confidence.wins;
  entry.backtestSamples = confidence.samples;
  entry.backtestLowerBoundPct = confidence.confidencePct;
  entry.relaxadoPorCadencia = cadenceFallback && score < CONFIG.pontuacaoMinimaSinal;
  const used = await pool.query(`SELECT 1 FROM sinais WHERE resultado_id = $1 LIMIT 1`, [round.id]);
  if (used.rows.length) { runtime.motivoSemSinal = 'rodada_ja_usada'; return; }
  const result = await pool.query(`INSERT INTO sinais (cor, estado, gale, resultado_id, empates, estrategia)
    VALUES ($1, 'PENDENTE', 0, $2, 0, $3) RETURNING *`, [entry.cor, round.id, entry.estrategia]);
  const signal = result.rows[0];
  runtime.ultimoSinal = { cor: signal.cor, estado: signal.estado, criado_em: signal.criado_em, estrategia: entry.estrategia };
  runtime.ultimaEstrategia = entry;
  runtime.motivoSemSinal = null;
  await telegramSend(msgNovoSinal(entry));
  log('SINAL CRIADO', { cor: entry.cor, estrategia: entry.estrategia, nome: entry.nome });
}
async function resolvePending(round) {
  const signal = await getPendingSignal();
  if (!signal || String(signal.resultado_id) === String(round.id)) return;
  const decision = settlementForRound(signal, round);
  if (decision.tipo === 'empate') {
    const updated = await pool.query(`UPDATE sinais SET estado='GREEN', resultado_final='Empate',
      empates=COALESCE(empates,0)+1 WHERE id=$1 AND estado='PENDENTE' RETURNING *`, [signal.id]);
    if (!updated.rows.length) return;
    await telegramSend(msgResultadoGreen(updated.rows[0], 'GREEN', true));
    await telegramSend(formatPlacar(await getHistoricoSinais()));
    log('SINAL GREEN EMPATE', { id: signal.id });
  } else if (decision.tipo === 'green') {
    const updated = await pool.query(`UPDATE sinais SET estado=$1, resultado_final=$2
      WHERE id=$3 AND estado='PENDENTE' RETURNING *`, [decision.estado, decision.resultadoFinal, signal.id]);
    if (!updated.rows.length) return;
    await telegramSend(msgResultadoGreen(updated.rows[0], decision.estado));
    await telegramSend(formatPlacar(await getHistoricoSinais()));
    log('SINAL RESOLVIDO', { id: signal.id, estado: decision.estado });
  } else if (decision.tipo === 'gale') {
    const updated = await pool.query(`UPDATE sinais SET gale=$1 WHERE id=$2 AND estado='PENDENTE' RETURNING *`, [decision.gale, signal.id]);
    if (updated.rows.length) await telegramSend(msgGale(updated.rows[0], decision.gale));
    log('GALE', { id: signal.id, gale: decision.gale });
  } else {
    const updated = await pool.query(`UPDATE sinais SET estado='LOSS', resultado_final=$1
      WHERE id=$2 AND estado='PENDENTE' RETURNING *`, [decision.resultadoFinal, signal.id]);
    if (!updated.rows.length) return;
    await telegramSend(msgResultadoLoss(updated.rows[0]));
    await telegramSend(formatPlacar(await getHistoricoSinais()));
    log('SINAL LOSS', { id: signal.id });
  }
}

async function syncOnce() {
  if (!pool) throw new Error('DATABASE_URL não configurada');
  if (runtime.sincronizacaoEmCurso) return { sucesso: false, ignorada: true, motivo: 'sync_em_curso' };
  runtime.sincronizacaoEmCurso = true;
  let lockClient = null;
  try {
    await ensureSchema();
    lockClient = await pool.connect();
    const lock = await lockClient.query(`SELECT pg_try_advisory_lock(741852963) AS locked`);
    if (!lock.rows[0]?.locked) return { sucesso: false, ignorada: true, motivo: 'outra_instancia_sincroniza' };
    const latestDB = await getLatestFromDB();
    runtime.ultimaRodadaDB = latestDB?.fim || null;
    runtime.ultimoIdDB = latestDB?.id || null;
    const watermark = latestDB?.fim ? new Date(latestDB.fim).getTime() : null;
    const rounds = await fetchRounds(latestDB);
    let inserted = 0, duplicates = 0, evaluated = 0, lastNew = null;
    for (const round of rounds) {
      const saved = await insertRound(round);
      if (!saved) { duplicates++; continue; }
      inserted++;
      lastNew = round;
      const isAfterWatermark = watermark != null && new Date(round.fim).getTime() > watermark;
      // A primeira carga importa histórico, mas não cria sinais retroativos.
      if (latestDB && isAfterWatermark) {
        await resolvePending(round);
        await maybeCreateSignal(round);
        evaluated++;
      }
    }
    runtime.ultimaSincronizacao = new Date().toISOString();
    runtime.duplicadosUltimoCiclo = duplicates;
    runtime.novosUltimoCiclo = inserted;
    runtime.erroSincronizacao = null;
    if (lastNew) {
      runtime.ultimaRodadaNova = new Date().toISOString();
      runtime.ultimoIdNovo = lastNew.id;
      runtime.ciclosSemNovos = 0;
    } else runtime.ciclosSemNovos++;
    const summary = {
      recebidos: rounds.length, novosGuardados: inserted, duplicados,
      sinaisAvaliados: evaluated, motivoSemSinal: runtime.motivoSemSinal,
      fonte: runtime.fonteAtual, latenciaAPIMs: runtime.latenciaAPIMs,
      ciclosSemNovos: runtime.ciclosSemNovos, horaAngola: horaAngola()
    };
    if (inserted > 0 || runtime.ciclosSemNovos % 8 === 0) log('SINCRONIZACAO', summary);
    return { sucesso: true, ...summary };
  } catch (error) {
    runtime.erroSincronizacao = error.message || String(error);
    console.error('[ERRO SINCRONIZACAO]', runtime.erroSincronizacao);
    return { sucesso: false, erro: runtime.erroSincronizacao };
  } finally {
    if (lockClient) {
      try { await lockClient.query(`SELECT pg_advisory_unlock(741852963)`); } catch (_) {}
      lockClient.release();
    }
    runtime.sincronizacaoEmCurso = false;
  }
}

function requireAdmin(req, res, next) {
  if (!ADMIN_TOKEN) return res.status(503).json({ sucesso: false, erro: 'ADMIN_TOKEN não configurado' });
  const supplied = String(req.get('x-admin-token') || '');
  const a = Buffer.from(supplied), b = Buffer.from(ADMIN_TOKEN);
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b))
    return res.status(401).json({ sucesso: false, erro: 'Token inválido' });
  next();
}
app.get('/', (req, res) => res.json({
  nome: 'Capunga Dados Bac Bo', fonte: runtime.fonteAtual || safeSource(API_URLS[0]),
  estado: 'online', horaAngola: horaAngola(),
  estrategias: ['REPETICAO', 'QUEBRA', 'SURF', 'DOIS_DOIS', 'FORCA', 'NUMERICA', 'MODELO_CONTEXTUAL']
}));
app.get('/health', async (req, res) => {
  let dbOk = false, dbError = null;
  try { if (!pool) throw new Error('DATABASE_URL não configurada'); await pool.query('SELECT 1'); dbOk = true; }
  catch (error) { dbError = error.message; }
  const ageMs = runtime.ultimaHoraDaAPI ? Date.now() - new Date(runtime.ultimaHoraDaAPI).getTime() : null;
  const stale = ageMs == null || ageMs > CONFIG.staleAfterMs || runtime.falhasAPIConsecutivas > 0;
  const healthy = dbOk && !runtime.erroSincronizacao;
  res.status(healthy ? 200 : 503).json({
    sucesso: healthy, estado: !dbOk ? 'db_indisponivel' : stale ? 'degradado' : 'online',
    dbOk, erroDB: dbError, horaAngola: horaAngola(),
    ultimaSincronizacao: runtime.ultimaSincronizacao, ultimaHoraDaAPI: runtime.ultimaHoraDaAPI,
    idadeUltimoResultadoMs: ageMs, stale, fonte: runtime.fonteAtual || safeSource(API_URLS[0]),
    latenciaAPIMs: runtime.latenciaAPIMs, falhasAPIConsecutivas: runtime.falhasAPIConsecutivas,
    erroAPI: runtime.ultimoErroAPI, novosUltimoCiclo: runtime.novosUltimoCiclo,
    ciclosSemNovos: runtime.ciclosSemNovos, motivoSemSinal: runtime.motivoSemSinal,
    sincronizacaoEmCurso: runtime.sincronizacaoEmCurso, ultimaEstrategia: runtime.ultimaEstrategia,
    ultimaAvaliacaoConfianca: runtime.ultimaAvaliacaoConfianca
  });
});
app.get('/diagnostico', async (req, res) => {
  let ultimoResultadoDB = null, totalResultados = 0, totalSinais = 0, pendentes = 0, recentes = [], erroDB = null;
  try {
    await ensureSchema();
    const r1 = await pool.query(`SELECT id, resultado, fim, pontos_jogador, pontos_banca FROM bacbo_resultados
      ORDER BY fim DESC NULLS LAST, seq DESC LIMIT 1`);
    ultimoResultadoDB = r1.rows[0] || null;
    totalResultados = (await pool.query('SELECT COUNT(*)::int AS total FROM bacbo_resultados')).rows[0].total;
    totalSinais = (await pool.query('SELECT COUNT(*)::int AS total FROM sinais')).rows[0].total;
    pendentes = (await pool.query(`SELECT COUNT(*)::int AS total FROM sinais WHERE estado='PENDENTE'`)).rows[0].total;
    recentes = (await pool.query(`SELECT resultado, pontos_jogador, pontos_banca FROM bacbo_resultados
      WHERE resultado IN ('Jogador','Banca') ORDER BY fim DESC NULLS LAST, seq DESC LIMIT 12`)).rows;
  } catch (error) { erroDB = error.message; }
  res.json({ sucesso: !erroDB, nome: 'Capunga Dados', fonte: runtime.fonteAtual || safeSource(API_URLS[0]),
    iniciadoEm: runtime.iniciadoEm, horaAngola: horaAngola(), telegramConfigurado: Boolean(BOT_TOKEN && CHAT_ID),
    httpAPI: runtime.ultimoHTTPAPI, erroAPI: runtime.ultimoErroAPI, erroDB,
    ultimaSincronizacao: runtime.ultimaSincronizacao, novosUltimoCiclo: runtime.novosUltimoCiclo,
    ciclosSemNovos: runtime.ciclosSemNovos, motivoSemSinal: runtime.motivoSemSinal,
    ultimoSinal: runtime.ultimoSinal, ultimaEstrategia: runtime.ultimaEstrategia,
    ultimaAvaliacaoConfianca: runtime.ultimaAvaliacaoConfianca,
    ultimosResultados: recentes, baseDados: { totalResultados, totalSinais, pendentes, ultimoResultadoDB }, configuracao: CONFIG });
});
app.get('/resultados', async (req, res) => {
  try {
    await ensureSchema();
    const limit = Math.min(Math.max(Number.parseInt(req.query.limit, 10) || 50, 1), 500);
    const result = await pool.query(`SELECT * FROM bacbo_resultados ORDER BY fim DESC NULLS LAST, seq DESC LIMIT $1`, [limit]);
    res.json({ sucesso: true, total: result.rows.length, resultados: result.rows });
  } catch (error) { res.status(500).json({ sucesso: false, erro: error.message }); }
});
app.get('/sinais', async (req, res) => {
  try {
    await ensureSchema();
    const limit = Math.min(Math.max(Number.parseInt(req.query.limit, 10) || 50, 1), 500);
    const result = await pool.query(`SELECT * FROM sinais ORDER BY criado_em DESC, id DESC LIMIT $1`, [limit]);
    res.json({ sucesso: true, total: result.rows.length, sinais: result.rows });
  } catch (error) { res.status(500).json({ sucesso: false, erro: error.message }); }
});
app.get('/estatisticas', async (req, res) => {
  try {
    await ensureSchema();
    const result = await pool.query(`SELECT COUNT(*)::int AS total,
      COUNT(*) FILTER (WHERE estado='GREEN')::int AS green, COUNT(*) FILTER (WHERE estado='G1')::int AS g1,
      COUNT(*) FILTER (WHERE estado='G2')::int AS g2, COUNT(*) FILTER (WHERE estado='LOSS')::int AS loss,
      COUNT(*) FILTER (WHERE estado='PENDENTE')::int AS pendentes,
      COUNT(*) FILTER (WHERE estado='CANCELADO')::int AS cancelados FROM sinais`);
    const stats = result.rows[0];
    const acertos = (stats.green || 0) + (stats.g1 || 0) + (stats.g2 || 0);
    const finalizados = acertos + (stats.loss || 0);
    const taxa = finalizados ? Number((acertos / finalizados * 100).toFixed(1)) : null;
    const porEstrategia = (await pool.query(`SELECT COALESCE(estrategia,'DESCONHECIDA') AS estrategia,
      COUNT(*)::int AS total, COUNT(*) FILTER (WHERE estado IN ('GREEN','G1','G2'))::int AS acertos,
      COUNT(*) FILTER (WHERE estado='LOSS')::int AS loss FROM sinais
      WHERE estado IN ('GREEN','G1','G2','LOSS') GROUP BY 1 ORDER BY total DESC`)).rows;
    const placarDia = await getPlacarDia();
    res.json({ sucesso: true, estatisticas: { ...stats, finalizados, acertos, taxaAcertoPercent: taxa },
      porEstrategia, placarDia, horaAngola: horaAngola() });
  } catch (error) { res.status(500).json({ sucesso: false, erro: error.message }); }
});
app.post('/db/preparar', requireAdmin, async (req, res) => {
  try { await ensureSchema(); res.json({ sucesso: true, mensagem: 'Tabelas preparadas' }); }
  catch (error) { res.status(500).json({ sucesso: false, erro: error.message }); }
});
app.post('/db/sincronizar', requireAdmin, async (req, res) => {
  const result = await syncOnce();
  res.status(result.sucesso || result.ignorada ? 200 : 500).json(result);
});
app.post('/telegram/teste', requireAdmin, async (req, res) => {
  const enviado = await telegramSend(['🎲 CAPUNGA', '✅ Teste OK'].join('\n'));
  res.status(enviado ? 200 : 502).json({ sucesso: enviado, erro: runtime.ultimoErroTelegram });
});
app.use((req, res) => res.status(404).json({ sucesso: false, erro: 'Rota não encontrada', caminho: req.path }));

async function start() {
  if (!DATABASE_URL) throw new Error('DATABASE_URL não configurada');
  await ensureSchema();
  server = app.listen(PORT, '0.0.0.0', () => {
    console.log('======================================');
    console.log('CAPUNGA DADOS — BAC BO');
    console.log(`Fonte: ${safeSource(API_URLS[0])}`);
    console.log(`Porta: ${PORT} | Angola: ${horaAngola()}`);
    console.log(`Sinais: ${CONFIG.sinaisAtivos} | Streak: ${CONFIG.streakMin} | Gale: ${CONFIG.maxGale}`);
    console.log('======================================');
  });
  const initial = await syncOnce();
  if (!initial.sucesso) console.error('[PRIMEIRA SYNC]', initial.erro || initial.motivo);
  else log('PRIMEIRA SYNC', { recebidos: initial.recebidos, novos: initial.novosGuardados });
  syncTimer = setInterval(() => { syncOnce().catch(err => console.error('[SYNC]', err.message)); }, CONFIG.intervaloSyncMs);
  if (typeof syncTimer.unref === 'function') syncTimer.unref();
}
async function shutdown() {
  if (syncTimer) clearInterval(syncTimer);
  if (server) await new Promise(resolve => server.close(resolve));
  if (pool) await pool.end();
}
process.on('SIGTERM', () => shutdown().then(() => process.exit(0)).catch(() => process.exit(1)));
process.on('SIGINT', () => shutdown().then(() => process.exit(0)).catch(() => process.exit(1)));
process.on('unhandledRejection', reason => console.error('[UNHANDLED]', reason));
process.on('uncaughtException', error => { console.error('[UNCAUGHT]', error); shutdown().finally(() => process.exit(1)); });

if (require.main === module) start().catch(error => { console.error('[FATAL]', error); process.exit(1); });
module.exports = { app, normalizeOutcome, extractItems, normalizeRound, parseNextLink, withPage, coversWatermark,
  streakLen, stratModelo, detectBestEntry, settlementForRound, wilsonLowerBound, backtestConfidence,
  scoreSinal, fallbackCadencia, formatPlacar, msgNovoSinal, msgResultadoGreen,
  msgResultadoLoss, msgGale, CONFIG };