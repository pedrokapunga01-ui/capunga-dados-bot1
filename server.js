
'use strict';

/**
 * CAPUNGA DADOS — BAC BO
 * API HTTP + PostgreSQL + Telegram
 *
 * Variáveis obrigatórias no Render:
 * DATABASE_URL
 * BOT_TOKEN
 * CHAT_ID
 * ADMIN_TOKEN
 * API_TOKEN
 *
 * Variáveis opcionais:
 * API_BACBO=https://apiscb.site/bacbo
 * SINAIS_ATIVOS=true
 * STREAK_MIN=3
 * MAX_GALE=2
 * EMPATE_PROTEGIDO=true
 * MAX_REDS_DIA=10
 * INTERVALO_MINIMO_MS=40000
 * SINAL_EXPIRA_MIN=10
 * INTERVALO_SYNC_MS=60000
 */

const express = require('express');
const cors = require('cors');
const axios = require('axios');
const { Pool } = require('pg');

const app = express();
app.disable('x-powered-by');
app.use(cors());
app.use(express.json({ limit: '1mb' }));

const PORT = Number(process.env.PORT || 10000);
const DATABASE_URL = process.env.DATABASE_URL || '';
const BOT_TOKEN = process.env.BOT_TOKEN || '';
const CHAT_ID = process.env.CHAT_ID || '';
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || '';
const API_TOKEN = process.env.API_TOKEN || '';
const API_BACBO = process.env.API_BACBO || 'https://apiscb.site/bacbo';

function boolEnv(value, fallback = false) {
  if (value === undefined || value === '') return fallback;
  return ['true', '1', 'yes', 'sim', 'on'].includes(
    String(value).trim().toLowerCase()
  );
}

function intEnv(name, fallback, min, max) {
  const n = Number.parseInt(process.env[name], 10);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, n));
}

const CONFIG = {
  sinaisAtivos: boolEnv(process.env.SINAIS_ATIVOS, true),
  streakMin: intEnv('STREAK_MIN', 3, 2, 12),
  maxGale: intEnv('MAX_GALE', 2, 0, 3),
  empateProtegido: boolEnv(process.env.EMPATE_PROTEGIDO, true),
  maxRedsDia: intEnv('MAX_REDS_DIA', 10, 1, 50),
  intervaloMinimoMs: intEnv('INTERVALO_MINIMO_MS', 40000, 15000, 3600000),
  sinalExpiraMin: intEnv('SINAL_EXPIRA_MIN', 10, 3, 60),
  intervaloSyncMs: intEnv('INTERVALO_SYNC_MS', 60000, 60000, 3600000)
};

const pool = DATABASE_URL
  ? new Pool({
      connectionString: DATABASE_URL,
      ssl: /localhost|127\.0\.0\.1/i.test(DATABASE_URL)
        ? false
        : { rejectUnauthorized: false },
      max: 5,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 15000
    })
  : null;

const runtime = {
  iniciadoEm: new Date().toISOString(),
  sincronizacaoEmCurso: false,
  sincronizacaoInicialConcluida: false,
  ultimaSincronizacao: null,
  ultimaRodadaNova: null,
  ultimoIdNovo: null,
  ultimaHoraDaAPI: null,
  quantidadeAPI: 0,
  itensRecebidosAPI: 0,
  duplicadosUltimoCiclo: 0,
  novosUltimoCiclo: 0,
  erroSincronizacao: null,
  ultimoSinal: null,
  ultimaMensagemTelegram: null,
  ultimoErroTelegram: null,
  ultimoHTTPAPI: null,
  ultimoErroAPI: null,
  ultimaRodadaDB: null,
  ultimoIdDB: null,
  ciclosSemNovos: 0,
  motivoSemSinal: null,
  ultimaEstrategia: null
};

let schemaPromise = null;
let server = null;
let syncTimer = null;

function log(tag, data) {
  console.log(
    `[${tag}]`,
    typeof data === 'string' ? data : JSON.stringify(data)
  );
}

function horaAngola() {
  return new Intl.DateTimeFormat('pt-PT', {
    timeZone: 'Africa/Luanda',
    dateStyle: 'short',
    timeStyle: 'medium',
    hour12: false
  }).format(new Date());
}

function corDisplay(cor) {
  if (cor === 'Banca') return '🔴 VERMELHO 🔴';
  if (cor === 'Jogador') return '🔵 AZUL 🔵';
  return String(cor || '');
}

function oposta(cor) {
  if (cor === 'Banca') return 'Jogador';
  if (cor === 'Jogador') return 'Banca';
  return null;
}

function normalizeOutcome(value) {
  if (value && typeof value === 'object') {
    value =
      value.outcome ??
      value.resultado ??
      value.winner ??
      value.name ??
      value.result;
  }

  const s = String(value ?? '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .trim()
    .toLowerCase()
    .replace(/[\s_-]+/g, '');

  if ([
    'a', 'blue', 'azul', 'player', 'jogador',
    'playerwin', 'playerwins', 'playerwon', 'playerwinner'
  ].includes(s)) return 'Jogador';

  if ([
    'v', 'red', 'vermelho', 'banker', 'banca',
    'bankerwin', 'bankerwins', 'bankerwon', 'bankerwinner'
  ].includes(s)) return 'Banca';

  if ([
    'e', 'tie', 'draw', 'empate', 'equal', 'push', 'tied'
  ].includes(s)) return 'Empate';

  return null;
}

function validDate(value) {
  if (value === undefined || value === null || value === '') return null;

  // Unix timestamp, em segundos ou milissegundos.
  if (typeof value === 'number' || /^\d{10,13}$/.test(String(value))) {
    let n = Number(value);
    if (!Number.isFinite(n)) return null;
    if (n < 1e12) n *= 1000;
    const d = new Date(n);
    return Number.isNaN(d.getTime()) ? null : d.toISOString();
  }

  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
}

function extractItems(body) {
  if (Array.isArray(body)) return body;
  if (!body || typeof body !== 'object') return [];

  const candidates = [
    body.data,
    body.results,
    body.items,
    body.resultados,
    body.rounds,
    body.events,
    body.data?.results,
    body.data?.items,
    body.data?.resultados,
    body.data?.rounds,
    body.data?.events
  ];

  const list = candidates.find(Array.isArray);
  if (list) return list;

  if (
    body.id != null &&
    (body.resultado != null || body.result != null || body.outcome != null)
  ) return [body];

  if (
    body.data &&
    typeof body.data === 'object' &&
    !Array.isArray(body.data) &&
    body.data.id != null
  ) return [body.data];

  return [];
}

function normalizeRound(item) {
  const d = item?.data || item?.round || item || {};
  const r = d.result && typeof d.result === 'object'
    ? d.result
    : item?.result && typeof item.result === 'object'
      ? item.result
      : {};

  const rawOutcome =
    r.outcome ??
    r.resultado ??
    r.winner ??
    d.outcome ??
    d.resultado ??
    d.winner ??
    item?.outcome ??
    item?.resultado ??
    item?.winner ??
    (typeof d.result === 'string' ? d.result : null) ??
    (typeof item?.result === 'string' ? item.result : null);

  const resultado = normalizeOutcome(rawOutcome);

  const rawDate =
    d.hora ??
    d.timestamp ??
    d.finishedAt ??
    d.settledAt ??
    d.endTime ??
    d.endedAt ??
    d.fim ??
    d.startedAt ??
    d.startTime ??
    item?.hora ??
    item?.timestamp ??
    item?.fim ??
    item?.finishedAt;

  const fim = validDate(rawDate);

  const id = String(
    d.id ??
    d.roundId ??
    d.round_id ??
    d.eventId ??
    item?.id ??
    item?.roundId ??
    item?.round_id ??
    item?.eventId ??
    ''
  ).trim();

  if (!id || !resultado || !fim) return null;

  const scoreJogador = Number(
    r.playerDice?.score ??
    d.playerDice?.score ??
    d.playerScore ??
    d.player_score ??
    d.pontos_jogador
  );

  const scoreBanca = Number(
    r.bankerDice?.score ??
    d.bankerDice?.score ??
    d.bankerScore ??
    d.banker_score ??
    d.pontos_banca
  );

  return {
    id,
    resultado,
    estado: 'Resolved',
    inicio: fim,
    fim,
    pontos_jogador: Number.isFinite(scoreJogador) ? scoreJogador : null,
    pontos_banca: Number.isFinite(scoreBanca) ? scoreBanca : null,
    multiplicador: d.multiplier == null ? null : String(d.multiplier)
  };
}

/* ================= TELEGRAM ================= */

async function telegramSend(texto) {
  if (!BOT_TOKEN || !CHAT_ID) {
    runtime.ultimoErroTelegram = 'BOT_TOKEN ou CHAT_ID não configurado';
    return false;
  }

  try {
    const response = await axios.post(
      `https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`,
      {
        chat_id: CHAT_ID,
        text: texto,
        disable_web_page_preview: true
      },
      { timeout: 15000 }
    );

    if (response.data?.ok === true) {
      runtime.ultimaMensagemTelegram = new Date().toISOString();
      runtime.ultimoErroTelegram = null;
      return true;
    }

    runtime.ultimoErroTelegram = JSON.stringify(response.data);
    return false;
  } catch (error) {
    runtime.ultimoErroTelegram = error.response?.data
      ? JSON.stringify(error.response.data)
      : error.message;

    console.error('[TELEGRAM]', runtime.ultimoErroTelegram);
    return false;
  }
}

/* ================= BASE DE DADOS ================= */

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

    await pool.query(`
      ALTER TABLE bacbo_resultados
      ADD COLUMN IF NOT EXISTS seq BIGSERIAL
    `);

    await pool.query(`
      ALTER TABLE bacbo_resultados
      ADD COLUMN IF NOT EXISTS guardado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
    `);

    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_bacbo_fim
      ON bacbo_resultados (fim DESC)
    `);

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

    await pool.query(`
      ALTER TABLE sinais ADD COLUMN IF NOT EXISTS estrategia TEXT
    `);

    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_sinais_estado ON sinais (estado)
    `);

    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_sinais_criado ON sinais (criado_em DESC)
    `);

    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_sinais_loss_dia
      ON sinais (criado_em) WHERE estado = 'LOSS'
    `);

    log('DB', { tabelasPreparadas: true });
  })();

  try {
    await schemaPromise;
  } catch (error) {
    schemaPromise = null;
    throw error;
  }
}

async function fetchRounds() {
  if (!API_TOKEN) {
    throw new Error(
      'API_TOKEN não configurado. É necessário obter um token válido do fornecedor.'
    );
  }

  try {
    const response = await axios.get(API_BACBO, {
      timeout: 20000,
      params: { token: API_TOKEN },
      headers: {
        Accept: 'application/json',
        'User-Agent': 'CapungaDadosBot/1.0'
      }
    });

    runtime.ultimoHTTPAPI = response.status;
    runtime.ultimoErroAPI = null;

    const items = extractItems(response.data);
    const unique = new Map();

    for (const item of items) {
      const round = normalizeRound(item);
      if (round) unique.set(round.id, round);
    }

    const rounds = [...unique.values()].sort(
      (a, b) => new Date(a.fim) - new Date(b.fim)
    );

    runtime.itensRecebidosAPI = items.length;
    runtime.quantidadeAPI = rounds.length;
    runtime.ultimaHoraDaAPI = rounds.length
      ? rounds[rounds.length - 1].fim
      : null;

    if (items.length > 0 && rounds.length === 0) {
      throw new Error(
        'A API respondeu, mas os resultados não correspondem ao formato esperado.'
      );
    }

    return rounds;
  } catch (error) {
    runtime.ultimoErroAPI = error.response
      ? `HTTP ${error.response.status}: ${JSON.stringify(error.response.data).slice(0, 500)}`
      : error.message || String(error);

    runtime.ultimoHTTPAPI = error.response?.status || null;
    console.error('[ERRO API]', runtime.ultimoErroAPI);
    throw error;
  }
}

async function insertRound(round) {
  const result = await pool.query(
    `INSERT INTO bacbo_resultados
      (id, resultado, estado, inicio, fim, pontos_jogador, pontos_banca, multiplicador)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
     ON CONFLICT (id) DO NOTHING
     RETURNING id`,
    [
      round.id,
      round.resultado,
      round.estado,
      round.inicio,
      round.fim,
      round.pontos_jogador,
      round.pontos_banca,
      round.multiplicador
    ]
  );

  return result.rows.length > 0;
}

async function getLatestFromDB() {
  const result = await pool.query(`
    SELECT id, resultado, fim
    FROM bacbo_resultados
    ORDER BY fim DESC NULLS LAST, seq DESC
    LIMIT 1
  `);

  return result.rows[0] || null;
}

async function getRecentOutcomes(limit = 40) {
  const result = await pool.query(
    `SELECT id, resultado, fim, pontos_jogador, pontos_banca, multiplicador
     FROM bacbo_resultados
     WHERE resultado IN ('Jogador', 'Banca')
     ORDER BY fim DESC NULLS LAST, seq DESC
     LIMIT $1`,
    [limit]
  );

  return result.rows;
}

/* ================= ESTRATÉGIAS ================= */

function streakLen(rows) {
  if (!rows.length) return 0;

  const cor = rows[0].resultado;
  if (!['Jogador', 'Banca'].includes(cor)) return 0;

  let n = 0;

  for (const row of rows) {
    if (row.resultado !== cor) break;
    n++;
  }

  return n;
}

function stratRepeticao(rows) {
  const n = streakLen(rows);
  if (n < CONFIG.streakMin) return null;

  return {
    cor: rows[0].resultado,
    estrategia: 'REPETICAO',
    nome: `Repetição x${n}`,
    emoji: '🔁',
    prioridade: Math.min(90, 50 + n * 10)
  };
}

function stratQuebra(rows) {
  const n = streakLen(rows);
  if (n < CONFIG.streakMin + 1) return null;

  return {
    cor: oposta(rows[0].resultado),
    estrategia: 'QUEBRA',
    nome: `Quebra x${n}`,
    emoji: '💥',
    prioridade: Math.min(88, 55 + n * 8)
  };
}

function stratSurf(rows) {
  if (rows.length < 4) return null;

  const a = rows[0].resultado;
  const b = rows[1].resultado;

  if (!['Jogador', 'Banca'].includes(a) ||
      !['Jogador', 'Banca'].includes(b) ||
      a === b) return null;

  for (let i = 0; i < 4; i++) {
    if (rows[i].resultado !== (i % 2 === 0 ? a : b)) return null;
  }

  return {
    cor: b,
    estrategia: 'SURF',
    nome: 'Surf alternado',
    emoji: '🌊',
    prioridade: 78
  };
}

function stratDoisDois(rows) {
  if (rows.length < 3) return null;

  const r = rows.map(x => x.resultado);

  if (
    r.length >= 4 &&
    r[0] === r[1] &&
    r[2] === r[3] &&
    r[0] !== r[2] &&
    ['Jogador', 'Banca'].includes(r[0])
  ) {
    return {
      cor: r[0],
      estrategia: 'DOIS_DOIS',
      nome: '2+2 bloco',
      emoji: '♟️',
      prioridade: 82
    };
  }

  if (
    r[0] !== r[1] &&
    r[1] === r[2] &&
    ['Jogador', 'Banca'].includes(r[0])
  ) {
    return {
      cor: r[0],
      estrategia: 'DOIS_DOIS',
      nome: '2+2 completa par',
      emoji: '♟️',
      prioridade: 75
    };
  }

  return null;
}

function stratForca(rows) {
  const sample = rows
    .slice(0, 12)
    .filter(r => ['Jogador', 'Banca'].includes(r.resultado));

  if (sample.length < 8) return null;

  const j = sample.filter(r => r.resultado === 'Jogador').length;
  const b = sample.length - j;
  const pctJ = j / sample.length;
  const pctB = b / sample.length;

  if (pctB >= 0.65) {
    return {
      cor: 'Banca',
      estrategia: 'FORCA',
      nome: `Força Banca ${Math.round(pctB * 100)}%`,
      emoji: '💪',
      prioridade: 72
    };
  }

  if (pctJ >= 0.65) {
    return {
      cor: 'Jogador',
      estrategia: 'FORCA',
      nome: `Força Jogador ${Math.round(pctJ * 100)}%`,
      emoji: '💪',
      prioridade: 72
    };
  }

  return null;
}

function stratNumerica(rows) {
  if (rows.length < 3) return null;

  const last = rows[0];
  const pj = Number(last.pontos_jogador);
  const pb = Number(last.pontos_banca);

  if (Number.isFinite(pj) && Number.isFinite(pb)) {
    if (last.resultado === 'Jogador' && pj >= 10 && pj > pb) {
      return {
        cor: 'Jogador',
        estrategia: 'NUMERICA',
        nome: `Numérica J(${pj})`,
        emoji: '🔢',
        prioridade: 68
      };
    }

    if (last.resultado === 'Banca' && pb >= 10 && pb > pj) {
      return {
        cor: 'Banca',
        estrategia: 'NUMERICA',
        nome: `Numérica B(${pb})`,
        emoji: '🔢',
        prioridade: 68
      };
    }
  }

  if (rows.length >= 3) {
    let corDom = null;
    let valido = true;

    for (let i = 0; i < 3; i++) {
      const r = rows[i];
      const a = Number(r.pontos_jogador);
      const b = Number(r.pontos_banca);

      if (
        !Number.isFinite(a) ||
        !Number.isFinite(b) ||
        Math.abs(a - b) < 4
      ) {
        valido = false;
        break;
      }

      if (!corDom) corDom = r.resultado;
      else if (r.resultado !== corDom) valido = false;
    }

    if (valido && corDom) {
      return {
        cor: corDom,
        estrategia: 'NUMERICA',
        nome: 'Numérica diferença x3',
        emoji: '🔢',
        prioridade: 70
      };
    }
  }

  if (rows.length >= 2) {
    const a = rows[0];
    const b = rows[1];

    if (
      a.resultado === b.resultado &&
      ['Jogador', 'Banca'].includes(a.resultado)
    ) {
      const s0 = a.resultado === 'Jogador'
        ? Number(a.pontos_jogador)
        : Number(a.pontos_banca);

      const s1 = a.resultado === 'Jogador'
        ? Number(b.pontos_jogador)
        : Number(b.pontos_banca);

      if (Number.isFinite(s0) && Number.isFinite(s1) && s0 <= 5 && s1 <= 5) {
        return {
          cor: oposta(a.resultado),
          estrategia: 'NUMERICA',
          nome: 'Numérica virada baixa',
          emoji: '🔢',
          prioridade: 65
        };
      }
    }
  }

  return null;
}

function detectBestEntry(rows) {
  if (!rows || rows.length < 2) return null;

  const candidates = [
    stratRepeticao(rows),
    stratQuebra(rows),
    stratDoisDois(rows),
    stratSurf(rows),
    stratForca(rows),
    stratNumerica(rows)
  ].filter(Boolean);

  if (!candidates.length) return null;

  const order = {
    REPETICAO: 6,
    QUEBRA: 5,
    DOIS_DOIS: 4,
    SURF: 3,
    FORCA: 2,
    NUMERICA: 1
  };

  candidates.sort((a, b) =>
    b.prioridade - a.prioridade ||
    (order[b.estrategia] || 0) - (order[a.estrategia] || 0)
  );

  return candidates[0];
}

/* ================= SINAIS E PLACAR ================= */

async function redsToday() {
  const result = await pool.query(`
    SELECT COUNT(*)::int AS total
    FROM sinais
    WHERE estado = 'LOSS'
      AND criado_em >=
        (date_trunc('day', NOW() AT TIME ZONE 'Africa/Luanda')
         AT TIME ZONE 'Africa/Luanda')
  `);

  return result.rows[0]?.total || 0;
}

async function getPlacarDia() {
  const result = await pool.query(`
    SELECT
      COUNT(*) FILTER (WHERE estado = 'GREEN')::int AS green,
      COUNT(*) FILTER (WHERE estado = 'G1')::int AS g1,
      COUNT(*) FILTER (WHERE estado = 'G2')::int AS g2,
      COUNT(*) FILTER (WHERE estado = 'LOSS')::int AS loss,
      COALESCE(SUM(empates), 0)::int AS empates
    FROM sinais
    WHERE criado_em >=
      (date_trunc('day', NOW() AT TIME ZONE 'Africa/Luanda')
       AT TIME ZONE 'Africa/Luanda')
  `);

  const s = result.rows[0] || {};
  const green = s.green || 0;
  const g1 = s.g1 || 0;
  const g2 = s.g2 || 0;
  const loss = s.loss || 0;
  const acertos = green + g1 + g2;
  const finalizados = acertos + loss;

  const recent = await pool.query(`
    SELECT estado FROM sinais
    WHERE criado_em >=
      (date_trunc('day', NOW() AT TIME ZONE 'Africa/Luanda')
       AT TIME ZONE 'Africa/Luanda')
      AND estado IN ('GREEN', 'G1', 'G2', 'LOSS')
    ORDER BY criado_em DESC, id DESC
    LIMIT 50
  `);

  let greensSeguidos = 0;

  for (const row of recent.rows) {
    if (['GREEN', 'G1', 'G2'].includes(row.estado)) greensSeguidos++;
    else break;
  }

  return {
    green,
    g1,
    g2,
    empates: s.empates || 0,
    loss,
    percent: finalizados
      ? Number(((acertos / finalizados) * 100).toFixed(1))
      : null,
    greensSeguidos
  };
}

function formatPlacar(p) {
  return [
    '',
    '════════════════════',
    '🏆 CAPUNGA PLACAR 🏆',
    '════════════════════',
    `✅ GREEN: ${p.green} | ⚡ G1: ${p.g1}`,
    `🔥 G2: ${p.g2} | 🟡 Empates: ${p.empates}`,
    `❌ LOSS: ${p.loss}`,
    `📊 Assertividade: ${p.percent == null ? 'N/D' : p.percent + '%'}`,
    `🚀 Sequência: ${p.greensSeguidos} green(s)`,
    '════════════════════'
  ].join('\n');
}

async function getPendingSignal() {
  const result = await pool.query(`
    SELECT * FROM sinais
    WHERE estado = 'PENDENTE'
    ORDER BY criado_em ASC, id ASC
    LIMIT 1
  `);

  return result.rows[0] || null;
}

async function expirePendingSignal(signal) {
  if (!signal) return false;

  const age = Date.now() - new Date(signal.criado_em).getTime();
  if (age < CONFIG.sinalExpiraMin * 60000) return false;

  await pool.query(
    `UPDATE sinais SET estado = 'CANCELADO'
     WHERE id = $1 AND estado = 'PENDENTE'`,
    [signal.id]
  );

  await telegramSend([
    '⏰ SINAL EXPIRADO',
    `Sinal #${signal.id}`,
    `🕒 ${horaAngola()}`
  ].join('\n'));

  log('SINAL EXPIRADO', { id: signal.id });
  return true;
}

function msgNovoSinal(entry) {
  return [
    '🎲🇺🇸 CAPUNGA DADOS 🇺🇸🎲',
    '━━━━━━━━━━━━━━━━━━━━',
    '🚨⚡ ENTRADA ⚡🚨',
    '━━━━━━━━━━━━━━━━━━━━',
    `🎯 Cor: ${corDisplay(entry.cor)}`,
    `${entry.emoji} Estratégia: ${entry.nome}`,
    '🛡️ Proteção: 🟡 EMPATE',
    `🐓 Gale máximo: ${CONFIG.maxGale}`,
    '💎 Gestão de banca obrigatória',
    '⚠️ Sinal estatístico, sem garantia de acerto.',
    `🕒 ${horaAngola()}`,
    '━━━━━━━━━━━━━━━━━━━━'
  ].join('\n');
}

function msgResultadoGreen(signal, state) {
  const titulo = state === 'GREEN'
    ? '✅ GREEN — SEM GALE'
    : state === 'G1'
      ? '⚡ GREEN G1'
      : '🔥 GREEN G2';

  return [
    '🎲🇺🇸 CAPUNGA DADOS 🇺🇸🎲',
    '📋 RESULTADO DO SINAL',
    titulo,
    `🎯 Entrada: ${corDisplay(signal.cor)}`,
    `🔢 Sinal #${signal.id}`,
    signal.estrategia ? `📌 ${signal.estrategia}` : '',
    `🕒 ${horaAngola()}`
  ].filter(Boolean).join('\n');
}

function msgResultadoLoss(signal) {
  return [
    '🎲🇺🇸 CAPUNGA DADOS 🇺🇸🎲',
    '📋 RESULTADO DO SINAL',
    '❌ LOSS FINAL',
    `🎯 Entrada: ${corDisplay(signal.cor)}`,
    `🐓 Gale: ${signal.gale || 0}/${CONFIG.maxGale}`,
    `🔢 Sinal #${signal.id}`,
    `🕒 ${horaAngola()}`
  ].join('\n');
}

function msgGale(signal, nextGale) {
  return [
    '🎲🇺🇸 CAPUNGA DADOS 🇺🇸🎲',
    `🐓 GALE ${nextGale}`,
    `🎯 Mantém: ${corDisplay(signal.cor)}`,
    `🔢 Sinal #${signal.id}`,
    `🕒 ${horaAngola()}`
  ].join('\n');
}

function msgEmpate(signal) {
  return [
    '🎲🇺🇸 CAPUNGA DADOS 🇺🇸🎲',
    '🟡 EMPATE PROTEGIDO',
    `Sinal #${signal.id} continua pendente`,
    `🎯 ${corDisplay(signal.cor)}`,
    `🕒 ${horaAngola()}`
  ].join('\n');
}

async function maybeCreateSignal(round) {
  if (!CONFIG.sinaisAtivos) {
    runtime.motivoSemSinal = 'sinais_desativados';
    return;
  }

  if (!['Jogador', 'Banca'].includes(round.resultado)) {
    runtime.motivoSemSinal = 'rodada_empate';
    return;
  }

  const pending = await getPendingSignal();

  if (pending && !(await expirePendingSignal(pending))) {
    runtime.motivoSemSinal = `pendente_#${pending.id}`;
    return;
  }

  const reds = await redsToday();

  if (reds >= CONFIG.maxRedsDia) {
    runtime.motivoSemSinal = `limite_reds_${reds}`;
    return;
  }

  const lastResult = await pool.query(`
    SELECT criado_em FROM sinais
    ORDER BY criado_em DESC, id DESC
    LIMIT 1
  `);

  const last = lastResult.rows[0];

  if (
    last?.criado_em &&
    Date.now() - new Date(last.criado_em).getTime() < CONFIG.intervaloMinimoMs
  ) {
    runtime.motivoSemSinal = 'intervalo_minimo';
    return;
  }

  const rows = await getRecentOutcomes(30);
  const entry = detectBestEntry(rows);

  if (!entry) {
    runtime.motivoSemSinal = `sem_setup[${rows.slice(0, 6).map(r => r.resultado[0]).join('')}]`;
    return;
  }

  const used = await pool.query(
    `SELECT 1 FROM sinais WHERE resultado_id = $1 LIMIT 1`,
    [round.id]
  );

  if (used.rows.length) {
    runtime.motivoSemSinal = 'rodada_ja_usada';
    return;
  }

  const result = await pool.query(
    `INSERT INTO sinais
      (cor, estado, gale, resultado_id, empates, estrategia)
     VALUES ($1, 'PENDENTE', 0, $2, 0, $3)
     RETURNING *`,
    [entry.cor, round.id, entry.estrategia]
  );

  const signal = result.rows[0];

  runtime.ultimoSinal = {
    id: signal.id,
    cor: signal.cor,
    estado: signal.estado,
    criado_em: signal.criado_em,
    estrategia: entry.estrategia
  };

  runtime.ultimaEstrategia = entry;
  runtime.motivoSemSinal = null;

  await telegramSend(msgNovoSinal(entry));

  log('SINAL CRIADO', {
    id: signal.id,
    cor: entry.cor,
    estrategia: entry.estrategia,
    nome: entry.nome
  });
}

async function resolvePending(round) {
  const signal = await getPendingSignal();
  if (!signal) return;

  if (String(signal.resultado_id) === String(round.id)) return;
  if (await expirePendingSignal(signal)) return;

  if (round.resultado === 'Empate') {
    if (!CONFIG.empateProtegido) return;

    await pool.query(
      `UPDATE sinais SET empates = COALESCE(empates, 0) + 1
       WHERE id = $1 AND estado = 'PENDENTE'`,
      [signal.id]
    );

    await telegramSend(msgEmpate(signal));
    return;
  }

  if (round.resultado === signal.cor) {
    const gale = Number(signal.gale || 0);
    const state = gale === 0 ? 'GREEN' : gale === 1 ? 'G1' : 'G2';

    await pool.query(
      `UPDATE sinais
       SET estado = $1, resultado_final = $2
       WHERE id = $3 AND estado = 'PENDENTE'`,
      [state, round.resultado, signal.id]
    );

    const placar = await getPlacarDia();

    await telegramSend(
      msgResultadoGreen(signal, state) + formatPlacar(placar)
    );

    log('SINAL RESOLVIDO', { id: signal.id, estado: state });
    return;
  }

  const nextGale = Number(signal.gale || 0) + 1;

  if (nextGale <= CONFIG.maxGale) {
    await pool.query(
      `UPDATE sinais SET gale = $1
       WHERE id = $2 AND estado = 'PENDENTE'`,
      [nextGale, signal.id]
    );

    await telegramSend(msgGale(signal, nextGale));
    log('GALE', { id: signal.id, gale: nextGale });
    return;
  }

  await pool.query(
    `UPDATE sinais
     SET estado = 'LOSS', resultado_final = $1
     WHERE id = $2 AND estado = 'PENDENTE'`,
    [round.resultado, signal.id]
  );

  const placar = await getPlacarDia();
  await telegramSend(msgResultadoLoss(signal) + formatPlacar(placar));

  log('SINAL LOSS', { id: signal.id });
}

/* ================= SINCRONIZAÇÃO ================= */

async function syncOnce() {
  if (!pool) throw new Error('DATABASE_URL não configurada');
  if (runtime.sincronizacaoEmCurso) return { sucesso: false, ignorada: true };

  runtime.sincronizacaoEmCurso = true;

  try {
    await ensureSchema();

    const latestDB = await getLatestFromDB();
    runtime.ultimaRodadaDB = latestDB?.fim || null;
    runtime.ultimoIdDB = latestDB?.id || null;

    const count = await pool.query(
      'SELECT COUNT(*)::int AS total FROM bacbo_resultados'
    );

    const hadHistory = count.rows[0].total > 0;
    const rounds = await fetchRounds();

    let inserted = 0;
    let duplicates = 0;
    let evaluated = 0;
    let lastNew = null;

    for (const round of rounds) {
      const saved = await insertRound(round);

      if (!saved) {
        duplicates++;
        continue;
      }

      inserted++;
      lastNew = round;

      // A primeira carga cria histórico. Não trata todo o histórico
      // antigo como resultados novos de uma sessão de apostas.
      if (hadHistory) {
        await resolvePending(round);
        await maybeCreateSignal(round);
        evaluated++;
      }
    }

    if (inserted === 0 && hadHistory) {
      const pending = await getPendingSignal();
      if (pending) await expirePendingSignal(pending);
    }

    runtime.sincronizacaoInicialConcluida = true;
    runtime.ultimaSincronizacao = new Date().toISOString();
    runtime.duplicadosUltimoCiclo = duplicates;
    runtime.novosUltimoCiclo = inserted;
    runtime.erroSincronizacao = null;

    if (lastNew) {
      runtime.ultimaRodadaNova = new Date().toISOString();
      runtime.ultimoIdNovo = lastNew.id;
      runtime.ciclosSemNovos = 0;
    } else {
      runtime.ciclosSemNovos++;
    }

    const summary = {
      recebidos: rounds.length,
      novosGuardados: inserted,
      duplicados: duplicates,
      sinaisAvaliados: evaluated,
      motivoSemSinal: runtime.motivoSemSinal,
      ciclosSemNovos: runtime.ciclosSemNovos,
      horaAngola: horaAngola()
    };

    if (inserted > 0 || runtime.ciclosSemNovos % 8 === 0) {
      log('SINCRONIZACAO', summary);
    }

    return { sucesso: true, ...summary };
  } catch (error) {
    runtime.erroSincronizacao = error.message || String(error);
    console.error('[ERRO SINCRONIZACAO]', runtime.erroSincronizacao);

    return { sucesso: false, erro: runtime.erroSincronizacao };
  } finally {
    runtime.sincronizacaoEmCurso = false;
  }
}

/* ================= ROTAS HTTP ================= */

function requireAdmin(req, res, next) {
  if (!ADMIN_TOKEN) {
    return res.status(503).json({
      sucesso: false,
      erro: 'ADMIN_TOKEN não configurado'
    });
  }

  if (req.get('x-admin-token') !== ADMIN_TOKEN) {
    return res.status(401).json({
      sucesso: false,
      erro: 'Token administrativo inválido'
    });
  }

  next();
}

app.get('/', (req, res) => {
  res.json({
    nome: 'Capunga Dados Multi-Estratégia',
    estado: 'online',
    horaAngola: horaAngola(),
    apiConfigurada: Boolean(API_TOKEN),
    estrategias: [
      'REPETICAO', 'QUEBRA', 'SURF',
      'DOIS_DOIS', 'FORCA', 'NUMERICA'
    ],
    rotas: [
      '/health',
      '/diagnostico',
      '/resultados',
      '/sinais',
      '/estatisticas'
    ]
  });
});

app.get('/health', (req, res) => {
  res.json({
    sucesso: true,
    estado: 'online',
    horaAngola: horaAngola(),
    ultimaSincronizacao: runtime.ultimaSincronizacao,
    novosUltimoCiclo: runtime.novosUltimoCiclo,
    ciclosSemNovos: runtime.ciclosSemNovos,
    erroAPI: runtime.ultimoErroAPI,
    motivoSemSinal: runtime.motivoSemSinal,
    ultimaEstrategia: runtime.ultimaEstrategia
  });
});

app.get('/diagnostico', async (req, res) => {
  let ultimoResultadoDB = null;
  let totalResultados = 0;
  let totalSinais = 0;
  let pendentes = 0;
  let recentes = [];

  try {
    if (pool) {
      await ensureSchema();

      const r1 = await pool.query(`
        SELECT id, resultado, fim, pontos_jogador, pontos_banca
        FROM bacbo_resultados
        ORDER BY fim DESC NULLS LAST, seq DESC
        LIMIT 1
      `);

      ultimoResultadoDB = r1.rows[0] || null;

      totalResultados = (
        await pool.query(
          'SELECT COUNT(*)::int AS total FROM bacbo_resultados'
        )
      ).rows[0].total;

      totalSinais = (
        await pool.query('SELECT COUNT(*)::int AS total FROM sinais')
      ).rows[0].total;

      pendentes = (
        await pool.query(
          `SELECT COUNT(*)::int AS total
           FROM sinais WHERE estado = 'PENDENTE'`
        )
      ).rows[0].total;

      recentes = (
        await pool.query(`
          SELECT resultado, pontos_jogador, pontos_banca, fim
          FROM bacbo_resultados
          ORDER BY fim DESC NULLS LAST, seq DESC
          LIMIT 12
        `)
      ).rows;
    }
  } catch (error) {
    console.error('[DIAGNOSTICO DB]', error.message);
  }

  res.json({
    sucesso: true,
    nome: 'Capunga Dados Multi-Estratégia',
    iniciadoEm: runtime.iniciadoEm,
    horaAngola: horaAngola(),
    telegramConfigurado: Boolean(BOT_TOKEN && CHAT_ID),
    apiConfigurada: Boolean(API_TOKEN),
    httpAPI: runtime.ultimoHTTPAPI,
    erroAPI: runtime.ultimoErroAPI,
    erroSincronizacao: runtime.erroSincronizacao,
    ultimaSincronizacao: runtime.ultimaSincronizacao,
    novosUltimoCiclo: runtime.novosUltimoCiclo,
    ciclosSemNovos: runtime.ciclosSemNovos,
    motivoSemSinal: runtime.motivoSemSinal,
    ultimoSinal: runtime.ultimoSinal,
    ultimaEstrategia: runtime.ultimaEstrategia,
    ultimosResultados: recentes,
    baseDados: {
      totalResultados,
      totalSinais,
      pendentes,
      ultimoResultadoDB
    },
    configuracao: CONFIG
  });
});

app.get('/resultados', async (req, res) => {
  try {
    await ensureSchema();

    const limit = Math.min(
      Math.max(Number.parseInt(req.query.limit, 10) || 50, 1),
      500
    );

    const result = await pool.query(
      `SELECT id, resultado, estado, inicio, fim,
              pontos_jogador, pontos_banca, multiplicador, guardado_em
       FROM bacbo_resultados
       ORDER BY fim DESC NULLS LAST, seq DESC
       LIMIT $1`,
      [limit]
    );

    res.json({
      sucesso: true,
      total: result.rows.length,
      resultados: result.rows
    });
  } catch (error) {
    res.status(500).json({ sucesso: false, erro: error.message });
  }
});

app.get('/sinais', async (req, res) => {
  try {
    await ensureSchema();

    const limit = Math.min(
      Math.max(Number.parseInt(req.query.limit, 10) || 50, 1),
      500
    );

    const result = await pool.query(
      `SELECT * FROM sinais
       ORDER BY criado_em DESC, id DESC
       LIMIT $1`,
      [limit]
    );

    res.json({
      sucesso: true,
      total: result.rows.length,
      sinais: result.rows
    });
  } catch (error) {
    res.status(500).json({ sucesso: false, erro: error.message });
  }
});

app.get('/estatisticas', async (req, res) => {
  try {
    await ensureSchema();

    const result = await pool.query(`
      SELECT
        COUNT(*)::int AS total,
        COUNT(*) FILTER (WHERE estado = 'GREEN')::int AS green,
        COUNT(*) FILTER (WHERE estado = 'G1')::int AS g1,
        COUNT(*) FILTER (WHERE estado = 'G2')::int AS g2,
        COUNT(*) FILTER (WHERE estado = 'LOSS')::int AS loss,
        COUNT(*) FILTER (WHERE estado = 'PENDENTE')::int AS pendentes,
        COUNT(*) FILTER (WHERE estado = 'CANCELADO')::int AS cancelados
      FROM sinais
    `);

    const stats = result.rows[0];
    const acertos = stats.green + stats.g1 + stats.g2;
    const finalizados = acertos + stats.loss;

    let porEstrategia = [];

    try {
      porEstrategia = (
        await pool.query(`
          SELECT
            COALESCE(estrategia, 'DESCONHECIDA') AS estrategia,
            COUNT(*)::int AS total,
            COUNT(*) FILTER (
              WHERE estado IN ('GREEN','G1','G2')
            )::int AS acertos,
            COUNT(*) FILTER (WHERE estado = 'LOSS')::int AS loss
          FROM sinais
          WHERE estado IN ('GREEN','G1','G2','LOSS')
          GROUP BY 1
          ORDER BY total DESC
        `)
      ).rows;
    } catch (_) {}

    let placarDia = null;

    try {
      placarDia = await getPlacarDia();
    } catch (_) {}

    res.json({
      sucesso: true,
      estatisticas: {
        ...stats,
        finalizados,
        acertos,
        taxaAcertoPercent: finalizados
          ? Number(((acertos / finalizados) * 100).toFixed(1))
          : null
      },
      porEstrategia,
      placarDia,
      horaAngola: horaAngola()
    });
  } catch (error) {
    res.status(500).json({ sucesso: false, erro: error.message });
  }
});

app.post('/db/preparar', requireAdmin, async (req, res) => {
  try {
    await ensureSchema();
    res.json({ sucesso: true, mensagem: 'Tabelas preparadas' });
  } catch (error) {
    res.status(500).json({ sucesso: false, erro: error.message });
  }
});

app.post('/db/sincronizar', requireAdmin, async (req, res) => {
  const result = await syncOnce();

  res.status(result.sucesso ? 200 : 500).json(result);
});

app.post('/telegram/teste', requireAdmin, async (req, res) => {
  const enviado = await telegramSend([
    '🎲🇺🇸 CAPUNGA DADOS 🇺🇸🎲',
    '✅ Teste do Telegram',
    `🕒 ${horaAngola()}`
  ].join('\n'));

  res.status(enviado ? 200 : 502).json({
    sucesso: enviado,
    erro: runtime.ultimoErroTelegram
  });
});

app.use((req, res) => {
  res.status(404).json({
    sucesso: false,
    erro: 'Rota não encontrada',
    caminho: req.path
  });
});

/* ================= ARRANQUE E ENCERRAMENTO ================= */

async function start() {
  if (!DATABASE_URL) {
    throw new Error('DATABASE_URL não configurada no Render');
  }

  await ensureSchema();

  server = app.listen(PORT, '0.0.0.0', () => {
    console.log('======================================');
    console.log('CAPUNGA DADOS — MULTI-ESTRATÉGIA');
    console.log(`Porta: ${PORT} | Angola: ${horaAngola()}`);
    console.log(`Sinais: ${CONFIG.sinaisAtivos}`);
    console.log(`Streak mínimo: ${CONFIG.streakMin}`);
    console.log(`Gale máximo: ${CONFIG.maxGale}`);
    console.log(`API configurada: ${Boolean(API_TOKEN)}`);
    console.log(`Intervalo sync: ${CONFIG.intervaloSyncMs}ms`);
    console.log('======================================');
  });

  const initial = await syncOnce();

  if (!initial.sucesso) {
    console.error('[PRIMEIRA SYNC]', initial.erro);
  } else {
    log('PRIMEIRA SYNC', {
      recebidos: initial.recebidos,
      novos: initial.novosGuardados
    });
  }

  syncTimer = setInterval(async () => {
    if (runtime.sincronizacaoEmCurso) return;

    try {
      await syncOnce();
    } catch (error) {
      console.error('[SYNC]', error.message);
    }
  }, CONFIG.intervaloSyncMs);

  if (typeof syncTimer.unref === 'function') syncTimer.unref();
}

process.on('unhandledRejection', reason => {
  console.error('[UNHANDLED]', reason);
});

process.on('uncaughtException', error => {
  console.error('[UNCAUGHT]', error);
});

async function shutdown() {
  if (syncTimer) clearInterval(syncTimer);

  if (server) {
    await new Promise(resolve => server.close(resolve));
  }

  if (pool) await pool.end();

  process.exit(0);
}

process.on('SIGTERM', () => shutdown().catch(() => process.exit(1)));
process.on('SIGINT', () => shutdown().catch(() => process.exit(1)));

start().catch(error => {
  console.error('[FATAL]', error);
  process.exit(1);
});
