
'use strict';

/**
 * CAPUNGA DADOS 🎲🇺🇸
 * Node.js + Express + PostgreSQL + Telegram
 *
 * Variáveis obrigatórias:
 * DATABASE_URL
 * BOT_TOKEN
 * CHAT_ID
 *
 * Variáveis opcionais:
 * SINAIS_ATIVOS=true
 * ADMIN_TOKEN=senha_forte
 * STREAK_MIN=4
 * MAX_GALE=2
 * EMPATE_PROTEGIDO=true
 * MAX_REDS_DIA=3
 * INTERVALO_MINIMO_MS=180000
 * SINAL_EXPIRA_MIN=12
 * INTERVALO_SYNC_MS=10000
 * MAX_ATRASO_SINAL_MIN=3
 * API_BACBO=https://api-cs.casino.org/svc-evolution-game-events/api/bacbo
 */

const express = require('express');
const cors = require('cors');
const axios = require('axios');
const { Pool } = require('pg');
const crypto = require('crypto');

const app = express();

app.disable('x-powered-by');
app.use(cors());
app.use(express.json({ limit: '1mb' }));

const PORT = Number(process.env.PORT || 10000);

const DATABASE_URL = process.env.DATABASE_URL || '';
const BOT_TOKEN = process.env.BOT_TOKEN || '';
const CHAT_ID = process.env.CHAT_ID || '';
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || '';

const API_BACBO =
  process.env.API_BACBO ||
  'https://api-cs.casino.org/svc-evolution-game-events/api/bacbo';

function boolEnv(value, fallback = false) {
  if (value === undefined || value === null || value === '') {
    return fallback;
  }

  return ['true', '1', 'yes', 'sim', 'on'].includes(
    String(value).trim().toLowerCase()
  );
}

function intEnv(name, fallback, min, max) {
  const value = Number.parseInt(process.env[name], 10);

  if (!Number.isFinite(value)) return fallback;

  return Math.max(min, Math.min(max, value));
}

const CONFIG = {
  sinaisAtivos: boolEnv(process.env.SINAIS_ATIVOS, false),
  streakMin: intEnv('STREAK_MIN', 4, 3, 20),
  maxGale: intEnv('MAX_GALE', 2, 0, 5),

  empateProtegido: boolEnv(
    process.env.EMPATE_PROTEGIDO,
    true
  ),

  maxRedsDia: intEnv('MAX_REDS_DIA', 3, 1, 50),

  intervaloMinimoMs: intEnv(
    'INTERVALO_MINIMO_MS',
    180000,
    0,
    86400000
  ),

  sinalExpiraMin: intEnv(
    'SINAL_EXPIRA_MIN',
    12,
    1,
    120
  ),

  intervaloSyncMs: intEnv(
    'INTERVALO_SYNC_MS',
    10000,
    5000,
    300000
  ),

  maxAtrasoSinalMin: intEnv(
    'MAX_ATRASO_SINAL_MIN',
    3,
    1,
    60
  )
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
  erroSincronizacao: null,
  ultimoSinal: null,
  ultimaMensagemTelegram: null,
  ultimoErroTelegram: null,
  ultimaRespostaAPI: null,
  ultimoHTTPAPI: null
};

let schemaPromise = null;
let syncTimer = null;

function log(tag, data) {
  console.log(`[${tag}]`, JSON.stringify(data));
}

function agoraAngola() {
  return new Intl.DateTimeFormat('pt-PT', {
    timeZone: 'Africa/Luanda',
    dateStyle: 'short',
    timeStyle: 'medium',
    hour12: false
  }).format(new Date());
}

function normalizeOutcome(value) {
  if (value && typeof value === 'object') {
    value =
      value.outcome ??
      value.resultado ??
      value.winner ??
      value.name;
  }

  const s = String(value ?? '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .trim()
    .toLowerCase()
    .replace(/[\s_-]+/g, '');

  if ([
    'player',
    'jogador',
    'playerwin',
    'playerwins',
    'playerwon',
    'playerwinner'
  ].includes(s)) {
    return 'Jogador';
  }

  if ([
    'banker',
    'banca',
    'bankerwin',
    'bankerwins',
    'bankerwon',
    'bankerwinner'
  ].includes(s)) {
    return 'Banca';
  }

  if ([
    'tie',
    'draw',
    'empate',
    'equal',
    'push',
    'tied'
  ].includes(s)) {
    return 'Empate';
  }

  return null;
}

function validDate(value) {
  if (!value) return null;

  const date = new Date(value);

  return Number.isNaN(date.getTime())
    ? null
    : date.toISOString();
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
    body.data?.events,
    body.data?.data
  ];

  for (const candidate of candidates) {
    if (Array.isArray(candidate)) return candidate;
  }

  return [];
}

/**
 * A API Evolution usa, entre outros:
 * item.data.id
 * item.data.settledAt
 * item.data.status = "Resolved"
 * item.data.result.outcome = "PlayerWon"/"BankerWon"/"Tie"
 *
 * Mantemos data.id como identificador principal para
 * preservar compatibilidade com o histórico existente.
 */
function normalizeRound(item) {
  const d = item?.data || item?.round || item || {};

  const r =
    d.result && typeof d.result === 'object'
      ? d.result
      : (
          item?.result &&
          typeof item.result === 'object'
        )
        ? item.result
        : {};

  const status = String(
    d.status ??
    d.estado ??
    item?.status ??
    item?.estado ??
    ''
  ).trim().toLowerCase();

  const startedAt = validDate(
    d.startedAt ??
    d.startTime ??
    d.started_at ??
    d.inicio ??
    item?.startedAt ??
    item?.inicio
  );

  const endedAt = validDate(
    d.settledAt ??
    d.endTime ??
    d.endedAt ??
    d.finishedAt ??
    d.fim ??
    item?.settledAt ??
    item?.fim
  );

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
    (
      typeof d.result === 'string'
        ? d.result
        : null
    ) ??
    (
      typeof item?.result === 'string'
        ? item.result
        : null
    );

  const outcome = normalizeOutcome(rawOutcome);

  const resolved =
    [
      'resolved',
      'settled',
      'finished',
      'complete',
      'completed',
      'resolvido',
      'concluido',
      'finalizado'
    ].some(word => status.includes(word)) ||
    Boolean(endedAt);

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

  if (!id || !outcome || !resolved || !endedAt) {
    return null;
  }

  const playerScore = Number(
    r.playerDice?.score ??
    d.playerDice?.score ??
    d.playerScore ??
    d.player_score ??
    d.pontos_jogador ??
    item?.pontos_jogador
  );

  const bankerScore = Number(
    r.bankerDice?.score ??
    d.bankerDice?.score ??
    d.bankerScore ??
    d.banker_score ??
    d.pontos_banca ??
    item?.pontos_banca
  );

  const multiplier =
    r.multiplier ??
    d.multiplier ??
    d.multiplicador ??
    item?.multiplier ??
    item?.multiplicador ??
    null;

  return {
    id,
    resultado: outcome,
    estado: 'Resolved',
    inicio: startedAt || endedAt,
    fim: endedAt,

    pontos_jogador: Number.isFinite(playerScore)
      ? playerScore
      : null,

    pontos_banca: Number.isFinite(bankerScore)
      ? bankerScore
      : null,

    multiplicador: multiplier === null
      ? null
      : String(multiplier)
  };
}

async function telegramSend(texto) {
  if (!BOT_TOKEN || !CHAT_ID) {
    runtime.ultimoErroTelegram =
      'BOT_TOKEN ou CHAT_ID não configurado';

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

    const ok = response.data?.ok === true;

    if (ok) {
      runtime.ultimaMensagemTelegram =
        new Date().toISOString();

      runtime.ultimoErroTelegram = null;
    } else {
      runtime.ultimoErroTelegram =
        JSON.stringify(response.data);
    }

    return ok;
  } catch (error) {
    runtime.ultimoErroTelegram =
      error.response?.data
        ? JSON.stringify(error.response.data)
        : error.message;

    console.error(
      '[TELEGRAM]',
      runtime.ultimoErroTelegram
    );

    return false;
  }
}

async function ensureSchema() {
  if (!pool) {
    throw new Error('DATABASE_URL não configurada');
  }

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
      ADD COLUMN IF NOT EXISTS guardado_em TIMESTAMPTZ
      NOT NULL DEFAULT NOW()
    `);

    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_bacbo_fim
      ON bacbo_resultados (fim DESC)
    `);

    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_bacbo_seq
      ON bacbo_resultados (seq)
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
        empates INTEGER NOT NULL DEFAULT 0
      )
    `);

    await pool.query(`
      ALTER TABLE sinais
      ADD COLUMN IF NOT EXISTS empates INTEGER
      NOT NULL DEFAULT 0
    `);

    await pool.query(`
      ALTER TABLE sinais
      ADD COLUMN IF NOT EXISTS resultado_id TEXT
    `);

    await pool.query(`
      ALTER TABLE sinais
      ADD COLUMN IF NOT EXISTS resultado_final TEXT
    `);

    await pool.query(`
      ALTER TABLE sinais
      ADD COLUMN IF NOT EXISTS gale INTEGER
      NOT NULL DEFAULT 0
    `);

    await pool.query(`
      ALTER TABLE sinais
      ADD COLUMN IF NOT EXISTS criado_em TIMESTAMPTZ
      NOT NULL DEFAULT NOW()
    `);

    await pool.query(`
      ALTER TABLE sinais
      ADD COLUMN IF NOT EXISTS estado TEXT
      NOT NULL DEFAULT 'PENDENTE'
    `);

    await pool.query(`
      ALTER TABLE sinais
      ADD COLUMN IF NOT EXISTS cor TEXT
    `);

    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_sinais_estado
      ON sinais (estado)
    `);

    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_sinais_criado
      ON sinais (criado_em DESC)
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
  const response = await axios.get(API_BACBO, {
    timeout: 20000,
    headers: {
      Accept: 'application/json',
      'User-Agent': 'CapungaDadosBot/1.0'
    }
  });

  runtime.ultimoHTTPAPI = response.status;

  const items = extractItems(response.data);
  const rounds = [];

  for (const item of items) {
    const round = normalizeRound(item);

    if (round) rounds.push(round);
  }

  const unique = new Map();

  for (const round of rounds) {
    unique.set(round.id, round);
  }

  const result = [...unique.values()].sort(
    (a, b) =>
      new Date(a.fim).getTime() -
      new Date(b.fim).getTime()
  );

  const latest = result[result.length - 1] || null;

  runtime.itensRecebidosAPI = items.length;
  runtime.quantidadeAPI = result.length;
  runtime.ultimaHoraDaAPI = latest?.fim || null;

  runtime.ultimaRespostaAPI = latest
    ? {
        id: latest.id,
        fim: latest.fim,
        resultado: latest.resultado
      }
    : null;

  log('API BACBO', {
    itensRecebidos: items.length,
    rodadasValidas: result.length,
    rodadaMaisRecente: latest?.id || null,
    horaMaisRecente: latest?.fim || null
  });

  if (items.length > 0 && result.length === 0) {
    log('AVISO API', {
      mensagem: 'A API respondeu, mas nenhuma rodada foi validada.',
      exemploChaves: Object.keys(items[0] || {}),
      exemploData: Object.keys(items[0]?.data || {}),
      exemploOutcome:
        items[0]?.data?.result?.outcome ?? null
    });
  }

  return result;
}

async function insertRound(round) {
  const result = await pool.query(
    `
      INSERT INTO bacbo_resultados (
        id,
        resultado,
        estado,
        inicio,
        fim,
        pontos_jogador,
        pontos_banca,
        multiplicador
      )
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
      ON CONFLICT (id) DO NOTHING
      RETURNING id
    `,
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

async function getRecentOutcomes(limit = 20) {
  const result = await pool.query(
    `
      SELECT id, resultado, fim
      FROM bacbo_resultados
      WHERE resultado IN ('Jogador', 'Banca')
      ORDER BY fim DESC NULLS LAST, seq DESC
      LIMIT $1
    `,
    [limit]
  );

  return result.rows;
}

function detectEntry(rows) {
  const min = CONFIG.streakMin;

  if (rows.length < min + 1) return null;

  const color = rows[0].resultado;

  if (!['Jogador', 'Banca'].includes(color)) {
    return null;
  }

  const recent = rows.slice(0, min);

  if (!recent.every(row => row.resultado === color)) {
    return null;
  }

  const previous = rows[min];

  if (!previous || previous.resultado === color) {
    return null;
  }

  return color;
}

async function redsToday() {
  const result = await pool.query(`
    SELECT COUNT(*)::int AS total
    FROM sinais
    WHERE estado = 'LOSS'
      AND criado_em >= date_trunc('day', NOW())
  `);

  return result.rows[0]?.total || 0;
}

async function getPendingSignal() {
  const result = await pool.query(`
    SELECT *
    FROM sinais
    WHERE estado = 'PENDENTE'
    ORDER BY criado_em ASC, id ASC
    LIMIT 1
  `);

  return result.rows[0] || null;
}

async function expirePendingSignal(signal) {
  if (!signal) return false;

  const ageMs =
    Date.now() - new Date(signal.criado_em).getTime();

  const expiryMs = CONFIG.sinalExpiraMin * 60 * 1000;

  if (ageMs < expiryMs) return false;

  await pool.query(
    `
      UPDATE sinais
      SET estado = 'CANCELADO'
      WHERE id = $1
        AND estado = 'PENDENTE'
    `,
    [signal.id]
  );

  await telegramSend(
    [
      '⌛ SINAL EXPIRADO',
      `Sinal: #${signal.id}`,
      'O sinal expirou sem conclusão.',
      'Não foi contabilizado como GREEN ou LOSS.',
      `Hora Angola: ${agoraAngola()}`
    ].join('\n')
  );

  log('SINAL EXPIRADO', { id: signal.id });

  return true;
}

async function maybeCreateSignal(round) {
  if (!CONFIG.sinaisAtivos) return;

  if (!['Jogador', 'Banca'].includes(round.resultado)) {
    return;
  }

  const pending = await getPendingSignal();

  if (pending) return;

  const latestSignalResult = await pool.query(`
    SELECT id, criado_em, estado
    FROM sinais
    ORDER BY criado_em DESC, id DESC
    LIMIT 1
  `);

  const last = latestSignalResult.rows[0];

  if (last?.criado_em) {
    const elapsed =
      Date.now() - new Date(last.criado_em).getTime();

    if (elapsed < CONFIG.intervaloMinimoMs) return;
  }

  if ((await redsToday()) >= CONFIG.maxRedsDia) {
    log('SINAIS PAUSADOS', {
      motivo: 'Limite diário de LOSS atingido',
      maxRedsDia: CONFIG.maxRedsDia
    });

    return;
  }

  const rows = await getRecentOutcomes(
    CONFIG.streakMin + 1
  );

  const color = detectEntry(rows);

  if (!color) return;

  const inserted = await pool.query(
    `
      INSERT INTO sinais (
        cor,
        estado,
        gale,
        resultado_id,
        resultado_final,
        empates
      )
      VALUES ($1, 'PENDENTE', 0, $2, NULL, 0)
      RETURNING *
    `,
    [color, round.id]
  );

  const signal = inserted.rows[0];

  runtime.ultimoSinal = {
    id: signal.id,
    cor: signal.cor,
    estado: signal.estado,
    criado_em: signal.criado_em
  };

  const sent = await telegramSend(
    [
      '🎲 CAPUNGA DADOS 🇺🇸',
      '',
      '📣 NOVO SINAL BAC BO',
      `🎯 Entrada: ${color.toUpperCase()}`,
      `🛡️ Gale máximo: ${CONFIG.maxGale}`,
      CONFIG.empateProtegido
        ? '🤝 Empate: protegido'
        : '🤝 Empate: não protegido',
      `🕒 Hora Angola: ${agoraAngola()}`,
      '',
      '⚠️ Sinal estatístico, sem garantia de acerto.'
    ].join('\n')
  );

  log('SINAL CRIADO', {
    id: signal.id,
    cor: color,
    rodada: round.id,
    telegramEnviado: sent
  });
}

async function resolvePending(round) {
  const signal = await getPendingSignal();

  if (!signal) return;

  // Não utilizar a rodada que originou o sinal.
  if (String(signal.resultado_id) === String(round.id)) {
    return;
  }

  if (await expirePendingSignal(signal)) {
    return;
  }

  if (round.resultado === 'Empate') {
    if (!CONFIG.empateProtegido) return;

    await pool.query(
      `
        UPDATE sinais
        SET empates = COALESCE(empates, 0) + 1
        WHERE id = $1
          AND estado = 'PENDENTE'
      `,
      [signal.id]
    );

    await telegramSend(
      [
        '🤝 EMPATE — SINAL PROTEGIDO',
        `Sinal: #${signal.id}`,
        'O sinal continua pendente.',
        `Hora Angola: ${agoraAngola()}`
      ].join('\n')
    );

    return;
  }

  if (round.resultado === signal.cor) {
    const finalState =
      Number(signal.gale || 0) === 0
        ? 'GREEN'
        : 'G1';

    await pool.query(
      `
        UPDATE sinais
        SET estado = $1,
            resultado_final = $2
        WHERE id = $3
          AND estado = 'PENDENTE'
      `,
      [finalState, round.resultado, signal.id]
    );

    await telegramSend(
      [
        '✅ RESULTADO DO SINAL',
        `Sinal: #${signal.id}`,
        `Entrada: ${signal.cor}`,
        `Resultado: ${
          finalState === 'GREEN'
            ? 'GREEN DE PRIMEIRA'
            : 'GREEN NO GALE ' + signal.gale
        }`,
        `Rodada: ${round.resultado}`,
        `Hora Angola: ${agoraAngola()}`
      ].join('\n')
    );

    return;
  }

  const nextGale = Number(signal.gale || 0) + 1;

  if (nextGale <= CONFIG.maxGale) {
    await pool.query(
      `
        UPDATE sinais
        SET gale = $1
        WHERE id = $2
          AND estado = 'PENDENTE'
      `,
      [nextGale, signal.id]
    );

    await telegramSend(
      [
        '🔁 PROTEÇÃO GALE',
        `Sinal: #${signal.id}`,
        `Entrada original: ${signal.cor}`,
        `Gale atual: ${nextGale}`,
        `Máximo: ${CONFIG.maxGale}`,
        `Hora Angola: ${agoraAngola()}`
      ].join('\n')
    );

    return;
  }

  await pool.query(
    `
      UPDATE sinais
      SET estado = 'LOSS',
          resultado_final = $1
      WHERE id = $2
        AND estado = 'PENDENTE'
    `,
    [round.resultado, signal.id]
  );

  await telegramSend(
    [
      '❌ SINAL FINALIZADO',
      `Sinal: #${signal.id}`,
      `Entrada: ${signal.cor}`,
      'Resultado final: LOSS',
      `Gales utilizados: ${CONFIG.maxGale}`,
      `Hora Angola: ${agoraAngola()}`
    ].join('\n')
  );
}

async function syncOnce() {
  if (!pool) {
    throw new Error('DATABASE_URL não configurada');
  }

  if (runtime.sincronizacaoEmCurso) {
    return { ignorada: true, motivo: 'Sincronização em curso' };
  }

  runtime.sincronizacaoEmCurso = true;

  try {
    await ensureSchema();

    const countResult = await pool.query(
  // ============================================================
// INICIALIZAÇÃO DO SERVIDOR
// ============================================================

async function start() {
  try {
    console.log('========================================');
    console.log('🎲 CAPUNGA DADOS — INICIALIZAÇÃO');
    console.log('========================================');

    if (!DATABASE_URL) {
      throw new Error('DATABASE_URL não configurada.');
    }

    
    if (!BOT_TOKEN || !CHAT_ID) {
      console.warn(
        '[AVISO] BOT_TOKEN ou CHAT_ID não configurado. ' +
        'As notificações Telegram não funcionarão.'
      );
    }

    await ensureSchema();

    console.log('[DB] Tabelas preparadas com sucesso.');

    app.listen(PORT, '0.0.0.0', () => {
      console.log(`🚀 Capunga Dados ativo na porta ${PORT}`);
      console.log(`🌍 Hora Angola: ${horaAngola()}`);
      console.log(
        `🎯 Sinais automáticos: ${SINAIS_ATIVOS ? 'ATIVOS' : 'DESATIVADOS'}`
      );
      console.log(`⏱️ Intervalo de sincronização: ${INTERVALO_SYNC_MS} ms`);
    });

    // Primeira sincronização
    try {
      await syncOnce();
    } catch (error) {
      runtime.erroSincronizacao =
        error?.message || String(error);

      console.error(
        '[ERRO NA PRIMEIRA SINCRONIZAÇÃO]',
        error
      );
    }

    // Sincronização contínua
    setInterval(async () => {
      if (runtime.sincronizacaoEmCurso) {
        return;
      }

      try {
        await syncOnce();
      } catch (error) {
        runtime.erroSincronizacao =
          error?.message || String(error);

 
      }
    }, INTERVALO_SYNC_MS);

  } catch (error) {
    console.error('[ERRO FATAL NA INICIALIZAÇÃO]', error);
    process.exit(1);
  }
}

// ============================================================
// TRATAMENTO DE ERROS DO PROCESSO
// ============================================================

process.on('unhandledRejection', (reason) => {
  console.error('[UNHANDLED REJECTION]', reason);
});

process.on('uncaughtException', (error) => {
  console.error('[UNCAUGHT EXCEPTION]', error);
});

process.on('SIGTERM', async () => {
  console.log('[SISTEMA] Recebido SIGTERM. A encerrar...');

  try {
    if (pool) {
      await pool.end();
    }
  } catch (error) {
    console.error('[ERRO AO FECHAR A BASE DE DADOS]', error);
  }

  process.exit(0);
});

// ============================================================
// EXECUTAR O SERVIDOR
// ============================================================

start().catch((error) => {
  console.error('[FATAL]', error);
  process.exit(1);
});
;
