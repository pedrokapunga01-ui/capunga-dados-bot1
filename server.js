'use strict';

/**
 * Capunga Dados 🎲🇺🇸
 * Node.js + Express + PostgreSQL + Telegram
 *
 * Variáveis obrigatórias:
 * DATABASE_URL, BOT_TOKEN, CHAT_ID
 *
 * Variáveis opcionais:
 * SINAIS_ATIVOS=true
 * ADMIN_TOKEN=uma_senha_forte
 * STREAK_MIN=4
 * MAX_GALE=2
 * EMPATE_PROTEGIDO=true
 * MAX_REDS_DIA=3
 * INTERVALO_MINIMO_MS=180000
 * SINAL_EXPIRA_MIN=12
 * INTERVALO_SYNC_MS=10000
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

const API_BACBO =
  process.env.API_BACBO ||
  'https://api-cs.casino.org/svc-evolution-game-events/api/bacbo';

const ADMIN_TOKEN = process.env.ADMIN_TOKEN || '';

function boolEnv(value, fallback = false) {
  if (value === undefined || value === null || value === '') {
    return fallback;
  }

  return ['true', '1', 'yes', 'sim', 'on']
    .includes(String(value).trim().toLowerCase());
}

function intEnv(name, fallback, min, max) {
  const parsed = Number.parseInt(process.env[name], 10);

  if (!Number.isFinite(parsed)) return fallback;

  return Math.max(min, Math.min(max, parsed));
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
  sinalExpiraMin: intEnv('SINAL_EXPIRA_MIN', 12, 1, 120),
  intervaloSyncMs: intEnv(
    'INTERVALO_SYNC_MS',
    10000,
    5000,
    300000
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
  duplicadosUltimoCiclo: 0,
  erroSincronizacao: null,
  ultimoSinal: null,
  ultimaMensagemTelegram: null,
  ultimoErroTelegram: null,
  ultimaRespostaAPI: null
};

let schemaPromise = null;
let syncTimer = null;

function log(tag, data) {
  console.log(
    `[${tag}]`,
    JSON.stringify(data)
  );
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
      value.result ??
      value.winner ??
      value.name;
  }

  const s = String(value ?? '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .trim()
    .toLowerCase();

  if (
    ['player', 'jogador', 'player wins', 'player_win']
      .includes(s)
  ) {
    return 'Jogador';
  }

  if (
    ['banker', 'banca', 'banker wins', 'banker_win']
      .includes(s)
  ) {
    return 'Banca';
  }

  if (
    ['tie', 'draw', 'empate', 'equal', 'push']
      .includes(s)
  ) {
    return 'Empate';
  }

  return null;
}

function validDate(value) {
  if (!value) return null;

  const d = new Date(value);

  return Number.isNaN(d.getTime())
    ? null
    : d.toISOString();
}

function extractItems(body) {
  if (Array.isArray(body)) return body;

  const candidates = [
    body?.data,
    body?.results,
    body?.items,
    body?.resultados,
    body?.rounds,
    body?.events,
    body?.data?.results,
    body?.data?.items,
    body?.data?.resultados,
    body?.data?.rounds,
    body?.data?.events,
    body?.data?.data
  ];

  for (const candidate of candidates) {
    if (Array.isArray(candidate)) return candidate;
  }

  return [];
}

function normalizeRound(item) {
  const d = item?.data || item?.round || item || {};
  const r =
    (d.result && typeof d.result === 'object')
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
  )
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase();

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

  const outcome = normalizeOutcome(
    r.outcome ??
    r.resultado ??
    r.result ??
    d.outcome ??
    d.resultado ??
    d.winner ??
    item?.outcome ??
    item?.resultado ??
    item?.winner ??
    (typeof d.result === 'string' ? d.result : null) ??
    (typeof item?.result === 'string' ? item.result : null)
  );

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
    ].some(s => status.includes(s)) ||
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
    d.playerDice?.score ??
    d.playerScore ??
    d.player_score ??
    d.pontos_jogador ??
    item?.pontos_jogador ??
    r.pontos_jogador
  );

  const bankerScore = Number(
    d.bankerDice?.score ??
    d.bankerScore ??
    d.banker_score ??
    d.pontos_banca ??
    item?.pontos_banca ??
    r.pontos_banca
  );

  const multiplier =
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

    runtime.ultimaMensagemTelegram = ok
      ? new Date().toISOString()
      : runtime.ultimaMensagemTelegram;

    runtime.ultimoErroTelegram = ok
      ? null
      : JSON.stringify(response.data);

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

  const items = extractItems(response.data);
  const rounds = items
    .map(normalizeRound)
    .filter(Boolean);

  const unique = new Map();

  for (const round of rounds) {
    unique.set(round.id, round);
  }

  const result = [...unique.values()]
    .sort((a, b) =>
      new Date(a.fim).getTime() -
      new Date(b.fim).getTime()
    );

  const latest = result[result.length - 1] || null;

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
      RETURNING *
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

  return result.rows[0] || null;
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

  const recent = rows.slice(0, min);
  const color = recent[0].resultado;

  if (!['Jogador', 'Banca'].includes(color)) {
    return null;
  }

  const sameStreak = recent.every(
    row => row.resultado === color
  );

  if (!sameStreak) return null;

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

async function maybeCreateSignal(round) {
  if (!CONFIG.sinaisAtivos) return;
  if (!['Jogador', 'Banca'].includes(round.resultado)) return;

  const latestSignal = await pool.query(`
    SELECT id, criado_em, estado
    FROM sinais
    ORDER BY criado_em DESC, id DESC
    LIMIT 1
  `);

  const last = latestSignal.rows[0];

  if (last?.estado === 'PENDENTE') return;

  if (last?.criado_em) {
    const elapsed =
      Date.now() - new Date(last.criado_em).getTime();

    if (elapsed < CONFIG.intervaloMinimoMs) return;
  }

  if ((await redsToday()) >= CONFIG.maxRedsDia) return;

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

  await telegramSend(
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
    rodada: round.id
  });
}

async function resolvePending(round) {
  const pending = await pool.query(`
    SELECT *
    FROM sinais
    WHERE estado = 'PENDENTE'
    ORDER BY criado_em ASC, id ASC
    LIMIT 1
  `);

  const signal = pending.rows[0];

  if (!signal) return;

  // A rodada que originou o sinal não o pode resolver.
  if (String(signal.resultado_id) === String(round.id)) {
    return;
  }

  if (round.resultado === 'Empate' && CONFIG.empateProtegido) {
    await pool.query(
      `
        UPDATE sinais
        SET empates = COALESCE(empates, 0) + 1
        WHERE id = $1
      `,
      [signal.id]
    );

    await telegramSend(
      [
        '🤝 EMPATE — SINAL PROTEGIDO',
        `Sinal: #${signal.id}`,
        'A entrada mantém-se pendente.',
        `Hora Angola: ${agoraAngola()}`
      ].join('\n')
    );

    return;
  }

  if (round.resultado === 'Empate') {
    return;
  }

  if (round.resultado === signal.cor) {
    await pool.query(
      `
        UPDATE sinais
        SET estado = $1,
            resultado_final = $2
        WHERE id = $3
      `,
      [
        signal.gale === 0 ? 'GREEN' : 'G1',
        round.resultado,
        signal.id
      ]
    );

    await telegramSend(
      [
        '✅ RESULTADO DO SINAL',
        `Sinal: #${signal.id}`,
        `Entrada: ${signal.cor}`,
        `Resultado: ${signal.gale === 0 ? 'GREEN' : 'GREEN NO GALE ' + signal.gale}`,
        `Resultado da rodada: ${round.resultado}`,
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
    `,
    [round.resultado, signal.id]
  );

  await telegramSend(
    [
      '❌ SINAL FINALIZADO',
      `Sinal: #${signal.id}`,
      `Entrada: ${signal.cor}`,
      `Resultado final: LOSS`,
      `Gales utilizados: ${CONFIG.maxGale}`,
      `Hora Angola: ${agoraAngola()}`
    ].join('\n')
  );
}

async function syncOnce() {
  if (!pool) throw new Error('DATABASE_URL não configurada');
  if (runtime.sincronizacaoEmCurso) return;

  runtime.sincronizacaoEmCurso = true;

  try {
    await ensureSchema();

    const countResult = await pool.query(
      'SELECT COUNT(*)::int AS total FROM bacbo_resultados'
    );

    const hadHistory = countResult.rows[0].total > 0;
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

      if (hadHistory) {
        // Resolve o sinal existente antes de procurar outro.
        await resolvePending(round);
        await maybeCreateSignal(round);
        evaluated++;
      }
    }

    runtime.sincronizacaoInicialConcluida = true;
    runtime.ultimaSincronizacao = new Date().toISOString();
    runtime.duplicadosUltimoCiclo = duplicates;
    runtime.erroSincronizacao = null;

    if (lastNew) {
      runtime.ultimaRodadaNova = new Date().toISOString();
      runtime.ultimoIdNovo = lastNew.id;
    }

    log('SINCRONIZACAO', {
      recebidos: rounds.length,
      novosGuardados: inserted,
      duplicados: duplicates,
      sinaisAvaliados: evaluated,
      primeiraSincronizacao: !runtime.sincronizacaoInicialConcluida,
      historicoPreexistente: hadHistory,
      ultimaRodadaNova: lastNew?.id || null,
      horaAPI: runtime.ultimaHoraDaAPI,
      horaAngola: agoraAngola()
    });
  } catch (error) {
    runtime.erroSincronizacao = error.message;

    console.error(
      '[ERRO SINCRONIZACAO]',
      error.response?.data || error.message
    );
  } finally {
    runtime.sincronizacaoEmCurso = false;
  }
}

async function requireDatabase(req, res, next) {
  if (!pool) {
    return res.status(503).json({
      sucesso: false,
      erro: 'DATABASE_URL não configurada'
    });
  }

  try {
    await ensureSchema();
    next();
  } catch (error) {
    res.status(500).json({
      sucesso: false,
      erro: 'Erro na base de dados',
      detalhe: error.message
    });
  }
}

function requireAdmin(req, res, next) {
  if (!ADMIN_TOKEN) {
    return res.status(503).json({
      sucesso: false,
      erro: 'Configure ADMIN_TOKEN no Render'
    });
  }

  const supplied =
    req.get('x-admin-token') ||
    req.query.token ||
    req.body?.token ||
    '';

  const a = Buffer.from(String(supplied));
  const b = Buffer.from(String(ADMIN_TOKEN));

  if (
    a.length !== b.length ||
    !crypto.timingSafeEqual(a, b)
  ) {
    return res.status(401).json({
      sucesso: false,
      erro: 'Não autorizado'
    });
  }

  next();
}

app.get('/', (req, res) => {
  res.json({
    nome: 'Capunga Dados 🎲🇺🇸',
    servidor: 'online',
    horaAngola: agoraAngola(),
    endpoints: [
      '/health',
      '/diagnostico',
      '/resultados',
      '/sinais',
      '/estatisticas',
      '/testes'
    ]
  });
});

app.get('/health', (req, res) => {
  res.json({
    sucesso: true,
    servidor: 'online',
    horaAngola: agoraAngola()
  });
});

app.get('/diagnostico', async (req, res) => {
  let dbOk = false;
  let dbError = null;

  if (pool) {
    try {
      await pool.query('SELECT 1');
      dbOk = true;
    } catch (error) {
      dbError = error.message;
    }
  }

  res.json({
    servidor: 'online',
    baseConfigurada: Boolean(DATABASE_URL),
    baseConectada: dbOk,
    erroBase: dbError,
    telegramConfigurado: Boolean(BOT_TOKEN && CHAT_ID),
    sinaisAtivos: CONFIG.sinaisAtivos,
    tabelasPreparadas: Boolean(schemaPromise),
    sincronizacaoEmCurso: runtime.sincronizacaoEmCurso,
    sincronizacaoInicialConcluida:
      runtime.sincronizacaoInicialConcluida,
    ultimaSincronizacao: runtime.ultimaSincronizacao,
    ultimaRodadaNova: runtime.ultimaRodadaNova,
    ultimoIdNovo: runtime.ultimoIdNovo,
    ultimaHoraDaAPI: runtime.ultimaHoraDaAPI,
    quantidadeAPI: runtime.quantidadeAPI,
    duplicadosUltimoCiclo: runtime.duplicadosUltimoCiclo,
    ultimaRespostaAPI: runtime.ultimaRespostaAPI,
    ultimoSinal: runtime.ultimoSinal,
    ultimaMensagemTelegram: runtime.ultimaMensagemTelegram,
    erroTelegram: runtime.ultimoErroTelegram,
    erroSincronizacao: runtime.erroSincronizacao,
    horaAngola: agoraAngola(),
    configuracao: {
      streakMin: CONFIG.streakMin,
      maxGale: CONFIG.maxGale,
      empateProtegido: CONFIG.empateProtegido,
      maxRedsDia: CONFIG.maxRedsDia,
      intervaloMinimoMs: CONFIG.intervaloMinimoMs,
      intervaloSyncMs: CONFIG.intervaloSyncMs
    }
  });
});

app.get('/resultados', requireDatabase, async (req, res) => {
  try {
    const limit = Math.min(
      500,
      Math.max(1, Number.parseInt(req.query.limit, 10) || 50)
    );

    const result = await pool.query(
      `SELECT id, resultado, estado, inicio, fim,
              pontos_jogador, pontos_banca, multiplicador,
              guardado_em
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
    res.status(500).json({
      sucesso: false,
      erro: error.message
    });
  }
});

app.get('/sinais', requireDatabase, async (req, res) => {
  try {
    const limit = Math.min(
      500,
      Math.max(1, Number.parseInt(req.query.limit, 10) || 50)
    );

    const result = await pool.query(
      `SELECT *
       FROM sinais
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
    res.status(500).json({
      sucesso: false,
      erro: error.message
    });
  }
});

app.get('/estatisticas', requireDatabase, async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        COUNT(*)::int AS total,
        COUNT(*) FILTER (
          WHERE estado = 'PENDENTE'
        )::int AS pendentes,
        COUNT(*) FILTER (
          WHERE estado = 'GREEN'
        )::int AS greens,
        COUNT(*) FILTER (
          WHERE estado = 'G1'
        )::int AS greens_gale,
        COUNT(*) FILTER (
          WHERE estado = 'LOSS'
        )::int AS losses,
        COALESCE(SUM(empates), 0)::int AS empates_protegidos
      FROM sinais
    `);

    res.json({
      sucesso: true,
      estatisticas: result.rows[0],
      horaAngola: agoraAngola()
    });
  } catch (error) {
    res.status(500).json({
      sucesso: false,
      erro: error.message
    });
  }
});

app.get('/testes', async (req, res) => {
  let database = false;
  let databaseError = null;

  try {
    if (pool) {
      await pool.query('SELECT 1');
      database = true;
    }
  } catch (error) {
    databaseError = error.message;
  }

  res.json({
    sucesso: true,
    servidor: true,
    database,
    databaseError,
    telegramConfigurado: Boolean(BOT_TOKEN && CHAT_ID),
    apiConfigurada: Boolean(API_BACBO),
    sinaisAtivos: CONFIG.sinaisAtivos,
    horaAngola: agoraAngola()
  });
});

app.post(
  '/db/preparar',
  requireAdmin,
  requireDatabase,
  async (req, res) => {
    res.json({
      sucesso: true,
      mensagem: 'Estrutura verificada e preparada'
    });
  }
);

app.post(
  '/db/sincronizar',
  requireAdmin,
  requireDatabase,
  async (req, res) => {
    await syncOnce();

    res.json({
      sucesso: !runtime.erroSincronizacao,
      ultimaSincronizacao: runtime.ultimaSincronizacao,
      ultimaHoraDaAPI: runtime.ultimaHoraDaAPI,
      ultimoIdNovo: runtime.ultimoIdNovo,
      erro: runtime.erroSincronizacao
    });
  }
);

app.post('/telegram/teste', requireAdmin, async (req, res) => {
  const ok = await telegramSend(
    [
      '✅ TESTE CAPUNGA DADOS',
      '',
      'A ligação ao Telegram foi testada.',
      `Hora Angola: ${agoraAngola()}`
    ].join('\n')
  );

  res.status(ok ? 200 : 502).json({
    sucesso: ok,
    erro: runtime.ultimoErroTelegram
  });
});

app.use((req, res) => {
  res.status(404).json({
    sucesso: false,
    erro: 'Rota não encontrada'
  });
});

async function start() {
  if (!DATABASE_URL) {
    console.error('ERRO: falta DATABASE_URL no Render');
  }

  if (!BOT_TOKEN || !CHAT_ID) {
    console.error('AVISO: falta BOT_TOKEN ou CHAT_ID');
  }

  if (pool) {
    try {
      await ensureSchema();
    } catch (error) {
      console.error('[DB INICIAL]', error.message);
    }
  }

  app.listen(PORT, '0.0.0.0', () => {
    console.log(
      `Capunga Dados 🎲🇺🇸 ativo na porta ${PORT}`
    );
    console.log(`Hora Angola: ${agoraAngola()}`);
    console.log(
      `Sinais automáticos: ${
        CONFIG.sinaisAtivos ? 'ATIVOS' : 'DESATIVADOS'
      }`
    );
  });

  if (pool) {
    await syncOnce();

    syncTimer = setInterval(() => {
      syncOnce().catch(error => {
        runtime.erroSincronizacao = error.message;
        console.error('[SYNC TIMER]', error.message);
      });
    }, CONFIG.intervaloSyncMs);

    if (typeof syncTimer.unref === 'function') {
      syncTimer.unref();
    }
  }
}

process.on('unhandledRejection', error => {
  console.error('[UNHANDLED REJECTION]', error);
});

process.on('uncaughtException', error => {
  console.error('[UNCAUGHT EXCEPTION]', error);
});

process.on('SIGTERM', async () => {
  if (syncTimer) clearInterval(syncTimer);

  try {
    if (pool) await pool.end();
  } finally {
    process.exit(0);
  }
});

start().catch(error => {
  console.error('[FATAL]', error);
  process.exit(1);
});
