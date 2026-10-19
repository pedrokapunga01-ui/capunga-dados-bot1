'use strict';

/**
 * CAPUNGA DADOS — BAC BO
 * Node.js + Express + PostgreSQL + Telegram
 *
 * Variáveis no Render:
 * DATABASE_URL
 * BOT_TOKEN
 * CHAT_ID
 * ADMIN_TOKEN
 *
 * SINAIS_ATIVOS=false por padrão.
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

const API_BACBO =
  process.env.API_BACBO ||
  'https://api-cs.casino.org/svc-evolution-game-events/api/bacbo';

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
  sinaisAtivos: boolEnv(process.env.SINAIS_ATIVOS, false),
  streakMin: intEnv('STREAK_MIN', 4, 3, 20),
  maxGale: intEnv('MAX_GALE', 1, 0, 5),
  empateProtegido: boolEnv(process.env.EMPATE_PROTEGIDO, true),
  maxRedsDia: intEnv('MAX_REDS_DIA', 3, 1, 50),
  intervaloMinimoMs: intEnv(
    'INTERVALO_MINIMO_MS', 180000, 0, 86400000
  ),
  sinalExpiraMin: intEnv('SINAL_EXPIRA_MIN', 12, 1, 120),
  intervaloSyncMs: intEnv(
    'INTERVALO_SYNC_MS', 15000, 5000, 300000
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
  ultimoHTTPAPI: null
};

let schemaPromise = null;
let server = null;
let syncTimer = null;

function log(tag, data) {
  console.log(`[${tag}]`, JSON.stringify(data));
}

function horaAngola() {
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
    'player', 'jogador', 'playerwin',
    'playerwins', 'playerwon', 'playerwinner'
  ].includes(s)) return 'Jogador';

  if ([
    'banker', 'banca', 'bankerwin',
    'bankerwins', 'bankerwon', 'bankerwinner',
    'red', 'vermelho'
  ].includes(s)) return 'Banca';

  if ([
    'tie', 'draw', 'empate', 'equal', 'push', 'tied'
  ].includes(s)) return 'Empate';

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

  return candidates.find(Array.isArray) || [];
}

function normalizeRound(item) {
  const d = item?.data || item?.round || item || {};

  const r = d.result && typeof d.result === 'object'
    ? d.result
    : item?.result && typeof item.result === 'object'
      ? item.result
      : {};

  const status = String(
    d.status ?? d.estado ?? item?.status ?? item?.estado ?? ''
  ).toLowerCase();

  const inicio = validDate(
    d.startedAt ?? d.startTime ?? d.started_at ??
    d.inicio ?? item?.startedAt ?? item?.inicio
  );

  const fim = validDate(
    d.settledAt ?? d.endTime ?? d.endedAt ??
    d.finishedAt ?? d.fim ?? item?.settledAt ?? item?.fim
  );

  const rawOutcome =
    r.outcome ?? r.resultado ?? r.winner ??
    d.outcome ?? d.resultado ?? d.winner ??
    item?.outcome ?? item?.resultado ?? item?.winner ??
    (typeof d.result === 'string' ? d.result : null) ??
    (typeof item?.result === 'string' ? item.result : null);

  const resultado = normalizeOutcome(rawOutcome);

  const resolvida = [
    'resolved', 'settled', 'finished', 'complete',
    'completed', 'resolvido', 'concluido', 'finalizado'
  ].some(word => status.includes(word)) || Boolean(fim);

  const id = String(
    d.id ?? d.roundId ?? d.round_id ?? d.eventId ??
    item?.id ?? item?.roundId ?? item?.round_id ??
    item?.eventId ?? ''
  ).trim();

  if (!id || !resultado || !resolvida || !fim) return null;

  const scoreJogador = Number(
    r.playerDice?.score ?? d.playerDice?.score ??
    d.playerScore ?? d.player_score ?? d.pontos_jogador
  );

  const scoreBanca = Number(
    r.bankerDice?.score ?? d.bankerDice?.score ??
    d.bankerScore ?? d.banker_score ?? d.pontos_banca
  );

  const multiplicador =
    r.multiplier ?? d.multiplier ?? d.multiplicador ?? null;

  return {
    id,
    resultado,
    estado: 'Resolved',
    inicio: inicio || fim,
    fim,
    pontos_jogador: Number.isFinite(scoreJogador)
      ? scoreJogador : null,
    pontos_banca: Number.isFinite(scoreBanca)
      ? scoreBanca : null,
    multiplicador: multiplicador == null
      ? null : String(multiplicador)
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
      ADD COLUMN IF NOT EXISTS guardado_em TIMESTAMPTZ
      NOT NULL DEFAULT NOW()
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
        empates INTEGER NOT NULL DEFAULT 0
      )
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
  runtime.ultimaHoraDaAPI = rounds.at(-1)?.fim || null;

  if (items.length && !rounds.length) {
    log('AVISO API', {
      mensagem: 'A API respondeu, mas não foram reconhecidas rodadas válidas.',
      exemplo: items[0]
    });
  }

  return rounds;
}

async function insertRound(round) {
  const result = await pool.query(`
    INSERT INTO bacbo_resultados (
      id, resultado, estado, inicio, fim,
      pontos_jogador, pontos_banca, multiplicador
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
    ON CONFLICT (id) DO NOTHING
    RETURNING id
  `, [
    round.id,
    round.resultado,
    round.estado,
    round.inicio,
    round.fim,
    round.pontos_jogador,
    round.pontos_banca,
    round.multiplicador
  ]);

  return result.rows.length > 0;
}

async function getRecentOutcomes(limit = 20) {
  const result = await pool.query(`
    SELECT id, resultado, fim
    FROM bacbo_resultados
    WHERE resultado IN ('Jogador', 'Banca')
    ORDER BY fim DESC NULLS LAST, seq DESC
    LIMIT $1
  `, [limit]);

  return result.rows;
}

function detectEntry(rows) {
  const min = CONFIG.streakMin;
  if (rows.length < min + 1) return null;

  const color = rows[0].resultado;
  if (!['Jogador', 'Banca'].includes(color)) return null;

  if (!rows.slice(0, min).every(row => row.resultado === color)) {
    return null;
  }

  if (rows[min]?.resultado === color) return null;

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

  const age = Date.now() - new Date(signal.criado_em).getTime();

  if (age < CONFIG.sinalExpiraMin * 60000) return false;

  await pool.query(`
    UPDATE sinais SET estado = 'CANCELADO'
    WHERE id = $1 AND estado = 'PENDENTE'
  `, [signal.id]);

  await telegramSend(
    `⌛ SINAL EXPIRADO\nSinal #${signal.id}\nHora Angola: ${horaAngola()}`
  );

  return true;
}

async function maybeCreateSignal(round) {
  if (!CONFIG.sinaisAtivos) return;
  if (!['Jogador', 'Banca'].includes(round.resultado)) return;
  if (await getPendingSignal()) return;
  if ((await redsToday()) >= CONFIG.maxRedsDia) return;

  const lastResult = await pool.query(`
    SELECT criado_em FROM sinais
    ORDER BY criado_em DESC, id DESC LIMIT 1
  `);

  const last = lastResult.rows[0];

  if (
    last?.criado_em &&
    Date.now() - new Date(last.criado_em).getTime()
      < CONFIG.intervaloMinimoMs
  ) {
    return;
  }

  const rows = await getRecentOutcomes(CONFIG.streakMin + 1);
  const color = detectEntry(rows);
  if (!color) return;

  const result = await pool.query(`
    INSERT INTO sinais (cor, estado, gale, resultado_id, empates)
    VALUES ($1, 'PENDENTE', 0, $2, 0)
    RETURNING *
  `, [color, round.id]);

  const signal = result.rows[0];

  runtime.ultimoSinal = {
    id: signal.id,
    cor: signal.cor,
    estado: signal.estado,
    criado_em: signal.criado_em
  };

  await telegramSend([
    '🎲 CAPUNGA DADOS',
    '📣 NOVO SINAL BAC BO',
    `🎯 Entrada: ${color.toUpperCase()}`,
    `🛡️ Gale máximo: ${CONFIG.maxGale}`,
    CONFIG.empateProtegido
      ? '🤝 Empate protegido'
      : '🤝 Empate não protegido',
    `🕒 Hora Angola: ${horaAngola()}`,
    '⚠️ Não existe garantia de acerto.'
  ].join('\n'));

  log('SINAL CRIADO', { id: signal.id, cor: color });
}

async function resolvePending(round) {
  const signal = await getPendingSignal();
  if (!signal) return;

  if (String(signal.resultado_id) === String(round.id)) return;
  if (await expirePendingSignal(signal)) return;

  if (round.resultado === 'Empate') {
    if (!CONFIG.empateProtegido) return;

    await pool.query(`
      UPDATE sinais
      SET empates = COALESCE(empates, 0) + 1
      WHERE id = $1 AND estado = 'PENDENTE'
    `, [signal.id]);

    await telegramSend(
      `🤝 EMPATE PROTEGIDO\nSinal #${signal.id}\nContinua pendente.\nHora Angola: ${horaAngola()}`
    );

    return;
  }

  if (round.resultado === signal.cor) {
    const state = Number(signal.gale || 0) === 0 ? 'GREEN' : 'G1';

    await pool.query(`
      UPDATE sinais
      SET estado = $1, resultado_final = $2
      WHERE id = $3 AND estado = 'PENDENTE'
    `, [state, round.resultado, signal.id]);

    await telegramSend([
      '✅ RESULTADO DO SINAL',
      `Sinal: #${signal.id}`,
      `Entrada: ${signal.cor}`,
      `Resultado: ${state}`,
      `Hora Angola: ${horaAngola()}`
    ].join('\n'));

    return;
  }

  const nextGale = Number(signal.gale || 0) + 1;

  if (nextGale <= CONFIG.maxGale) {
    await pool.query(`
      UPDATE sinais SET gale = $1
      WHERE id = $2 AND estado = 'PENDENTE'
    `, [nextGale, signal.id]);

    await telegramSend(
      `🔁 GALE ${nextGale}\nSinal #${signal.id}\nEntrada original: ${signal.cor}\nHora Angola: ${horaAngola()}`
    );

    return;
  }

  await pool.query(`
    UPDATE sinais
    SET estado = 'LOSS', resultado_final = $1
    WHERE id = $2 AND estado = 'PENDENTE'
  `, [round.resultado, signal.id]);

  await telegramSend(
    `❌ LOSS FINAL\nSinal #${signal.id}\nHora Angola: ${horaAngola()}`
  );
}

async function syncOnce() {
  if (!pool) throw new Error('DATABASE_URL não configurada');

  if (runtime.sincronizacaoEmCurso) {
    return { sucesso: false, ignorada: true };
  }

  runtime.sincronizacaoEmCurso = true;

  try {
    await ensureSchema();

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

      // Na primeira importação, guardar histórico sem emitir
      // sinais retroativos.
      if (hadHistory) {
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

    // CORREÇÃO: a variável definida acima chama-se duplicates.
    const summary = {
      recebidos: rounds.length,
      novosGuardados: inserted,
      duplicados: duplicates,
      sinaisAvaliados: evaluated,
      historicoPreexistente: hadHistory,
      ultimaRodadaNova: lastNew?.id || null,
      horaAngola: horaAngola()
    };

    log('SINCRONIZACAO', summary);

    return { sucesso: true, ...summary };
  } catch (error) {
    runtime.erroSincronizacao = error.message || String(error);
    console.error('[ERRO SINCRONIZACAO]', error.message);

    return {
      sucesso: false,
      erro: runtime.erroSincronizacao
    };
  } finally {
    runtime.sincronizacaoEmCurso = false;
  }
}

function requireAdmin(req, res, next) {
  if (!ADMIN_TOKEN) {
    return res.status(503).json({
      sucesso: false,
      erro: 'ADMIN_TOKEN não configurado no Render'
    });
  }

  const token = req.get('x-admin-token');

  if (token !== ADMIN_TOKEN) {
    return res.status(401).json({
      sucesso: false,
      erro: 'Token administrativo inválido'
    });
  }

  next();
}

app.get('/', (req, res) => {
  res.json({
    nome: 'Capunga Dados',
    estado: 'online',
    horaAngola: horaAngola(),
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
    baseConfigurada: Boolean(pool),
    horaAngola: horaAngola(),
    ultimaSincronizacao: runtime.ultimaSincronizacao,
    erroSincronizacao: runtime.erroSincronizacao
  });
});

app.get('/diagnostico', (req, res) => {
  res.json({
    sucesso: true,
    nome: 'Capunga Dados',
    iniciadoEm: runtime.iniciadoEm,
    horaAngola: horaAngola(),
    baseConfigurada: Boolean(pool),
    telegramConfigurado: Boolean(BOT_TOKEN && CHAT_ID),
    api: API_BACBO,
    httpAPI: runtime.ultimoHTTPAPI,
    itensRecebidosAPI: runtime.itensRecebidosAPI,
    rodadasValidasAPI: runtime.quantidadeAPI,
    ultimaHoraDaAPI: runtime.ultimaHoraDaAPI,
    ultimaSincronizacao: runtime.ultimaSincronizacao,
    ultimaRodadaNova: runtime.ultimaRodadaNova,
    ultimoIdNovo: runtime.ultimoIdNovo,
    duplicadosUltimoCiclo: runtime.duplicadosUltimoCiclo,
    sincronizacaoEmCurso: runtime.sincronizacaoEmCurso,
    erroSincronizacao: runtime.erroSincronizacao,
    ultimoErroTelegram: runtime.ultimoErroTelegram,
    configuracao: {
      sinaisAtivos: CONFIG.sinaisAtivos,
      streakMin: CONFIG.streakMin,
      maxGale: CONFIG.maxGale,
      empateProtegido: CONFIG.empateProtegido,
      maxRedsDia: CONFIG.maxRedsDia,
      intervaloSyncMs: CONFIG.intervaloSyncMs
    }
  });
});

app.get('/resultados', async (req, res) => {
  try {
    await ensureSchema();

    const limit = Math.min(
      Math.max(Number.parseInt(req.query.limit, 10) || 50, 1),
      500
    );

    const result = await pool.query(`
      SELECT id, resultado, estado, inicio, fim,
             pontos_jogador, pontos_banca, multiplicador
      FROM bacbo_resultados
      ORDER BY fim DESC NULLS LAST, seq DESC
      LIMIT $1
    `, [limit]);

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

    const result = await pool.query(`
      SELECT * FROM sinais
      ORDER BY criado_em DESC, id DESC
      LIMIT $1
    `, [limit]);

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
        COUNT(*) FILTER (WHERE estado = 'LOSS')::int AS loss,
        COUNT(*) FILTER (WHERE estado = 'PENDENTE')::int AS pendentes,
        COUNT(*) FILTER (WHERE estado = 'CANCELADO')::int AS cancelados
      FROM sinais
    `);

    res.json({
      sucesso: true,
      estatisticas: result.rows[0],
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
  try {
    const result = await syncOnce();
    res.status(result.sucesso ? 200 : 500).json(result);
  } catch (error) {
    res.status(500).json({
      sucesso: false,
      erro: error.message || String(error)
    });
  }
});

app.post('/telegram/teste', requireAdmin, async (req, res) => {
  const enviado = await telegramSend(
    `✅ Teste do Capunga Dados concluído.\nHora Angola: ${horaAngola()}`
  );

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

async function start() {
  if (!DATABASE_URL) {
    throw new Error('DATABASE_URL não configurada no Render');
  }

  await ensureSchema();

  server = app.listen(PORT, '0.0.0.0', () => {
    console.log('======================================');
    console.log('CAPUNGA DADOS — SERVIDOR INICIADO');
    console.log(`Porta: ${PORT}`);
    console.log(`Hora Angola: ${horaAngola()}`);
    console.log(`Sinais automáticos: ${CONFIG.sinaisAtivos}`);
    console.log('======================================');
  });

  const initial = await syncOnce();

  if (!initial.sucesso) {
    console.error('[PRIMEIRA SINCRONIZAÇÃO]', initial.erro);
  }

  syncTimer = setInterval(async () => {
    if (runtime.sincronizacaoEmCurso) return;
    await syncOnce();
  }, CONFIG.intervaloSyncMs);

  if (typeof syncTimer.unref === 'function') syncTimer.unref();
}

process.on('unhandledRejection', reason => {
  console.error('[UNHANDLED REJECTION]', reason);
});

process.on('uncaughtException', error => {
  console.error('[UNCAUGHT EXCEPTION]', error);
});

async function shutdown() {
  if (syncTimer) clearInterval(syncTimer);

  if (server) {
    await new Promise(resolve => server.close(resolve));
  }

  if (pool) await pool.end();

  process.exit(0);
}

process.on('SIGTERM', () => {
  shutdown().catch(error => {
    console.error('[SHUTDOWN]', error);
    process.exit(1);
  });
});

process.on('SIGINT', () => {
  shutdown().catch(error => {
    console.error('[SHUTDOWN]', error);
    process.exit(1);
  });
});

start().catch(error => {
  console.error('[ERRO FATAL]', error);
  process.exit(1);
});
