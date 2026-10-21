
'use strict';

/**
 * CAPUNGA BAC BO — RENDER
 * Node.js + Express + PostgreSQL + Telegram
 *
 * Gale máximo: 2
 * Empate: devolução; mantém o sinal aberto
 * Resultados: RED / BLUE / TIE
 *
 * Variáveis:
 * DATABASE_URL
 * BOT_TOKEN
 * CHAT_ID
 * INGEST_TOKEN
 * API_BACBO
 *
 * Opcionais:
 * PORT=10000
 * POLL_INTERVAL_MS=60000
 * MIN_AMOSTRA=40
 * MIN_PROPORCAO=0.58
 * MAX_RESULTADOS=300
 */

const express = require('express');
const { Pool } = require('pg');
const crypto = require('crypto');

const app = express();
app.disable('x-powered-by');
app.use(express.json({ limit: '32kb' }));

const PORT = Number(process.env.PORT || 10000);
const POLL_INTERVAL_MS = Math.max(
  30000,
  Number(process.env.POLL_INTERVAL_MS || 60000)
);
const MIN_AMOSTRA = Math.max(
  20,
  Number(process.env.MIN_AMOSTRA || 40)
);
const MIN_PROPORCAO = Math.min(
  0.90,
  Math.max(0.50, Number(process.env.MIN_PROPORCAO || 0.58))
);
const MAX_RESULTADOS = Math.max(
  50,
  Number(process.env.MAX_RESULTADOS || 300)
);

const DATABASE_URL = process.env.DATABASE_URL;
const BOT_TOKEN = process.env.BOT_TOKEN;
const CHAT_ID = process.env.CHAT_ID;
const INGEST_TOKEN = process.env.INGEST_TOKEN;
const API_BACBO = process.env.API_BACBO;

if (!DATABASE_URL) {
  throw new Error('Configura DATABASE_URL no Render.');
}

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: process.env.PGSSLMODE === 'disable'
    ? false
    : { rejectUnauthorized: false },
  max: 5,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000
});

let server;
let pollTimer;
let polling = false;
let shuttingDown = false;
let startedAt = Date.now();

const stats = {
  apiRequests: 0,
  apiErrors: 0,
  resultsInserted: 0,
  signalsCreated: 0,
  lastPollAt: null,
  lastResultAt: null,
  lastError: null
};

function normalizeOutcome(value) {
  const v = String(value ?? '').trim().toUpperCase();

  if (['A', 'AZUL', 'BLUE', 'PLAYER'].includes(v)) {
    return 'BLUE';
  }

  if (['V', 'VERMELHO', 'RED', 'BANKER'].includes(v)) {
    return 'RED';
  }

  if (['E', 'EMPATE', 'TIE', 'DRAW'].includes(v)) {
    return 'TIE';
  }

  return null;
}

function safeError(error) {
  return String(error?.message || error || 'Erro desconhecido')
    .replace(/https?:\/\/\S+/gi, '[URL ocultada]')
    .slice(0, 300);
}

function jsonError(res, status, message) {
  return res.status(status).json({ ok: false, error: message });
}

function isConfiguredSecret() {
  return typeof INGEST_TOKEN === 'string' &&
    INGEST_TOKEN.length >= 32;
}

function requireIngestAuth(req, res, next) {
  if (!isConfiguredSecret()) {
    return jsonError(
      res,
      503,
      'INGEST_TOKEN não configurado corretamente no Render.'
    );
  }

  const auth = req.get('authorization') || '';
  const expected = `Bearer ${INGEST_TOKEN}`;
  const actualBuffer = Buffer.from(auth);
  const expectedBuffer = Buffer.from(expected);

  if (
    actualBuffer.length !== expectedBuffer.length ||
    !crypto.timingSafeEqual(actualBuffer, expectedBuffer)
  ) {
    return jsonError(res, 401, 'Não autorizado.');
  }

  next();
}

async function telegram(message) {
  if (!BOT_TOKEN || !CHAT_ID) {
    console.log('[TELEGRAM NÃO CONFIGURADO]', message);
    return false;
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 10000);

  try {
    const response = await fetch(
      `https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          chat_id: CHAT_ID,
          text: message,
          disable_web_page_preview: true
        }),
        signal: controller.signal
      }
    );

    const data = await response.json().catch(() => ({}));

    if (!response.ok || !data.ok) {
      throw new Error('Falha no envio para o Telegram.');
    }

    return true;
  } finally {
    clearTimeout(timeout);
  }
}

async function logEvent(type, detail = {}) {
  try {
    await pool.query(
      `INSERT INTO eventos (tipo, detalhe)
       VALUES ($1, $2::jsonb)`,
      [type, JSON.stringify(detail)]
    );
  } catch (error) {
    console.error('Erro ao guardar evento:', safeError(error));
  }
}

async function initDatabase() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS resultados (
      id BIGSERIAL PRIMARY KEY,
      rodada_id TEXT UNIQUE NOT NULL,
      resultado TEXT NOT NULL
        CHECK (resultado IN ('RED', 'BLUE', 'TIE')),
      hora_resultado TIMESTAMPTZ,
      criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS sinais (
      id BIGSERIAL PRIMARY KEY,
      sinal_id TEXT UNIQUE NOT NULL,
      previsao TEXT NOT NULL
        CHECK (previsao IN ('RED', 'BLUE')),
      estado TEXT NOT NULL DEFAULT 'PENDENTE',
      gale_level INTEGER NOT NULL DEFAULT 0,
      confianca NUMERIC(6,5),
      amostra INTEGER NOT NULL DEFAULT 0,
      resultado_final TEXT,
      criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      atualizado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      resolvido_em TIMESTAMPTZ
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS eventos (
      id BIGSERIAL PRIMARY KEY,
      tipo TEXT NOT NULL,
      detalhe JSONB NOT NULL DEFAULT '{}'::jsonb,
      criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  // Adiciona colunas em instalações compatíveis mais antigas.
  await pool.query(`
    ALTER TABLE sinais
    ADD COLUMN IF NOT EXISTS gale_level INTEGER NOT NULL DEFAULT 0
  `);

  await pool.query(`
    ALTER TABLE sinais
    ADD COLUMN IF NOT EXISTS resultado_final TEXT
  `);

  await pool.query(`
    ALTER TABLE sinais
    ADD COLUMN IF NOT EXISTS atualizado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
  `);

  await pool.query(`
    ALTER TABLE sinais
    ADD COLUMN IF NOT EXISTS resolvido_em TIMESTAMPTZ
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_resultados_criado
    ON resultados (criado_em DESC)
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_sinais_estado
    ON sinais (estado, criado_em DESC)
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_eventos_criado
    ON eventos (criado_em DESC)
  `);

  console.log('Estrutura PostgreSQL verificada.');
}

async function getRecentResults(limit = MAX_RESULTADOS) {
  const result = await pool.query(
    `SELECT rodada_id, resultado, hora_resultado, criado_em
     FROM resultados
     ORDER BY criado_em DESC, id DESC
     LIMIT $1`,
    [limit]
  );

  return result.rows;
}

async function hasPendingSignal(client = pool) {
  const result = await client.query(
    `SELECT id
     FROM sinais
     WHERE estado = 'PENDENTE'
     ORDER BY criado_em ASC, id ASC
     LIMIT 1`
  );

  return result.rows[0] || null;
}

/**
 * Motor estatístico básico.
 *
 * A frequência histórica não é uma probabilidade garantida
 * do próximo resultado. Esta função não garante vitórias.
 */
function analyzeHistory(rows) {
  const usable = [...rows]
    .reverse()
    .filter(row =>
      row.resultado === 'RED' || row.resultado === 'BLUE'
    );

  if (usable.length < MIN_AMOSTRA) {
    return {
      eligible: false,
      reason: 'AMOSTRA_INSUFICIENTE',
      sample: usable.length
    };
  }

  const window = usable.slice(-Math.min(100, usable.length));

  let red = 0;
  let blue = 0;

  for (const row of window) {
    if (row.resultado === 'RED') red++;
    if (row.resultado === 'BLUE') blue++;
  }

  const total = window.length;
  const redRate = red / total;
  const blueRate = blue / total;
  const prediction = redRate >= blueRate ? 'RED' : 'BLUE';
  const proportion = Math.max(redRate, blueRate);

  if (proportion < MIN_PROPORCAO) {
    return {
      eligible: false,
      reason: 'FILTRO_HISTORICO',
      sample: total,
      redRate,
      blueRate
    };
  }

  return {
    eligible: true,
    prediction,
    proportion,
    sample: total,
    redRate,
    blueRate
  };
}

async function createSignal(analysis) {
  if (!analysis.eligible) return null;

  const client = await pool.connect();
  let created = null;

  try {
    await client.query('BEGIN');

    // Serializa a criação para impedir dois sinais simultâneos
    // quando chegam resultados ao mesmo tempo.
    await client.query(
      'SELECT pg_advisory_xact_lock($1)',
      [748291]
    );

    const pending = await hasPendingSignal(client);

    if (pending) {
      await client.query('COMMIT');
      return null;
    }

    const signalId = crypto.randomUUID();

    const inserted = await client.query(
      `INSERT INTO sinais
        (sinal_id, previsao, estado, gale_level, confianca, amostra)
       VALUES ($1, $2, 'PENDENTE', 0, $3, $4)
       RETURNING *`,
      [
        signalId,
        analysis.prediction,
        analysis.proportion,
        analysis.sample
      ]
    );

    created = inserted.rows[0];

    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }

  if (!created) return null;

  stats.signalsCreated++;

  await logEvent('SINAL_CRIADO', {
    sinalId: created.sinal_id,
    prediction: analysis.prediction,
    sample: analysis.sample,
    historicalRate: analysis.proportion
  });

  const message = [
    '🎲 CAPUNGA BAC BO',
    '',
    `Sinal: ${analysis.prediction}`,
    'Entrada: inicial',
    'Proteção: Gale máximo 2',
    `Amostra: ${analysis.sample}`,
    `Frequência histórica: ${(analysis.proportion * 100).toFixed(1)}%`,
    '',
    'A frequência histórica não garante o próximo resultado.',
    `ID: ${created.sinal_id}`
  ].join('\n');

  try {
    await telegram(message);
  } catch (error) {
    console.error('Falha ao enviar sinal:', safeError(error));
  }

  return created;
}

async function settleSignal(resultRow) {
  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    const pending = await client.query(
      `SELECT *
       FROM sinais
       WHERE estado = 'PENDENTE'
       ORDER BY criado_em ASC, id ASC
       LIMIT 1
       FOR UPDATE`
    );

    if (!pending.rowCount) {
      await client.query('COMMIT');
      return { action: 'NO_ACTIVE_SIGNAL' };
    }

    const signal = pending.rows[0];
    const outcome = resultRow.resultado;

    // Empate: devolução, sem avançar o Gale.
    if (outcome === 'TIE') {
      await client.query(
        `UPDATE sinais
         SET atualizado_em = NOW()
         WHERE id = $1`,
        [signal.id]
      );

      await client.query('COMMIT');

      return {
        action: 'TIE_REFUND',
        signalId: signal.sinal_id,
        gale: signal.gale_level
      };
    }

    if (outcome === signal.previsao) {
      const label = signal.gale_level === 0
        ? 'GREEN'
        : `G${signal.gale_level}`;

      await client.query(
        `UPDATE sinais
         SET estado = $2,
             resultado_final = $3,
             atualizado_em = NOW(),
             resolvido_em = NOW()
         WHERE id = $1`,
        [signal.id, label, outcome]
      );

      await client.query('COMMIT');

      return {
        action: 'WIN',
        signalId: signal.sinal_id,
        label,
        prediction: signal.previsao,
        outcome
      };
    }

    if (signal.gale_level < 2) {
      const nextLevel = signal.gale_level + 1;

      await client.query(
        `UPDATE sinais
         SET gale_level = $2,
             atualizado_em = NOW()
         WHERE id = $1`,
        [signal.id, nextLevel]
      );

      await client.query('COMMIT');

      return {
        action: 'GALE',
        signalId: signal.sinal_id,
        level: nextLevel,
        prediction: signal.previsao,
        outcome
      };
    }

    await client.query(
      `UPDATE sinais
       SET estado = 'LOSS',
           resultado_final = $2,
           atualizado_em = NOW(),
           resolvido_em = NOW()
       WHERE id = $1`,
      [signal.id, outcome]
    );

    await client.query('COMMIT');

    return {
      action: 'LOSS',
      signalId: signal.sinal_id,
      prediction: signal.previsao,
      outcome
    };
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}

async function announceSettlement(result) {
  if (!result || result.action === 'NO_ACTIVE_SIGNAL') return;

  let message;

  switch (result.action) {
    case 'TIE_REFUND':
      message =
        `🤝 EMPATE — DEVOLUÇÃO\n` +
        `Sinal: ${result.signalId}\n` +
        `Gale mantido: ${result.gale}`;
      break;

    case 'WIN':
      message =
        `✅ ${result.label}\n` +
        `Sinal: ${result.signalId}\n` +
        `Previsão: ${result.prediction}\n` +
        `Resultado: ${result.outcome}`;
      break;

    case 'GALE':
      message =
        `⚠️ GALE ${result.level}\n` +
        `Sinal: ${result.signalId}\n` +
        `Resultado: ${result.outcome}\n` +
        `Previsão mantida: ${result.prediction}`;
      break;

    case 'LOSS':
      message =
        `❌ LOSS — GALE 2 ESGOTADO\n` +
        `Sinal: ${result.signalId}\n` +
        `Previsão: ${result.prediction}\n` +
        `Resultado: ${result.outcome}`;
      break;

    default:
      return;
  }

  try {
    await telegram(message);
  } catch (error) {
    console.error('Erro Telegram:', safeError(error));
  }

  await logEvent('SINAL_RESOLVIDO', result);
}

function extractApiResults(payload) {
  const list = Array.isArray(payload)
    ? payload
    : payload?.resultados ??
      payload?.results ??
      payload?.data?.resultados ??
      payload?.data?.results;

  if (!Array.isArray(list)) {
    throw new Error(
      'Formato da API desconhecido. Ajusta extractApiResults à resposta real.'
    );
  }

  return list.map(item => {
    const rodadaId = String(
      item.id ?? item.round_id ?? item.roundId ?? ''
    ).trim();

    const resultado = normalizeOutcome(
      item.resultado ?? item.result ?? item.winner
    );

    const rawDate =
      item.hora ?? item.timestamp ?? item.created_at;

    const date = rawDate ? new Date(rawDate) : null;

    const horaResultado =
      date && !Number.isNaN(date.getTime())
        ? date.toISOString()
        : null;

    if (!rodadaId || !resultado) return null;

    return {
      rodadaId,
      resultado,
      horaResultado
    };
  }).filter(Boolean);
}

async function fetchApiResults() {
  if (!API_BACBO) return [];

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 15000);

  try {
    stats.apiRequests++;

    const response = await fetch(API_BACBO, {
      headers: { Accept: 'application/json' },
      signal: controller.signal
    });

    if (!response.ok) {
      throw new Error(`API respondeu HTTP ${response.status}`);
    }

    return extractApiResults(await response.json());
  } finally {
    clearTimeout(timeout);
  }
}

async function storeResult(item) {
  const inserted = await pool.query(
    `INSERT INTO resultados
       (rodada_id, resultado, hora_resultado)
     VALUES ($1, $2, $3)
     ON CONFLICT (rodada_id) DO NOTHING
     RETURNING id, rodada_id, resultado, hora_resultado, criado_em`,
    [item.rodadaId, item.resultado, item.horaResultado]
  );

  if (!inserted.rowCount) return false;

  const resultRow = inserted.rows[0];

  stats.resultsInserted++;
  stats.lastResultAt = new Date().toISOString();

  await logEvent('RESULTADO_RECEBIDO', {
    rodadaId: resultRow.rodada_id,
    resultado: resultRow.resultado
  });

  const settled = await settleSignal(resultRow);
  await announceSettlement(settled);

  // Criar outro sinal somente quando o anterior foi encerrado
  // ou quando ainda não havia sinal ativo.
  if (
    settled.action === 'NO_ACTIVE_SIGNAL' ||
    settled.action === 'WIN' ||
    settled.action === 'LOSS'
  ) {
    const pending = await hasPendingSignal();

    if (!pending) {
      const history = await getRecentResults(MAX_RESULTADOS);
      const analysis = analyzeHistory(history);
      await createSignal(analysis);
    }
  }

  return true;
}

async function pollOnce() {
  if (polling || shuttingDown || !API_BACBO) return;

  polling = true;
  stats.lastPollAt = new Date().toISOString();

  try {
    const results = await fetchApiResults();

    // Só funciona corretamente se a API fornecer IDs únicos
    // e os resultados vierem numa lista ordenável por antiguidade.
    for (const item of results.reverse()) {
      await storeResult(item);
    }

    stats.lastError = null;
  } catch (error) {
    stats.apiErrors++;
    stats.lastError = safeError(error);

    console.error('Erro na sincronização:', stats.lastError);

    await logEvent('ERRO_API', { message: stats.lastError });
  } finally {
    polling = false;
  }
}

app.get('/health', async (req, res) => {
  try {
    await pool.query('SELECT 1');

    res.json({
      ok: true,
      service: 'Capunga Bac Bo',
      uptimeSeconds: Math.floor((Date.now() - startedAt) / 1000),
      database: 'connected',
      apiConfigured: Boolean(API_BACBO),
      ingestConfigured: isConfiguredSecret(),
      telegramConfigured: Boolean(BOT_TOKEN && CHAT_ID),
      lastPollAt: stats.lastPollAt,
      lastResultAt: stats.lastResultAt,
      lastError: stats.lastError
    });
  } catch {
    res.status(503).json({
      ok: false,
      database: 'disconnected'
    });
  }
});

app.get('/api/stats', requireIngestAuth, async (req, res) => {
  try {
    const results = await pool.query(`
      SELECT resultado, COUNT(*)::int AS total
      FROM resultados
      GROUP BY resultado
    `);

    const signals = await pool.query(`
      SELECT estado, COUNT(*)::int AS total
      FROM sinais
      GROUP BY estado
    `);

    res.json({
      resultados: results.rows,
      sinais: signals.rows,
      runtime: stats
    });
  } catch (error) {
    console.error('Erro nas estatísticas:', safeError(error));
    jsonError(res, 500, 'Falha nas estatísticas.');
  }
});

app.post('/api/result', requireIngestAuth, async (req, res) => {
  try {
    const rodadaId = String(req.body?.rodada_id ?? '').trim();
    const resultado = normalizeOutcome(req.body?.resultado);

    if (!rodadaId || rodadaId.length > 150 || !resultado) {
      return jsonError(
        res,
        400,
        'Fornece rodada_id e resultado RED, BLUE ou TIE.'
      );
    }

    const rawDate = req.body?.hora;
    const date = rawDate ? new Date(rawDate) : null;

    const horaResultado =
      date && !Number.isNaN(date.getTime())
        ? date.toISOString()
        : null;

    const inserted = await storeResult({
      rodadaId,
      resultado,
      horaResultado
    });

    res.status(inserted ? 201 : 200).json({
      ok: true,
      duplicate: !inserted,
      rodada_id: rodadaId,
      resultado
    });
  } catch (error) {
    console.error('Erro ao registar resultado:', safeError(error));
    jsonError(res, 500, 'Não foi possível guardar o resultado.');
  }
});

app.get('/api/history', requireIngestAuth, async (req, res) => {
  try {
    const requested = Number(req.query.limit || 30);
    const limit = Math.min(100, Math.max(
      1,
      Number.isFinite(requested) ? requested : 30
    ));

    const result = await pool.query(
      `SELECT rodada_id, resultado, hora_resultado, criado_em
       FROM resultados
       ORDER BY criado_em DESC, id DESC
       LIMIT $1`,
      [limit]
    );

    res.json({ results: result.rows });
  } catch (error) {
    console.error('Erro no histórico:', safeError(error));
    jsonError(res, 500, 'Erro ao consultar histórico.');
  }
});

async function start() {
  await initDatabase();

  server = app.listen(PORT, '0.0.0.0', () => {
    console.log(`Capunga Bac Bo ativo na porta ${PORT}`);
    console.log(`API configurada: ${Boolean(API_BACBO)}`);
    console.log(`Token de ingestão configurado: ${isConfiguredSecret()}`);
  });

  if (API_BACBO) {
    await pollOnce();

    pollTimer = setInterval(() => {
      pollOnce().catch(error => {
        console.error('Erro no ciclo:', safeError(error));
      });
    }, POLL_INTERVAL_MS);
  } else {
    console.warn(
      'API_BACBO ausente. Nenhum resultado será obtido automaticamente.'
    );
  }
}

async function shutdown() {
  if (shuttingDown) return;
  shuttingDown = true;

  console.log('A encerrar servidor...');

  if (pollTimer) clearInterval(pollTimer);

  if (server) {
    await new Promise(resolve => server.close(resolve));
  }

  try {
    await pool.end();
  } catch (error) {
    console.error('Erro ao fechar PostgreSQL:', safeError(error));
  }

  process.exit(0);
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

process.on('unhandledRejection', error => {
  console.error('Promise rejeitada:', safeError(error));
});

start().catch(error => {
  console.error('Falha ao iniciar:', safeError(error));
  process.exit(1);
});
