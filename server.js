
'use strict';

/**
 * CAPUNGA BAC BO — RENDER / NODE.JS
 * PostgreSQL + Telegram + API de resultados configurável
 *
 * Gale máximo: 2
 * Empate: devolução; o sinal continua aberto
 * Sem garantia de acerto.
 *
 * Variáveis obrigatórias:
 * DATABASE_URL
 * BOT_TOKEN
 * CHAT_ID
 * INGEST_TOKEN
 * API_BACBO
 *
 * Opcional:
 * POLL_INTERVAL_MS (padrão 60000)
 * MIN_AMOSTRA (padrão 40)
 * MIN_PROPORCAO (padrão 0.58)
 * MAX_RESULTADOS (padrão 300)
 * PORT
 */

const express = require('express');
const { Pool } = require('pg');
const crypto = require('crypto');

const app = express();
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
  throw new Error('Falta configurar DATABASE_URL.');
}

if (!INGEST_TOKEN || INGEST_TOKEN.length < 24) {
  throw new Error('INGEST_TOKEN deve ter pelo menos 24 caracteres.');
}

if (!BOT_TOKEN || !CHAT_ID) {
  console.warn('Telegram não configurado completamente.');
}

if (!API_BACBO) {
  console.warn(
    'API_BACBO não configurada. A ingestão automática ficará inativa.'
  );
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

let startedAt = Date.now();
let polling = false;
let pollTimer = null;
let shuttingDown = false;

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
  const v = String(value ?? '')
    .trim()
    .toUpperCase();

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
  // Não expor URLs que possam conter tokens.
  return String(error?.message || error || 'Erro desconhecido')
    .replace(/https?:\/\/\S+/gi, '[URL ocultada]')
    .slice(0, 300);
}

async function telegram(message) {
  if (!BOT_TOKEN || !CHAT_ID) {
    console.log('[TELEGRAM DESATIVADO]', message);
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
      throw new Error('Telegram recusou o envio.');
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
       VALUES ($1, $2)`,
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
      gale_level INTEGER NOT NULL DEFAULT 0
        CHECK (gale_level BETWEEN 0 AND 2),
      confianca NUMERIC(6, 5),
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

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_resultados_data
    ON resultados (criado_em DESC)
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_sinais_estado
    ON sinais (estado, criado_em DESC)
  `);

  // Uma só sequência ativa de sinais por servidor.
  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_um_sinal_pendente
    ON sinais ((estado))
    WHERE estado = 'PENDENTE'
  `);

  // Compatibilidade com tabelas já existentes.
  await pool.query(`
    ALTER TABLE sinais
    ADD COLUMN IF NOT EXISTS gale_level INTEGER NOT NULL DEFAULT 0
  `);

  await pool.query(`
    ALTER TABLE sinais
    ADD COLUMN IF NOT EXISTS resultado_final TEXT
  `);

  console.log('PostgreSQL inicializado.');
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

function analyzeHistory(rows) {
  const ordered = [...rows].reverse();
  const usable = ordered.filter(
    row => row.resultado === 'RED' || row.resultado === 'BLUE'
  );

  if (usable.length < MIN_AMOSTRA) {
    return {
      eligible: false,
      reason: 'AMOSTRA_INSUFICIENTE',
      sample: usable.length
    };
  }

  const window = usable.slice(-Math.min(100, usable.length));
  const counts = {
    RED: 0,
    BLUE: 0
  };

  for (const row of window) {
    counts[row.resultado]++;
  }

  const total = window.length;
  const redRate = counts.RED / total;
  const blueRate = counts.BLUE / total;

  const prediction = redRate >= blueRate ? 'RED' : 'BLUE';
  const proportion = Math.max(redRate, blueRate);

  // Frequência histórica não equivale à probabilidade futura.
  // Este filtro é apenas exploratório.
  if (proportion < MIN_PROPORCAO) {
    return {
      eligible: false,
      reason: 'SEM_DIFERENCA_HISTORICA_SUFICIENTE',
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

  const signalId = crypto.randomUUID();

  try {
    const result = await pool.query(
      `INSERT INTO sinais
         (sinal_id, previsao, estado, gale_level, confianca, amostra)
       VALUES ($1, $2, 'PENDENTE', 0, $3, $4)
       ON CONFLICT DO NOTHING
       RETURNING *`,
      [
        signalId,
        analysis.prediction,
        analysis.proportion,
        analysis.sample
      ]
    );

    if (!result.rowCount) return null;

    stats.signalsCreated++;

    const message = [
      '🎲 CAPUNGA BAC BO',
      '',
      `Sinal: ${analysis.prediction}`,
      'Entrada: inicial',
      'Proteção: Gale máximo 2',
      `Amostra histórica: ${analysis.sample}`,
      `Frequência histórica observada: ${(analysis.proportion * 100).toFixed(1)}%`,
      '',
      'AVISO: estatística histórica, não garantia de acerto.',
      `ID: ${signalId}`
    ].join('\n');

    try {
      await telegram(message);
    } catch (error) {
      console.error('Erro Telegram:', safeError(error));
    }

    await logEvent('SINAL_CRIADO', {
      sinalId,
      prediction: analysis.prediction,
      sample: analysis.sample,
      historicalRate: analysis.proportion
    });

    return result.rows[0];
  } catch (error) {
    if (error.code === '23505') return null;
    throw error;
  }
}

async function settleSignal(resultRow) {
  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    const active = await client.query(
      `SELECT *
       FROM sinais
       WHERE estado = 'PENDENTE'
       ORDER BY criado_em ASC
       LIMIT 1
       FOR UPDATE`
    );

    if (!active.rowCount) {
      await client.query('COMMIT');
      return { action: 'NO_ACTIVE_SIGNAL' };
    }

    const signal = active.rows[0];
    const outcome = resultRow.resultado;

    if (outcome === 'TIE') {
      // Empate devolve: não avançar Gale.
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
        `Resultado contrário: ${result.outcome}\n` +
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
    console.error('Falha de notificação:', safeError(error));
  }

  await logEvent('SINAL_RESOLVIDO', result);
}

function extractApiResults(payload) {
  // Adaptador para respostas com resultados em array.
  const list = Array.isArray(payload)
    ? payload
    : payload?.resultados ??
      payload?.results ??
      payload?.data?.resultados ??
      payload?.data?.results;

  if (!Array.isArray(list)) {
    throw new Error(
      'Formato de API desconhecido: falta um array de resultados.'
    );
  }

  return list.map(item => {
    const rodadaId = String(
      item.id ?? item.round_id ?? item.roundId ?? ''
    ).trim();

    const resultado = normalizeOutcome(
      item.resultado ?? item.result ?? item.winner
    );

    const rawDate = item.hora ?? item.timestamp ?? item.created_at;
    const date = rawDate ? new Date(rawDate) : null;
    const validDate = date && !Number.isNaN(date.getTime())
      ? date.toISOString()
      : null;

    if (!rodadaId || !resultado) return null;

    return {
      rodadaId,
      resultado,
      horaResultado: validDate
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
      method: 'GET',
      headers: { Accept: 'application/json' },
      signal: controller.signal
    });

    if (!response.ok) {
      throw new Error(`API de resultados respondeu HTTP ${response.status}`);
    }

    const payload = await response.json();
    return extractApiResults(payload);
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

  // Gerar um novo sinal apenas depois de um resultado novo
  // e quando não existir um sinal ativo.
  if (
    settled.action === 'NO_ACTIVE_SIGNAL' ||
    settled.action === 'WIN' ||
    settled.action === 'LOSS'
  ) {
    const active = await pool.query(
      `SELECT 1 FROM sinais
       WHERE estado = 'PENDENTE'
       LIMIT 1`
    );

    if (!active.rowCount) {
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

    // A maioria das APIs devolve o mais recente primeiro.
    // Processar do mais antigo para o mais recente.
    for (const item of results.reverse()) {
      await storeResult(item);
    }

    stats.lastError = null;
  } catch (error) {
    stats.apiErrors++;
    stats.lastError = safeError(error);

    console.error('Erro na sincronização:', stats.lastError);

    await logEvent('ERRO_API', {
      message: stats.lastError
    });
  } finally {
    polling = false;
  }
}

function requireIngestAuth(req, res, next) {
  const auth = req.get('authorization') || '';
  const expected = `Bearer ${INGEST_TOKEN}`;

  const actualBuffer = Buffer.from(auth);
  const expectedBuffer = Buffer.from(expected);

  if (
    actualBuffer.length !== expectedBuffer.length ||
    !crypto.timingSafeEqual(actualBuffer, expectedBuffer)
  ) {
    return res.status(401).json({ error: 'Não autorizado' });
  }

  next();
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
      runtime: {
        apiRequests: stats.apiRequests,
        apiErrors: stats.apiErrors,
        resultsInserted: stats.resultsInserted,
        signalsCreated: stats.signalsCreated,
        lastPollAt: stats.lastPollAt,
        lastResultAt: stats.lastResultAt,
        lastError: stats.lastError
      }
    });
  } catch (error) {
    console.error('Erro nas estatísticas:', safeError(error));
    res.status(500).json({ error: 'Falha nas estatísticas' });
  }
});

// Entrada manual/externa para uma fonte de resultados autorizada.
app.post('/api/result', requireIngestAuth, async (req, res) => {
  try {
    const rodadaId = String(req.body?.rodada_id ?? '').trim();
    const resultado = normalizeOutcome(req.body?.resultado);

    if (!rodadaId || rodadaId.length > 150 || !resultado) {
      return res.status(400).json({
        error: 'É necessário fornecer rodada_id e resultado válido.'
      });
    }

    const rawDate = req.body?.hora;
    const date = rawDate ? new Date(rawDate) : null;
    const horaResultado = date && !Number.isNaN(date.getTime())
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
    res.status(500).json({ error: 'Não foi possível guardar o resultado.' });
  }
});

app.get('/api/history', requireIngestAuth, async (req, res) => {
  try {
    const limit = Math.min(
      100,
      Math.max(1, Number(req.query.limit || 30))
    );

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
    res.status(500).json({ error: 'Erro ao consultar histórico.' });
  }
});

async function start() {
  await initDatabase();

  app.listen(PORT, '0.0.0.0', () => {
    console.log(`Servidor ativo na porta ${PORT}`);
  });

  if (API_BACBO) {
    await pollOnce();

    pollTimer = setInterval(() => {
      pollOnce().catch(error => {
        console.error('Erro no ciclo:', safeError(error));
      });
    }, POLL_INTERVAL_MS);
  }
}

async function shutdown() {
  if (shuttingDown) return;

  shuttingDown = true;
  console.log('A encerrar servidor...');

  if (pollTimer) clearInterval(pollTimer);

  try {
    await pool.end();
  } catch (error) {
    console.error('Erro ao fechar PostgreSQL:', safeError(error));
  }

  process.exit(0);
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

start().catch(error => {
  console.error('Falha ao iniciar:', safeError(error));
  process.exit(1);
});
