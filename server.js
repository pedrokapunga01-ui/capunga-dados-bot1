'use strict';

/**
 * CAPUNGA DADOS — COLETOR DE MESA BAC BO
 *
 * Coleta resultados em tempo quase real, guarda no PostgreSQL
 * e detecta duplicações e lacunas no histórico.
 *
 * Fonte primária:
 * Feed JSON público do CasinoScores.
 *
 * Fonte alternativa:
 * Scraper Puppeteer da página pública CasinoScores.
 *
 * Não faz login, apostas, depósitos ou saques.
 */

try {
  require('dotenv').config();
} catch (error) {
  if (
    error.code !== 'MODULE_NOT_FOUND' ||
    !/Cannot find module ['"]dotenv['"]/.test(error.message)
  ) {
    throw error;
  }
}

const express = require('express');
const cors = require('cors');
const crypto = require('node:crypto');
const { Pool } = require('pg');

const {
  CasinoScoresSource,
  DEFAULT_URL
} = require('./browser-source');

const {
  CasinoScoresApiSource,
  DEFAULT_API_URL
} = require('./casino-api-source');

const app = express();

app.disable('x-powered-by');

app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Referrer-Policy', 'no-referrer');
  res.setHeader('X-Frame-Options', 'DENY');
  next();
});

const origins = (process.env.CORS_ORIGINS || '')
  .split(',')
  .map(value => value.trim())
  .filter(Boolean);

app.use(cors({
  origin(origin, callback) {
    if (!origin || origins.includes(origin)) {
      return callback(null, true);
    }

    return callback(null, false);
  },
  allowedHeaders: ['Content-Type', 'x-admin-token'],
  methods: ['GET', 'POST', 'OPTIONS']
}));

app.use(express.json({ limit: '1mb' }));

const PORT = Number(process.env.PORT || 10000);
const DATABASE_URL = String(process.env.DATABASE_URL || '').trim();
const ADMIN_TOKEN = String(process.env.ADMIN_TOKEN || '').trim();

const BACBO_SOURCE_MODE = String(
  process.env.BACBO_SOURCE_MODE || 'api'
).trim().toLowerCase();

const BACBO_API_URL = String(
  process.env.BACBO_API_URL || DEFAULT_API_URL
).trim();

const BACBO_SOURCE_URL = String(
  process.env.BACBO_SOURCE_URL || DEFAULT_URL
).trim();

const POLL_INTERVAL_MS = Math.max(
  2000,
  Math.min(60000, Number(process.env.POLL_INTERVAL_MS || 5000))
);

const BROWSER_NAV_TIMEOUT_MS = Math.max(
  10000,
  Math.min(120000, Number(process.env.BROWSER_NAV_TIMEOUT_MS || 30000))
);

const BROWSER_WAIT_MS = Math.max(
  500,
  Math.min(15000, Number(process.env.BROWSER_WAIT_MS || 4000))
);

const STALE_AFTER_MS = Math.max(
  30000,
  Math.min(600000, Number(process.env.STALE_AFTER_MS || 180000))
);

const RESULTS_LIMIT = Math.max(
  20,
  Math.min(500, Number(process.env.RESULTS_LIMIT || 250))
);

const BOOTSTRAP_LIMIT = Math.max(
  20,
  Math.min(500, Number(process.env.BOOTSTRAP_LIMIT || 100))
);

const pool = DATABASE_URL
  ? new Pool({
      connectionString: DATABASE_URL,
      ssl: /localhost|127\.0\.0\.1/i.test(DATABASE_URL)
        ? false
        : {
            rejectUnauthorized:
              String(
                process.env.PGSSL_REJECT_UNAUTHORIZED || 'true'
              ) !== 'false'
          },
      max: 5,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 15000
    })
  : null;

const runtime = {
  startedAt: new Date().toISOString(),

  polling: false,

  browserConnected: false,
  pageUrl: BACBO_SOURCE_URL,

  sourceMode: BACBO_SOURCE_MODE,

  endpoint:
    BACBO_SOURCE_MODE === 'browser'
      ? BACBO_SOURCE_URL
      : BACBO_API_URL,

  lastPollAt: null,
  lastNewAt: null,
  lastSettledAt: null,
  lastSourceId: null,

  lastHttpStatus: null,
  lastLatencyMs: null,
  lastError: null,
  consecutiveFailures: 0,

  dataIntegrity: 'unknown',
  gapDetected: false,
  gapReason: null,

  snapshotSize: 0,
  newResultsLastPoll: 0,
  duplicatesLastPoll: 0,

  orientation: null,
  totalStored: 0
};

let schemaPromise = null;
let pollTimer = null;
let server = null;
let previousSnapshot = [];

const source =
  BACBO_SOURCE_MODE === 'browser'
    ? new CasinoScoresSource({
        url: BACBO_SOURCE_URL,
        navigationTimeoutMs: BROWSER_NAV_TIMEOUT_MS,
        waitMs: BROWSER_WAIT_MS,
        runtime
      })
    : new CasinoScoresApiSource({
        url: BACBO_API_URL,
        runtime
      });

function nowIso() {
  return new Date().toISOString();
}

function log(tag, value) {
  console.log(
    `[${tag}]`,
    typeof value === 'string'
      ? value
      : JSON.stringify(value)
  );
}

function safeError(error) {
  return (
    error?.response?.data?.message ||
    error?.response?.data?.error ||
    error?.message ||
    String(error)
  );
}

function safeSource(url) {
  try {
    return new URL(url).host;
  } catch (_) {
    return 'fonte-configurada';
  }
}

function timingSafeEqualText(a, b) {
  const first = Buffer.from(String(a || ''));
  const second = Buffer.from(String(b || ''));

  return (
    first.length === second.length &&
    crypto.timingSafeEqual(first, second)
  );
}

function normalizeOutcome(value) {
  if (value && typeof value === 'object') {
    value =
      value.outcome ??
      value.resultado ??
      value.winner ??
      value.name;
  }

  const text = String(value ?? '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .trim()
    .toLowerCase();

  if (
    [
      'player',
      'jogador',
      'playerwin',
      'playerwins',
      'playerwon',
      'blue',
      'p'
    ].includes(text)
  ) {
    return 'Jogador';
  }

  if (
    [
      'banker',
      'banca',
      'bankerwin',
      'bankerwins',
      'bankerwon',
      'red',
      'b'
    ].includes(text)
  ) {
    return 'Banca';
  }

  if (
    [
      'tie',
      'draw',
      'empate',
      'equal',
      'push',
      'tied',
      'yellow',
      'e'
    ].includes(text)
  ) {
    return 'Empate';
  }

  return null;
}

function parseMaybeJson(value) {
  if (typeof value !== 'string') {
    return value;
  }

  try {
    return JSON.parse(value);
  } catch (_) {
    return value;
  }
}

function extractItems(body) {
  const parsed = parseMaybeJson(body);

  if (Array.isArray(parsed)) {
    return parsed;
  }

  if (!parsed || typeof parsed !== 'object') {
    return [];
  }

  const candidates = [
    parsed.results,
    parsed.resultados,
    parsed.data,
    parsed.items,
    parsed.rounds,
    parsed.events,
    parsed.history,
    parsed.records,
    parsed.content,

    parsed.data?.results,
    parsed.data?.resultados,
    parsed.data?.items,
    parsed.data?.rounds,
    parsed.data?.events,
    parsed.data?.history,
    parsed.data?.records,
    parsed.data?.content,
    parsed.data?.data
  ];

  for (const candidate of candidates) {
    const items = parseMaybeJson(candidate);

    if (Array.isArray(items)) {
      return items;
    }
  }

  return [];
}

function validDate(value) {
  if (!value) {
    return null;
  }

  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    return null;
  }

  return date.toISOString();
}

function normalizeResult(item, index) {
  const parsed = parseMaybeJson(item);

  const data =
    parsed?.data &&
    typeof parsed.data === 'object' &&
    !Array.isArray(parsed.data)
      ? parsed.data
      : parsed || {};

  const result =
    data.result && typeof data.result === 'object'
      ? data.result
      : parsed?.result &&
          typeof parsed.result === 'object'
        ? parsed.result
        : {};

  const player =
    data.player ||
    data.playerData ||
    result.player ||
    {};

  const banker =
    data.banker ||
    data.bankerData ||
    result.banker ||
    {};

  const outcome = normalizeOutcome(
    data.outcome ??
      data.resultado ??
      data.winner ??
      result.outcome ??
      result.resultado ??
      result.winner ??
      parsed?.outcome ??
      parsed?.resultado
  );

  if (!outcome) {
    return null;
  }

  const sourceId = String(
    data.id ??
      data.roundId ??
      data.round_id ??
      data.eventId ??
      data.gameId ??
      parsed?.id ??
      parsed?.roundId ??
      parsed?.round_id ??
      parsed?.eventId ??
      ''
  ).trim() || null;

  const settledAt = validDate(
    data.settledAt ??
      data.settled_at ??
      data.finishedAt ??
      data.endTime ??
      data.endedAt ??
      data.timestamp ??
      parsed?.settledAt ??
      parsed?.settled_at ??
      parsed?.finishedAt ??
      parsed?.timestamp
  );

  const playerScore = Number(
    player.score ??
      data.playerScore ??
      data.player_score ??
      data.pontos_jogador ??
      result.playerScore ??
      result.playerDice?.score
  );

  const bankerScore = Number(
    banker.score ??
      data.bankerScore ??
      data.banker_score ??
      data.pontos_banca ??
      result.bankerScore ??
      result.bankerDice?.score
  );

  const multiplier =
    data.multiplier ??
    data.multiplicador ??
    result.multiplier ??
    null;

  const clean = {
    sourceId,
    outcome,
    settledAt,

    playerScore: Number.isFinite(playerScore)
      ? playerScore
      : null,

    bankerScore: Number.isFinite(bankerScore)
      ? bankerScore
      : null,

    multiplier:
      multiplier == null
        ? null
        : String(multiplier),

    sourceIndex: index,
    raw: parsed
  };

  clean.signature = resultSignature(clean);

  clean.sourceKey = clean.sourceId
    ? `id:${clean.sourceId}`
    : `hash:${clean.signature}`;

  clean.idReliable = Boolean(clean.sourceId);

  return clean;
}

function resultSignature(result) {
  return crypto
    .createHash('sha256')
    .update(
      JSON.stringify({
        outcome: result.outcome,
        settledAt: result.settledAt,
        playerScore: result.playerScore,
        bankerScore: result.bankerScore,
        multiplier: result.multiplier,
        payout: result.payout || null,
        winners: result.winners || null
      })
    )
    .digest('hex');
}

function sameResult(first, second) {
  return (
    first?.sourceKey &&
    first.sourceKey === second?.sourceKey
  );
}

function sequenceEquals(first, second) {
  return (
    first.length === second.length &&
    first.every((item, index) =>
      sameResult(item, second[index])
    )
  );
}

/**
 * Descobre novas rodadas sem assumir a orientação da fonte.
 *
 * Se não houver sobreposição entre snapshots, marca lacuna.
 * O sistema não inventa resultados ausentes.
 */
function diffSnapshots(previous, current) {
  if (!previous.length) {
    return {
      newItems: current,
      orientation: null,
      gap: false,
      reason: 'bootstrap'
    };
  }

  if (sequenceEquals(previous, current)) {
    return {
      newItems: [],
      orientation: null,
      gap: false,
      reason: 'unchanged'
    };
  }

  const maxOverlap = Math.min(
    previous.length,
    current.length
  );

  for (let size = maxOverlap; size >= 1; size--) {
    const previousSuffix =
      previous.slice(previous.length - size);

    const currentPrefix =
      current.slice(0, size);

    if (sequenceEquals(previousSuffix, currentPrefix)) {
      return {
        newItems: current.slice(size),
        orientation: 'oldest_first',
        gap: false,
        reason: 'overlap'
      };
    }

    const previousPrefix =
      previous.slice(0, size);

    const currentSuffix =
      current.slice(current.length - size);

    if (sequenceEquals(previousPrefix, currentSuffix)) {
      return {
        newItems: current.slice(
          0,
          current.length - size
        ),
        orientation: 'newest_first',
        gap: false,
        reason: 'overlap'
      };
    }
  }

  const previousKeys = new Set(
    previous.map(item => item.sourceKey)
  );

  const currentKeys = new Set(
    current.map(item => item.sourceKey)
  );

  const contained =
    [...currentKeys].every(key =>
      previousKeys.has(key)
    ) ||
    [...previousKeys].every(key =>
      currentKeys.has(key)
    );

  if (contained) {
    return {
      newItems: [],
      orientation: null,
      gap: false,
      reason: 'window_changed_without_new_result'
    };
  }

  return {
    newItems: [],
    orientation: null,
    gap: true,
    reason: 'sem_sobreposicao'
  };
}

async function ensureSchema() {
  if (!pool) {
    throw new Error('DATABASE_URL não configurada');
  }

  if (schemaPromise) {
    return schemaPromise;
  }

  schemaPromise = (async () => {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS mesa_resultados (
        source_key TEXT PRIMARY KEY,
        source_id TEXT,
        resultado TEXT NOT NULL,
        settled_at TIMESTAMPTZ,
        player_score INTEGER,
        banker_score INTEGER,
        multiplier TEXT,
        id_reliable BOOLEAN NOT NULL DEFAULT FALSE,
        source TEXT NOT NULL,
        raw JSONB NOT NULL DEFAULT '{}'::jsonb,
        observado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);

    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_mesa_settled_at
      ON mesa_resultados (
        settled_at DESC NULLS LAST,
        observado_em DESC
      )
    `);

    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_mesa_source_id
      ON mesa_resultados (source_id)
    `);

    log('DB', 'schema mesa_resultados pronto');
  })();

  try {
    await schemaPromise;
  } catch (error) {
    schemaPromise = null;
    throw error;
  }

  return schemaPromise;
}

async function fetchSnapshot() {
  const started = Date.now();

  const items = await source.snapshot();

  runtime.lastLatencyMs = Date.now() - started;

  const normalize =
    BACBO_SOURCE_MODE === 'browser'
      ? normalizeBrowserResult
      : normalizeResult;

  const results = items
    .map(normalize)
    .filter(Boolean)
    .slice(0, RESULTS_LIMIT);

  if (!results.length) {
    throw new Error(
      'A fonte respondeu sem resultados Bac Bo reconhecíveis'
    );
  }

  return results;
}

function parseFinishedAt(value) {
  const text = String(value || '').trim();

  const match = text.match(
    /(\d{1,2})\s+([A-Za-zÀ-ÿ]+)\s+(\d{1,2}):(\d{2})/i
  );

  if (!match) {
    return null;
  }

  const months = {
    jan: 0,
    fev: 1,
    feb: 1,
    mar: 2,
    abr: 3,
    apr: 3,
    mai: 4,
    may: 4,
    jun: 5,
    jul: 6,
    ago: 7,
    aug: 7,
    set: 8,
    sep: 8,
    out: 9,
    oct: 9,
    nov: 10,
    dez: 11,
    dec: 11
  };

  const month =
    months[match[2].slice(0, 3).toLowerCase()];

  if (month == null) {
    return null;
  }

  const now = new Date();

  let date = new Date(
    Date.UTC(
      now.getUTCFullYear(),
      month,
      Number(match[1]),
      Number(match[3]),
      Number(match[4])
    )
  );

  if (date.getTime() > now.getTime() + 86400000) {
    date = new Date(
      Date.UTC(
        now.getUTCFullYear() - 1,
        month,
        Number(match[1]),
        Number(match[3]),
        Number(match[4])
      )
    );
  }

  return date.toISOString();
}

function normalizeBrowserResult(item, index) {
  const playerScore = Number.isFinite(
    Number(item.playerScore)
  )
    ? Number(item.playerScore)
    : null;

  const bankerScore = Number.isFinite(
    Number(item.bankerScore)
  )
    ? Number(item.bankerScore)
    : null;

  const outcome =
    playerScore == null || bankerScore == null
      ? null
      : playerScore === bankerScore
        ? 'Empate'
        : playerScore > bankerScore
          ? 'Jogador'
          : 'Banca';

  if (!outcome) {
    return null;
  }

  const result = {
    sourceId: item.sourceId || null,
    outcome,
    settledAt: parseFinishedAt(item.finished),
    playerScore,
    bankerScore,
    multiplier: null,
    payout: item.payout || null,
    winners: item.winners || null,
    sourceIndex: index,
    raw: item
  };

  result.signature = resultSignature(result);

  result.sourceKey = result.sourceId
    ? `id:${result.sourceId}`
    : `hash:${result.signature}`;

  result.idReliable = Boolean(result.sourceId);

  return result;
}

async function insertResult(result) {
  const query = `
    INSERT INTO mesa_resultados (
      source_key,
      source_id,
      resultado,
      settled_at,
      player_score,
      banker_score,
      multiplier,
      id_reliable,
      source,
      raw
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
    ON CONFLICT (source_key)
    DO NOTHING
    RETURNING source_key
  `;

  const values = [
    result.sourceKey,
    result.sourceId,
    result.outcome,
    result.settledAt,
    result.playerScore,
    result.bankerScore,
    result.multiplier,
    result.idReliable,
    safeSource(runtime.endpoint),
    result.raw || {}
  ];

  const saved = await pool.query(query, values);

  return saved.rows.length > 0;
}

async function getLatestResults(limit = 100) {
  const result = await pool.query(
    `
      SELECT
        source_key,
        source_id,
        resultado,
        settled_at,
        player_score,
        banker_score,
        multiplier,
        id_reliable,
        source,
        observado_em
      FROM mesa_resultados
      ORDER BY
        settled_at DESC NULLS LAST,
        observado_em DESC
      LIMIT $1
    `,
    [limit]
  );

  return result.rows;
}

async function pollOnce() {
  if (runtime.polling) {
    return {
      success: false,
      skipped: true,
      reason: 'poll_em_curso'
    };
  }

  runtime.polling = true;

  try {
    await ensureSchema();

    const snapshot = await fetchSnapshot();

    const diff = diffSnapshots(
      previousSnapshot,
      snapshot
    );

    runtime.snapshotSize = snapshot.length;
    runtime.orientation = diff.orientation;
    runtime.lastPollAt = nowIso();
    runtime.lastError = null;
    runtime.consecutiveFailures = 0;

    if (diff.gap) {
      runtime.dataIntegrity = 'gap';
      runtime.gapDetected = true;
      runtime.gapReason = diff.reason;
      runtime.newResultsLastPoll = 0;

      log('LACUNA', {
        motivo: diff.reason,
        snapshotAnterior: previousSnapshot.length,
        snapshotAtual: snapshot.length
      });

      previousSnapshot = snapshot;

      return {
        success: false,
        gap: true,
        reason: diff.reason
      };
    }

    let inserted = 0;
    let duplicates = 0;

    for (const result of diff.newItems) {
      if (await insertResult(result)) {
        inserted++;
      } else {
        duplicates++;
      }
    }

    previousSnapshot = snapshot;

    runtime.dataIntegrity = 'ok';
    runtime.gapDetected = false;
    runtime.gapReason = null;
    runtime.newResultsLastPoll = inserted;
    runtime.duplicatesLastPoll = duplicates;

    runtime.totalStored = Number(
      (
        await pool.query(
          'SELECT COUNT(*)::int AS total FROM mesa_resultados'
        )
      ).rows[0].total || 0
    );

    if (inserted > 0) {
      runtime.lastNewAt = nowIso();

      const newest =
        diff.newItems[diff.newItems.length - 1];

      runtime.lastSettledAt =
        newest?.settledAt ||
        runtime.lastSettledAt;

      runtime.lastSourceId =
        newest?.sourceId ||
        runtime.lastSourceId;

      log('MESA', {
        novos: inserted,
        duplicados: duplicates,
        total: runtime.totalStored,
        fonte: safeSource(runtime.endpoint)
      });
    }

    return {
      success: true,
      inserted,
      duplicates,
      integrity: runtime.dataIntegrity
    };
  } catch (error) {
    runtime.lastError = safeError(error);
    runtime.consecutiveFailures++;
    runtime.dataIntegrity = 'error';

    log('ERRO_COLETA', runtime.lastError);

    return {
      success: false,
      error: runtime.lastError
    };
  } finally {
    runtime.polling = false;
  }
}

function healthPayload(dbOk, dbError = null) {
  const lastPollAge = runtime.lastPollAt
    ? Date.now() -
      new Date(runtime.lastPollAt).getTime()
    : null;

  const lastDataAt =
    runtime.lastNewAt ||
    runtime.lastPollAt;

  const lastDataAge = lastDataAt
    ? Date.now() -
      new Date(lastDataAt).getTime()
    : null;

  const stale =
    lastDataAge == null ||
    lastDataAge > STALE_AFTER_MS;

  const healthy =
    dbOk &&
    runtime.dataIntegrity === 'ok' &&
    !stale &&
    runtime.consecutiveFailures === 0;

  return {
    success: healthy,

    state: !dbOk
      ? 'db_unavailable'
      : runtime.gapDetected
        ? 'history_gap'
        : stale
          ? 'stale'
          : runtime.dataIntegrity,

    dbOk,
    dbError,

    source: safeSource(runtime.endpoint),
    pollIntervalMs: POLL_INTERVAL_MS,

    lastPollAt: runtime.lastPollAt,
    lastPollAgeMs: lastPollAge,
    lastDataAgeMs: lastDataAge,

    stale,

    ...runtime
  };
}

function requireAdmin(req, res, next) {
  if (!ADMIN_TOKEN) {
    return res.status(503).json({
      success: false,
      error: 'ADMIN_TOKEN não configurado'
    });
  }

  if (
    !timingSafeEqualText(
      req.get('x-admin-token'),
      ADMIN_TOKEN
    )
  ) {
    return res.status(401).json({
      success: false,
      error: 'Token inválido'
    });
  }

  next();
}

app.get('/', (_req, res) => {
  res.json({
    name: 'Capunga Dados — coletor de mesa Bac Bo',
    mode: 'table-monitor',
    source: safeSource(runtime.endpoint),
    pollIntervalMs: POLL_INTERVAL_MS,
    signals: 'disabled_until_analysis_layer_is_added'
  });
});

app.get('/ping', (_req, res) => {
  res.type('text').send('pong');
});

app.get('/health', async (_req, res) => {
  let dbOk = false;
  let dbError = null;

  try {
    await ensureSchema();
    await pool.query('SELECT 1');
    dbOk = true;
  } catch (error) {
    dbError = safeError(error);
  }

  const payload = healthPayload(dbOk, dbError);

  res
    .status(payload.success ? 200 : 503)
    .json(payload);
});

app.get('/mesa', (_req, res) => {
  res.json({
    success: true,
    source: safeSource(runtime.endpoint),
    runtime
  });
});

app.get('/resultados', async (req, res) => {
  try {
    await ensureSchema();

    const limit = Math.max(
      1,
      Math.min(
        500,
        Number.parseInt(req.query.limit, 10) || 100
      )
    );

    const rows = await getLatestResults(limit);

    res.json({
      success: true,
      total: rows.length,
      resultados: rows
    });
  } catch (error) {
    res.status(503).json({
      success: false,
      error: safeError(error)
    });
  }
});

app.post('/admin/poll', requireAdmin, async (_req, res) => {
  const result = await pollOnce();

  res
    .status(
      result.success || result.skipped
        ? 200
        : 503
    )
    .json(result);
});

app.use((_req, res) => {
  res.status(404).json({
    success: false,
    error: 'Rota não encontrada'
  });
});

async function start() {
  if (!pool) {
    throw new Error('DATABASE_URL não configurada');
  }

  await ensureSchema();

  server = app.listen(PORT, '0.0.0.0', () => {
    log('START', {
      port: PORT,
      source: safeSource(runtime.endpoint),
      pollIntervalMs: POLL_INTERVAL_MS,
      mode: 'table-monitor'
    });
  });

  const first = await pollOnce();

  if (!first.success) {
    log('PRIMEIRA_COLETA', first);
  }

  pollTimer = setInterval(() => {
    pollOnce().catch(error => {
      log('POLL_FATAL', safeError(error));
    });
  }, POLL_INTERVAL_MS);
}

async function shutdown() {
  if (pollTimer) {
    clearInterval(pollTimer);
  }

  if (server) {
    await new Promise(resolve =>
      server.close(resolve)
    );
  }

  if (source?.close) {
    await source.close();
  }

  if (pool) {
    await pool.end();
  }
}

process.on('SIGTERM', () => {
  shutdown()
    .then(() => process.exit(0))
    .catch(() => process.exit(1));
});

process.on('SIGINT', () => {
  shutdown()
    .then(() => process.exit(0))
    .catch(() => process.exit(1));
});

process.on('unhandledRejection', reason => {
  log('UNHANDLED', safeError(reason));
});

process.on('uncaughtException', error => {
  log('UNCAUGHT', safeError(error));

  shutdown().finally(() => {
    process.exit(1);
  });
});

if (require.main === module) {
  start().catch(error => {
    log('FATAL', safeError(error));
    process.exit(1);
  });
}

module.exports = {
  app,
  normalizeOutcome,
  extractItems,
  normalizeResult,
  normalizeBrowserResult,
  resultSignature,
  diffSnapshots,
  healthPayload,
  POLL_INTERVAL_MS
};
