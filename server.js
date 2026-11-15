'use strict';

// BAC BO Telegram Signals — v3 (orientado a eventos de ronda, com diagnóstico de latência)
//
// Variáveis obrigatórias no Render:
//   BOT_TOKEN, CHAT_ID, ADMIN_TOKEN (>=24 caracteres), DATABASE_URL
// Opcionais:
//   HISTORY_TZ              Africa/Luanda | America/New_York | America/Sao_Paulo | Europe/Lisbon | UTC
//   FEED_URL                URL alternativo do feed (padrão: ioscout.io)
//   FEED_INTERVAL_MS        500–10000 (padrão 1000)
//   MAX_TRIGGER_AGE_MS      idade máxima da ronda para ainda enviar sinal (padrão 8000)
//   SIGNAL_LATENCY_SLACK_MS tolerância acima da latência normal do feed (padrão 6000)
//   SELF_URL                URL público (no Render usa-se RENDER_EXTERNAL_URL)
//
// Princípios:
//  1. O sinal só sai LOGO A SEGUIR a uma ronda nova (nunca a meio de uma ronda).
//  2. Se a ronda chegou tarde demais para dar tempo de apostar, o sinal é adiado
//     para a ronda seguinte (melhor sem sinal do que sinal fora de tempo).
//  3. Resultados e avisos de gale saem imediatamente, sem filas bloqueantes.
//  4. Tudo é medido: /admin/diagnostics mostra latência do feed e do Telegram.

const express = require('express');
const crypto = require('crypto');
const { Pool } = require('pg');

const app = express();
app.disable('x-powered-by');
app.use(express.json({ limit: '32kb' }));

const PORT = Number(process.env.PORT || 3000);

const FEED_URL =
  String(process.env.FEED_URL || '').trim() ||
  'https://ioscout.io/api/live.php?game=bac-bo';

const FEED_INTERVAL_MS = Math.min(
  10000,
  Math.max(500, Number(process.env.FEED_INTERVAL_MS) || 1000)
);
const FEED_TIMEOUT_MS = 4000;
const FEED_MAX_RETRY_MS = 10000;
const TELEGRAM_TIMEOUT_MS = 6000;

const MAX_TRIGGER_AGE_MS = Math.max(
  1000,
  Number(process.env.MAX_TRIGGER_AGE_MS) || 8000
);
const SIGNAL_LATENCY_SLACK_MS = Math.max(
  1000,
  Number(process.env.SIGNAL_LATENCY_SLACK_MS) || 6000
);

const MAX_FEED_ROUND_AGE_MS = 5 * 60 * 1000;
const FRESH_SEEN_WINDOW_MS = 3 * 60 * 1000;
const SIGNAL_MAX_PENDING_MS = 10 * 60 * 1000;
const FEED_DEAD_RESTART_MS = 10 * 60 * 1000;
const SUPERVISOR_MS = 3000;
const KEEPALIVE_MS = 4 * 60 * 1000;

const DEFAULT_SIGNAL_INTERVAL_MIN = 3;
const DEFAULT_GALES = 2;
const MAX_ACTIVE_SIGNALS = 1;

const REQUIRED_ENV = ['BOT_TOKEN', 'CHAT_ID', 'ADMIN_TOKEN', 'DATABASE_URL'];

const ALLOWED_TIMEZONES = new Set([
  'Africa/Luanda',
  'America/New_York',
  'America/Sao_Paulo',
  'Europe/Lisbon',
  'UTC'
]);

const BOOT_AT = Date.now();
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function validateEnvironment() {
  const missing = REQUIRED_ENV.filter(
    (name) => !String(process.env[name] || '').trim()
  );

  if (missing.length) {
    throw new Error(`Variáveis obrigatórias ausentes: ${missing.join(', ')}`);
  }

  if (!Number.isInteger(PORT) || PORT < 1 || PORT > 65535) {
    throw new Error('PORT inválida.');
  }

  if (String(process.env.ADMIN_TOKEN).length < 24) {
    throw new Error('ADMIN_TOKEN deve ter pelo menos 24 caracteres aleatórios.');
  }

  try {
    new URL(process.env.DATABASE_URL);
  } catch {
    throw new Error('DATABASE_URL inválida.');
  }

  try {
    new URL(FEED_URL);
  } catch {
    throw new Error('FEED_URL inválida.');
  }
}

validateEnvironment();

const HISTORY_TZ = ALLOWED_TIMEZONES.has(process.env.HISTORY_TZ)
  ? process.env.HISTORY_TZ
  : 'Africa/Luanda';

const dbUrl = new URL(process.env.DATABASE_URL);

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl:
    dbUrl.searchParams.get('sslmode') === 'disable'
      ? false
      : { rejectUnauthorized: false },
  max: 5,
  connectionTimeoutMillis: 10000,
  idleTimeoutMillis: 30000,
  statement_timeout: 8000,
  query_timeout: 10000,
  keepAlive: true
});

pool.on('error', (error) => {
  console.error('[db] Erro numa ligação inativa:', error.message);
});

// Repete uma operação de base de dados algumas vezes antes de desistir.
async function retryDb(task, label, attempts = 3) {
  let lastError;

  for (let i = 1; i <= attempts; i += 1) {
    try {
      return await task();
    } catch (error) {
      lastError = error;

      console.error(
        `[db] ${label} falhou (tentativa ${i}/${attempts}): ${error.message}`
      );

      if (i < attempts) await sleep(200 * i);
    }
  }

  throw lastError;
}

// ---------------------------------------------------------------------------
// Telegram — chamadas diretas (sem fila), com retentativas curtas
// ---------------------------------------------------------------------------

async function telegramCall(method, payload, label) {
  const maxAttempts = 3;

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      const response = await fetch(
        `https://api.telegram.org/bot${process.env.BOT_TOKEN}/${method}`,
        {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify(payload),
          signal: AbortSignal.timeout(TELEGRAM_TIMEOUT_MS)
        }
      );

      const data = await response.json().catch(() => ({}));

      if (response.ok && data.ok) return data.result;

      const error = new Error(
        `${label}: ${data.description || `HTTP ${response.status}`}`
      );

      error.permanent =
        response.status >= 400 &&
        response.status < 500 &&
        response.status !== 429;

      const retryAfter = Number(data?.parameters?.retry_after);

      if (Number.isFinite(retryAfter) && retryAfter > 0) {
        error.retryAfterMs = Math.min(5000, retryAfter * 1000);
      }

      throw error;
    } catch (error) {
      if (error.permanent || attempt === maxAttempts) throw error;

      const wait = error.retryAfterMs ?? 300 * 2 ** (attempt - 1);

      console.warn(
        `[telegram] ${label} falhou (tentativa ${attempt}/${maxAttempts}): ${error.message}; nova tentativa em ${wait} ms.`
      );

      await sleep(wait);
    }
  }

  throw new Error(`${label}: falha desconhecida.`);
}

const bot = {
  sendMessage(chatId, text) {
    return telegramCall(
      'sendMessage',
      { chat_id: chatId, text, disable_web_page_preview: true },
      'Telegram sendMessage'
    );
  },

  deleteMessage(chatId, messageId) {
    return telegramCall(
      'deleteMessage',
      { chat_id: chatId, message_id: messageId },
      'Telegram deleteMessage'
    );
  }
};

let httpServer;
let shuttingDown = false;

function asyncRoute(handler) {
  return (req, res, next) =>
    Promise.resolve(handler(req, res, next)).catch(next);
}

function requireAdmin(req, res, next) {
  const supplied = Buffer.from(String(req.get('x-admin-token') || ''));
  const expected = Buffer.from(String(process.env.ADMIN_TOKEN || ''));

  if (
    supplied.length !== expected.length ||
    !crypto.timingSafeEqual(supplied, expected)
  ) {
    return res.status(401).json({ error: 'Token administrativo inválido.' });
  }

  next();
}

function normalizeSector(value) {
  if (value === 'BankerWon' || value === 'B') return 'B';
  if (value === 'PlayerWon' || value === 'P') return 'P';
  if (value === 'Tie' || value === 'T') return 'T';
  return null;
}

function parseFeedTime(value) {
  if (!value) return null;

  const normalized = String(value).includes('T')
    ? String(value)
    : String(value).replace(' ', 'T');

  const hasTimezone = /(?:[zZ]|[+-]\d{2}:\d{2})$/.test(normalized);
  const date = new Date(hasTimezone ? normalized : `${normalized}Z`);

  return Number.isNaN(date.getTime()) ? null : date;
}

function optionalNumber(value) {
  if (value === null || value === undefined || value === '') return null;

  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

const median = (values) => {
  if (!values.length) return null;

  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);

  return sorted.length % 2
    ? sorted[mid]
    : Math.round((sorted[mid - 1] + sorted[mid]) / 2);
};

// ---------------------------------------------------------------------------
// Feed ao vivo — ciclo contínuo, mede a latência de cada ronda
// ---------------------------------------------------------------------------

const LATENCY_VALID_MIN_MS = -3000;
const LATENCY_VALID_MAX_MS = 180000;

class LiveFeed {
  constructor() {
    this.rounds = []; // mais recente primeiro
    this.lastUpdate = null; // última resposta válida
    this.lastRoundAt = null; // hora (do feed) da ronda mais recente
    this.lastNewRoundSeenAt = null; // relógio do servidor
    this.errors = 0;
    this.loaded = false;
    this.stopped = false;
    this.looping = false;
    this.loopTickAt = Date.now();
    this.lastPollMs = null;
    this.delay = FEED_INTERVAL_MS;
    this.listeners = new Set();
    this.roundQueue = Promise.resolve();
    this.latencies = []; // ms entre o fim da ronda e a nossa deteção
  }

  onRound(listener) {
    this.listeners.add(listener);
  }

  getRounds(limit = 50) {
    return this.rounds.slice(0, limit);
  }

  latest() {
    return this.rounds[0] || null;
  }

  isRoundFresh() {
    if (this.lastRoundAt) {
      const age = Date.now() - this.lastRoundAt.getTime();
      if (age >= -60000 && age <= MAX_FEED_ROUND_AGE_MS) return true;
    }

    return Boolean(
      this.lastNewRoundSeenAt &&
        Date.now() - this.lastNewRoundSeenAt.getTime() <= FRESH_SEEN_WINDOW_MS
    );
  }

  recordLatency(round) {
    const latency = round.detectedAt - round.at.getTime();

    round.latencyMs = latency;

    if (latency >= LATENCY_VALID_MIN_MS && latency <= LATENCY_VALID_MAX_MS) {
      this.latencies.push(latency);
      if (this.latencies.length > 50) this.latencies.shift();
    }
  }

  // Latência "normal" do feed (percentil 20 das últimas medições).
  latencyBaseline() {
    const samples = this.latencies.slice(-30);

    if (samples.length < 5) return null;

    const sorted = [...samples].sort((a, b) => a - b);
    return sorted[Math.floor(sorted.length * 0.2)];
  }

  // A ronda chegou dentro do atraso habitual? (se não, já é tarde para apostar)
  isRoundTimely(round) {
    const baseline = this.latencyBaseline();
    const latency = round.latencyMs;

    if (baseline === null || latency === undefined) return true;
    if (latency < LATENCY_VALID_MIN_MS || latency > LATENCY_VALID_MAX_MS) {
      return true;
    }

    return latency <= baseline + SIGNAL_LATENCY_SLACK_MS;
  }

  cadenceMs() {
    const sample = this.rounds.slice(0, 21);
    const diffs = [];

    for (let i = 0; i < sample.length - 1; i += 1) {
      const diff = sample[i].at.getTime() - sample[i + 1].at.getTime();
      if (diff > 5000 && diff < 120000) diffs.push(diff);
    }

    return median(diffs);
  }

  stats() {
    const samples = this.latencies;

    return {
      url: FEED_URL,
      pollIntervalMs: FEED_INTERVAL_MS,
      lastPollMs: this.lastPollMs,
      samples: samples.length,
      lastLatencyMs: samples.length ? samples[samples.length - 1] : null,
      minLatencyMs: samples.length ? Math.min(...samples) : null,
      medianLatencyMs: median(samples),
      maxLatencyMs: samples.length ? Math.max(...samples) : null,
      baselineMs: this.latencyBaseline(),
      roundCadenceMs: this.cadenceMs(),
      errors: this.errors
    };
  }

  async poll() {
    const startedAt = Date.now();
    const separator = FEED_URL.includes('?') ? '&' : '?';

    const response = await fetch(`${FEED_URL}${separator}_=${startedAt}`, {
      headers: {
        'User-Agent': 'BACBO-Telegram-Signals/3.0',
        Accept: 'application/json',
        'Cache-Control': 'no-cache, no-store',
        Pragma: 'no-cache'
      },
      signal: AbortSignal.timeout(FEED_TIMEOUT_MS),
      cache: 'no-store'
    });

    const receivedAt = Date.now();

    if (!response.ok) {
      throw new Error(`Feed respondeu HTTP ${response.status}`);
    }

    const contentType = response.headers.get('content-type') || '';

    if (!contentType.includes('json')) {
      throw new Error(
        `Tipo de conteúdo inesperado: ${contentType || 'desconhecido'}`
      );
    }

    const payload = await response.json();

    if (!payload || !Array.isArray(payload.rounds)) {
      throw new Error('Resposta sem array rounds.');
    }

    const parsed = payload.rounds
      .filter((item) => item && typeof item === 'object')
      .map((item) => {
        const side = normalizeSector(item.sector);
        const at = parseFeedTime(item.settled_at);

        return {
          key: `${item.settled_at}|${side}`,
          settledAt: item.settled_at,
          at,
          side,
          multiplier: item.multiplier,
          playerScore: optionalNumber(
            item.playerScore ??
              item.player_score ??
              item.player?.score ??
              item.player_total
          ),
          bankerScore: optionalNumber(
            item.bankerScore ??
              item.banker_score ??
              item.banker?.score ??
              item.banker_total
          )
        };
      })
      .filter((item) => item.side && item.at)
      .sort((a, b) => b.at - a.at);

    if (!parsed.length) {
      throw new Error('Feed sem rodadas reconhecíveis.');
    }

    const known = new Set(this.rounds.map((item) => item.key));
    const isInitial = !this.loaded;
    const fresh = [];

    for (const round of parsed) {
      if (known.has(round.key)) continue;

      known.add(round.key);

      round.detectedAt = receivedAt;
      round.initial = isInitial;

      if (!isInitial) this.recordLatency(round);

      fresh.push(round);
    }

    this.rounds = [...fresh, ...this.rounds]
      .sort((a, b) => b.at - a.at)
      .slice(0, 500);

    this.lastPollMs = receivedAt - startedAt;
    this.lastUpdate = new Date(receivedAt);
    this.lastRoundAt = this.rounds[0].at;

    if (isInitial) {
      this.loaded = true;

      console.log(
        `[feed] Ligado: ${this.rounds.length} rondas válidas; última ronda ${this.rounds[0].settledAt}.`
      );
    } else if (fresh.length) {
      this.lastNewRoundSeenAt = new Date(receivedAt);
    }

    for (const round of fresh.slice().sort((a, b) => a.at - b.at)) {
      if (!round.initial) {
        const score =
          hasScore(round) ? ` ${round.playerScore}×${round.bankerScore}` : '';

        console.log(
          `[feed] Ronda nova ${round.side}${score} • latência ${(round.latencyMs / 1000).toFixed(1)} s • poll ${this.lastPollMs} ms`
        );
      }

      this.enqueueRound(round);
    }
  }

  // Ciclo contínuo, taxa fixa; nunca morre por erro.
  async run() {
    if (this.looping) return;

    this.looping = true;

    while (!this.stopped) {
      const started = Date.now();
      this.loopTickAt = started;

      try {
        await this.poll();

        this.errors = 0;
        this.delay = FEED_INTERVAL_MS;
      } catch (error) {
        this.errors += 1;

        this.delay = Math.min(
          FEED_MAX_RETRY_MS,
          Math.max(FEED_INTERVAL_MS, this.delay * 2)
        );

        console.error(
          `[feed] Falha ${this.errors}: ${error.message}; nova tentativa em ${this.delay} ms.`
        );
      }

      await sleep(Math.max(100, this.delay - (Date.now() - started)));
    }

    this.looping = false;
  }

  ensureRunning() {
    if (this.stopped || this.looping) return;

    console.warn('[feed] Ciclo parado; a reativar.');
    void this.run();
  }

  enqueueRound(round) {
    this.roundQueue = this.roundQueue
      .then(async () => {
        const maxAttempts = 5;

        for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
          if (shuttingDown) return;

          try {
            for (const listener of this.listeners) {
              await listener(round);
            }

            return;
          } catch (error) {
            console.error(
              `[feed] Processamento da ronda falhou (tentativa ${attempt}/${maxAttempts}): ${error.message}`
            );

            if (attempt < maxAttempts) {
              await sleep(Math.min(5000, 300 * 2 ** (attempt - 1)));
            }
          }
        }

        console.error(
          `[feed] Ronda ${round.settledAt} descartada após falhas repetidas.`
        );
      })
      .catch((error) => {
        console.error('[feed] Falha ao processar ronda:', error.message);
      });
  }

  start() {
    void this.run();
  }

  stop() {
    this.stopped = true;
  }
}

const feed = new LiveFeed();

// ---------------------------------------------------------------------------
// Base de dados
// ---------------------------------------------------------------------------

async function initializeDatabase() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS settings (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS signals (
      id BIGSERIAL PRIMARY KEY,
      entry TEXT NOT NULL CHECK (entry IN ('P', 'B')),
      strategy TEXT NOT NULL,
      detail TEXT NOT NULL DEFAULT '',
      gales INTEGER NOT NULL DEFAULT 2 CHECK (gales BETWEEN 0 AND 3),
      attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
      result TEXT NOT NULL DEFAULT 'pendente',
      result_notification_pending BOOLEAN NOT NULL DEFAULT false,
      gale_message_id BIGINT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      settled_at TIMESTAMPTZ
    );

    ALTER TABLE signals ADD COLUMN IF NOT EXISTS detail TEXT NOT NULL DEFAULT '';
    ALTER TABLE signals ADD COLUMN IF NOT EXISTS settled_at TIMESTAMPTZ;
    ALTER TABLE signals ADD COLUMN IF NOT EXISTS gale_message_id BIGINT;
    ALTER TABLE signals ADD COLUMN IF NOT EXISTS result_notification_pending BOOLEAN NOT NULL DEFAULT false;
    ALTER TABLE signals ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0;
    ALTER TABLE signals ADD COLUMN IF NOT EXISTS gales INTEGER NOT NULL DEFAULT 2;
    ALTER TABLE signals ADD COLUMN IF NOT EXISTS baseline_at TIMESTAMPTZ;

    CREATE INDEX IF NOT EXISTS signals_created_at_idx ON signals (created_at DESC);
    CREATE INDEX IF NOT EXISTS signals_result_idx ON signals (result);
    CREATE INDEX IF NOT EXISTS signals_pending_idx ON signals (result) WHERE result = 'pendente';
    CREATE INDEX IF NOT EXISTS signals_notification_pending_idx
      ON signals (result_notification_pending) WHERE result_notification_pending = true;
  `);

  console.log('[db] PostgreSQL ligado e esquema verificado.');
}

async function getSetting(key, fallback) {
  const result = await pool.query('SELECT value FROM settings WHERE key = $1', [
    key
  ]);

  return result.rowCount ? result.rows[0].value : fallback;
}

async function setSetting(key, value) {
  await pool.query(
    `INSERT INTO settings (key, value)
     VALUES ($1, $2)
     ON CONFLICT (key)
     DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()`,
    [key, String(value)]
  );
}

async function insertSignal(signal) {
  const result = await pool.query(
    `INSERT INTO signals (entry, strategy, detail, gales, baseline_at)
     VALUES ($1, $2, $3, $4, $5)
     RETURNING id, created_at`,
    [
      signal.entry,
      signal.strategy,
      signal.detail,
      signal.gales,
      signal.baselineAt
    ]
  );

  return result.rows[0];
}

async function updateSignal(id, { attempts = null, result = null } = {}) {
  await pool.query(
    `UPDATE signals
     SET
       attempts = COALESCE($1::int, attempts),
       result = COALESCE($2::text, result),
       settled_at = CASE
         WHEN $2::text IS NULL THEN settled_at
         ELSE NOW()
       END,
       result_notification_pending = CASE
         WHEN $2::text IN ('green','g1','g2','empate','eg1','eg2','red')
         THEN true
         ELSE result_notification_pending
       END
     WHERE id = $3`,
    [attempts, result, id]
  );
}

async function markOutcomeNotified(id) {
  await pool.query(
    `UPDATE signals SET result_notification_pending = false WHERE id = $1`,
    [id]
  );
}

async function saveGaleMessageId(id, messageId) {
  await pool.query(`UPDATE signals SET gale_message_id = $1 WHERE id = $2`, [
    messageId,
    id
  ]);
}

async function statistics() {
  const { rows } = await pool.query(`
    SELECT
      COUNT(*)::int AS total,
      COUNT(*) FILTER (WHERE result IN ('green','g1','g2'))::int AS green,
      COUNT(*) FILTER (WHERE result = 'green')::int AS direct,
      COUNT(*) FILTER (WHERE result IN ('g1','g2'))::int AS gale,
      COUNT(*) FILTER (WHERE result IN ('empate','eg1','eg2'))::int AS tie,
      COUNT(*) FILTER (WHERE result = 'red')::int AS red,
      COUNT(*) FILTER (WHERE result = 'pendente')::int AS pending,
      COUNT(*) FILTER (WHERE result = 'cancelado')::int AS cancelled
    FROM signals
  `);

  const data = rows[0];
  const decided = data.green + data.red;

  return {
    ...data,
    assertividade: decided
      ? Number(((data.green / decided) * 100).toFixed(1))
      : null
  };
}

async function dailyHistory() {
  const { rows } = await pool.query(
    `
    WITH today AS (
      SELECT
        result,
        ROW_NUMBER() OVER (ORDER BY settled_at DESC, id DESC) AS rn
      FROM signals
      WHERE
        result IN ('green','g1','g2','empate','eg1','eg2','red')
        AND settled_at IS NOT NULL
        AND (settled_at AT TIME ZONE $1)::date = (NOW() AT TIME ZONE $1)::date
    )
    SELECT
      (NOW() AT TIME ZONE $1)::date::text AS day,
      COUNT(*)::int AS resolved,
      COUNT(*) FILTER (WHERE result = 'green')::int AS direct,
      COUNT(*) FILTER (WHERE result = 'g1')::int AS g1,
      COUNT(*) FILTER (WHERE result = 'g2')::int AS g2,
      COUNT(*) FILTER (WHERE result IN ('green','g1','g2'))::int AS greens,
      COUNT(*) FILTER (WHERE result = 'empate')::int AS tiedirect,
      COUNT(*) FILTER (WHERE result = 'eg1')::int AS eg1,
      COUNT(*) FILTER (WHERE result = 'eg2')::int AS eg2,
      COUNT(*) FILTER (WHERE result IN ('empate','eg1','eg2'))::int AS ties,
      COUNT(*) FILTER (WHERE result = 'red')::int AS losses,
      COALESCE(
        (
          SELECT COUNT(*)::int
          FROM today t
          WHERE t.rn < COALESCE(
            (SELECT MIN(rn) FROM today WHERE result NOT IN ('green','g1','g2')),
            2147483647
          )
        ),
        0
      ) AS greenstreak
    FROM today
    `,
    [HISTORY_TZ]
  );

  return rows[0];
}

function dailyLine(daily) {
  return [
    `📊 HISTÓRICO DO DIA — ${daily.day}`,
    `🎯 Resolvidos: ${daily.resolved}`,
    `🟢 Green sem gale: ${daily.direct} • G1: ${daily.g1} • G2: ${daily.g2}`,
    `🟡 Empate: ${daily.tiedirect} • EG1: ${daily.eg1} • EG2: ${daily.eg2}`,
    `🔴 Loss: ${daily.losses} • 🔥 Greens seguidos: ${daily.greenstreak}`
  ].join('\n');
}

function resultMessage(status, signal) {
  const labels = {
    green: '🟢 GREEN XIRA — DE PRIMEIRA! 🎉🥳',
    g1: '🟢 GREEN XIRA — GALE 1! 🎊💚',
    g2: '🟢 GREEN XIRA — GALE 2! 🎉🛡️',
    empate: '🟡 EMPATE! PROTEÇÃO ATIVADA — ENTRADA ENCERRADA 🛡️🤝',
    eg1: '🟡 EMPATE NO GALE 1 — ENTRADA ENCERRADA 🛡️🤝',
    eg2: '🟡 EMPATE NO GALE 2 — ENTRADA ENCERRADA 🛡️🤝',
    red: '🔴 TÁ MALÊ — LOSS NESTA ENTRADA 😕💔'
  };

  return `${labels[status] || '📣 RESULTADO DA ENTRADA'}\n🎲 Sinal #${signal.id}`;
}

async function sendOutcome(status, signal, includeHistory = true) {
  let text = resultMessage(status, signal);

  if (includeHistory) {
    try {
      // O placar não pode atrasar o resultado: máximo 1,5 s.
      const daily = await Promise.race([
        dailyHistory(),
        sleep(1500).then(() => {
          throw new Error('tempo esgotado');
        })
      ]);

      text += `\n\n${dailyLine(daily)}`;
    } catch (error) {
      console.error(
        '[db] Placar diário indisponível nesta mensagem:',
        error.message
      );
    }
  }

  await bot.sendMessage(process.env.CHAT_ID, text);

  try {
    await markOutcomeNotified(signal.id);
  } catch (error) {
    console.error(
      `[db] Resultado #${signal.id} enviado, mas não foi marcado como notificado:`,
      error.message
    );
  }
}

// ---------------------------------------------------------------------------
// Estratégias
// ---------------------------------------------------------------------------

const opposite = (side) => (side === 'P' ? 'B' : 'P');
const isSide = (side) => side === 'P' || side === 'B';

function hasScore(round) {
  return (
    Number.isFinite(round.playerScore) && Number.isFinite(round.bankerScore)
  );
}

const scoreFor = (round) =>
  round.side === 'P' ? round.playerScore : round.bankerScore;

const margin = (round) => Math.abs(round.playerScore - round.bankerScore);

const rangeOf = (score) => {
  if (score <= 2) return 'MB';
  if (score <= 4) return 'B';
  if (score <= 6) return 'M';
  if (score <= 8) return 'A';
  return 'MA';
};

function currentStreak(sequence) {
  if (!sequence.length) return 0;

  let n = 0;

  while (n < sequence.length && sequence[n].side === sequence[0].side) {
    n += 1;
  }

  return n;
}

function alternatingLength(sequence) {
  if (sequence.length < 2) return sequence.length;

  let n = 1;

  while (n < sequence.length && sequence[n].side !== sequence[n - 1].side) {
    n += 1;
  }

  return n;
}

function detectRegime(sequence) {
  const sample = sequence.slice(0, 15);

  if (sample.length < 6) {
    return { trend: 0.5, chop: 0.5, label: 'neutro' };
  }

  let changes = 0;

  for (let i = 1; i < sample.length; i += 1) {
    if (sample[i].side !== sample[i - 1].side) changes += 1;
  }

  const chop = changes / (sample.length - 1);

  return {
    trend: 1 - chop,
    chop,
    label: chop >= 0.65 ? 'errático' : chop <= 0.35 ? 'tendência' : 'neutro'
  };
}

function runStrategies(rows) {
  const seq = rows.filter((round) => isSide(round.side));

  if (seq.length < 2) return [];

  const out = [];

  const add = (side, key, name, strength, type = 'seq') => {
    if (isSide(side)) out.push({ side, key, name, strength, type });
  };

  const n = currentStreak(seq);

  if (n >= 3 && n <= 7) {
    const strengths = [0, 0, 0, 72, 78, 84, 86, 86];
    add(seq[0].side, `SEQ_REP_${n}`, `Sequência x${n}`, strengths[n], 'seq');
  }

  if (n >= 4 && n <= 8) {
    const strengths = [0, 0, 0, 0, 74, 80, 84, 85, 85];
    add(
      opposite(seq[0].side),
      `SEQ_QUEBRA_${n}`,
      `Quebra x${n}`,
      strengths[n],
      'seq'
    );
  }

  const alt = alternatingLength(seq);

  if (alt >= 4) {
    add(
      opposite(seq[0].side),
      `SEQ_SURF_${alt}`,
      `Surf ${alt}`,
      alt >= 6 ? 84 : alt >= 5 ? 78 : 72,
      'seq'
    );
  }

  if (seq.length >= 4) {
    const [a, b, c, d] = seq.slice(0, 4).map((item) => item.side);

    if (a === b && c === d && a !== c) {
      add(c, 'SEQ_2x2', 'Bloco 2+2', 76, 'seq');
    }

    if (a !== b && b === c && c === d) {
      add(a, 'SEQ_3_1', '3→1 confirmado', 78, 'seq');
    }
  }

  if (seq.length >= 3) {
    const [a, b, c] = seq.slice(0, 3).map((item) => item.side);

    if (a !== b && b === c) {
      add(c, 'SEQ_MOMENTUM_2', 'Momentum 2', 77, 'seq');
    }
  }

  if (
    n === 4 &&
    seq.slice(0, 4).every((round) => hasScore(round) && margin(round) <= 2)
  ) {
    add(
      opposite(seq[0].side),
      'SEQ_BREAK_WEAK_4',
      'Quebra fraca x4',
      79,
      'seq'
    );
  }

  if (seq.length >= 5) {
    const [a, b, c, d, e] = seq.slice(0, 5).map((item) => item.side);

    if (a === b && b === c && c !== d && d === e) {
      add(a, 'SEQ_EXTEND', 'Streak extend', 80, 'seq');
    }
  }

  if (
    n >= 4 &&
    n <= 6 &&
    seq.slice(0, n).every((round) => hasScore(round) && margin(round) >= 3)
  ) {
    add(seq[0].side, 'SEQ_STRONG_STREAK', `Dragon ${n}`, 82, 'seq');
  }

  if (alt >= 4 && alt <= 10) {
    add(opposite(seq[0].side), 'SEQ_PINGPONG', `Ping-pong ${alt}`, 79, 'seq');
  }

  if (
    alt >= 3 &&
    seq.slice(0, alt).every((round) => hasScore(round) && margin(round) >= 3)
  ) {
    add(
      opposite(seq[0].side),
      'SEQ_ALT_STRONG',
      `Alternância forte ${alt}`,
      78,
      'seq'
    );
  }

  const last20 = seq.slice(0, 20);

  if (last20.length >= 15) {
    const bankers = last20.filter((round) => round.side === 'B').length;
    const players = last20.length - bankers;

    if (bankers >= 12) add('B', 'BANKER_BIAS_20', 'Viés Banca', 80, 'seq');
    if (players >= 12) add('P', 'BANKER_BIAS_20', 'Viés Jogador', 80, 'seq');
  }

  if (seq.length >= 3 && seq.slice(0, 3).every(hasScore)) {
    const [a, b, c] = seq.slice(0, 3);
    const sa = scoreFor(a);
    const sb = scoreFor(b);
    const sc = scoreFor(c);
    const same = a.side === b.side && b.side === c.side;

    if (
      same &&
      ((sb - sa === 1 && sc - sb === 1) || (sa - sb === 1 && sb - sc === 1))
    ) {
      add(a.side, 'NUM_ESCADA', `Escada ${sa}→${sb}→${sc}`, 88, 'num');
    }

    if (same && sa <= 3 && sb <= 3 && sc <= 3) {
      add(opposite(a.side), 'NUM_THREE_LOW', 'Virada de baixos x3', 81, 'num');
    }

    if (same && margin(a) >= 4 && margin(b) >= 4 && margin(c) >= 4) {
      add(a.side, 'NUM_DIFF4x3', 'Margem ≥4 ×3', 85, 'num');
    }

    if (same && sa < sb && sb < sc) {
      add(
        a.side,
        'NUM_SCORE_TREND_3',
        `Escala ascendente ${sa}→${sc}`,
        82,
        'num'
      );
    }
  }

  if (
    seq.length >= 2 &&
    seq[0].side === seq[1].side &&
    seq.slice(0, 2).every(hasScore)
  ) {
    const [a, b] = seq.slice(0, 2);

    if (scoreFor(a) === scoreFor(b)) {
      add(a.side, 'NUM_REPETE', `Score repetido ${scoreFor(a)}`, 84, 'num');
    }

    if (scoreFor(a) <= 4 && scoreFor(b) <= 4) {
      add(opposite(a.side), 'NUM_VIRADA_BAIXA', 'Virada de baixos', 80, 'num');
    }

    if (margin(a) >= 5 && margin(b) >= 5) {
      add(a.side, 'NUM_DOUBLE_HIGH', 'Dupla margem alta', 84, 'num');
    }

    const ra = rangeOf(scoreFor(a));
    const rb = rangeOf(scoreFor(b));

    if ((ra === 'MB' && rb === 'MA') || (ra === 'MA' && rb === 'MB')) {
      add(
        opposite(a.side),
        'NUM_SALTO_RANGE',
        `Salto ${scoreFor(b)}→${scoreFor(a)}`,
        81,
        'num'
      );
    }
  }

  const scoredRows = rows.slice(0, 8).filter(hasScore);

  if (scoredRows.length >= 6) {
    const playerMean =
      scoredRows.reduce((sum, round) => sum + round.playerScore, 0) /
      scoredRows.length;

    const bankerMean =
      scoredRows.reduce((sum, round) => sum + round.bankerScore, 0) /
      scoredRows.length;

    const difference = playerMean - bankerMean;

    if (Math.abs(difference) >= 1.5) {
      add(
        difference > 0 ? 'P' : 'B',
        'NUM_TENDENCIA',
        `Tendência ${playerMean.toFixed(1)}×${bankerMean.toFixed(1)}`,
        Math.abs(difference) >= 2.2 ? 84 : 76,
        'num'
      );
    }
  }

  if (seq[0] && hasScore(seq[0])) {
    const score = scoreFor(seq[0]);

    if (score === 0) {
      add(opposite(seq[0].side), 'NUM_GATILHO_0', 'Gatilho 0', 86, 'num');
    }

    if (score === 1) {
      add(opposite(seq[0].side), 'NUM_GATILHO_1', 'Gatilho 1', 84, 'num');
    }

    if (score === 9) {
      add(opposite(seq[0].side), 'NUM_GATILHO_9', 'Gatilho 9', 88, 'num');
    }

    if (rows[0] === seq[0] && margin(seq[0]) >= 5) {
      add(
        seq[0].side,
        'NUM_MARGEM5',
        `Margem alta ${seq[0].playerScore}×${seq[0].bankerScore}`,
        82,
        'num'
      );
    }
  }

  if (seq.length >= 4 && seq.slice(0, 4).every(hasScore)) {
    const scores = seq.slice(0, 4).map(scoreFor);

    if (
      scores[1] < scores[0] &&
      scores[2] < scores[1] &&
      scores[3] < scores[2]
    ) {
      add(
        opposite(seq[0].side),
        'MOMENTUM_SHIFT',
        'Mudança de momentum',
        78,
        'num'
      );
    }
  }

  if (rows[0]?.side === 'T' && seq.length) {
    add(seq[0].side, 'NUM_POS_EMPATE', 'Pós-empate', 74, 'num');
  }

  if (rows.slice(0, 3).filter((round) => round.side === 'T').length >= 2) {
    const prior = rows.find((round) => isSide(round.side));

    if (prior) {
      add(prior.side, 'TIE_CASCADE', 'Cascata de empates', 76, 'num');
    }
  }

  if (rows[0]?.side === 'T' && isSide(rows[1]?.side) && seq.length) {
    add(opposite(seq[0].side), 'TIE_SINGLE', 'Empate isolado', 77, 'num');
  }

  if (
    rows[0]?.side === 'T' &&
    rows[1]?.side === 'T' &&
    isSide(rows[2]?.side)
  ) {
    add(rows[2].side, 'TIE_DOUBLE_BACK', 'Retorno pós-duplo empate', 78, 'num');
  }

  if (seq.length >= 10) {
    const sample = seq.slice(0, 10);
    const p = sample.filter((round) => round.side === 'P').length;
    const b = sample.length - p;

    if (p !== b) {
      add(
        p > b ? 'P' : 'B',
        'FREQ_10',
        'Frequência nas últimas 10',
        70 + Math.min(10, Math.abs(p - b)),
        'freq'
      );
    }
  }

  return out;
}

function analyzeResults(rows) {
  const seq = rows.filter((round) => isSide(round.side));

  if (seq.length < 5) {
    throw new Error('Histórico válido insuficiente para analisar.');
  }

  const regime = detectRegime(seq);

  let candidates = runStrategies(rows).filter((candidate) => {
    if (candidate.type !== 'seq') return true;

    if (/QUEBRA|SURF|PINGPONG|2x2|EXTEND|3_1|MOMENTUM|ALT/.test(candidate.key)) {
      return regime.chop >= 0.4;
    }

    if (/REP|STRONG_STREAK|BANKER_BIAS/.test(candidate.key)) {
      return regime.trend >= 0.4;
    }

    return true;
  });

  if (!candidates.length) {
    const sample = seq.slice(0, 10);
    const p = sample.filter((round) => round.side === 'P').length;
    const b = sample.length - p;

    candidates = [
      {
        side: p >= b ? 'P' : 'B',
        key: 'FREQ_FALLBACK',
        name: 'Frequência combinada de reserva',
        strength: 65,
        type: 'freq'
      }
    ];
  }

  const familyOf = (candidate) => {
    if (candidate.key.startsWith('TIE_')) return 'empate';
    if (candidate.type === 'seq') return 'sequência';
    if (candidate.type === 'freq') return 'frequência';
    return 'pontuação';
  };

  const bySide = new Map();

  for (const candidate of candidates) {
    if (!bySide.has(candidate.side)) bySide.set(candidate.side, []);
    bySide.get(candidate.side).push(candidate);
  }

  const ranked = [...bySide.entries()]
    .map(([side, votes]) => {
      const families = new Set(votes.map(familyOf));

      const score =
        votes.reduce((sum, vote) => sum + vote.strength, 0) +
        Math.max(0, families.size - 1) * 4;

      return { side, votes, score, families };
    })
    .sort((a, b) => b.score - a.score);

  const winner = ranked[0];

  const best = winner.votes
    .slice()
    .sort((a, b) => b.strength - a.strength)[0];

  const supporting = winner.votes.filter((vote) => vote.side === winner.side);

  return {
    entry: winner.side,
    strategy: best.name,
    detail:
      `Compilação ${supporting.length} detetores; ` +
      `famílias ${[...winner.families].join(' + ')}; ` +
      `marcador de combinação ${winner.score}; ` +
      `regime ${regime.label}. ` +
      'O marcador não é probabilidade nem garantia de resultado.',
    strategyKey: best.key,
    consensus: supporting.map((vote) => vote.key),
    score: winner.score
  };
}

// ---------------------------------------------------------------------------
// Motor de sinais — orientado a eventos de ronda
// ---------------------------------------------------------------------------

const sideLabel = (side) => (side === 'B' ? '🔴 BANCA' : '🔵 JOGADOR');
const noHistory = (status) => ['empate', 'eg1', 'eg2'].includes(status);

class SignalEngine {
  constructor() {
    this.running = false;
    this.intervalMin = DEFAULT_SIGNAL_INTERVAL_MIN;
    this.gales = DEFAULT_GALES;
    this.activeSignals = [];
    this.sending = false;
    this.lastSignalAt = 0;
    this.skipped = { stale: 0, latency: 0, notDue: 0 };
    this.supervisorTimer = null;
    this.supervising = false;
    this.recoveringOutcomes = false;
    this.outcomeRecoveryTimer = null;
    this.notifying = new Set();

    // As rondas chegam UMA A UMA, em ordem (a fila está no feed).
    feed.onRound((round) => this.handleRound(round));
  }

  async restorePending() {
    const { rows } = await pool.query(
      `SELECT id, entry, strategy, detail, gales, attempts,
              created_at, baseline_at, gale_message_id
       FROM signals
       WHERE result = 'pendente'
       ORDER BY created_at ASC`
    );

    const restored = [];

    for (const row of rows) {
      const createdAt = new Date(row.created_at);

      if (Date.now() - createdAt.getTime() > SIGNAL_MAX_PENDING_MS) {
        await updateSignal(row.id, { result: 'cancelado' });
        console.warn(`[sinais] Sinal antigo #${row.id} cancelado no arranque.`);
        continue;
      }

      restored.push({
        id: String(row.id),
        entry: row.entry,
        strategy: row.strategy,
        detail: row.detail,
        gales: row.gales,
        attempts: row.attempts,
        createdAt,
        baselineAt: row.baseline_at ? new Date(row.baseline_at) : createdAt,
        galeMessageId: row.gale_message_id ? Number(row.gale_message_id) : null,
        lastRoundKey: null
      });
    }

    this.activeSignals = restored;

    if (restored.length) {
      console.log(
        `[sinais] ${restored.length} sinal(is) pendente(s) restaurado(s) após reinício.`
      );
    }
  }

  async recoverUnsentOutcomes() {
    if (this.recoveringOutcomes || shuttingDown) return;

    this.recoveringOutcomes = true;

    try {
      await pool.query(
        `UPDATE signals
         SET result_notification_pending = false
         WHERE result_notification_pending = true
           AND settled_at < NOW() - INTERVAL '30 minutes'`
      );

      const { rows } = await pool.query(
        `SELECT id, entry, result, attempts, gale_message_id
         FROM signals
         WHERE result_notification_pending = true
         ORDER BY settled_at ASC`
      );

      for (const row of rows) {
        const id = String(row.id);

        if (this.notifying.has(id)) continue;

        const signal = {
          id,
          entry: row.entry,
          attempts: row.attempts,
          galeMessageId: row.gale_message_id ? Number(row.gale_message_id) : null
        };

        this.notifying.add(id);

        try {
          await sendOutcome(row.result, signal, !noHistory(row.result));
          void this.clearGaleNotice(signal);

          console.log(`[sinais] Notificação do resultado #${id} recuperada.`);
        } catch (error) {
          console.error(
            `[telegram] Resultado #${id} continua pendente de notificação:`,
            error.message
          );
        } finally {
          this.notifying.delete(id);
        }
      }
    } finally {
      this.recoveringOutcomes = false;
    }
  }

  // Chamado para cada ronda nova, pela ordem em que aconteceram.
  async handleRound(round) {
    for (const signal of [...this.activeSignals]) {
      await this.settle(signal, round);
    }

    if (!round.initial) {
      await this.maybeSendSignal(round);
    }
  }

  async maybeSendSignal(round) {
    if (!this.running || shuttingDown || this.sending) return;
    if (this.activeSignals.length >= MAX_ACTIVE_SIGNALS) return;

    const intervalMs = this.intervalMin * 60 * 1000;

    if (Date.now() - this.lastSignalAt < intervalMs) {
      this.skipped.notDue += 1;
      return;
    }

    // Só vale a ronda mais recente e recém-detetada: é isso que dá tempo de apostar.
    const latest = feed.latest();

    if (!latest || latest.key !== round.key) {
      this.skipped.stale += 1;
      console.warn('[sinais] Sinal adiado: já existe ronda mais recente.');
      return;
    }

    const sinceDetected = Date.now() - round.detectedAt;

    if (sinceDetected > MAX_TRIGGER_AGE_MS) {
      this.skipped.stale += 1;

      console.warn(
        `[sinais] Sinal adiado: ronda detetada há ${sinceDetected} ms (limite ${MAX_TRIGGER_AGE_MS} ms).`
      );

      return;
    }

    if (!feed.isRoundTimely(round)) {
      this.skipped.latency += 1;

      console.warn(
        `[sinais] Sinal adiado: ronda chegou com latência anormal (${round.latencyMs} ms).`
      );

      return;
    }

    this.sending = true;

    try {
      await this.sendSignal(round);
    } catch (error) {
      console.error('[sinais] Erro ao enviar sinal:', error.message);
    } finally {
      this.sending = false;
    }
  }

  async sendSignal(round) {
    // A análise usa o histórico até esta ronda, nem mais nem menos.
    const history = feed.rounds.filter((item) => item.at <= round.at).slice(0, 50);
    const analysis = analyzeResults(history);

    const saved = await insertSignal({
      ...analysis,
      gales: this.gales,
      baselineAt: round.at
    });

    const signal = {
      id: String(saved.id),
      entry: analysis.entry,
      strategy: analysis.strategy,
      detail: analysis.detail,
      gales: this.gales,
      attempts: 0,
      createdAt: new Date(saved.created_at),
      baselineAt: round.at,
      galeMessageId: null,
      lastRoundKey: null
    };

    this.activeSignals.push(signal);

    const message = [
      '🎲🔥 SINAL BAC BO 🔥🎲',
      '',
      '✅ ENTRADA CONFIRMADA ✅',
      `🎯 Jogar: ${sideLabel(signal.entry)}`,
      '🛡️ Proteção: 🟡 EMPATE',
      `🔄 Até ${signal.gales} gale(s) • ${signal.gales + 1} tentativa(s)`,
      `🧠 Estratégia: ${signal.strategy}`,
      '',
      `🔖 Sinal #${signal.id}`
    ].join('\n');

    const t0 = Date.now();

    try {
      await bot.sendMessage(process.env.CHAT_ID, message);
    } catch (error) {
      this.activeSignals = this.activeSignals.filter(
        (item) => item.id !== signal.id
      );

      try {
        await updateSignal(signal.id, { result: 'cancelado' });
      } catch (databaseError) {
        console.error(
          `[db] Falha ao cancelar sinal #${signal.id}:`,
          databaseError.message
        );
      }

      throw error;
    }

    this.lastSignalAt = Date.now();

    console.log(
      `[sinais] Enviado #${signal.id} ${signal.entry}/${signal.strategy} • Telegram ${Date.now() - t0} ms • desde a ronda ${Date.now() - round.detectedAt} ms`
    );
  }

  async settle(signal, round) {
    // Só contam rondas posteriores à ronda-base do sinal.
    if (round.at.getTime() <= signal.baselineAt.getTime()) return;
    if (signal.lastRoundKey === round.key) return;

    signal.lastRoundKey = round.key;

    if (round.side === 'T') {
      const status =
        signal.attempts === 0 ? 'empate' : signal.attempts === 1 ? 'eg1' : 'eg2';

      await this.finishSignal(signal, status, round);
      return;
    }

    if (round.side === signal.entry) {
      const status =
        signal.attempts === 0 ? 'green' : signal.attempts === 1 ? 'g1' : 'g2';

      await this.finishSignal(signal, status, round);
      return;
    }

    const nextAttempt = signal.attempts + 1;

    if (nextAttempt > signal.gales) {
      await this.finishSignal(signal, 'red', round, nextAttempt);
      return;
    }

    // Gale: o aviso sai já; a gravação na base de dados corre em paralelo.
    signal.attempts = nextAttempt;

    const galeText = [
      `🔄⚠️ GALE ${nextAttempt} — VAMOS À PRÓXIMA!`,
      `🎯 Manter: ${sideLabel(signal.entry)}`,
      '🛡️ Empate continua protegido',
      `🔖 Sinal #${signal.id}`
    ].join('\n');

    const sendTask = (async () => {
      const t0 = Date.now();
      const sent = await bot.sendMessage(process.env.CHAT_ID, galeText);

      signal.galeMessageId = sent.message_id;

      console.log(
        `[sinais] Gale ${nextAttempt} #${signal.id} • Telegram ${Date.now() - t0} ms • desde a ronda ${Date.now() - round.detectedAt} ms`
      );

      await saveGaleMessageId(signal.id, sent.message_id).catch((error) =>
        console.error(
          `[db] Falha ao guardar ID do aviso de gale #${signal.id}:`,
          error.message
        )
      );
    })();

    const dbTask = retryDb(
      () => updateSignal(signal.id, { attempts: nextAttempt }),
      `gale #${signal.id}`
    );

    const [sendResult, dbResult] = await Promise.allSettled([sendTask, dbTask]);

    if (sendResult.status === 'rejected') {
      console.error(
        '[telegram] Falha ao enviar aviso de gale:',
        sendResult.reason?.message
      );
    }

    if (dbResult.status === 'rejected') {
      console.error(
        `[db] Gale #${signal.id} não gravado:`,
        dbResult.reason?.message
      );
    }
  }

  async finishSignal(signal, status, round, attempts = signal.attempts) {
    signal.attempts = attempts;

    // Fora da lista ativa de imediato: nada o volta a processar.
    this.activeSignals = this.activeSignals.filter(
      (item) => item.id !== signal.id
    );

    this.notifying.add(signal.id);

    try {
      try {
        await retryDb(
          () => updateSignal(signal.id, { attempts, result: status }),
          `resultado #${signal.id}`
        );
      } catch (error) {
        console.error(
          `[db] Resultado #${signal.id} não gravado; a notificar na mesma:`,
          error.message
        );
      }

      const t0 = Date.now();

      try {
        await sendOutcome(status, signal, !noHistory(status));

        console.log(
          `[sinais] #${signal.id}: ${status.toUpperCase()} • Telegram ${Date.now() - t0} ms • desde a ronda ${Date.now() - round.detectedAt} ms`
        );
      } catch (error) {
        console.error(
          `[telegram] Falha ao enviar resultado #${signal.id} (será recuperado):`,
          error.message
        );
      }
    } finally {
      this.notifying.delete(signal.id);
    }

    // Apagar o aviso de gale nunca atrasa nada.
    void this.clearGaleNotice(signal);
  }

  async start() {
    if (this.running) {
      return { started: false, reason: 'O motor já está ativo.' };
    }

    await setSetting('running', 'true');
    this.running = true;

    return { started: true, waitingForFeed: !feed.loaded };
  }

  async stop() {
    await setSetting('running', 'false');
    this.running = false;

    return {
      stopped: true,
      pendingSignalsKept: this.activeSignals.length
    };
  }

  // Supervisor: só limpeza e autorrecuperação (nunca envia sinais).
  startSupervisor() {
    if (this.supervisorTimer) return;

    this.supervisorTimer = setInterval(() => {
      this.supervise().catch((error) =>
        console.error('[supervisor] Erro:', error.message)
      );
    }, SUPERVISOR_MS);
  }

  async supervise() {
    if (this.supervising || shuttingDown) return;

    this.supervising = true;

    try {
      feed.ensureRunning();

      const lastOk = feed.lastUpdate ? feed.lastUpdate.getTime() : BOOT_AT;

      if (Date.now() - lastOk > FEED_DEAD_RESTART_MS) {
        console.error('[supervisor] Feed sem resposta há demasiado tempo; a reiniciar.');
        void shutdown('feed-dead', 1);
        return;
      }

      if (feed.looping && Date.now() - feed.loopTickAt > 60000) {
        console.error('[supervisor] Ciclo do feed bloqueado; a reiniciar.');
        void shutdown('feed-loop-stuck', 1);
        return;
      }

      for (const signal of [...this.activeSignals]) {
        if (Date.now() - signal.createdAt.getTime() > SIGNAL_MAX_PENDING_MS) {
          await this.cancelSignal(signal, true);
        }
      }
    } finally {
      this.supervising = false;
    }
  }

  async cancelSignal(signal, notify) {
    this.activeSignals = this.activeSignals.filter(
      (item) => item.id !== signal.id
    );

    await retryDb(
      () => updateSignal(signal.id, { result: 'cancelado' }),
      `cancelar #${signal.id}`
    );

    console.warn(`[sinais] Sinal #${signal.id} cancelado.`);

    if (notify) {
      try {
        await bot.sendMessage(
          process.env.CHAT_ID,
          `⚠️ Sinal #${signal.id} encerrado sem resultado (instabilidade na fonte). Ignorar.`
        );
      } catch (error) {
        console.error('[telegram] Aviso de cancelamento falhou:', error.message);
      }
    }

    void this.clearGaleNotice(signal);
  }

  async clearGaleNotice(signal) {
    if (!signal.galeMessageId) return;

    const messageId = signal.galeMessageId;

    signal.galeMessageId = null;

    try {
      await bot.deleteMessage(process.env.CHAT_ID, messageId);
    } catch (error) {
      console.error(
        `[telegram] Aviso de gale #${signal.id} não removido:`,
        error.message
      );
    }

    try {
      await saveGaleMessageId(signal.id, null);
    } catch {
      // ignorado
    }
  }
}

const engine = new SignalEngine();

// ---------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------

app.get('/ping', (_req, res) => {
  res.status(200).type('text/plain').send('ok');
});

app.get('/', (_req, res) => {
  res.json({
    name: 'BAC BO Telegram Signal Service',
    status: engine.running ? 'running' : 'stopped',
    feed: feed.loaded ? 'connected' : 'waiting'
  });
});

app.get('/health', (_req, res) => {
  const lastOk = feed.lastUpdate ? feed.lastUpdate.getTime() : BOOT_AT;
  const feedFresh = Date.now() - lastOk < 3 * 60 * 1000;
  const healthy = feedFresh && !shuttingDown;

  res.status(healthy ? 200 : 503).json({
    ok: healthy,
    feed: feed.loaded,
    feedFresh,
    roundFresh: feed.isRoundFresh(),
    lastUpdate: feed.lastUpdate,
    lastRoundAt: feed.lastRoundAt,
    feedErrors: feed.errors,
    running: engine.running,
    activeSignals: engine.activeSignals.length,
    uptimeSec: Math.round(process.uptime())
  });
});

app.get(
  '/admin/status',
  requireAdmin,
  asyncRoute(async (_req, res) => {
    const stats = await statistics();

    res.json({
      running: engine.running,
      intervalMin: engine.intervalMin,
      gales: engine.gales,
      activeSignals: engine.activeSignals,
      skipped: engine.skipped,
      statistics: stats,
      historyTimezone: HISTORY_TZ,
      feed: {
        loaded: feed.loaded,
        lastUpdate: feed.lastUpdate,
        lastRoundAt: feed.lastRoundAt,
        lastNewRoundSeenAt: feed.lastNewRoundSeenAt,
        roundFresh: feed.isRoundFresh(),
        errors: feed.errors,
        recentRounds: feed.rounds.slice(0, 10).map((round) => ({
          settledAt: round.settledAt,
          side: round.side,
          playerScore: round.playerScore,
          bankerScore: round.bankerScore,
          latencyMs: round.latencyMs ?? null
        }))
      }
    });
  })
);

// Mostra ONDE está o atraso: no feed ou no Telegram.
app.get(
  '/admin/diagnostics',
  requireAdmin,
  asyncRoute(async (_req, res) => {
    const telegram = [];

    for (let i = 0; i < 3; i += 1) {
      const t0 = Date.now();

      try {
        await telegramCall('getMe', {}, 'Telegram getMe');
        telegram.push(Date.now() - t0);
      } catch (error) {
        telegram.push(`erro: ${error.message}`);
      }
    }

    const t1 = Date.now();
    let dbMs = null;

    try {
      await pool.query('SELECT 1');
      dbMs = Date.now() - t1;
    } catch (error) {
      dbMs = `erro: ${error.message}`;
    }

    const feedStats = feed.stats();

    res.json({
      feed: feedStats,
      telegramRoundtripMs: telegram,
      databaseRoundtripMs: dbMs,
      skippedSignals: engine.skipped,
      reading:
        'latência do feed = tempo entre o fim da ronda (settled_at) e a nossa deteção. ' +
        'Se medianLatencyMs for alta, o atraso vem da fonte do feed, não do bot.'
    });
  })
);

app.post(
  '/admin/start',
  requireAdmin,
  asyncRoute(async (_req, res) => res.json(await engine.start()))
);

app.post(
  '/admin/stop',
  requireAdmin,
  asyncRoute(async (_req, res) => res.json(await engine.stop()))
);

app.post(
  '/admin/cancel-pending',
  requireAdmin,
  asyncRoute(async (_req, res) => {
    const signals = [...engine.activeSignals];

    for (const signal of signals) {
      await engine.cancelSignal(signal, false);
    }

    res.json({ ok: true, cancelled: signals.length });
  })
);

app.post(
  '/admin/interval',
  requireAdmin,
  asyncRoute(async (req, res) => {
    const value = Number(req.body?.minutes);

    if (!Number.isInteger(value) || value < 1 || value > 3) {
      return res.status(400).json({
        error:
          'minutes deve ser um inteiro entre 1 e 3; máximo permitido: 3 minutos.'
      });
    }

    engine.intervalMin = value;
    await setSetting('interval_min', value);

    res.json({ ok: true, intervalMin: value });
  })
);

app.post(
  '/admin/gales',
  requireAdmin,
  asyncRoute(async (req, res) => {
    const value = Number(req.body?.gales);

    if (!Number.isInteger(value) || value < 0 || value > 3) {
      return res
        .status(400)
        .json({ error: 'gales deve ser um inteiro entre 0 e 3.' });
    }

    engine.gales = value;
    await setSetting('gales', value);

    res.json({ ok: true, gales: value });
  })
);

app.get(
  '/admin/signals',
  requireAdmin,
  asyncRoute(async (_req, res) => {
    const { rows } = await pool.query(
      `SELECT id, entry, strategy, detail, gales, attempts, result,
              created_at, settled_at
       FROM signals
       ORDER BY id DESC
       LIMIT 100`
    );

    res.json(rows);
  })
);

app.use((err, _req, res, _next) => {
  console.error('[http] Erro:', err.message);

  if (res.headersSent) return;

  res.status(500).json({ error: 'Erro interno do servidor.' });
});

// ---------------------------------------------------------------------------
// Arranque / paragem
// ---------------------------------------------------------------------------

function startKeepAlive() {
  const selfUrl = process.env.SELF_URL || process.env.RENDER_EXTERNAL_URL;

  if (!selfUrl) {
    console.warn(
      '[keepalive] Sem SELF_URL/RENDER_EXTERNAL_URL: configure um monitor externo a chamar /ping a cada 5 min.'
    );
    return;
  }

  const target = `${selfUrl.replace(/\/+$/, '')}/ping`;

  setInterval(() => {
    fetch(target, { signal: AbortSignal.timeout(8000), cache: 'no-store' }).catch(
      (error) => console.warn('[keepalive] Auto-ping falhou:', error.message)
    );
  }, KEEPALIVE_MS);

  console.log(`[keepalive] Auto-ping ativo → ${target}`);
}

async function startServer() {
  await initializeDatabase();

  const interval = Number(
    await getSetting('interval_min', DEFAULT_SIGNAL_INTERVAL_MIN)
  );
  const gales = Number(await getSetting('gales', DEFAULT_GALES));

  if (Number.isInteger(interval) && interval >= 1) {
    engine.intervalMin = Math.min(3, interval);

    if (engine.intervalMin !== interval) {
      await setSetting('interval_min', engine.intervalMin);
    }
  }

  if (Number.isInteger(gales) && gales >= 0 && gales <= 3) {
    engine.gales = gales;
  }

  await engine.restorePending();

  httpServer = app.listen(PORT, '0.0.0.0', () => {
    console.log(`[server] A escutar na porta ${PORT}.`);
  });

  httpServer.keepAliveTimeout = 65000;

  // Arranque automático SEMPRE (24/7).
  engine.running = true;
  await setSetting('running', 'true');

  feed.start();
  engine.startSupervisor();
  startKeepAlive();

  void engine
    .recoverUnsentOutcomes()
    .catch((error) =>
      console.error('[db] Recuperação inicial de resultados falhou:', error.message)
    );

  engine.outcomeRecoveryTimer = setInterval(() => {
    void engine
      .recoverUnsentOutcomes()
      .catch((error) =>
        console.error('[db] Recuperação periódica falhou:', error.message)
      );
  }, 30000);

  console.log(
    '[server] Motor ativo 24/7: sinais só após rondas novas; supervisor e keep-alive em execução.'
  );
}

async function shutdown(reason, exitCode = 0) {
  if (shuttingDown) return;

  shuttingDown = true;

  console.log(`[server] A terminar (${reason})...`);

  feed.stop();

  if (engine.supervisorTimer) clearInterval(engine.supervisorTimer);
  if (engine.outcomeRecoveryTimer) clearInterval(engine.outcomeRecoveryTimer);

  setTimeout(() => process.exit(exitCode), 8000).unref();

  if (httpServer) {
    await new Promise((resolve) => {
      httpServer.close(resolve);
      setTimeout(resolve, 3000).unref();
    });
  }

  await pool.end().catch(() => {});

  process.exit(exitCode);
}

process.on('SIGTERM', () => void shutdown('SIGTERM', 0));
process.on('SIGINT', () => void shutdown('SIGINT', 0));

process.on('unhandledRejection', (error) => {
  console.error('[process] Rejeição não tratada:', error?.message || error);
});

process.on('uncaughtException', (error) => {
  console.error(
    '[process] Exceção não tratada:',
    error?.stack || error?.message || error
  );

  void shutdown('uncaughtException', 1);
});

startServer().catch((error) => {
  console.error(
    '[server] Falha fatal no arranque:',
    error?.stack || error?.message || error
  );

  process.exit(1);
});
