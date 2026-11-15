'use strict';

// BAC BO Telegram Signals — Render Web Service (v2: 24/7 auto-recuperável)
// Variáveis obrigatórias no Render:
//   BOT_TOKEN, CHAT_ID, ADMIN_TOKEN (>=24 caracteres), DATABASE_URL
// Opcionais:
//   HISTORY_TZ (Africa/Luanda, America/New_York, America/Sao_Paulo, Europe/Lisbon, UTC)
//   FEED_INTERVAL_MS (1000–10000, padrão 2000)
//   SELF_URL (URL pública; no Render usa-se RENDER_EXTERNAL_URL automaticamente)

const express = require('express');
const crypto = require('crypto');
const { Pool } = require('pg');

const app = express();
app.disable('x-powered-by');
app.use(express.json({ limit: '32kb' }));

const PORT = Number(process.env.PORT || 3000);
const FEED_URL = 'https://ioscout.io/api/live.php?game=bac-bo';
const FEED_INTERVAL_MS = Math.min(
  10000,
  Math.max(1000, Number(process.env.FEED_INTERVAL_MS) || 2000)
);
const FEED_MAX_RETRY_MS = 15000;
const REQUEST_TIMEOUT_MS = 10000;
const MAX_FEED_ROUND_AGE_MS = 5 * 60 * 1000;
const FRESH_SEEN_WINDOW_MS = 3 * 60 * 1000;

const SIGNAL_MAX_PENDING_MS = 10 * 60 * 1000; // sinal preso é cancelado
const FEED_DEAD_RESTART_MS = 10 * 60 * 1000; // sem feed → reinício automático
const SUPERVISOR_MS = 2000;
const OVERDUE_GRACE_MS = 45 * 1000;
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

// ---------------------------------------------------------------------------
// Telegram — fila ordenada com retentativas (429 / 5xx / rede)
// ---------------------------------------------------------------------------

let telegramQueue = Promise.resolve();

async function telegramCall(method, payload, label) {
  const maxAttempts = 4;

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      const response = await fetch(
        `https://api.telegram.org/bot${process.env.BOT_TOKEN}/${method}`,
        {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify(payload),
          signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS)
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
        error.retryAfterMs = Math.min(10000, retryAfter * 1000);
      }

      throw error;
    } catch (error) {
      if (error.permanent || attempt === maxAttempts) throw error;

      const wait =
        error.retryAfterMs ?? Math.min(8000, 500 * 2 ** (attempt - 1));

      console.warn(
        `[telegram] ${label} falhou (tentativa ${attempt}/${maxAttempts}): ${error.message}; nova tentativa em ${wait} ms.`
      );

      await sleep(wait);
    }
  }

  throw new Error(`${label}: falha desconhecida.`);
}

function enqueueTelegram(task) {
  const result = telegramQueue.then(task, task);
  telegramQueue = result.catch(() => {});
  return result;
}

const bot = {
  sendMessage(chatId, text, options = {}) {
    return enqueueTelegram(() =>
      telegramCall(
        'sendMessage',
        {
          chat_id: chatId,
          text,
          disable_web_page_preview: options.disable_web_page_preview ?? true
        },
        'Telegram sendMessage'
      )
    );
  },

  deleteMessage(chatId, messageId) {
    return enqueueTelegram(() =>
      telegramCall(
        'deleteMessage',
        { chat_id: chatId, message_id: messageId },
        'Telegram deleteMessage'
      )
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

// ---------------------------------------------------------------------------
// Feed ao vivo
// ---------------------------------------------------------------------------

class LiveFeed {
  constructor() {
    this.rounds = [];
    this.lastUpdate = null; // última resposta válida do feed
    this.lastRoundAt = null; // hora (do feed) da última ronda
    this.lastNewRoundSeenAt = null; // relógio do servidor: última ronda nova
    this.errors = 0;
    this.loaded = false;
    this.fetching = false;
    this.timer = null;
    this.listeners = new Set();
    this.roundQueue = Promise.resolve();
    this.nextDelay = FEED_INTERVAL_MS;
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

  // Frescura robusta: vale a hora do feed OU ter visto uma ronda nova há pouco
  // (protege contra fusos horários diferentes no campo settled_at).
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

  async fetchResults() {
    if (this.fetching || shuttingDown) return;

    this.fetching = true;

    try {
      const response = await fetch(FEED_URL, {
        headers: {
          'User-Agent': 'BACBO-Telegram-Signals/2.0',
          Accept: 'application/json'
        },
        signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
        cache: 'no-store'
      });

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

      const rounds = payload.rounds
        .filter((item) => item && typeof item === 'object')
        .map((item) => ({
          settledAt: item.settled_at,
          at: parseFeedTime(item.settled_at),
          side: normalizeSector(item.sector),
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
        }))
        .filter((item) => item.side && item.at)
        .sort((a, b) => b.at - a.at);

      if (!rounds.length) {
        throw new Error('Feed sem rodadas reconhecíveis.');
      }

      const existing = new Set(this.rounds.map((item) => item.settledAt));
      const fresh = [];

      for (const round of rounds) {
        if (!existing.has(round.settledAt)) {
          fresh.push(round);
          existing.add(round.settledAt);
        }
      }

      this.rounds = [...fresh, ...this.rounds]
        .sort((a, b) => b.at - a.at)
        .slice(0, 500);

      this.lastUpdate = new Date();
      this.lastRoundAt = this.rounds[0].at;
      this.errors = 0;
      this.nextDelay = FEED_INTERVAL_MS;

      if (!this.loaded) {
        this.loaded = true;
        console.log(
          `[feed] Ligado: ${this.rounds.length} rondas válidas; última ronda ${this.rounds[0].settledAt}.`
        );
      } else if (fresh.length) {
        this.lastNewRoundSeenAt = new Date();
      }

      for (const round of fresh.slice().sort((a, b) => a.at - b.at)) {
        this.enqueueRound(round);
      }
    } catch (error) {
      this.errors += 1;

      this.nextDelay = Math.min(
        FEED_MAX_RETRY_MS,
        Math.max(FEED_INTERVAL_MS, this.nextDelay * 2)
      );

      console.error(
        `[feed] Falha ${this.errors}: ${error.message}; nova tentativa em ${this.nextDelay} ms.`
      );
    } finally {
      this.fetching = false;
      this.schedule();
    }
  }

  schedule() {
    if (shuttingDown) return;

    if (this.timer) clearTimeout(this.timer);

    this.timer = setTimeout(() => {
      this.timer = null;

      this.fetchResults().catch((error) =>
        console.error('[feed] Erro inesperado no polling:', error.message)
      );
    }, this.nextDelay);
  }

  // Chamado pelo supervisor: se o ciclo de polling morreu, reativa-o.
  ensureRunning() {
    if (shuttingDown || this.fetching || this.timer) return;

    console.warn('[feed] Polling parado; a reativar.');
    void this.fetchResults();
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
            const delay = Math.min(
              15000,
              500 * 2 ** Math.min(attempt - 1, 5)
            );

            console.error(
              `[feed] Processamento da ronda falhou (tentativa ${attempt}/${maxAttempts}): ${error.message}`
            );

            if (attempt < maxAttempts) await sleep(delay);
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
    if (!this.fetching && !shuttingDown) void this.fetchResults();
  }

  stop() {
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
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
      ) AS greenstreak,
      (
        SELECT COUNT(*)::int
        FROM signals s
        WHERE
          s.result = 'pendente'
          AND s.attempts > 0
          AND (s.created_at AT TIME ZONE $1)::date = (NOW() AT TIME ZONE $1)::date
      ) AS ingale
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
      const daily = await dailyHistory();
      text += `\n\n${dailyLine(daily)}`;
    } catch (error) {
      console.error(
        '[db] Não foi possível montar o placar diário:',
        error.message
      );

      text += '\n\n📊 Placar diário temporariamente indisponível.';
    }
  }

  await bot.sendMessage(process.env.CHAT_ID, text);

  try {
    await markOutcomeNotified(signal.id);
  } catch (error) {
    console.error(
      `[db] Resultado #${signal.id} enviado, mas não foi possível marcar como notificado:`,
      error.message
    );
  }
}

// ---------------------------------------------------------------------------
// Estratégias
// ---------------------------------------------------------------------------

const opposite = (side) => (side === 'P' ? 'B' : 'P');
const isSide = (side) => side === 'P' || side === 'B';

const hasScore = (round) =>
  Number.isFinite(round.playerScore) && Number.isFinite(round.bankerScore);

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
// Motor de sinais
// ---------------------------------------------------------------------------

const sideLabel = (side) => (side === 'B' ? '🔴 BANCA' : '🔵 JOGADOR');

class SignalEngine {
  constructor() {
    this.running = false;
    this.intervalMin = DEFAULT_SIGNAL_INTERVAL_MIN;
    this.gales = DEFAULT_GALES;
    this.activeSignals = [];
    this.sending = false;
    this.lastSignalAt = 0;
    this.blockedUntil = 0;
    this.supervisorTimer = null;
    this.supervising = false;
    this.recoveringOutcomes = false;
    this.outcomeRecoveryTimer = null;
    this.notifying = new Set();

    // Devolve a promessa: as rondas são processadas UMA A UMA, em ordem.
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
      // Resultados com mais de 30 min já não interessam: fecha em silêncio.
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
          await sendOutcome(
            row.result,
            signal,
            !['empate', 'eg1', 'eg2'].includes(row.result)
          );

          await this.clearGaleNotice(signal);

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

  // trigger: 'round' (logo após uma ronda nova) ou 'timer' (reserva)
  async trySendSignal(trigger) {
    if (!this.running || !feed.loaded || shuttingDown) return null;
    if (this.sending) return null;
    if (this.activeSignals.length >= MAX_ACTIVE_SIGNALS) return null;
    if (Date.now() < this.blockedUntil) return null;

    const elapsed = Date.now() - this.lastSignalAt;
    const intervalMs = this.intervalMin * 60 * 1000;
    const needed = trigger === 'round' ? intervalMs : intervalMs + OVERDUE_GRACE_MS;

    if (elapsed < needed) return null;

    if (!feed.isRoundFresh()) {
      console.warn('[sinais] Sinal adiado: feed sem rondas recentes.');
      return null;
    }

    this.sending = true;

    try {
      return await this.sendSignal();
    } catch (error) {
      this.blockedUntil = Date.now() + 10000;

      console.error('[sinais] Erro ao enviar sinal:', error.message);
      return null;
    } finally {
      this.sending = false;
    }
  }

  async sendSignal() {
    const latest = feed.latest();

    if (!latest) return null;

    const analysis = analyzeResults(feed.getRounds(50));
    const baselineAt = latest.at;

    const saved = await insertSignal({
      ...analysis,
      gales: this.gales,
      baselineAt
    });

    const signal = {
      id: String(saved.id),
      entry: analysis.entry,
      strategy: analysis.strategy,
      detail: analysis.detail,
      gales: this.gales,
      attempts: 0,
      createdAt: new Date(saved.created_at),
      baselineAt,
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

    try {
      await bot.sendMessage(process.env.CHAT_ID, message);
    } catch (error) {
      try {
        await updateSignal(signal.id, { result: 'cancelado' });
      } catch (databaseError) {
        console.error(
          `[db] Falha ao cancelar sinal #${signal.id}:`,
          databaseError.message
        );
      }

      this.activeSignals = this.activeSignals.filter(
        (item) => item.id !== signal.id
      );

      throw error;
    }

    this.lastSignalAt = Date.now();

    console.log(`[sinais] Enviado #${signal.id}: ${signal.entry}/${signal.strategy}.`);

    return signal;
  }

  async start() {
    if (this.running) {
      return { started: false, reason: 'O motor já está ativo.' };
    }

    await setSetting('running', 'true');
    this.running = true;

    void this.trySendSignal('round');

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

  // Supervisor: autorrecuperação (feed, sinais presos, envio de reserva).
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

      await this.cancelStale();
      await this.trySendSignal('timer');
    } finally {
      this.supervising = false;
    }
  }

  async cancelStale() {
    for (const signal of [...this.activeSignals]) {
      if (Date.now() - signal.createdAt.getTime() <= SIGNAL_MAX_PENDING_MS) {
        continue;
      }

      await this.cancelSignal(signal, true);
    }
  }

  async cancelSignal(signal, notify) {
    await updateSignal(signal.id, { result: 'cancelado' });

    this.activeSignals = this.activeSignals.filter(
      (item) => item.id !== signal.id
    );

    console.warn(`[sinais] Sinal #${signal.id} cancelado (sem resultado a tempo).`);

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

    await this.clearGaleNotice(signal);
  }

  async finishSignal(signal, status, attempts = signal.attempts) {
    await updateSignal(signal.id, { attempts, result: status });

    signal.attempts = attempts;

    this.activeSignals = this.activeSignals.filter(
      (item) => item.id !== signal.id
    );

    this.notifying.add(signal.id);

    try {
      await sendOutcome(
        status,
        signal,
        !['empate', 'eg1', 'eg2'].includes(status)
      );
    } catch (error) {
      console.error(
        `[telegram] Falha ao enviar resultado #${signal.id} (será recuperado):`,
        error.message
      );
    } finally {
      this.notifying.delete(signal.id);
    }

    await this.clearGaleNotice(signal);
  }

  async handleRound(round) {
    for (const signal of [...this.activeSignals]) {
      await this.settle(signal, round);
    }

    // Só a ronda mais recente dispara novo sinal (evita rajadas de reprocesso).
    const latest = feed.latest();

    if (latest && latest.settledAt === round.settledAt) {
      await this.trySendSignal('round');
    }
  }

  async settle(signal, round) {
    // Só conta rondas posteriores à ronda-base do sinal.
    if (round.at.getTime() <= signal.baselineAt.getTime()) return;
    if (signal.lastRoundKey === round.settledAt) return;

    if (round.side === 'T') {
      const status =
        signal.attempts === 0 ? 'empate' : signal.attempts === 1 ? 'eg1' : 'eg2';

      await this.finishSignal(signal, status);
      console.log(`[sinais] #${signal.id}: ${status.toUpperCase()} (proteção no empate).`);
      return;
    }

    if (round.side === signal.entry) {
      const status =
        signal.attempts === 0 ? 'green' : signal.attempts === 1 ? 'g1' : 'g2';

      await this.finishSignal(signal, status);
      console.log(`[sinais] #${signal.id}: ${status.toUpperCase()}.`);
      return;
    }

    const nextAttempt = signal.attempts + 1;

    if (nextAttempt > signal.gales) {
      await this.finishSignal(signal, 'red', nextAttempt);
      console.log(`[sinais] #${signal.id}: RED.`);
      return;
    }

    await updateSignal(signal.id, { attempts: nextAttempt });

    signal.attempts = nextAttempt;
    signal.lastRoundKey = round.settledAt;

    const galeText = [
      `🔄⚠️ GALE ${nextAttempt} — VAMOS À PRÓXIMA!`,
      `🎯 Manter: ${sideLabel(signal.entry)}`,
      '🛡️ Empate continua protegido',
      `🔖 Sinal #${signal.id}`
    ].join('\n');

    try {
      const sent = await bot.sendMessage(process.env.CHAT_ID, galeText);

      signal.galeMessageId = sent.message_id;

      try {
        await saveGaleMessageId(signal.id, sent.message_id);
      } catch (error) {
        console.error(
          `[db] Falha ao guardar ID do aviso de gale #${signal.id}:`,
          error.message
        );
      }
    } catch (error) {
      console.error('[telegram] Falha ao enviar aviso de gale:', error.message);
    }
  }

  async clearGaleNotice(signal) {
    if (!signal.galeMessageId) return;

    try {
      await bot.deleteMessage(process.env.CHAT_ID, signal.galeMessageId);

      signal.galeMessageId = null;

      await saveGaleMessageId(signal.id, null);

      console.log(`[telegram] Aviso de gale removido para o sinal #${signal.id}.`);
    } catch (error) {
      console.error(
        `[telegram] Não foi possível remover aviso de gale #${signal.id}:`,
        error.message
      );

      // Mensagem já apagada / inexistente: não insistir.
      signal.galeMessageId = null;

      try {
        await saveGaleMessageId(signal.id, null);
      } catch {
        // ignorado
      }
    }
  }
}

const engine = new SignalEngine();

// ---------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------

// Endpoint leve para keep-alive (UptimeRobot / cron-job.org / auto-ping).
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
      statistics: stats,
      historyTimezone: HISTORY_TZ,
      feed: {
        loaded: feed.loaded,
        lastUpdate: feed.lastUpdate,
        lastRoundAt: feed.lastRoundAt,
        lastNewRoundSeenAt: feed.lastNewRoundSeenAt,
        roundFresh: feed.isRoundFresh(),
        errors: feed.errors,
        recentRounds: feed.rounds.slice(0, 10)
      }
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

// Liberta o motor se algum sinal ficar preso.
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

  feed.start();
  engine.startSupervisor();
  startKeepAlive();

  // Arranque automático SEMPRE (24/7).
  engine.running = true;
  await setSetting('running', 'true');

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

  console.log('[server] Motor ativo 24/7; supervisor e keep-alive em execução.');
}

async function shutdown(reason, exitCode = 0) {
  if (shuttingDown) return;

  shuttingDown = true;

  console.log(`[server] A terminar (${reason})...`);

  feed.stop();

  if (engine.supervisorTimer) clearInterval(engine.supervisorTimer);
  if (engine.outcomeRecoveryTimer) clearInterval(engine.outcomeRecoveryTimer);

  // Rede de segurança: nunca ficar pendurado.
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
