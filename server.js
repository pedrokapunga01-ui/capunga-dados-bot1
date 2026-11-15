'use strict';

// BAC BO Telegram Signals — Render Web Service
// Variáveis obrigatórias no Render:
// BOT_TOKEN, CHAT_ID, ADMIN_TOKEN, DATABASE_URL
// O Render fornece PORT automaticamente.

const express = require('express');
const crypto = require('crypto');
const { Pool } = require('pg');

const app = express();
app.disable('x-powered-by');
app.use(express.json({ limit: '32kb' }));

const PORT = Number(process.env.PORT || 3000);
const FEED_URL = 'https://ioscout.io/api/live.php?game=bac-bo';
const FEED_INTERVAL_MS = 5000;
const FEED_MAX_RETRY_MS = 30000;
const REQUEST_TIMEOUT_MS = 10000;
const MAX_FEED_ROUND_AGE_MS = 5 * 60 * 1000;
const DEFAULT_SIGNAL_INTERVAL_MIN = 3;
const DEFAULT_GALES = 2;
const MAX_ACTIVE_SIGNALS = 1;
const NAMES = { P: 'PLAYER 🔵', B: 'BANCO 🔴' };

const ALLOWED_TIMEZONES = new Set([
  'Africa/Luanda',
  'America/New_York',
  'America/Sao_Paulo',
  'Europe/Lisbon',
  'UTC'
]);

const REQUIRED_ENV = ['BOT_TOKEN', 'CHAT_ID', 'ADMIN_TOKEN', 'DATABASE_URL'];

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
  keepAlive: true
});

pool.on('error', (error) => {
  console.error('[db] Erro numa ligação inativa:', error.message);
});

let telegramQueue = Promise.resolve();

const bot = {
  async sendMessage(chatId, text, options = {}) {
    const run = async () => {
      const response = await fetch(
        `https://api.telegram.org/bot${process.env.BOT_TOKEN}/sendMessage`,
        {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({
            chat_id: chatId,
            text,
            disable_web_page_preview:
              options.disable_web_page_preview ?? true
          }),
          signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS)
        }
      );

      const payload = await response.json().catch(() => ({}));

      if (!response.ok || !payload.ok) {
        throw new Error(
          `Telegram API: ${payload.description || `HTTP ${response.status}`}`
        );
      }

      return payload.result;
    };

    const result = telegramQueue.then(run, run);
    telegramQueue = result.catch(() => {});
    return result;
  },

  async deleteMessage(chatId, messageId) {
    const run = async () => {
      const response = await fetch(
        `https://api.telegram.org/bot${process.env.BOT_TOKEN}/deleteMessage`,
        {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({
            chat_id: chatId,
            message_id: messageId
          }),
          signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS)
        }
      );

      const payload = await response.json().catch(() => ({}));

      if (!response.ok || !payload.ok) {
        throw new Error(
          `Telegram deleteMessage: ${
            payload.description || `HTTP ${response.status}`
          }`
        );
      }

      return payload.result;
    };

    const result = telegramQueue.then(run, run);
    telegramQueue = result.catch(() => {});
    return result;
  }
};

let httpServer;
let shuttingDown = false;

function asyncRoute(handler) {
  return (req, res, next) =>
    Promise.resolve(handler(req, res, next)).catch(next);
}

function requireAdmin(req, res, next) {
  const supplied = String(req.get('x-admin-token') || '');
  const expected = String(process.env.ADMIN_TOKEN || '');

  const suppliedBuffer = Buffer.from(supplied);
  const expectedBuffer = Buffer.from(expected);

  if (
    suppliedBuffer.length !== expectedBuffer.length ||
    !crypto.timingSafeEqual(suppliedBuffer, expectedBuffer)
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

class LiveFeed {
  constructor() {
    this.rounds = [];
    this.lastUpdate = null;
    this.lastRoundAt = null;
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

  getResults(limit = 50) {
    return this.rounds.slice(0, limit).map((round) => round.side);
  }

  getRounds(limit = 50) {
    return this.rounds.slice(0, limit);
  }

  isRoundFresh() {
    if (!this.lastRoundAt) return false;

    const age = Date.now() - this.lastRoundAt.getTime();
    return age >= 0 && age <= MAX_FEED_ROUND_AGE_MS;
  }

  async fetchResults() {
    if (this.fetching || shuttingDown) return;

    this.fetching = true;

    try {
      const response = await fetch(FEED_URL, {
        headers: {
          'User-Agent': 'BACBO-Telegram-Signals/1.0',
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

      this.rounds = [...fresh, ...this.rounds].slice(0, 500);
      this.lastUpdate = new Date();
      this.lastRoundAt = rounds[0].at;
      this.errors = 0;
      this.nextDelay = FEED_INTERVAL_MS;

      if (!this.loaded) {
        this.loaded = true;
        console.log(
          `[feed] Ligado: ${this.rounds.length} rondas válidas; última ronda ${rounds[0].settledAt}.`
        );
      }

      for (const round of fresh.slice().reverse()) {
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

      if (!shuttingDown) {
        this.timer = setTimeout(() => {
          this.fetchResults().catch((error) =>
            console.error('[feed] Erro inesperado no polling:', error.message)
          );
        }, this.nextDelay);

        this.timer.unref();
      }
    }
  }

  enqueueRound(round) {
    this.roundQueue = this.roundQueue
      .then(async () => {
        let attempt = 0;

        while (!shuttingDown) {
          try {
            for (const listener of this.listeners) {
              await listener(round);
            }

            return;
          } catch (error) {
            attempt += 1;

            const delay = Math.min(
              30000,
              500 * 2 ** Math.min(attempt - 1, 6)
            );

            console.error(
              `[feed] Processamento da ronda falhou (tentativa ${attempt}); nova tentativa em ${delay} ms: ${error.message}`
            );

            await new Promise((resolve) => setTimeout(resolve, delay));
          }
        }
      })
      .catch((error) => {
        console.error('[feed] Falha ao processar ronda:', error.message);
      });
  }

  start() {
    if (!this.timer && !this.fetching && !shuttingDown) {
      void this.fetchResults();
    }
  }

  stop() {
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
  }
}

const feed = new LiveFeed();

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
      result TEXT NOT NULL DEFAULT 'pendente'
        CHECK (
          result IN (
            'pendente',
            'green',
            'g1',
            'g2',
            'empate',
            'eg1',
            'eg2',
            'red',
            'cancelado'
          )
        ),
      result_notification_pending BOOLEAN NOT NULL DEFAULT false,
      gale_message_id BIGINT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      settled_at TIMESTAMPTZ
    );

    CREATE INDEX IF NOT EXISTS signals_created_at_idx
      ON signals (created_at DESC);

    CREATE INDEX IF NOT EXISTS signals_result_idx
      ON signals (result);

    CREATE INDEX IF NOT EXISTS signals_pending_idx
      ON signals (result)
      WHERE result = 'pendente';

    CREATE INDEX IF NOT EXISTS signals_notification_pending_idx
      ON signals (result_notification_pending)
      WHERE result_notification_pending = true;
  `);

  console.log('[db] PostgreSQL ligado e esquema verificado.');
}

async function getSetting(key, fallback) {
  const result = await pool.query(
    'SELECT value FROM settings WHERE key = $1',
    [key]
  );

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
    `INSERT INTO signals (entry, strategy, detail, gales)
     VALUES ($1, $2, $3, $4)
     RETURNING id, created_at`,
    [signal.entry, signal.strategy, signal.detail, signal.gales]
  );

  return result.rows[0];
}

async function updateSignal(id, { attempts = null, result = null } = {}) {
  await pool.query(
    `UPDATE signals
     SET attempts = COALESCE($1, attempts),
         result = COALESCE($2, result),
         settled_at = CASE
           WHEN $2 IS NULL THEN settled_at
           ELSE NOW()
         END,
         result_notification_pending = CASE
           WHEN $2 IN ('green', 'g1', 'g2', 'empate', 'eg1', 'eg2', 'red')
           THEN true
           ELSE result_notification_pending
         END
     WHERE id = $3`,
    [attempts, result, id]
  );
}

async function markOutcomeNotified(id) {
  await pool.query(
    'UPDATE signals SET result_notification_pending = false WHERE id = $1',
    [id]
  );
}

async function saveGaleMessageId(id, messageId) {
  await pool.query(
    'UPDATE signals SET gale_message_id = $1 WHERE id = $2',
    [messageId, id]
  );
}

async function statistics() {
  const { rows } = await pool.query(`
    SELECT
      COUNT(*)::int AS total,
      COUNT(*) FILTER (WHERE result IN ('green', 'g1', 'g2'))::int AS green,
      COUNT(*) FILTER (WHERE result = 'green')::int AS direct,
      COUNT(*) FILTER (WHERE result IN ('g1', 'g2'))::int AS gale,
      COUNT(*) FILTER (
        WHERE result IN ('empate', 'eg1', 'eg2')
      )::int AS tie,
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
        ROW_NUMBER() OVER (
          ORDER BY settled_at DESC, id DESC
        ) AS rn
      FROM signals
      WHERE
        result IN ('green', 'g1', 'g2', 'empate', 'eg1', 'eg2', 'red')
        AND settled_at IS NOT NULL
        AND (settled_at AT TIME ZONE $1)::date =
            (NOW() AT TIME ZONE $1)::date
    )
    SELECT
      (NOW() AT TIME ZONE $1)::date::text AS day,
      COUNT(*)::int AS resolved,
      COUNT(*) FILTER (WHERE result = 'green')::int AS direct,
      COUNT(*) FILTER (WHERE result = 'g1')::int AS g1,
      COUNT(*) FILTER (WHERE result = 'g2')::int AS g2,
      COUNT(*) FILTER (
        WHERE result IN ('green', 'g1', 'g2')
      )::int AS greens,
      COUNT(*) FILTER (WHERE result = 'empate')::int AS tiedirect,
      COUNT(*) FILTER (WHERE result = 'eg1')::int AS eg1,
      COUNT(*) FILTER (WHERE result = 'eg2')::int AS eg2,
      COUNT(*) FILTER (
        WHERE result IN ('empate', 'eg1', 'eg2')
      )::int AS ties,
      COUNT(*) FILTER (WHERE result = 'red')::int AS losses,
      COALESCE(
        (
          SELECT COUNT(*)::int
          FROM today t
          WHERE t.rn < COALESCE(
            (
              SELECT MIN(rn)
              FROM today
              WHERE result NOT IN ('green', 'g1', 'g2')
            ),
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
          AND (s.created_at AT TIME ZONE $1)::date =
              (NOW() AT TIME ZONE $1)::date
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

  return `${labels[status] || '📣 RESULTADO DA ENTRADA'}\n🎲 Sinal #${
    signal.id
  }`;
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

const opposite = (side) => (side === 'P' ? 'B' : 'P');
const isSide = (side) => side === 'P' || side === 'B';
const hasScore = (round) =>
  Number.isFinite(round.playerScore) && Number.isFinite(round.bankerScore);

const scoreFor = (round) =>
  round.side === 'P' ? round.playerScore : round.bankerScore;

const margin = (round) =>
  Math.abs(round.playerScore - round.bankerScore);

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

  while (
    n < sequence.length &&
    sequence[n].side === sequence[0].side
  ) {
    n += 1;
  }

  return n;
}

function alternatingLength(sequence) {
  if (sequence.length < 2) return sequence.length;

  let n = 1;

  while (
    n < sequence.length &&
    sequence[n].side !== sequence[n - 1].side
  ) {
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
    label:
      chop >= 0.65
        ? 'errático'
        : chop <= 0.35
          ? 'tendência'
          : 'neutro'
  };
}

function runStrategies(rows) {
  const seq = rows.filter((round) => isSide(round.side));

  if (seq.length < 2) return [];

  const out = [];

  const add = (side, key, name, strength, type = 'seq') => {
    if (isSide(side)) {
      out.push({ side, key, name, strength, type });
    }
  };

  const n = currentStreak(seq);

  if (n >= 3 && n <= 7) {
    const strengths = [0, 0, 0, 72, 78, 84, 86, 86];
    add(
      seq[0].side,
      `SEQ_REP_${n}`,
      `Sequência x${n}`,
      strengths[n],
      'seq'
    );
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
    seq
      .slice(0, 4)
      .every((round) => hasScore(round) && margin(round) <= 2)
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
    seq
      .slice(0, n)
      .every((round) => hasScore(round) && margin(round) >= 3)
  ) {
    add(
      seq[0].side,
      'SEQ_STRONG_STREAK',
      `Dragon ${n}`,
      82,
      'seq'
    );
  }

  if (alt >= 4 && alt <= 10) {
    add(
      opposite(seq[0].side),
      'SEQ_PINGPONG',
      `Ping-pong ${alt}`,
      79,
      'seq'
    );
  }

  if (
    alt >= 3 &&
    seq
      .slice(0, alt)
      .every((round) => hasScore(round) && margin(round) >= 3)
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

    if (bankers >= 12) {
      add('B', 'BANKER_BIAS_20', 'Viés Banca', 80, 'seq');
    }

    if (players >= 12) {
      add('P', 'BANKER_BIAS_20', 'Viés Jogador', 80, 'seq');
    }
  }

  if (
    seq.length >= 3 &&
    seq.slice(0, 3).every(hasScore)
  ) {
    const [a, b, c] = seq.slice(0, 3);
    const sa = scoreFor(a);
    const sb = scoreFor(b);
    const sc = scoreFor(c);

    if (
      a.side === b.side &&
      b.side === c.side &&
      ((sb - sa === 1 && sc - sb === 1) ||
        (sa - sb === 1 && sb - sc === 1))
    ) {
      add(
        a.side,
        'NUM_ESCADA',
        `Escada ${sa}→${sb}→${sc}`,
        88,
        'num'
      );
    }

    if (
      a.side === b.side &&
      b.side === c.side &&
      sa <= 3 &&
      sb <= 3 &&
      sc <= 3
    ) {
      add(
        opposite(a.side),
        'NUM_THREE_LOW',
        'Virada de baixos x3',
        81,
        'num'
      );
    }

    if (
      a.side === b.side &&
      b.side === c.side &&
      margin(a) >= 4 &&
      margin(b) >= 4 &&
      margin(c) >= 4
    ) {
      add(
        a.side,
        'NUM_DIFF4x3',
        'Margem ≥4 ×3',
        85,
        'num'
      );
    }

    if (
      a.side === b.side &&
      b.side === c.side &&
      sa < sb &&
      sb < sc
    ) {
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
      add(
        a.side,
        'NUM_REPETE',
        `Score repetido ${scoreFor(a)}`,
        84,
        'num'
      );
    }

    if (scoreFor(a) <= 4 && scoreFor(b) <= 4) {
      add(
        opposite(a.side),
        'NUM_VIRADA_BAIXA',
        'Virada de baixos',
        80,
        'num'
      );
    }

    if (margin(a) >= 5 && margin(b) >= 5) {
      add(
        a.side,
        'NUM_DOUBLE_HIGH',
        'Dupla margem alta',
        84,
        'num'
      );
    }

    const ra = rangeOf(scoreFor(a));
    const rb = rangeOf(scoreFor(b));

    if (
      (ra === 'MB' && rb === 'MA') ||
      (ra === 'MA' && rb === 'MB')
    ) {
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
      add(
        opposite(seq[0].side),
        'NUM_GATILHO_0',
        'Gatilho 0',
        86,
        'num'
      );
    }

    if (score === 1) {
      add(
        opposite(seq[0].side),
        'NUM_GATILHO_1',
        'Gatilho 1',
        84,
        'num'
      );
    }

    if (score === 9) {
      add(
        opposite(seq[0].side),
        'NUM_GATILHO_9',
        'Gatilho 9',
        88,
        'num'
      );
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

  if (
    rows[0]?.side === 'T' &&
    isSide(rows[1]?.side) &&
    seq.length
  ) {
    add(
      opposite(seq[0].side),
      'TIE_SINGLE',
      'Empate isolado',
      77,
      'num'
    );
  }

  if (
    rows[0]?.side === 'T' &&
    rows[1]?.side === 'T' &&
    isSide(rows[2]?.side)
  ) {
    add(
      rows[2].side,
      'TIE_DOUBLE_BACK',
      'Retorno pós-duplo empate',
      78,
      'num'
    );
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

    if (
      /QUEBRA|SURF|PINGPONG|2x2|EXTEND|3_1|MOMENTUM|ALT/.test(
        candidate.key
      )
    ) {
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
    if (!bySide.has(candidate.side)) {
      bySide.set(candidate.side, []);
    }

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

  const supporting = winner.votes.filter(
    (vote) => vote.side === winner.side
  );

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

class SignalEngine {
  constructor() {
    this.running = false;
    this.intervalMin = DEFAULT_SIGNAL_INTERVAL_MIN;
    this.gales = DEFAULT_GALES;
    this.activeSignals = [];
    this.timer = null;
    this.sending = false;
    this.recoveringOutcomes = false;
    this.outcomeRecoveryTimer = null;

    feed.onRound((round) => {
      this.handleRound(round).catch((error) =>
        console.error(
          '[sinais] Erro ao processar ronda:',
          error.message
        )
      );
    });
  }

  async restorePending() {
    const { rows } = await pool.query(
      `SELECT
        id,
        entry,
        strategy,
        detail,
        gales,
        attempts,
        created_at,
        gale_message_id
      FROM signals
      WHERE result = 'pendente'
      ORDER BY created_at ASC`
    );

    this.activeSignals = rows.map((row) => ({
      ...row,
      id: String(row.id),
      createdAt: new Date(row.created_at),
      attempts: row.attempts,
      galeMessageId: row.gale_message_id
    }));

    if (rows.length) {
      console.log(
        `[sinais] ${rows.length} sinal(is) pendente(s) restaurado(s) após reinício.`
      );
    }
  }

  async recoverUnsentOutcomes() {
    if (this.recoveringOutcomes || shuttingDown) return;

    this.recoveringOutcomes = true;

    try {
      const { rows } = await pool.query(
        `SELECT
          id,
          entry,
          result,
          attempts,
          gale_message_id
        FROM signals
        WHERE result_notification_pending = true
        ORDER BY settled_at ASC`
      );

      for (const row of rows) {
        const signal = {
          id: String(row.id),
          entry: row.entry,
          attempts: row.attempts,
          galeMessageId: row.gale_message_id
        };

        try {
          await sendOutcome(
            row.result,
            signal,
            !['empate', 'eg1', 'eg2'].includes(row.result)
          );

          await this.clearGaleNotice(signal);

          console.log(
            `[sinais] Notificação de resultado #${signal.id} recuperada após reinício.`
          );
        } catch (error) {
          console.error(
            `[telegram] Resultado #${signal.id} continua pendente de notificação:`,
            error.message
          );
        }
      }
    } finally {
      this.recoveringOutcomes = false;
    }
  }

  async sendSignal() {
    if (!this.running || !feed.loaded) return null;

    if (!feed.isRoundFresh()) {
      console.warn(
        '[sinais] Sinal adiado: última ronda do feed está desatualizada.'
      );
      return null;
    }

    if (this.sending) return null;

    if (this.activeSignals.length >= MAX_ACTIVE_SIGNALS) {
      return null;
    }

    this.sending = true;

    let signal = null;

    try {
      const analysis = analyzeResults(feed.getRounds(50));
      const saved = await insertSignal({
        ...analysis,
        gales: this.gales
      });

      signal = {
        id: String(saved.id),
        ...analysis,
        gales: this.gales,
        attempts: 0,
        createdAt: new Date(saved.created_at),
        galeMessageId: null
      };

      this.activeSignals.push(signal);

      const attempts = this.gales + 1;

      const message = [
        '🎲🔥 SINAL BAC BO 🔥🎲',
        '',
        '✅ ENTRADA CONFIRMADA ✅',
        `🎯 Jogar: ${
          signal.entry === 'B' ? '🔴 BANCA' : '🔵 JOGADOR'
        }`,
        '🛡️ Proteção: 🟡 EMPATE',
        `🔄 Até ${signal.gales} gale(s) • ${attempts} tentativa(s)`,
        `🧠 Estratégia: ${signal.strategy}`,
        '',
        `🔖 Sinal #${signal.id}`
      ].join('\n');

      await bot.sendMessage(process.env.CHAT_ID, message, {
        disable_web_page_preview: true
      });

      console.log(
        `[sinais] Enviado #${signal.id}: ${signal.entry}/${signal.strategy}.`
      );

      return signal;
    } catch (error) {
      if (signal) {
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
      }

      throw error;
    } finally {
      this.sending = false;
    }
  }

  async start() {
    if (this.running) {
      return { started: false, reason: 'O motor já está ativo.' };
    }

    await setSetting('running', 'true');
    this.running = true;

    if (feed.loaded && this.activeSignals.length === 0) {
      try {
        await this.sendSignal();
      } catch (error) {
        console.error(
          '[sinais] Emissão inicial falhou; será tentada novamente:',
          error.message
        );
      }
    }

    this.scheduleNext();

    return {
      started: true,
      waitingForFeed: !feed.loaded
    };
  }

  async stop() {
    await setSetting('running', 'false');
    this.running = false;

    if (this.timer) clearTimeout(this.timer);
    this.timer = null;

    return {
      stopped: true,
      pendingSignalsKept: this.activeSignals.length
    };
  }

  scheduleNext() {
    if (!this.running || this.timer || shuttingDown) return;

    this.timer = setTimeout(async () => {
      this.timer = null;
      this.scheduleNext();

      try {
        await this.sendSignal();
      } catch (error) {
        console.error(
          '[sinais] Erro ao enviar sinal:',
          error.message
        );
      }
    }, this.intervalMin * 60 * 1000);

    this.timer.unref();
  }

  async finishSignal(signal, status) {
    await updateSignal(signal.id, {
      attempts: signal.attempts,
      result: status
    });

    this.activeSignals = this.activeSignals.filter(
      (item) => item.id !== signal.id
    );

    try {
      await sendOutcome(
        status,
        signal,
        !['empate', 'eg1', 'eg2'].includes(status)
      );
    } catch (error) {
      console.error(
        `[telegram] Falha ao enviar resultado #${signal.id}:`,
        error.message
      );
    }

    await this.clearGaleNotice(signal);
  }

  async handleRound(round) {
    for (const signal of [...this.activeSignals]) {
      if (round.at.getTime() <= signal.createdAt.getTime()) continue;

      if (round.side === 'T') {
        const status =
          signal.attempts === 0
            ? 'empate'
            : signal.attempts === 1
              ? 'eg1'
              : 'eg2';

        await this.finishSignal(signal, status);

        console.log(
          `[sinais] #${signal.id}: ${status.toUpperCase()} (proteção no empate).`
        );

        continue;
      }

      if (round.side === signal.entry) {
        const status =
          signal.attempts === 0
            ? 'green'
            : signal.attempts === 1
              ? 'g1'
              : 'g2';

        await this.finishSignal(signal, status);

        console.log(
          `[sinais] #${signal.id}: ${status.toUpperCase()}.`
        );

        continue;
      }

      const nextAttempt = signal.attempts + 1;

      if (nextAttempt <= signal.gales) {
        await updateSignal(signal.id, { attempts: nextAttempt });
        signal.attempts = nextAttempt;

        const galeText = [
          '🔄⚠️ GALE ' + nextAttempt + ' — VAMOS À PRÓXIMA!',
          `🎯 Manter: ${
            signal.entry === 'B' ? '🔴 BANCA' : '🔵 JOGADOR'
          }`,
          '🛡️ Empate continua protegido',
          `🔖 Sinal #${signal.id}`
        ].join('\n');

        try {
          const sent = await bot.sendMessage(
            process.env.CHAT_ID,
            galeText
          );

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
          console.error(
            '[telegram] Falha ao enviar aviso de gale:',
            error.message
          );
        }

        continue;
      }

      await updateSignal(signal.id, {
        attempts: nextAttempt,
        result: 'red'
      });

      signal.attempts = nextAttempt;

      await this.finishSignal(signal, 'red');

      console.log(`[sinais] #${signal.id}: RED.`);
    }
  }

  async clearGaleNotice(signal) {
    if (!signal.galeMessageId) return;

    try {
      await bot.deleteMessage(
        process.env.CHAT_ID,
        signal.galeMessageId
      );

      signal.galeMessageId = null;

      await saveGaleMessageId(signal.id, null);

      console.log(
        `[telegram] Aviso de gale removido para o sinal #${signal.id}.`
      );
    } catch (error) {
      console.error(
        `[telegram] Não foi possível remover aviso de gale #${signal.id}:`,
        error.message
      );
    }
  }
}

const engine = new SignalEngine();

app.get('/', (_req, res) => {
  res.json({
    name: 'BAC BO Telegram Signal Service',
    status: engine.running ? 'running' : 'stopped',
    feed: feed.loaded ? 'connected' : 'waiting'
  });
});

app.get('/health', (_req, res) => {
  const responseFresh =
    feed.lastUpdate &&
    Date.now() - feed.lastUpdate.getTime() < 60000;

  const roundFresh = feed.isRoundFresh();
  const healthy = Boolean(
    responseFresh && roundFresh && !shuttingDown
  );

  res.status(healthy ? 200 : 503).json({
    ok: healthy,
    feed: feed.loaded,
    feedFresh: Boolean(responseFresh),
    roundFresh,
    lastUpdate: feed.lastUpdate,
    lastRoundAt: feed.lastRoundAt,
    feedErrors: feed.errors,
    running: engine.running
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

app.post(
  '/admin/interval',
  requireAdmin,
  asyncRoute(async (req, res) => {
    const value = Number(req.body?.minutes);

    if (
      !Number.isInteger(value) ||
      value < 1 ||
      value > 3
    ) {
      return res.status(400).json({
        error:
          'minutes deve ser um inteiro entre 1 e 3; máximo permitido: 3 minutos.'
      });
    }

    engine.intervalMin = value;
    await setSetting('interval_min', value);

    if (engine.timer) clearTimeout(engine.timer);
    engine.timer = null;
    engine.scheduleNext();

    res.json({ ok: true, intervalMin: value });
  })
);

app.post(
  '/admin/gales',
  requireAdmin,
  asyncRoute(async (req, res) => {
    const value = Number(req.body?.gales);

    if (
      !Number.isInteger(value) ||
      value < 0 ||
      value > 3
    ) {
      return res.status(400).json({
        error: 'gales deve ser um inteiro entre 0 e 3.'
      });
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
      `SELECT
        id,
        entry,
        strategy,
        detail,
        gales,
        attempts,
        result,
        created_at,
        settled_at
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

  feed.start();

  setImmediate(() => {
    void engine
      .recoverUnsentOutcomes()
      .catch((error) =>
        console.error(
          '[db] Recuperação inicial de resultados falhou:',
          error.message
        )
      );
  });

  engine.outcomeRecoveryTimer = setInterval(() => {
    void engine
      .recoverUnsentOutcomes()
      .catch((error) =>
        console.error(
          '[db] Recuperação periódica de resultados falhou:',
          error.message
        )
      );
  }, 30000);

  engine.outcomeRecoveryTimer.unref();

  const waitUntilReady = setInterval(async () => {
    if (shuttingDown || !feed.loaded) return;

    clearInterval(waitUntilReady);

    if (!engine.running) {
      try {
        await engine.start();
      } catch (error) {
        console.error(
          '[server] Arranque automático falhou:',
          error.message
        );
      }

      return;
    }

    if (engine.activeSignals.length === 0) {
      try {
        await engine.sendSignal();
      } catch (error) {
        console.error(
          '[server] Primeiro sinal falhou; nova tentativa agendada:',
          error.message
        );
      }
    }
  }, 500);

  waitUntilReady.unref();

  console.log(
    '[server] Arranque automático ativado; à espera do feed válido.'
  );
}

async function shutdown(signal, exitCode = 0) {
  if (shuttingDown) return;

  shuttingDown = true;

  console.log(`[server] A terminar (${signal})...`);

  feed.stop();

  if (engine.timer) clearTimeout(engine.timer);
  if (engine.outcomeRecoveryTimer) {
    clearInterval(engine.outcomeRecoveryTimer);
  }

  if (httpServer) {
    await new Promise((resolve) => {
      httpServer.close(resolve);
      setTimeout(resolve, 5000).unref();
    });
  }

  await pool.end().catch(() => {});

  process.exit(exitCode);
}

process.on('SIGTERM', () => void shutdown('SIGTERM', 0));
process.on('SIGINT', () => void shutdown('SIGINT', 0));

process.on('unhandledRejection', (error) => {
  console.error(
    '[process] Rejeição não tratada:',
    error?.message || error
  );
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