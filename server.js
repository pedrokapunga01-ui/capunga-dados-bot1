// ============================================================
// BOT DE SINAIS BAC BO — TELEGRAM
// Arquivo único: server.js
// Deploy: Render Web Service
//
// Variáveis obrigatórias:
// BOT_TOKEN, CHAT_ID, ADMIN_TOKEN
//
// Variáveis opcionais:
// DATABASE_URL, FEED_URL, PORT
// ============================================================

const express = require('express');
const TelegramBot = require('node-telegram-bot-api');
const { Pool } = require('pg');

const app = express();
app.use(express.json());

const DEFAULT_FEED_URL = 'https://ioscout.io/api/live.php?game=bac-bo';
const FEED_INTERVAL_MS = 30000;

const NAMES = {
  P: 'PLAYER 🔵',
  B: 'BANCO 🔴'
};

// ------------------------------------------------------------
// Banco de dados
// ------------------------------------------------------------

const useDatabase = Boolean(process.env.DATABASE_URL);

const pool = useDatabase
  ? new Pool({
      connectionString: process.env.DATABASE_URL,
      ssl: { rejectUnauthorized: false },
      max: 5
    })
  : null;

const memoryDatabase = {
  settings: new Map(),
  signals: [],
  sequence: 0
};

async function initializeDatabase() {
  if (!useDatabase) {
    console.log('[db] DATABASE_URL não definida. Usando memória.');
    return;
  }

  await pool.query(`
    CREATE TABLE IF NOT EXISTS settings (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS signals (
      id SERIAL PRIMARY KEY,
      entry TEXT NOT NULL,
      strategy TEXT NOT NULL DEFAULT '',
      gales INTEGER NOT NULL DEFAULT 2,
      attempts INTEGER NOT NULL DEFAULT 0,
      result TEXT NOT NULL DEFAULT 'pendente',
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  console.log('[db] Banco de dados inicializado.');
}

async function getSetting(key, fallback = null) {
  if (!useDatabase) {
    return memoryDatabase.settings.has(key)
      ? memoryDatabase.settings.get(key)
      : fallback;
  }

  const result = await pool.query(
    'SELECT value FROM settings WHERE key = $1',
    [key]
  );

  return result.rowCount > 0 ? result.rows[0].value : fallback;
}

async function setSetting(key, value) {
  if (!useDatabase) {
    memoryDatabase.settings.set(key, String(value));
    return;
  }

  await pool.query(
    `
    INSERT INTO settings (key, value)
    VALUES ($1, $2)
    ON CONFLICT (key)
    DO UPDATE SET value = EXCLUDED.value
    `,
    [key, String(value)]
  );
}

async function createSignal(entry, strategy, gales) {
  if (!useDatabase) {
    memoryDatabase.sequence += 1;

    const signal = {
      id: memoryDatabase.sequence,
      entry,
      strategy,
      gales,
      attempts: 0,
      result: 'pendente',
      created_at: new Date().toISOString()
    };

    memoryDatabase.signals.push(signal);
    return signal.id;
  }

  const result = await pool.query(
    `
    INSERT INTO signals (entry, strategy, gales)
    VALUES ($1, $2, $3)
    RETURNING id
    `,
    [entry, strategy, gales]
  );

  return result.rows[0].id;
}

async function updateSignal(id, data = {}) {
  const attempts = data.attempts ?? null;
  const result = data.result ?? null;

  if (!useDatabase) {
    const signal = memoryDatabase.signals.find(
      (item) => item.id === id
    );

    if (signal) {
      if (attempts !== null) signal.attempts = attempts;
      if (result !== null) signal.result = result;
    }

    return;
  }

  await pool.query(
    `
    UPDATE signals
    SET attempts = COALESCE($1, attempts),
        result = COALESCE($2, result)
    WHERE id = $3
    `,
    [attempts, result, id]
  );
}

async function listSignals(limit = 50) {
  if (!useDatabase) {
    return memoryDatabase.signals.slice(-limit).reverse();
  }

  const result = await pool.query(
    `
    SELECT id, entry, strategy, gales, attempts, result, created_at
    FROM signals
    ORDER BY id DESC
    LIMIT $1
    `,
    [limit]
  );

  return result.rows;
}

async function getStatistics() {
  const signals = await listSignals(1000);
  const total = signals.length;

  const green = signals.filter(
    (signal) => signal.result === 'green'
  ).length;

  const red = signals.filter(
    (signal) => signal.result === 'red'
  ).length;

  const pending = total - green - red;

  const assertividade =
    green + red > 0
      ? ((green / (green + red)) * 100).toFixed(1)
      : '—';

  return {
    total,
    green,
    red,
    pending,
    assertividade
  };
}

// ------------------------------------------------------------
// Telegram
// ------------------------------------------------------------

let telegramBot = null;

function getTelegramBot() {
  if (!telegramBot) {
    telegramBot = new TelegramBot(process.env.BOT_TOKEN, {
      polling: true
    });
  }

  return telegramBot;
}

async function sendTelegramMessage(message) {
  const chatId = process.env.CHAT_ID;

  if (!process.env.BOT_TOKEN || !chatId) {
    console.warn(
      '[telegram] BOT_TOKEN ou CHAT_ID ausentes. Mensagem não enviada.'
    );

    console.log('[telegram] Preview:\n' + message);
    return;
  }

  await getTelegramBot().sendMessage(chatId, message);
}

// ------------------------------------------------------------
// Estratégias
// ------------------------------------------------------------

function getOppositeSide(side) {
  return side === 'P' ? 'B' : 'P';
}

function analyzeResults(results) {
  const sequence = results
    .filter((result) => result !== 'T')
    .slice(0, 15);

  if (sequence.length < 5) {
    return {
      entry: sequence[0] === 'P' ? 'P' : 'B',
      strategy: 'Início de sessão',
      detail: 'Histórico insuficiente. Seguindo o último resultado.'
    };
  }

  let streak = 1;

  while (
    streak < sequence.length &&
    sequence[streak] === sequence[0]
  ) {
    streak += 1;
  }

  if (streak >= 3) {
    return {
      entry: sequence[0],
      strategy: 'Tendência',
      detail: `${streak}x ${NAMES[sequence[0]]} seguidas. Seguindo a força.`
    };
  }

  const window = Math.min(6, sequence.length);
  let isAlternating = true;

  for (let index = 0; index < window - 1; index += 1) {
    if (sequence[index] === sequence[index + 1]) {
      isAlternating = false;
      break;
    }
  }

  if (isAlternating && window >= 4) {
    return {
      entry: getOppositeSide(sequence[0]),
      strategy: 'Alternância',
      detail: `Padrão alternado em ${window} rodadas. Entrando no próximo da sequência.`
    };
  }

  const last10 = sequence.slice(0, 10);

  const playerWins = last10.filter(
    (result) => result === 'P'
  ).length;

  const bankerWins = last10.filter(
    (result) => result === 'B'
  ).length;

  if (playerWins === bankerWins) {
    return {
      entry: sequence[0],
      strategy: 'Frequência',
      detail: `Empate ${playerWins}x${bankerWins} nas últimas 10 rodadas. Seguindo o último resultado.`
    };
  }

  const majority = playerWins > bankerWins ? 'P' : 'B';
  const majorityWins =
    majority === 'P' ? playerWins : bankerWins;

  return {
    entry: majority,
    strategy: 'Frequência',
    detail: `${NAMES[majority]} venceu ${majorityWins} das últimas 10 rodadas.`
  };
}

// ------------------------------------------------------------
// Feed da API
// ------------------------------------------------------------

function normalizeSector(sector) {
  if (sector === 'BankerWon') return 'B';
  if (sector === 'PlayerWon') return 'P';
  if (sector === 'Tie') return 'T';
  return null;
}

class LiveFeed {
  constructor(url = process.env.FEED_URL || DEFAULT_FEED_URL) {
    this.url = url;
    this.rounds = [];
    this.lastUpdate = null;
    this.errors = 0;
    this.listeners = [];
    this.timer = null;
    this.loaded = false;
  }

  onRound(callback) {
    this.listeners.push(callback);
  }

  getResults(limit = 50) {
    return this.rounds
      .slice(0, limit)
      .map((round) => round.side);
  }

  async fetchResults() {
    try {
      const response = await fetch(this.url, {
        headers: {
          'User-Agent':
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
        },
        cache: 'no-store'
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }

      const data = await response.json();
      const incomingRounds = data.rounds || [];

      const normalizedRounds = incomingRounds
        .map((round) => ({
          settled_at: round.settled_at,
          side: normalizeSector(round.sector),
          multiplier: round.multiplier,
          winners: round.winners
        }))
        .filter((round) => round.side !== null);

      if (normalizedRounds.length === 0) {
        throw new Error('A API não retornou rodadas.');
      }

      const knownRounds = new Set(
        this.rounds.map((round) => round.settled_at)
      );

      const newRounds = [];

      for (const round of normalizedRounds) {
        if (knownRounds.has(round.settled_at)) break;
        newRounds.push(round);
      }

      this.rounds = [...newRounds, ...this.rounds].slice(0, 500);
      this.lastUpdate = new Date();
      this.errors = 0;

      if (!this.loaded) {
        this.loaded = true;

        console.log(
          `[feed] Histórico carregado: ${this.rounds.length} rodadas.`
        );

        return;
      }

      const chronologicalRounds = newRounds.slice().reverse();

      for (const round of chronologicalRounds) {
        for (const callback of this.listeners) {
          try {
            await callback(round);
          } catch (error) {
            console.error(
              '[feed] Erro ao processar rodada:',
              error.message
            );
          }
        }
      }
    } catch (error) {
      this.errors += 1;

      console.error(
        `[feed] Erro ao buscar resultados (${this.errors}): ${error.message}`
      );
    }
  }

  start() {
    this.fetchResults();

    this.timer = setInterval(() => {
      this.fetchResults();
    }, FEED_INTERVAL_MS);

    if (this.timer.unref) this.timer.unref();

    console.log(
      `[feed] Monitorando resultados a cada ${FEED_INTERVAL_MS / 1000} segundos.`
    );
  }

  stop() {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }
}

// ------------------------------------------------------------
// Motor de sinais
// ------------------------------------------------------------

class SignalEngine {
  constructor(feed) {
    this.feed = feed;
    this.running = false;
    this.intervalMin = 5;
    this.gales = 2;
    this.timer = null;
    this.currentSignal = null;
    this.signalCount = 0;

    this.feed.onRound((round) => {
      this.handleRound(round);
    });
  }

  async sendSignal() {
    const analysis = analyzeResults(this.feed.getResults(50));

    const signalId = await createSignal(
      analysis.entry,
      analysis.strategy,
      this.gales
    );

    this.currentSignal = {
      id: signalId,
      entry: analysis.entry,
      strategy: analysis.strategy,
      detail: analysis.detail,
      gales: this.gales,
      attempts: 0
    };

    this.signalCount += 1;

    const totalAttempts = this.gales + 1;

    const message = [
      '🎯 SINAL CONFIRMADO — BAC BO',
      '',
      `🎲 Entrada: ${NAMES[analysis.entry]}`,
      `📊 Estratégia: ${analysis.strategy}`,
      `📈 Leitura: ${analysis.detail}`,
      `🛡️ Proteção: até ${this.gales} ${this.gales === 1 ? 'gale' : 'gales'} (${totalAttempts} ${totalAttempts === 1 ? 'tentativa' : 'tentativas'})`,
      '',
      '✅ Entre na próxima rodada e siga a proteção!',
      `#sinal${this.signalCount}`
    ].join('\n');

    await sendTelegramMessage(message);

    console.log(
      `[sinais] Sinal #${this.signalCount} (${signalId}): ${analysis.entry} — ${analysis.strategy}.`
    );

    return this.currentSignal;
  }

  async handleRound(round) {
    const signal = this.currentSignal;

    if (!signal) return;

    if (round.side === 'T') return;

    if (round.side === signal.entry) {
      this.currentSignal = null;

      const where =
        signal.attempts === 0
          ? 'direto'
          : `no gale ${signal.attempts}`;

      await updateSignal(signal.id, {
        attempts: signal.attempts,
        result: 'green'
      });

      const statistics = await getStatistics();

      await sendTelegramMessage(
        [
          `✅ GREEN — ${NAMES[signal.entry]} venceu ${where}!`,
          '',
          `📈 Assertividade: ${statistics.assertividade}% (${statistics.green}G / ${statistics.red}R)`
        ].join('\n')
      );

      console.log(`[sinais] Sinal ${signal.id}: GREEN ${where}.`);
      return;
    }

    signal.attempts += 1;

    if (signal.attempts <= signal.gales) {
      await updateSignal(signal.id, {
        attempts: signal.attempts
      });

      await sendTelegramMessage(
        [
          `🔁 Não deu! Vá para o GALE ${signal.attempts}.`,
          `🎲 Repita a entrada: ${NAMES[signal.entry]}`
        ].join('\n')
      );

      return;
    }

    this.currentSignal = null;

    await updateSignal(signal.id, {
      attempts: signal.attempts,
      result: 'red'
    });

    const statistics = await getStatistics();

    await sendTelegramMessage(
      [
        '❌ RED — proteção encerrada nesta entrada.',
        '',
        `📈 Assertividade: ${statistics.assertividade}% (${statistics.green}G / ${statistics.red}R)`,
        'Aguarde o próximo sinal. 🍀'
      ].join('\n')
    );

    console.log(`[sinais] Sinal ${signal.id}: RED.`);
  }

  async start() {
    if (this.running) {
      return {
        started: false,
        reason: 'O bot já está rodando.'
      };
    }

    this.running = true;
    await setSetting('running', 'true');

    try {
      await this.sendSignal();
    } catch (error) {
      console.error(
        '[sinais] Erro ao enviar o primeiro sinal:',
        error.message
      );
    }

    this.scheduleNextSignal();

    return { started: true };
  }

  async stop() {
    if (!this.running) {
      return {
        stopped: false,
        reason: 'O bot já está parado.'
      };
    }

    this.running = false;

    if (this.timer) clearTimeout(this.timer);

    this.timer = null;
    await setSetting('running', 'false');

    return { stopped: true };
  }

  scheduleNextSignal() {
    if (!this.running) return;

    this.timer = setTimeout(async () => {
      try {
        await this.sendSignal();
      } catch (error) {
        console.error(
          '[sinais] Erro ao enviar sinal:',
          error.message
        );
      }

      this.scheduleNextSignal();
    }, this.intervalMin * 60 * 1000);

    if (this.timer.unref) this.timer.unref();

    console.log(
      `[sinais] Próximo sinal agendado em ${this.intervalMin} minuto(s).`
    );
  }
}

// ------------------------------------------------------------
// Instâncias
// ------------------------------------------------------------

const feed = new LiveFeed();
const engine = new SignalEngine(feed);

// ------------------------------------------------------------
// Autenticação
// ------------------------------------------------------------

function requireAdmin(request, response, next) {
  const token =
    request.get('x-admin-token') ||
    request.query.admin_token;

  if (!process.env.ADMIN_TOKEN) {
    return response.status(500).json({
      error: 'ADMIN_TOKEN não está configurado no servidor.'
    });
  }

  if (token !== process.env.ADMIN_TOKEN) {
    return response.status(401).json({
      error: 'Token de administrador inválido.'
    });
  }

  next();
}

// ------------------------------------------------------------
// Rotas
// ------------------------------------------------------------

app.get('/', (request, response) => {
  response.json({
    name: 'Bot de Sinais BAC',
    status: engine.running ? 'rodando' : 'parado',
    intervalo: `${engine.intervalMin} min`,
    feed: feed.lastUpdate ? 'ao vivo' : 'carregando'
  });
});

app.get('/health', (request, response) => {
  response.json({
    ok: true,
    feed: Boolean(feed.lastUpdate)
  });
});

app.get(
  '/admin/status',
  requireAdmin,
  async (request, response) => {
    response.json({
      running: engine.running,
      intervalMin: engine.intervalMin,
      gales: engine.gales,
      signalCount: engine.signalCount,
      currentSignal: engine.currentSignal,
      statistics: await getStatistics(),
      feed: {
        lastUpdate: feed.lastUpdate,
        errors: feed.errors,
        rounds: feed.rounds.slice(0, 24)
      }
    });
  }
);

app.post(
  '/admin/start',
  requireAdmin,
  async (request, response) => {
    response.json(await engine.start());
  }
);

app.post(
  '/admin/stop',
  requireAdmin,
  async (request, response) => {
    response.json(await engine.stop());
  }
);

app.post(
  '/admin/interval',
  requireAdmin,
  async (request, response) => {
    const minutes = Number(
      request.body && request.body.minutes
    );

    if (
      !Number.isFinite(minutes) ||
      minutes < 1 ||
      minutes > 120
    ) {
      return response.status(400).json({
        error: 'Envie { "minutes": 1-120 }.'
      });
    }

    engine.intervalMin = minutes;
    await setSetting('interval_min', String(minutes));

    response.json({
      ok: true,
      intervalMin: minutes
    });
  }
);

app.post(
  '/admin/gales',
  requireAdmin,
  async (request, response) => {
    const gales = Number(
      request.body && request.body.gales
    );

    if (
      !Number.isFinite(gales) ||
      gales < 0 ||
      gales > 3
    ) {
      return response.status(400).json({
        error: 'Envie { "gales": 0-3 }.'
      });
    }

    engine.gales = gales;
    await setSetting('gales', String(gales));

    response.json({
      ok: true,
      gales
    });
  }
);

app.post(
  '/admin/signal',
  requireAdmin,
  async (request, response) => {
    response.json({
      ok: true,
      signal: await engine.sendSignal()
    });
  }
);

app.get(
  '/admin/signals',
  requireAdmin,
  async (request, response) => {
    response.json(await listSignals(50));
  }
);

// ------------------------------------------------------------
// Inicialização
// ------------------------------------------------------------

const PORT = process.env.PORT || 3000;

(async () => {
  try {
    await initializeDatabase();

    const savedInterval = await getSetting('interval_min');

    if (savedInterval && Number(savedInterval) >= 1) {
      engine.intervalMin = Number(savedInterval);
    }

    const savedGales = await getSetting('gales');

    if (savedGales !== null && Number(savedGales) >= 0) {
      engine.gales = Number(savedGales);
    }

    feed.start();

    app.listen(PORT, () => {
      console.log(
        `[server] Bot de sinais BAC rodando na porta ${PORT}.`
      );
    });

    const wasRunning =
      (await getSetting('running', 'false')) === 'true';

    if (wasRunning) {
      console.log(
        '[server] O bot estava rodando antes do restart. Retomando sinais...'
      );

      await engine.start();
    }
  } catch (error) {
    console.error(
      '[server] Falha na inicialização:',
      error.message
    );

    process.exit(1);
  }
})();