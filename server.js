'use strict';

/**
 * CAPUNGA DADOS — BAC BO MONITOR (versão endurecida)
 * Node.js + Express + PostgreSQL + Axios
 *
 * Mitigações:
 * - Retries + backoff na API
 * - Schema seguro e idempotente
 * - Detecção de gaps / perda de rodadas
 * - Teste ponta-a-ponta
 * - Alertas independentes do Telegram (log + webhook opcional)
 * - Melhor resiliência a reinícios do Render
 *
 * Variáveis obrigatórias:
 *   DATABASE_URL
 *
 * Variáveis opcionais:
 *   BOT_TOKEN, CHAT_ID, ADMIN_TOKEN,
 *   API_BACBO, INTERVALO_SYNC_MS (5000-30000),
 *   ALERT_WEBHOOK
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
const ALERT_WEBHOOK = process.env.ALERT_WEBHOOK || '';

const API_BACBO = process.env.API_BACBO ||
  'https://api-cs.casino.org/svc-evolution-game-events/api/bacbo';

const INTERVALO_SYNC_MS = Math.max(
  5000,
  Math.min(Number.parseInt(process.env.INTERVALO_SYNC_MS, 10) || 6000, 30000)
);

const pool = DATABASE_URL
  ? new Pool({
      connectionString: DATABASE_URL,
      ssl: /localhost|127\.0\.0\.1/i.test(DATABASE_URL)
        ? false
        : { rejectUnauthorized: false },
      max: 8,
      idleTimeoutMillis: 20000,
      connectionTimeoutMillis: 12000
    })
  : null;

if (pool) {
  pool.on('error', (err) => {
    log('POOL_ERROR', err.message);
    runtime.erroDB = err.message;
  });
}

const runtime = {
  iniciadoEm: new Date().toISOString(),
  ultimaSincronizacao: null,
  ultimaRodadaNova: null,
  ultimoIdNovo: null,
  ultimoHTTPAPI: null,
  erroAPI: null,
  erroDB: null,
  erroTelegram: null,
  ultimaMensagemTelegram: null,
  recebidosUltimoCiclo: 0,
  novosUltimoCiclo: 0,
  atualizadosUltimoCiclo: 0,
  inalteradosUltimoCiclo: 0,
  ciclosSemNovos: 0,
  errosConsecutivos: 0,
  sincronizacaoEmCurso: false,
  ultimaDuracaoMs: null,
  gapDetectado: null
};

let schemaPromise = null;
let timer = null;
let server = null;
let shuttingDown = false;

function log(tag, data) {
  const payload = typeof data === 'string' ? data : JSON.stringify(data);
  console.log(`[\( {new Date().toISOString()}] [ \){tag}] ${payload}`);
}

function horaAngola() {
  return new Intl.DateTimeFormat('pt-PT', {
    timeZone: 'Africa/Luanda',
    dateStyle: 'short',
    timeStyle: 'medium',
    hour12: false
  }).format(new Date());
}

async function alertIndependent(msg) {
  // Sempre loga (canal independente do Telegram)
  log('ALERTA', msg);

  if (ALERT_WEBHOOK) {
    try {
      await axios.post(ALERT_WEBHOOK, {
        text: `[Capunga BacBo] ${msg}`,
        time: new Date().toISOString()
      }, { timeout: 8000 });
    } catch (e) {
      log('ALERT_WEBHOOK_FAIL', e.message);
    }
  }
}

function normalizeOutcome(value) {
  if (value && typeof value === 'object') {
    value = value.outcome ?? value.resultado ?? value.winner ?? value.name;
  }

  const s = String(value ?? '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .trim()
    .toLowerCase()
    .replace(/[\s_-]+/g, '');

  if (['player', 'jogador', 'playerwin', 'playerwins', 'playerwon', 'blue'].includes(s)) {
    return 'Jogador';
  }
  if (['banker', 'banca', 'bankerwin', 'bankerwins', 'bankerwon', 'red'].includes(s)) {
    return 'Banca';
  }
  if (['tie', 'draw', 'empate', 'equal', 'push', 'tied'].includes(s)) {
    return 'Empate';
  }
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
  const r = (d.result && typeof d.result === 'object')
    ? d.result
    : (item?.result && typeof item.result === 'object' ? item.result : {});

  const status = String(d.status ?? d.estado ?? item?.status ?? item?.estado ?? '').toLowerCase();

  const fim = validDate(
    d.settledAt ?? d.endTime ?? d.endedAt ?? d.finishedAt ?? d.fim ??
    item?.settledAt ?? item?.fim
  );

  const inicio = validDate(
    d.startedAt ?? d.startTime ?? d.started_at ?? d.inicio ??
    item?.startedAt ?? item?.inicio
  );

  const rawOutcome =
    r.outcome ?? r.resultado ?? r.winner ??
    d.outcome ?? d.resultado ?? d.winner ??
    item?.outcome ?? item?.resultado ?? item?.winner ??
    (typeof d.result === 'string' ? d.result : null) ??
    (typeof item?.result === 'string' ? item.result : null);

  const resultado = normalizeOutcome(rawOutcome);

  const resolvida = [
    'resolved', 'settled', 'finished', 'complete', 'completed',
    'resolvido', 'concluido', 'finalizado'
  ].some(w => status.includes(w)) || Boolean(fim);

  const id = String(
    d.id ?? d.roundId ?? d.round_id ?? d.eventId ??
    item?.id ?? item?.roundId ?? item?.round_id ?? item?.eventId ?? ''
  ).trim();

  if (!id || !resultado || !resolvida || !fim) return null;

  function optionalScore(value) {
    if (value === null || value === undefined || value === '') return null;
    const n = Number(value);
    return Number.isFinite(n) && Number.isInteger(n) && n >= 0 ? n : null;
  }

  const pj = optionalScore(
    r.playerDice?.score ?? d.playerDice?.score ?? d.playerScore ?? d.player_score ?? d.pontos_jogador
  );
  const pb = optionalScore(
    r.bankerDice?.score ?? d.bankerDice?.score ?? d.bankerScore ?? d.banker_score ?? d.pontos_banca
  );

  const multiplicador = r.multiplier ?? d.multiplier ?? d.multiplicador ?? null;

  return {
    id,
    resultado,
    estado: 'Resolved',
    inicio: inicio || fim,
    fim,
    pontos_jogador: pj,
    pontos_banca: pb,
    multiplicador: multiplicador == null ? null : String(multiplicador)
  };
}

async function ensureSchema() {
  if (!pool) throw new Error('DATABASE_URL não configurada');
  if (schemaPromise) return schemaPromise;

  schemaPromise = (async () => {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS bacbo_resultados (
        id              TEXT PRIMARY KEY,
        resultado       TEXT NOT NULL,
        estado          TEXT NOT NULL DEFAULT 'Resolved',
        inicio          TIMESTAMPTZ,
        fim             TIMESTAMPTZ,
        pontos_jogador  INTEGER,
        pontos_banca    INTEGER,
        multiplicador   TEXT,
        guardado_em     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        seq             BIGINT GENERATED ALWAYS AS IDENTITY
      )
    `);

    // Índices seguros
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_bacbo_fim
      ON bacbo_resultados (fim DESC NULLS LAST)
    `);
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_bacbo_seq
      ON bacbo_resultados (seq DESC)
    `);

    log('DB', { tabelasPreparadas: true });
  })();

  try {
    await schemaPromise;
  } catch (err) {
    schemaPromise = null;
    throw err;
  }
}

async function telegramSend(texto) {
  if (!BOT_TOKEN || !CHAT_ID) {
    runtime.erroTelegram = 'BOT_TOKEN ou CHAT_ID não configurado';
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
      { timeout: 12000 }
    );

    if (response.data?.ok === true) {
      runtime.ultimaMensagemTelegram = new Date().toISOString();
      runtime.erroTelegram = null;
      return true;
    }

    runtime.erroTelegram = JSON.stringify(response.data);
    return false;
  } catch (error) {
    runtime.erroTelegram = error.response?.data
      ? JSON.stringify(error.response.data)
      : error.message;
    log('ERRO TELEGRAM', runtime.erroTelegram);
    return false;
  }
}

async function fetchRoundsWithRetry(maxAttempts = 3) {
  let lastError = null;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const response = await axios.get(API_BACBO, {
        timeout: 18000,
        headers: {
          Accept: 'application/json',
          'User-Agent': 'CapungaDadosMonitor/4.0'
        }
      });

      runtime.ultimoHTTPAPI = response.status;
      runtime.erroAPI = null;

      const items = extractItems(response.data);
      const unique = new Map();

      for (const item of items) {
        const round = normalizeRound(item);
        if (round) unique.set(round.id, round);
      }

      const rounds = [...unique.values()].sort(
        (a, b) => new Date(a.fim) - new Date(b.fim)
      );

      runtime.recebidosUltimoCiclo = items.length;

      if (items.length > 0 && rounds.length === 0) {
        runtime.erroAPI = 'API respondeu, mas nenhum resultado final reconhecível';
      }

      return rounds;
    } catch (err) {
      lastError = err;
      runtime.ultimoHTTPAPI = err.response?.status || null;
      runtime.erroAPI = err.message;

      if (attempt < maxAttempts) {
        const delay = 800 * Math.pow(2, attempt - 1);
        await new Promise(r => setTimeout(r, delay));
      }
    }
  }

  throw lastError || new Error('Falha desconhecida na API');
}

async function saveRound(round) {
  const existing = await pool.query(
    `SELECT id, resultado, fim, pontos_jogador, pontos_banca, multiplicador
     FROM bacbo_resultados WHERE id = $1`,
    [round.id]
  );

  const wasExisting = existing.rows.length > 0;

  const result = await pool.query(
    `INSERT INTO bacbo_resultados (
       id, resultado, estado, inicio, fim,
       pontos_jogador, pontos_banca, multiplicador
     )
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
     ON CONFLICT (id) DO UPDATE SET
       resultado       = EXCLUDED.resultado,
       estado          = EXCLUDED.estado,
       inicio          = COALESCE(EXCLUDED.inicio, bacbo_resultados.inicio),
       fim             = COALESCE(EXCLUDED.fim, bacbo_resultados.fim),
       pontos_jogador  = COALESCE(EXCLUDED.pontos_jogador, bacbo_resultados.pontos_jogador),
       pontos_banca    = COALESCE(EXCLUDED.pontos_banca, bacbo_resultados.pontos_banca),
       multiplicador   = COALESCE(EXCLUDED.multiplicador, bacbo_resultados.multiplicador)
     RETURNING id`,
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

  if (!result.rows.length) return { tipo: 'inalterado' };

  if (!wasExisting) return { tipo: 'novo' };

  const old = existing.rows[0];
  const changed =
    old.resultado !== round.resultado ||
    new Date(old.fim).getTime() !== new Date(round.fim).getTime() ||
    (round.pontos_jogador !== null && old.pontos_jogador !== round.pontos_jogador) ||
    (round.pontos_banca !== null && old.pontos_banca !== round.pontos_banca) ||
    (round.multiplicador !== null && old.multiplicador !== round.multiplicador);

  return { tipo: changed ? 'atualizado' : 'inalterado' };
}

async function syncOnce() {
  if (!pool) throw new Error('DATABASE_URL não configurada');
  if (runtime.sincronizacaoEmCurso || shuttingDown) {
    return { sucesso: false, ignorada: true };
  }

  runtime.sincronizacaoEmCurso = true;
  const started = Date.now();
  let novos = 0, atualizados = 0, inalterados = 0;

  try {
    await ensureSchema();
    const rounds = await fetchRoundsWithRetry();

    // Detecção simples de gap (API devolve \~10 itens)
    if (runtime.ultimaSincronizacao && rounds.length > 0) {
      const oldestInBatch = new Date(rounds[0].fim).getTime();
      const lastSync = new Date(runtime.ultimaSincronizacao).getTime();
      const gapMs = oldestInBatch - lastSync;
      if (gapMs > 3 * 60 * 1000) { // > 3 min
        runtime.gapDetectado = {
          gapMs,
          oldestInBatch: rounds[0].fim,
          lastSync: runtime.ultimaSincronizacao
        };
        await alertIndependent(
          `Possível perda de rodadas (gap \~${Math.round(gapMs / 1000)}s). API devolve histórico curto.`
        );
      } else {
        runtime.gapDetectado = null;
      }
    }

    for (const round of rounds) {
      const saved = await saveRound(round);

      if (saved.tipo === 'novo') {
        novos++;
        runtime.ultimaRodadaNova = new Date().toISOString();
        runtime.ultimoIdNovo = round.id;

        await telegramSend([
          '🎲 CAPUNGA DADOS — NOVO RESULTADO',
          `Rodada: ${round.id}`,
          `Resultado: ${round.resultado}`,
          `Hora Angola: ${horaAngola()}`,
          'Registo informativo; não prevê resultados futuros.'
        ].join('\n'));
      } else if (saved.tipo === 'atualizado') {
        atualizados++;
      } else {
        inalterados++;
      }
    }

    runtime.novosUltimoCiclo = novos;
    runtime.atualizadosUltimoCiclo = atualizados;
    runtime.inalteradosUltimoCiclo = inalterados;
    runtime.ciclosSemNovos = novos === 0 ? runtime.ciclosSemNovos + 1 : 0;
    runtime.ultimaSincronizacao = new Date().toISOString();
    runtime.ultimaDuracaoMs = Date.now() - started;
    runtime.erroDB = null;
    runtime.errosConsecutivos = 0;

    const summary = {
      sucesso: true,
      recebidos: rounds.length,
      novos,
      atualizados,
      inalterados,
      ciclosSemNovos: runtime.ciclosSemNovos,
      duracaoMs: runtime.ultimaDuracaoMs,
      horaAngola: horaAngola()
    };

    if (novos > 0 || atualizados > 0 || runtime.ciclosSemNovos % 10 === 0) {
      log('SINCRONIZACAO', summary);
    }

    return summary;
  } catch (error) {
    runtime.erroDB = error.message;
    runtime.ultimaDuracaoMs = Date.now() - started;
    runtime.errosConsecutivos++;

    if (error.response) {
      runtime.ultimoHTTPAPI = error.response.status;
      runtime.erroAPI = error.message;
    }

    log('ERRO SINCRONIZACAO', { mensagem: error.message });

    if (runtime.errosConsecutivos >= 3) {
      await alertIndependent(
        `3+ erros consecutivos de sincronização: ${error.message}`
      );
    }

    return { sucesso: false, erro: error.message };
  } finally {
    runtime.sincronizacaoEmCurso = false;
  }
}

function requireAdmin(req, res, next) {
  if (!ADMIN_TOKEN) {
    return res.status(503).json({ sucesso: false, erro: 'ADMIN_TOKEN não configurado' });
  }
  if (req.get('x-admin-token') !== ADMIN_TOKEN) {
    return res.status(401).json({ sucesso: false, erro: 'Token inválido' });
  }
  next();
}

// ---------- Rotas ----------

app.get('/', (_req, res) => {
  res.json({
    nome: 'Capunga Dados — BAC BO Monitor',
    estado: 'online',
    horaAngola: horaAngola(),
    rotas: ['/health', '/diagnostico', '/resultados', '/estatisticas', '/teste-e2e']
  });
});

app.get('/health', (_req, res) => {
  res.json({
    sucesso: !runtime.erroDB,
    estado: 'online',
    iniciadoEm: runtime.iniciadoEm,
    horaAngola: horaAngola(),
    ultimaSincronizacao: runtime.ultimaSincronizacao,
    ultimoIdNovo: runtime.ultimoIdNovo,
    ultimoHTTPAPI: runtime.ultimoHTTPAPI,
    erroAPI: runtime.erroAPI,
    erroDB: runtime.erroDB,
    erroTelegram: runtime.erroTelegram,
    novosUltimoCiclo: runtime.novosUltimoCiclo,
    atualizadosUltimoCiclo: runtime.atualizadosUltimoCiclo,
    inalteradosUltimoCiclo: runtime.inalteradosUltimoCiclo,
    ciclosSemNovos: runtime.ciclosSemNovos,
    errosConsecutivos: runtime.errosConsecutivos,
    ultimaDuracaoMs: runtime.ultimaDuracaoMs,
    gapDetectado: runtime.gapDetectado
  });
});

app.get('/diagnostico', async (_req, res) => {
  try {
    await ensureSchema();
    const [count, latest, recent] = await Promise.all([
      pool.query(`SELECT COUNT(*)::int AS total FROM bacbo_resultados`),
      pool.query(`
        SELECT id, resultado, fim, guardado_em, pontos_jogador, pontos_banca, multiplicador
        FROM bacbo_resultados
        ORDER BY fim DESC NULLS LAST, seq DESC LIMIT 1
      `),
      pool.query(`
        SELECT id, resultado, fim, pontos_jogador, pontos_banca, multiplicador
        FROM bacbo_resultados
        ORDER BY fim DESC NULLS LAST, seq DESC LIMIT 10
      `)
    ]);

    res.json({
      sucesso: true,
      nome: 'Capunga Dados — BAC BO Monitor',
      horaAngola: horaAngola(),
      api: { http: runtime.ultimoHTTPAPI, erro: runtime.erroAPI },
      baseDados: {
        totalResultados: Number(count.rows[0].total),
        ultimoResultado: latest.rows[0] || null
      },
      recentes: recent.rows,
      sincronizacao: {
        ultimaSincronizacao: runtime.ultimaSincronizacao,
        novos: runtime.novosUltimoCiclo,
        atualizados: runtime.atualizadosUltimoCiclo,
        inalterados: runtime.inalteradosUltimoCiclo,
        ciclosSemNovos: runtime.ciclosSemNovos,
        duracaoMs: runtime.ultimaDuracaoMs,
        gapDetectado: runtime.gapDetectado
      },
      telegram: {
        configurado: Boolean(BOT_TOKEN && CHAT_ID),
        ultimaMensagem: runtime.ultimaMensagemTelegram,
        erro: runtime.erroTelegram
      }
    });
  } catch (error) {
    res.status(500).json({ sucesso: false, erro: error.message });
  }
});

app.get('/resultados', async (req, res) => {
  try {
    await ensureSchema();
    const parsed = Number.parseInt(req.query.limit, 10);
    const limit = Math.min(Math.max(Number.isFinite(parsed) ? parsed : 50, 1), 500);

    const result = await pool.query(`
      SELECT id, resultado, estado, inicio, fim,
             pontos_jogador, pontos_banca, multiplicador, guardado_em
      FROM bacbo_resultados
      ORDER BY fim DESC NULLS LAST, seq DESC
      LIMIT $1
    `, [limit]);

    res.json({ sucesso: true, total: result.rows.length, resultados: result.rows });
  } catch (error) {
    res.status(500).json({ sucesso: false, erro: error.message });
  }
});

app.get('/estatisticas', async (_req, res) => {
  try {
    await ensureSchema();
    const result = await pool.query(`
      SELECT
        COUNT(*)::int AS total,
        COUNT(*) FILTER (WHERE resultado = 'Jogador')::int AS jogador,
        COUNT(*) FILTER (WHERE resultado = 'Banca')::int AS banca,
        COUNT(*) FILTER (WHERE resultado = 'Empate')::int AS empate,
        COUNT(*) FILTER (WHERE pontos_jogador IS NULL OR pontos_banca IS NULL)::int AS sem_pontuacoes,
        MAX(fim) AS ultima_rodada
      FROM bacbo_resultados
    `);
    res.json({ sucesso: true, estatisticas: result.rows[0], horaAngola: horaAngola() });
  } catch (error) {
    res.status(500).json({ sucesso: false, erro: error.message });
  }
});

// Teste ponta-a-ponta (admin)
app.post('/teste-e2e', requireAdmin, async (_req, res) => {
  const steps = [];
  try {
    // 1. API
    const rounds = await fetchRoundsWithRetry(2);
    steps.push({ passo: 'API', ok: rounds.length > 0, detalhe: `${rounds.length} rodadas normalizadas` });

    // 2. Schema + escrita
    await ensureSchema();
    if (rounds.length > 0) {
      const saved = await saveRound(rounds[rounds.length - 1]);
      steps.push({ passo: 'DB_write', ok: true, detalhe: saved.tipo });
    } else {
      steps.push({ passo: 'DB_write', ok: false, detalhe: 'sem rodadas para gravar' });
    }

    // 3. Leitura
    const read = await pool.query(`SELECT COUNT(*)::int AS total FROM bacbo_resultados`);
    steps.push({ passo: 'DB_read', ok: true, detalhe: `total=${read.rows[0].total}` });

    // 4. Telegram (opcional)
    if (BOT_TOKEN && CHAT_ID) {
      const sent = await telegramSend(`🧪 Teste E2E Capunga — ${horaAngola()}`);
      steps.push({ passo: 'Telegram', ok: sent, detalhe: runtime.erroTelegram || 'ok' });
    } else {
      steps.push({ passo: 'Telegram', ok: false, detalhe: 'não configurado' });
    }

    const sucesso = steps.every(s => s.ok || s.passo === 'Telegram');
    res.status(sucesso ? 200 : 500).json({ sucesso, steps });
  } catch (error) {
    res.status(500).json({ sucesso: false, erro: error.message, steps });
  }
});

app.post('/db/preparar', requireAdmin, async (_req, res) => {
  try {
    await ensureSchema();
    res.json({ sucesso: true, mensagem: 'Esquema verificado; histórico preservado.' });
  } catch (error) {
    res.status(500).json({ sucesso: false, erro: error.message });
  }
});

app.post('/db/sincronizar', requireAdmin, async (_req, res) => {
  const result = await syncOnce();
  res.status(result.sucesso ? 200 : 500).json(result);
});

app.post('/telegram/teste', requireAdmin, async (_req, res) => {
  const sent = await telegramSend([
    '🎲 CAPUNGA DADOS — MONITOR',
    'Teste de ligação Telegram.',
    `Hora Angola: ${horaAngola()}`
  ].join('\n'));
  res.status(sent ? 200 : 502).json({ sucesso: sent, erro: runtime.erroTelegram });
});

app.use((req, res) => {
  res.status(404).json({ sucesso: false, erro: 'Rota não encontrada', caminho: req.path });
});

// ---------- Lifecycle ----------

async function start() {
  if (!DATABASE_URL) {
    throw new Error('DATABASE_URL não configurada no Render');
  }

  await ensureSchema();

  server = app.listen(PORT, '0.0.0.0', () => {
    log('START', {
      nome: 'CAPUNGA DADOS — BAC BO MONITOR v4',
      porta: PORT,
      horaAngola: horaAngola(),
      intervaloSyncMs: INTERVALO_SYNC_MS
    });
  });

  // Primeira sincronização imediata (recupera o que a API ainda tiver)
  const initial = await syncOnce();
  log('PRIMEIRA SINCRONIZACAO', initial);

  timer = setInterval(() => {
    if (shuttingDown || runtime.sincronizacaoEmCurso) return;
    syncOnce().catch(err => {
      runtime.erroDB = err.message;
      log('ERRO CICLO', err.message);
    });
  }, INTERVALO_SYNC_MS);
}

async function shutdown() {
  if (shuttingDown) return;
  shuttingDown = true;
  if (timer) clearInterval(timer);
  if (server) await new Promise(resolve => server.close(resolve));
  if (pool) await pool.end();
  process.exit(0);
}

process.on('SIGTERM', () => shutdown().catch(() => process.exit(1)));
process.on('SIGINT', () => shutdown().catch(() => process.exit(1)));
process.on('unhandledRejection', reason => log('UNHANDLED REJECTION', String(reason)));

start().catch(error => {
  log('FATAL', error.message);
  process.exit(1);
});