'use strict';

/**
 * CAPUNGA DADOS — BAC BO
 * Node.js + Express + PostgreSQL + Telegram
 * Arranque automático no Render.
 *
 * Ficheiros necessários:
 *   ./bot/tick.js
 *   ./bot/telegram.js
 *   ./bot/db.js
 *   ./bot/migrate.js
 *
 * Variáveis obrigatórias:
 *   DATABASE_URL
 *   BOT_TOKEN
 *   CHAT_ID
 *
 * Variáveis opcionais:
 *   PORT=10000
 *   INTERVALO_SYNC_MS=6000
 *   KEEP_ALIVE_MS=60000
 *   KEEP_ALIVE_URL
 *   KEEP_ALIVE_URLS
 *   PAINEL_TOKEN
 *   DATABASE_SSL=false
 */

const express = require('express');
const cors = require('cors');

const app = express();

app.disable('x-powered-by');
app.use(cors());
app.use(express.json({ limit: '1mb' }));

const PORT = limitar(
  process.env.PORT,
  10000,
  1,
  65535
);

const DATABASE_URL = (process.env.DATABASE_URL || '').trim();
const BOT_TOKEN = (process.env.BOT_TOKEN || '').trim();
const CHAT_ID = (process.env.CHAT_ID || '').trim();
const PAINEL_TOKEN = (process.env.PAINEL_TOKEN || '').trim();

const INTERVALO_SYNC_MS = limitar(
  process.env.INTERVALO_SYNC_MS,
  6000,
  4000,
  30000
);

const KEEP_ALIVE_MS = limitar(
  process.env.KEEP_ALIVE_MS,
  60000,
  15000,
  900000
);

function limitar(valor, padrao, minimo, maximo) {
  const n = Number.parseInt(valor, 10);

  if (!Number.isFinite(n)) {
    return padrao;
  }

  return Math.max(minimo, Math.min(maximo, n));
}

function normalizarUrlSaude(valor) {
  if (!valor) return '';

  const base = String(valor).trim().replace(/\/+$/, '');

  if (!base) return '';

  return /\/health$/i.test(base)
    ? base
    : `${base}/health`;
}

const KEEP_ALIVE_URLS = [
  process.env.KEEP_ALIVE_URL,
  process.env.RENDER_EXTERNAL_URL,
  process.env.PUBLIC_URL,
  ...(process.env.KEEP_ALIVE_URLS || '').split(',')
]
  .map(normalizarUrlSaude)
  .filter(Boolean)
  .filter((url, index, lista) => lista.indexOf(url) === index);

const runtime = {
  iniciadoEm: new Date().toISOString(),
  ciclos: 0,
  ultimoPainel: null,
  ultimoErro: null,
  ultimoKeepAlive: null,
  ultimoKeepAliveUrl: null,
  keepAliveAtivo: false,
  encerrando: false
};

let bot = null;
let server = null;
let cicloTimer = null;
let keepAliveTimer = null;
let encerrando = false;
let cicloEmExecucao = false;

/* =====================================================
   KEEP-ALIVE
   Apenas monitoriza URLs; não garante disponibilidade
   permanente do plano de alojamento.
===================================================== */

async function keepAliveTick() {
  if (encerrando || KEEP_ALIVE_URLS.length === 0) {
    return;
  }

  let sucesso = false;

  for (const url of KEEP_ALIVE_URLS) {
    try {
      const resposta = await fetch(url, {
        method: 'GET',
        headers: {
          'User-Agent': 'capunga-dados-monitor/1.0'
        },
        signal: AbortSignal.timeout(10000)
      });

      if (!resposta.ok) {
        continue;
      }

      sucesso = true;
      runtime.ultimoKeepAlive = new Date().toISOString();
      runtime.ultimoKeepAliveUrl = url;
      runtime.keepAliveAtivo = true;
      break;
    } catch (erro) {
      // Tenta o próximo URL configurado.
    }
  }

  if (!sucesso) {
    runtime.keepAliveAtivo = false;
  }
}

/* =====================================================
   CICLO AUTOMÁTICO DO MOTOR
===================================================== */

function agendarProximoCiclo() {
  if (encerrando) return;

  cicloTimer = setTimeout(() => {
    ciclo().catch((erro) => {
      runtime.ultimoErro = erro.message;
      console.error('[CICLO]', erro.message);
    });
  }, INTERVALO_SYNC_MS);
}

async function ciclo() {
  if (encerrando || cicloEmExecucao) return;

  cicloEmExecucao = true;

  try {
    if (!bot || typeof bot.tickAndSnapshot !== 'function') {
      throw new Error('Motor do bot não inicializado.');
    }

    const painel = await bot.tickAndSnapshot();

    runtime.ultimoPainel = painel || null;
    runtime.ultimoErro = painel && painel.erro
      ? String(painel.erro)
      : null;

    runtime.ciclos++;

    if (runtime.ultimoErro) {
      console.error('[MOTOR]', runtime.ultimoErro);
    }
  } catch (erro) {
    runtime.ultimoErro = erro.message;
    console.error('[CICLO]', erro.message);
  } finally {
    cicloEmExecucao = false;
    agendarProximoCiclo();
  }
}

/* =====================================================
   ROTAS HTTP
===================================================== */

app.get('/', (_req, res) => {
  res.json({
    nome: 'CAPUNGA DADOS — BAC BO',
    estado: encerrando ? 'encerrando' : 'online',
    iniciadoEm: runtime.iniciadoEm,
    hora: new Date().toISOString()
  });
});

function saude(_req, res) {
  const painel = runtime.ultimoPainel;

  const baseConfigurada = Boolean(DATABASE_URL);
  const telegramConfigurado = Boolean(BOT_TOKEN && CHAT_ID);
  const motorInicializado = Boolean(bot);

  res.status(encerrando ? 503 : 200).json({
    sucesso: !encerrando,
    estado: encerrando ? 'encerrando' : 'online',
    baseConfigurada,
    telegramConfigurado,
    motorInicializado,
    ciclos: runtime.ciclos,
    hora: painel && painel.hora ? painel.hora : null,
    motivo: painel && painel.motivo
      ? painel.motivo
      : 'A aguardar primeira sincronização',
    aoVivo: painel ? Boolean(painel.aoVivo) : false,
    retomado: painel ? Boolean(painel.retomado) : false,
    ultimaSincronizacao: painel
      ? painel.sincronizadoEm || null
      : null,
    ultimoErro: runtime.ultimoErro,
    ultimoKeepAlive: runtime.ultimoKeepAlive,
    ultimoKeepAliveUrl: runtime.ultimoKeepAliveUrl,
    keepAliveAtivo: runtime.keepAliveAtivo
  });
}

app.get('/health', saude);
app.get('/api/health', saude);

/* Estado detalhado */
app.get('/status', async (_req, res) => {
  let baseDadosOK = false;
  let totaisSinais = null;

  try {
    if (!bot || typeof bot.getSql !== 'function') {
      throw new Error('Motor ainda não inicializado.');
    }

    const sql = await bot.getSql();

    const resultado = await sql.query(
      'SELECT estado, COUNT(*)::int AS quantidade ' +
      'FROM bacbo_sinais GROUP BY estado ORDER BY estado'
    );

    totaisSinais = resultado.rows || resultado;
    baseDadosOK = true;
  } catch (erro) {
    runtime.ultimoErro = erro.message;
  }

  const painel = runtime.ultimoPainel;

  res.json({
    nome: 'CAPUNGA DADOS — BAC BO',
    estado: encerrando ? 'encerrando' : 'online',
    iniciadoEm: runtime.iniciadoEm,
    ciclos: runtime.ciclos,
    ultimaSincronizacao: painel
      ? painel.sincronizadoEm || null
      : null,
    motivo: painel
      ? painel.motivoHumano || painel.motivo || null
      : null,
    sinalAberto: painel ? painel.aberto ?? null : null,
    folhaDoDia: painel ? painel.folha ?? null : null,
    totalRodadas: painel && painel.totais
      ? painel.totais.rodadas ?? null
      : null,
    baseDadosOK,
    totaisSinais,
    ultimoErro: runtime.ultimoErro
  });
});

/* Painel privado, se PAINEL_TOKEN estiver definido */
app.get('/api/painel', (req, res) => {
  if (PAINEL_TOKEN) {
    const bearer = req.headers.authorization || '';
    const autorizado =
      req.query.token === PAINEL_TOKEN ||
      bearer === `Bearer ${PAINEL_TOKEN}`;

    if (!autorizado) {
      return res.status(401).json({
        sucesso: false,
        erro: 'nao_autorizado'
      });
    }
  }

  if (!runtime.ultimoPainel) {
    return res.status(503).json({
      sucesso: false,
      estado: 'iniciando',
      erro: runtime.ultimoErro || 'A aguardar primeira sincronização'
    });
  }

  res.json(runtime.ultimoPainel);
});

/* Resposta para rotas inexistentes */
app.use((_req, res) => {
  res.status(404).json({
    sucesso: false,
    erro: 'rota_nao_encontrada'
  });
});

/* Erros de JSON e de middleware */
app.use((erro, _req, res, _next) => {
  console.error('[HTTP]', erro.message);

  if (res.headersSent) return;

  res.status(400).json({
    sucesso: false,
    erro: 'pedido_invalido'
  });
});

/* =====================================================
   ARRANQUE
===================================================== */

async function iniciar() {
  if (!DATABASE_URL) {
    throw new Error('Configura DATABASE_URL no Render.');
  }

  if (!BOT_TOKEN) {
    throw new Error('Configura BOT_TOKEN no Render.');
  }

  if (!CHAT_ID) {
    throw new Error('Configura CHAT_ID no Render.');
  }

  // Carrega os módulos ESM da pasta ./bot.
  const [tick, telegram, db, migrate] = await Promise.all([
    import('./bot/tick.js'),
    import('./bot/telegram.js'),
    import('./bot/db.js'),
    import('./bot/migrate.js')
  ]);

  if (typeof tick.tickAndSnapshot !== 'function') {
    throw new Error(
      './bot/tick.js não exporta tickAndSnapshot().'
    );
  }

  if (typeof db.getSql !== 'function') {
    throw new Error(
      './bot/db.js não exporta getSql().'
    );
  }

  if (typeof migrate.migrate !== 'function') {
    throw new Error(
      './bot/migrate.js não exporta migrate().'
    );
  }

  bot = {
    tickAndSnapshot: tick.tickAndSnapshot,
    telegramConfigurado: telegram.telegramConfigurado,
    getSql: db.getSql,
    closeDb: db.closeDb
  };

  // Assegura que as tabelas existem antes de iniciar o HTTP.
  await migrate.migrate();

  console.log('[DB] Migração concluída.');

  server = app.listen(PORT, '0.0.0.0', () => {
    console.log(`[HTTP] Servidor iniciado na porta ${PORT}.`);
  });

  server.on('error', (erro) => {
    runtime.ultimoErro = erro.message;
    console.error('[HTTP FATAL]', erro.message);
  });

  if (KEEP_ALIVE_URLS.length > 0) {
    keepAliveTimer = setInterval(() => {
      keepAliveTick().catch((erro) => {
        console.error('[KEEP-ALIVE]', erro.message);
      });
    }, KEEP_ALIVE_MS);

    if (typeof keepAliveTimer.unref === 'function') {
      keepAliveTimer.unref();
    }

    setTimeout(() => {
      keepAliveTick().catch(() => {});
    }, 15000);
  }

  console.log('[BOT] A iniciar o ciclo automático.');
  console.log(`[BOT] Intervalo: ${INTERVALO_SYNC_MS} ms.`);

  // Primeira execução imediata.
  await ciclo();
}

/* =====================================================
   ENCERRAMENTO CONTROLADO
===================================================== */

async function encerrar() {
  if (encerrando) return;

  encerrando = true;
  runtime.encerrando = true;

  if (cicloTimer) {
    clearTimeout(cicloTimer);
    cicloTimer = null;
  }

  if (keepAliveTimer) {
    clearInterval(keepAliveTimer);
    keepAliveTimer = null;
  }

  if (server) {
    await new Promise((resolve) => {
      server.close(() => resolve());
    });
  }

  if (bot && typeof bot.closeDb === 'function') {
    try {
      await bot.closeDb();
    } catch (erro) {
      console.error('[DB CLOSE]', erro.message);
    }
  }

  console.log('[BOT] Servidor encerrado corretamente.');
}

process.on('SIGTERM', () => {
  encerrar().catch((erro) => {
    console.error('[SIGTERM]', erro.message);
    process.exitCode = 1;
  });
});

process.on('SIGINT', () => {
  encerrar().catch((erro) => {
    console.error('[SIGINT]', erro.message);
    process.exitCode = 1;
  });
});

process.on('unhandledRejection', (erro) => {
  console.error('[UNHANDLED REJECTION]', erro);
});

process.on('uncaughtException', (erro) => {
  console.error('[UNCAUGHT EXCEPTION]', erro);
  process.exitCode = 1;

  encerrar().catch(() => {});
});

/* Arranque com falha explícita para o Render detetar */
iniciar().catch((erro) => {
  console.error('[FATAL]', erro.message);
  process.exitCode = 1;
});