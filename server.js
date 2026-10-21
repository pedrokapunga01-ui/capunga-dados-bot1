
'use strict';

import express from 'express';
import cors from 'cors';
import pg from 'pg';

const { Pool } = pg;
const app = express();

app.use(cors());
app.use(express.json({ limit: '100kb' }));

// ======================================================
// CAPUNGA DADOS — BAC BO
// Servidor autónomo: PostgreSQL + Telegram + API HTTP
// Sem dependência de ficheiros externos da pasta /bot
// ======================================================

const PORT = Number(process.env.PORT || 10000);
const DATABASE_URL = process.env.DATABASE_URL;
const BOT_TOKEN = process.env.BOT_TOKEN;
const CHAT_ID = process.env.CHAT_ID;
const INGEST_TOKEN = process.env.INGEST_TOKEN;
const API_BACBO = process.env.API_BACBO || '';
const INTERVALO_SYNC_MS = Math.max(
  10000,
  Number(process.env.INTERVALO_SYNC_MS || 30000)
);
const MIN_AMOSTRA = Math.max(
  5,
  Number(process.env.MIN_AMOSTRA || 10)
);
const MIN_CONFIANCA = Math.min(
  95,
  Math.max(50, Number(process.env.MIN_CONFIANCA || 65))
);

if (!DATABASE_URL) {
  console.error('[FATAL] Falta configurar DATABASE_URL no Render.');
  process.exit(1);
}

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: process.env.DATABASE_SSL === 'false'
    ? false
    : { rejectUnauthorized: false },
  connectionTimeoutMillis: 15000,
  idleTimeoutMillis: 30000,
  max: 5
});

let cicloEmExecucao = false;
let ultimoErro = null;
let ultimaSincronizacao = null;
let iniciado = false;

// ======================================================
// UTILITÁRIOS
// ======================================================

function normalizarResultado(valor) {
  if (valor === undefined || valor === null) return null;

  const v = String(valor)
    .trim()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toUpperCase();

  if (['RED', 'VERMELHO', 'PLAYER', 'P'].includes(v)) return 'RED';
  if (['BLUE', 'AZUL', 'BANKER', 'B'].includes(v)) return 'BLUE';
  if (['TIE', 'DRAW', 'EMPATE', 'EQUAL'].includes(v)) return 'TIE';

  return null;
}

function dataValida(valor) {
  if (!valor) return new Date();
  const d = new Date(valor);
  return Number.isNaN(d.getTime()) ? new Date() : d;
}

async function telegram(texto) {
  if (!BOT_TOKEN || !CHAT_ID) {
    console.warn('[TELEGRAM] BOT_TOKEN ou CHAT_ID não configurado.');
    return false;
  }

  const url = `https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`;

  const response = await fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      chat_id: CHAT_ID,
      text: texto,
      disable_web_page_preview: true
    }),
    signal: AbortSignal.timeout(15000)
  });

  const data = await response.json().catch(() => ({}));

  if (!response.ok || data.ok !== true) {
    throw new Error(
      `Telegram recusou a mensagem: ${data.description || response.status}`
    );
  }

  return true;
}

async function criarEstrutura() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS resultados (
      id BIGSERIAL PRIMARY KEY,
      rodada_id TEXT UNIQUE,
      resultado TEXT NOT NULL CHECK (resultado IN ('RED','BLUE','TIE')),
      ocorrido_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      origem TEXT NOT NULL DEFAULT 'ingest'
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS sinais (
      id BIGSERIAL PRIMARY KEY,
      previsao TEXT NOT NULL CHECK (previsao IN ('RED','BLUE')),
      estado TEXT NOT NULL DEFAULT 'ABERTO',
      gale INTEGER NOT NULL DEFAULT 0,
      resultado_final TEXT,
      criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      fechado_em TIMESTAMPTZ
    )
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_resultados_data
    ON resultados (ocorrido_em DESC)
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_sinais_estado
    ON sinais (estado, criado_em DESC)
  `);

  console.log('[DB] Estrutura PostgreSQL verificada.');
}

async function resultadosRecentes(limite = 50) {
  const r = await pool.query(
    `SELECT id, rodada_id, resultado, ocorrido_em
     FROM resultados
     ORDER BY id DESC
     LIMIT $1`,
    [limite]
  );
  return r.rows;
}

// ======================================================
// ANÁLISE ESTATÍSTICA
// Não é garantia de previsão nem de lucro.
// ======================================================

async function calcularSinal() {
  const r = await pool.query(
    `SELECT resultado
     FROM resultados
     WHERE resultado IN ('RED','BLUE')
     ORDER BY id DESC
     LIMIT 10`
  );

  if (r.rows.length < MIN_AMOSTRA) {
    return {
      previsao: null,
      amostra: r.rows.length,
      confianca: 0,
      motivo: 'Amostra insuficiente'
    };
  }

  const reds = r.rows.filter(x => x.resultado === 'RED').length;
  const blues = r.rows.filter(x => x.resultado === 'BLUE').length;
  const total = reds + blues;

  if (!total || reds === blues) {
    return {
      previsao: null,
      amostra: total,
      confianca: 0,
      motivo: 'Sem predominância estatística'
    };
  }

  const previsao = reds > blues ? 'RED' : 'BLUE';
  const maior = Math.max(reds, blues);
  const confianca = Math.round((maior / total) * 100);

  if (confianca < MIN_CONFIANCA) {
    return {
      previsao: null,
      amostra: total,
      confianca,
      motivo: 'Confiança abaixo do limite configurado'
    };
  }

  return {
    previsao,
    amostra: total,
    confianca,
    motivo: 'Predominância na amostra recente'
  };
}

// ======================================================
// REGISTO E PROCESSAMENTO DOS RESULTADOS
// ======================================================

async function registarResultado({
  resultado,
  rodadaId = null,
  ocorridoEm = null,
  origem = 'ingest'
}) {
  const valor = normalizarResultado(resultado);

  if (!valor) {
    throw new Error('Resultado inválido. Usa RED, BLUE ou TIE.');
  }

  const idRodada = rodadaId ? String(rodadaId).slice(0, 180) : null;

  // Evita registar a mesma rodada duas vezes quando há ID.
  if (idRodada) {
    const existente = await pool.query(
      'SELECT id FROM resultados WHERE rodada_id = $1',
      [idRodada]
    );

    if (existente.rowCount > 0) {
      return { duplicado: true, resultado: valor };
    }
  }

  const inserido = await pool.query(
    `INSERT INTO resultados
       (rodada_id, resultado, ocorrido_em, origem)
     VALUES ($1, $2, $3, $4)
     RETURNING id, rodada_id, resultado, ocorrido_em`,
    [idRodada, valor, dataValida(ocorridoEm), String(origem).slice(0, 80)]
  );

  const novo = inserido.rows[0];

  // Empate: devolve o sinal aberto, sem consumir Gale.
  const aberto = await pool.query(
    `SELECT id, previsao, gale
     FROM sinais
     WHERE estado = 'ABERTO'
     ORDER BY id DESC
     LIMIT 1`
  );

  if (aberto.rowCount > 0) {
    const sinal = aberto.rows[0];

    if (valor === 'TIE') {
      await pool.query(
        `UPDATE sinais
         SET estado = 'EMPATE_DEVOLVIDO',
             resultado_final = 'TIE',
             fechado_em = NOW()
         WHERE id = $1 AND estado = 'ABERTO'`,
        [sinal.id]
      );

      await telegram(
        `♻️ EMPATE — DEVOLUÇÃO\n` +
        `Sinal #${sinal.id}\n` +
        `Resultado: Empate\n` +
        `Registado como devolvido.`
      ).catch(e => console.error('[TELEGRAM]', e.message));
    } else if (valor === sinal.previsao) {
      await pool.query(
        `UPDATE sinais
         SET estado = $2, resultado_final = $3, fechado_em = NOW()
         WHERE id = $1`,
        [sinal.id, sinal.gale === 0 ? 'GREEN' : 'GREEN_GALE', valor]
      );

      await telegram(
        `✅ ${sinal.gale === 0 ? 'GREEN' : 'GREEN NO GALE'}\n` +
        `Sinal #${sinal.id}\n` +
        `Resultado: ${valor}`
      ).catch(e => console.error('[TELEGRAM]', e.message));
    } else if (sinal.gale < 2) {
      await pool.query(
        `UPDATE sinais
         SET gale = gale + 1
         WHERE id = $1`,
        [sinal.id]
      );

      await telegram(
        `⚠️ GALE ${sinal.gale + 1}\n` +
        `Sinal #${sinal.id}\n` +
        `Resultado recebido: ${valor}\n` +
        `Previsão original: ${sinal.previsao}`
      ).catch(e => console.error('[TELEGRAM]', e.message));
    } else {
      await pool.query(
        `UPDATE sinais
         SET estado = 'LOSS',
             resultado_final = $2,
             fechado_em = NOW()
         WHERE id = $1`,
        [sinal.id, valor]
      );

      await telegram(
        `❌ LOSS após Gale 2\n` +
        `Sinal #${sinal.id}\n` +
        `Resultado: ${valor}`
      ).catch(e => console.error('[TELEGRAM]', e.message));
    }
  }

  // Só cria um novo sinal quando não há outro aberto.
  const aindaAberto = await pool.query(
    `SELECT id FROM sinais WHERE estado = 'ABERTO' LIMIT 1`
  );

  if (aindaAberto.rowCount === 0 && valor !== 'TIE') {
    const analise = await calcularSinal();

    if (analise.previsao) {
      const novoSinal = await pool.query(
        `INSERT INTO sinais (previsao)
         VALUES ($1)
         RETURNING id, previsao, criado_em`,
        [analise.previsao]
      );

      const s = novoSinal.rows[0];

      await telegram(
        `🎲 CAPUNGA DADOS — BAC BO\n\n` +
        `📢 NOVO SINAL #${s.id}\n` +
        `Previsão: ${s.previsao}\n` +
        `Proteção: até Gale 2\n` +
        `Amostra: ${analise.amostra} resultados\n` +
        `Indicador histórico: ${analise.confianca}%\n\n` +
        `⚠️ Indicador estatístico, não garantia de acerto.`
      ).catch(e => console.error('[TELEGRAM]', e.message));
    }
  }

  return { duplicado: false, resultado: novo };
}

// ======================================================
// EXTRAÇÃO DE RESULTADOS DE JSON
// API_BACBO deve apontar para um endpoint JSON real.
// ======================================================

function extrairListaJSON(data) {
  if (Array.isArray(data)) return data;

  if (data && typeof data === 'object') {
    for (const chave of [
      'results', 'resultados', 'history', 'data', 'items', 'rounds'
    ]) {
      if (Array.isArray(data[chave])) return data[chave];
    }
  }

  return null;
}

function extrairResultado(item) {
  if (typeof item === 'string') return normalizarResultado(item);
  if (!item || typeof item !== 'object') return null;

  for (const chave of [
    'result', 'resultado', 'winner', 'outcome', 'color', 'side'
  ]) {
    const v = normalizarResultado(item[chave]);
    if (v) return v;
  }

  return null;
}

async function sincronizarAPI() {
  if (!API_BACBO) return;

  const response = await fetch(API_BACBO, {
    headers: { accept: 'application/json' },
    signal: AbortSignal.timeout(12000)
  });

  if (!response.ok) {
    throw new Error(`API_BACBO respondeu HTTP ${response.status}`);
  }

  const data = await response.json();
  const lista = extrairListaJSON(data);

  if (!lista) {
    throw new Error(
      'A resposta da API não contém uma lista JSON reconhecida. ' +
      'É necessário adaptar o parser ao formato real da fonte.'
    );
  }

  // Processa do mais antigo para o mais recente, quando a API
  // devolve os itens do mais recente para o mais antigo.
  for (const item of [...lista].reverse()) {
    const resultado = extrairResultado(item);
    if (!resultado) continue;

    const rodadaId =
      item?.id ??
      item?.roundId ??
      item?.round_id ??
      item?.gameId ??
      null;

    const ocorridoEm =
      item?.timestamp ??
      item?.created_at ??
      item?.date ??
      item?.time ??
      null;

    await registarResultado({
      resultado,
      rodadaId,
      ocorridoEm,
      origem: 'API_BACBO'
    });
  }

  ultimaSincronizacao = new Date().toISOString();
}

// ======================================================
// ROTAS HTTP
// ======================================================

app.get('/', (_req, res) => {
  res.json({
    nome: 'CAPUNGA DADOS — BAC BO',
    estado: 'online',
    versao: 'autonoma-1.0.0',
    fonteAutomaticaConfigurada: Boolean(API_BACBO),
    telegramConfigurado: Boolean(BOT_TOKEN && CHAT_ID),
    hora: new Date().toISOString()
  });
});

app.get('/health', async (_req, res) => {
  try {
    await pool.query('SELECT 1');
    res.json({
      ok: true,
      servidor: 'online',
      database: 'connected',
      apiBacboConfigurada: Boolean(API_BACBO),
      telegramConfigurado: Boolean(BOT_TOKEN && CHAT_ID),
      ultimaSincronizacao,
      ultimoErro
    });
  } catch (e) {
    res.status(503).json({
      ok: false,
      erro: e.message
    });
  }
});

app.get('/api/history', async (req, res) => {
  try {
    const limite = Math.min(
      200,
      Math.max(1, Number(req.query.limit || 50))
    );
    const rows = await resultadosRecentes(limite);
    res.json({ ok: true, total: rows.length, resultados: rows });
  } catch (e) {
    res.status(500).json({ ok: false, erro: e.message });
  }
});

app.get('/api/stats', async (_req, res) => {
  try {
    const r = await pool.query(`
      SELECT
        COUNT(*) FILTER (WHERE estado = 'GREEN') AS green,
        COUNT(*) FILTER (WHERE estado = 'GREEN_GALE') AS green_gale,
        COUNT(*) FILTER (WHERE estado = 'LOSS') AS loss,
        COUNT(*) FILTER (WHERE estado = 'EMPATE_DEVOLVIDO') AS empate_devolvido,
        COUNT(*) FILTER (WHERE estado = 'ABERTO') AS abertos
      FROM sinais
    `);

    res.json({ ok: true, estatisticas: r.rows[0] });
  } catch (e) {
    res.status(500).json({ ok: false, erro: e.message });
  }
});

// Endpoint protegido para enviar resultados manualmente ou por integração.
app.post('/api/result', async (req, res) => {
  try {
    if (!INGEST_TOKEN) {
      return res.status(503).json({
        ok: false,
        erro: 'INGEST_TOKEN não está configurado no Render.'
      });
    }

    const token = req.headers['x-ingest-token'];
    if (token !== INGEST_TOKEN) {
      return res.status(401).json({ ok: false, erro: 'Não autorizado.' });
    }

    const resultado = req.body?.resultado ?? req.body?.result;
    const rodadaId = req.body?.rodada_id ?? req.body?.round_id ?? req.body?.id;
    const ocorridoEm = req.body?.ocorrido_em ?? req.body?.timestamp;

    const out = await registarResultado({
      resultado,
      rodadaId,
      ocorridoEm,
      origem: 'api-result'
    });

    res.status(201).json({ ok: true, ...out });
  } catch (e) {
    res.status(400).json({ ok: false, erro: e.message });
  }
});

app.get('/api/painel', async (req, res) => {
  const painelToken = process.env.PAINEL_TOKEN;

  if (painelToken && req.headers['x-panel-token'] !== painelToken) {
    return res.status(401).json({ ok: false, erro: 'Não autorizado.' });
  }

  try {
    const [resultados, sinais] = await Promise.all([
      pool.query(
        'SELECT * FROM resultados ORDER BY id DESC LIMIT 30'
      ),
      pool.query(
        'SELECT * FROM sinais ORDER BY id DESC LIMIT 30'
      )
    ]);

    res.json({
      ok: true,
      resultados: resultados.rows,
      sinais: sinais.rows,
      ultimaSincronizacao,
      ultimoErro
    });
  } catch (e) {
    res.status(500).json({ ok: false, erro: e.message });
  }
});

// ======================================================
// CICLO DE SINCRONIZAÇÃO
// ======================================================

async function ciclo() {
  if (cicloEmExecucao || !API_BACBO) return;

  cicloEmExecucao = true;

  try {
    await sincronizarAPI();
    ultimoErro = null;
  } catch (e) {
    ultimoErro = e.message;
    console.error('[SYNC]', e.message);
  } finally {
    cicloEmExecucao = false;
  }
}

// ======================================================
// ARRANQUE
// ======================================================

async function iniciar() {
  try {
    await pool.query('SELECT 1');
    await criarEstrutura();

    const server = app.listen(PORT, '0.0.0.0', () => {
      console.log(`[SERVER] Ativo na porta ${PORT}`);
      console.log(`[DB] PostgreSQL conectado`);
      console.log(`[TELEGRAM] Configurado: ${Boolean(BOT_TOKEN && CHAT_ID)}`);
      console.log(`[API BAC BO] Configurada: ${Boolean(API_BACBO)}`);

      if (!API_BACBO) {
        console.warn(
          '[AVISO] API_BACBO ausente. ' +
          'O servidor não consegue obter resultados ao vivo sozinho.'
        );
      }
      if (!INGEST_TOKEN) {
        console.warn(
          '[AVISO] INGEST_TOKEN ausente. ' +
          'A rota protegida /api/result ficará desativada.'
        );
      }
    });

    server.on('error', err => {
      console.error('[HTTP FATAL]', err);
      process.exit(1);
    });

    if (API_BACBO) {
      await ciclo();
      setInterval(ciclo, INTERVALO_SYNC_MS);
    }

    iniciado = true;
  } catch (e) {
    console.error('[FATAL] Não foi possível iniciar:', e.message);
    process.exit(1);
  }
}

process.on('unhandledRejection', err => {
  console.error('[UNHANDLED REJECTION]', err);
});

process.on('uncaughtException', err => {
  console.error('[UNCAUGHT EXCEPTION]', err);
  process.exit(1);
});

process.on('SIGTERM', async () => {
  console.log('[SERVER] A encerrar...');
  try {
    await pool.end();
  } finally {
    process.exit(0);
  }
});

iniciar();
