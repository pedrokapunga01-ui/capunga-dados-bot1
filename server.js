'use strict';

/**
 * CAPUNGA DADOS — BAC BO (reconfigurado)
 * Servidor com validação estatística e Gale máximo 2.
 *
 * Variáveis obrigatórias:
 *   DATABASE_URL
 *   BOT_TOKEN
 *   CHAT_ID
 *   INGEST_TOKEN
 *
 * Opcionais:
 *   API_BACBO          (URL da API de resultados; se vazio usa só POST /resultado)
 *   PORT               (default 3000)
 *   MIN_AMOSTRA        (default 300)
 *   MIN_ACERTO         (default 52)
 *   INTERVALO_SYNC_MS  (default 8000)
 *   MIN_INTERVAL_SINAL_MS (default 90000)
 *   SINAL_EXPIRA_MS    (default 480000 = 8 min)
 *   DATABASE_SSL       (default true)
 */

const express = require('express');
const cors = require('cors');
const axios = require('axios');
const { Pool } = require('pg');

const app = express();
app.use(cors());
app.use(express.json({ limit: '1mb' }));

const PORT = Number(process.env.PORT || 3000);
const DATABASE_URL = process.env.DATABASE_URL;
const BOT_TOKEN = (process.env.BOT_TOKEN || '').trim();
const CHAT_ID = (process.env.CHAT_ID || '').trim();
const INGEST_TOKEN = (process.env.INGEST_TOKEN || '').trim();
const API_BACBO = (process.env.API_BACBO || '').trim();

const MAX_GALE = 2;

const MIN_AMOSTRA = Math.max(80, Number(process.env.MIN_AMOSTRA || 300));
const MIN_ACERTO = Math.min(100, Math.max(0, Number(process.env.MIN_ACERTO || 52)));
const INTERVALO_SYNC_MS = Math.max(4000, Number(process.env.INTERVALO_SYNC_MS || 8000));
const MIN_INTERVAL_SINAL_MS = Math.max(30000, Number(process.env.MIN_INTERVAL_SINAL_MS || 90000));
const SINAL_EXPIRA_MS = Math.max(120000, Number(process.env.SINAL_EXPIRA_MS || 480000));

if (!DATABASE_URL) {
  console.error('[FATAL] Configura DATABASE_URL.');
  process.exit(1);
}
if (!INGEST_TOKEN) {
  console.error('[FATAL] Configura INGEST_TOKEN.');
  process.exit(1);
}

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: process.env.DATABASE_SSL === 'false' ? false : { rejectUnauthorized: false },
  max: 8,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000
});

let sincronizando = false;
let ultimaTentativaSinal = 0;
let timerSync = null;
let lastSyncAt = null;
let lastError = null;
let totalProcessados = 0;

// ─── Helpers ───────────────────────────────────────────────────────────────

function normalizarResultado(valor) {
  if (valor == null) return null;
  if (typeof valor === 'object') {
    valor = valor.outcome ?? valor.resultado ?? valor.winner ?? valor.color ?? valor.name ?? '';
  }
  const v = String(valor).normalize('NFD').replace(/[\u0300-\u036f]/g, '').trim().toLowerCase().replace(/[\s_-]+/g, '');

  // RED / Banca / Banker
  if (['red', 'vermelho', 'r', 'banca', 'banker', 'bankerwin', 'bankerwins', 'bankerwon'].includes(v) || v.startsWith('banker')) {
    return 'RED';
  }
  // BLUE / Jogador / Player
  if (['blue', 'azul', 'b', 'jogador', 'player', 'playerwin', 'playerwins', 'playerwon'].includes(v) || v.startsWith('player')) {
    return 'BLUE';
  }
  // TIE / Empate
  if (['tie', 'empate', 'draw', 't', 'equal', 'push', 'tied'].includes(v) || v.startsWith('tie') || v.startsWith('empate')) {
    return 'TIE';
  }
  return null;
}

function normalizarId(valor) {
  if (valor === undefined || valor === null) return null;
  const id = String(valor).trim();
  return id ? id.slice(0, 180) : null;
}

function labelCor(cor) {
  if (cor === 'RED') return '🔴 BANCA';
  if (cor === 'BLUE') return '🔵 JOGADOR';
  return '🟡 EMPATE';
}

// ─── Telegram ──────────────────────────────────────────────────────────────

async function telegram(texto) {
  if (!BOT_TOKEN || !CHAT_ID) {
    console.warn('[TELEGRAM] BOT_TOKEN ou CHAT_ID ausente.');
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
    return response.data?.ok === true;
  } catch (err) {
    console.error('[TELEGRAM]', err.response?.data || err.message);
    lastError = `telegram: ${err.message}`;
    return false;
  }
}

// ─── Schema ────────────────────────────────────────────────────────────────

async function criarTabelas() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS bacbo_resultados (
      id BIGSERIAL PRIMARY KEY,
      rodada_id TEXT UNIQUE NOT NULL,
      resultado TEXT NOT NULL CHECK (resultado IN ('RED', 'BLUE', 'TIE')),
      ocorrido_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_bacbo_resultados_data ON bacbo_resultados (id DESC)`);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS bacbo_testes (
      id BIGSERIAL PRIMARY KEY,
      rodada_id TEXT UNIQUE NOT NULL,
      previsao TEXT NOT NULL CHECK (previsao IN ('RED', 'BLUE')),
      estado TEXT NOT NULL DEFAULT 'PENDENTE' CHECK (estado IN ('PENDENTE', 'GREEN', 'LOSS')),
      resultado TEXT,
      criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      resolvido_em TIMESTAMPTZ
    )
  `);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_bacbo_testes_estado ON bacbo_testes (estado, id DESC)`);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS sinais (
      id BIGSERIAL PRIMARY KEY,
      rodada_origem TEXT NOT NULL,
      previsao TEXT NOT NULL CHECK (previsao IN ('RED', 'BLUE')),
      estado TEXT NOT NULL DEFAULT 'PENDENTE'
        CHECK (estado IN ('PENDENTE', 'GREEN', 'LOSS', 'CANCELADO')),
      gale INTEGER NOT NULL DEFAULT 0,
      resultado_final TEXT,
      taxa_teste NUMERIC(6,2),
      amostra_teste INTEGER,
      criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      resolvido_em TIMESTAMPTZ
    )
  `);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_sinais_estado ON sinais (estado, id DESC)`);

  console.log('[DB] Tabelas verificadas.');
}

// ─── Leitura ───────────────────────────────────────────────────────────────

async function obterUltimosResultados(limite = 20) {
  const { rows } = await pool.query(
    `SELECT rodada_id, resultado, ocorrido_em
     FROM bacbo_resultados
     ORDER BY id DESC
     LIMIT $1`,
    [limite]
  );
  return rows.reverse();
}

async function obterSinalPendente() {
  const { rows } = await pool.query(`
    SELECT * FROM sinais
    WHERE estado = 'PENDENTE'
    ORDER BY id ASC
    LIMIT 1
  `);
  return rows[0] || null;
}

// ─── Expirar sinais presos ─────────────────────────────────────────────────

async function expirarSinaisAntigos() {
  const { rows } = await pool.query(
    `UPDATE sinais
     SET estado = 'CANCELADO', resolvido_em = NOW()
     WHERE estado = 'PENDENTE'
       AND criado_em < NOW() - ($1::text || ' milliseconds')::interval
     RETURNING id, previsao`,
    [String(SINAL_EXPIRA_MS)]
  );
  for (const s of rows) {
    await telegram(
      `⚠️ SINAL ANULADO · EXPIRADO\n` +
      `Sinal #${s.id}\n` +
      `Previsão: ${labelCor(s.previsao)}\n` +
      `Não foi contado como Green nem Loss.`
    );
    console.log('[EXPIRA] Sinal', s.id, 'cancelado por timeout');
  }
}

// ─── Lógica de previsão (experimental, seletiva) ───────────────────────────

/**
 * Gera previsão só em cenários de forte desequilíbrio recente.
 * Não promete edge; serve apenas como filtro experimental.
 */
function gerarPrevisao(resultados) {
  const decisivos = resultados
    .map(r => r.resultado)
    .filter(r => r === 'RED' || r === 'BLUE');

  if (decisivos.length < 10) return null;

  const janela12 = decisivos.slice(-12);
  const janela8 = decisivos.slice(-8);
  const janela6 = decisivos.slice(-6);

  const contar = (arr, cor) => arr.filter(x => x === cor).length;

  const r12 = contar(janela12, 'RED');
  const b12 = janela12.length - r12;
  const r8 = contar(janela8, 'RED');
  const b8 = janela8.length - r8;
  const r6 = contar(janela6, 'RED');
  const b6 = janela6.length - r6;

  // Pressão forte na janela longa
  if (r12 >= 10) return 'RED';
  if (b12 >= 10) return 'BLUE';

  // Pressão média + confirmação na janela curta
  if (r8 >= 7 && r6 >= 5) return 'RED';
  if (b8 >= 7 && b6 >= 5) return 'BLUE';

  // Streak atual muito longa
  let streak = 1;
  const ultimo = decisivos[decisivos.length - 1];
  for (let i = decisivos.length - 2; i >= 0; i--) {
    if (decisivos[i] === ultimo) streak++;
    else break;
  }
  if (streak >= 6) return ultimo; // continua a streak

  return null;
}

// ─── Testes internos (calibração) ──────────────────────────────────────────

async function resolverTestes(resultadoAtual, rodadaAtualId) {
  const { rows } = await pool.query(`
    SELECT id, rodada_id, previsao
    FROM bacbo_testes
    WHERE estado = 'PENDENTE'
    ORDER BY id ASC
  `);

  for (const teste of rows) {
    if (teste.rodada_id === rodadaAtualId) continue;

    const green = resultadoAtual === 'TIE' || resultadoAtual === teste.previsao;

    await pool.query(
      `UPDATE bacbo_testes
       SET estado = $1, resultado = $2, resolvido_em = NOW()
       WHERE id = $3 AND estado = 'PENDENTE'`,
      [green ? 'GREEN' : 'LOSS', resultadoAtual, teste.id]
    );
  }
}

async function obterEstatisticasTeste() {
  const { rows } = await pool.query(`
    SELECT estado
    FROM bacbo_testes
    WHERE estado IN ('GREEN', 'LOSS')
    ORDER BY resolvido_em DESC, id DESC
    LIMIT 200
  `);

  const amostra = rows.length;
  const greens = rows.filter(r => r.estado === 'GREEN').length;
  const taxa = amostra ? (greens * 100) / amostra : 0;

  const recentes = rows.slice(0, 40);
  const greensRecentes = recentes.filter(r => r.estado === 'GREEN').length;
  const taxaRecente = recentes.length ? (greensRecentes * 100) / recentes.length : 0;

  return {
    amostra,
    greens,
    losses: amostra - greens,
    taxa: Number(taxa.toFixed(2)),
    amostra40: recentes.length,
    taxa40: Number(taxaRecente.toFixed(2)),
    autorizado:
      amostra >= MIN_AMOSTRA &&
      recentes.length >= 30 &&
      taxa >= MIN_ACERTO &&
      taxaRecente >= (MIN_ACERTO - 3)
  };
}

async function criarTeste(rodadaId, previsao) {
  await pool.query(
    `INSERT INTO bacbo_testes (rodada_id, previsao)
     VALUES ($1, $2)
     ON CONFLICT (rodada_id) DO NOTHING`,
    [rodadaId, previsao]
  );
}

// ─── Resolução de sinais ───────────────────────────────────────────────────

async function resolverSinal(resultado, rodadaId) {
  const sinal = await obterSinalPendente();
  if (!sinal) return;
  if (sinal.rodada_origem === rodadaId) return;

  // Empate protegido
  if (resultado === 'TIE') {
    await pool.query(
      `UPDATE sinais
       SET estado = 'GREEN', resultado_final = 'TIE', resolvido_em = NOW()
       WHERE id = $1 AND estado = 'PENDENTE'`,
      [sinal.id]
    );
    await telegram(
      `🟢 GREEN · EMPATE PROTEGIDO\n` +
      `Sinal #${sinal.id}\n` +
      `Previsão: ${labelCor(sinal.previsao)}\n` +
      `Gale usado: ${sinal.gale}`
    );
    return;
  }

  if (resultado === sinal.previsao) {
    await pool.query(
      `UPDATE sinais
       SET estado = 'GREEN', resultado_final = $2, resolvido_em = NOW()
       WHERE id = $1 AND estado = 'PENDENTE'`,
      [sinal.id, resultado]
    );
    await telegram(
      `🟢 GREEN · CAPUNGA DADOS\n` +
      `Sinal #${sinal.id}\n` +
      `Previsão: ${labelCor(sinal.previsao)}\n` +
      `Resultado: ${labelCor(resultado)}\n` +
      `Gale usado: ${sinal.gale}`
    );
    return;
  }

  // Errou → Gale
  if (Number(sinal.gale) < MAX_GALE) {
    const novoGale = Number(sinal.gale) + 1;
    await pool.query(
      `UPDATE sinais SET gale = $2 WHERE id = $1 AND estado = 'PENDENTE'`,
      [sinal.id, novoGale]
    );
    await telegram(
      `🐔 GALE ${novoGale}\n` +
      `Sinal #${sinal.id}\n` +
      `Manter: ${labelCor(sinal.previsao)}\n` +
      `Resultado visto: ${labelCor(resultado)}`
    );
    return;
  }

  // Loss final
  await pool.query(
    `UPDATE sinais
     SET estado = 'LOSS', resultado_final = $2, resolvido_em = NOW()
     WHERE id = $1 AND estado = 'PENDENTE'`,
    [sinal.id, resultado]
  );
  await telegram(
    `🔴 LOSS · GALE MÁXIMO\n` +
    `Sinal #${sinal.id}\n` +
    `Previsão: ${labelCor(sinal.previsao)}\n` +
    `Resultado: ${labelCor(resultado)}\n` +
    `Gales: ${MAX_GALE}`
  );
}

// ─── Envio de novo sinal ───────────────────────────────────────────────────

async function tentarEnviarSinal(rodadaId, previsao) {
  if (!previsao) return;
  if (await obterSinalPendente()) return;
  if (Date.now() - ultimaTentativaSinal < MIN_INTERVAL_SINAL_MS) return;

  const stats = await obterEstatisticasTeste();
  if (!stats.autorizado) {
    console.log('[FILTRO] Bloqueado:', JSON.stringify({
      amostra: stats.amostra,
      taxa: stats.taxa,
      taxa40: stats.taxa40,
      minAmostra: MIN_AMOSTRA,
      minAcerto: MIN_ACERTO
    }));
    return;
  }

  const insert = await pool.query(
    `INSERT INTO sinais (rodada_origem, previsao, taxa_teste, amostra_teste)
     VALUES ($1, $2, $3, $4)
     RETURNING id`,
    [rodadaId, previsao, stats.taxa, stats.amostra]
  );
  const sinal = insert.rows[0];
  ultimaTentativaSinal = Date.now();

  const enviado = await telegram(
    `🎲 CAPUNGA DADOS · BAC BO\n\n` +
    `📢 ENTRADA\n` +
    `Previsão: ${labelCor(previsao)}\n` +
    `Gale máximo: ${MAX_GALE}\n` +
    `Empate protegido\n\n` +
    `📊 Testes: ${stats.amostra} · Taxa ${stats.taxa}%\n` +
    `Últimos 40: ${stats.taxa40}%\n\n` +
    `ID: ${sinal.id}`
  );

  if (!enviado) {
    console.error('[TELEGRAM] Falha no envio do sinal', sinal.id);
  } else {
    console.log('[SINAL] Enviado #', sinal.id, previsao);
  }
}

// ─── Processamento de uma rodada ───────────────────────────────────────────

async function processarResultado(rodadaId, resultado, ocorridoEm) {
  const id = normalizarId(rodadaId);
  const res = normalizarResultado(resultado);

  if (!id || !res) {
    throw new Error('rodada_id ou resultado inválido.');
  }

  const insert = await pool.query(
    `INSERT INTO bacbo_resultados (rodada_id, resultado, ocorrido_em)
     VALUES ($1, $2, COALESCE($3::timestamptz, NOW()))
     ON CONFLICT (rodada_id) DO NOTHING
     RETURNING id`,
    [id, res, ocorridoEm || null]
  );

  if (!insert.rowCount) {
    return { duplicado: true, rodada_id: id };
  }

  totalProcessados += 1;

  // 1. Expira sinais antigos
  await expirarSinaisAntigos();

  // 2. Resolve testes e sinal pendente com este resultado
  await resolverTestes(res, id);
  await resolverSinal(res, id);

  // 3. Gera nova previsão para o futuro
  const historico = await obterUltimosResultados(20);
  const previsao = gerarPrevisao(historico);

  if (previsao) {
    await criarTeste(id, previsao);
    await tentarEnviarSinal(id, previsao);
  }

  return {
    duplicado: false,
    rodada_id: id,
    resultado: res,
    previsao_teste: previsao
  };
}

// ─── Auth ──────────────────────────────────────────────────────────────────

function autenticarEntrada(req, res, next) {
  if (req.get('x-ingest-token') !== INGEST_TOKEN) {
    return res.status(401).json({ ok: false, erro: 'Token inválido.' });
  }
  next();
}

// ─── Rotas ─────────────────────────────────────────────────────────────────

app.get('/', (_req, res) => {
  res.json({
    nome: 'CAPUNGA DADOS — BAC BO',
    estado: 'online',
    gale_maximo: MAX_GALE,
    versao: '2.0-reconfig'
  });
});

app.get('/health', async (_req, res) => {
  try {
    await pool.query('SELECT 1');
    res.json({
      ok: true,
      database: 'connected',
      lastSyncAt,
      totalProcessados,
      lastError,
      telegramConfigured: Boolean(BOT_TOKEN && CHAT_ID)
    });
  } catch (err) {
    res.status(503).json({ ok: false, database: 'error', erro: err.message });
  }
});

app.get('/status', async (_req, res) => {
  try {
    const stats = await obterEstatisticasTeste();
    const pendente = await obterSinalPendente();

    const { rows: totais } = await pool.query(`
      SELECT
        COUNT(*) FILTER (WHERE estado = 'GREEN')::int AS greens,
        COUNT(*) FILTER (WHERE estado = 'LOSS')::int AS losses,
        COUNT(*) FILTER (WHERE estado = 'PENDENTE')::int AS pendentes,
        COUNT(*) FILTER (WHERE estado = 'CANCELADO')::int AS cancelados
      FROM sinais
    `);

    const { rows: rodadas } = await pool.query(`SELECT COUNT(*)::int AS total FROM bacbo_resultados`);

    res.json({
      ok: true,
      estrategia_autorizada: stats.autorizado,
      criterios: {
        amostra_minima: MIN_AMOSTRA,
        taxa_minima: MIN_ACERTO,
        gale_maximo: MAX_GALE,
        intervalo_min_sinal_ms: MIN_INTERVAL_SINAL_MS
      },
      testes: stats,
      sinais: totais[0],
      sinal_pendente: pendente,
      rodadas_registadas: rodadas[0].total,
      lastSyncAt,
      totalProcessados
    });
  } catch (err) {
    res.status(500).json({ ok: false, erro: err.message });
  }
});

app.get('/estatisticas', async (_req, res) => {
  try {
    const testes = await obterEstatisticasTeste();
    const { rows } = await pool.query(`
      SELECT estado, COUNT(*)::int AS total
      FROM sinais
      GROUP BY estado
      ORDER BY estado
    `);
    res.json({ ok: true, testes, sinais_por_estado: rows });
  } catch (err) {
    res.status(500).json({ ok: false, erro: err.message });
  }
});

app.post('/resultado', autenticarEntrada, async (req, res) => {
  try {
    const { rodada_id, resultado, ocorrido_em } = req.body || {};
    const dados = await processarResultado(rodada_id, resultado, ocorrido_em);
    res.json({ ok: true, ...dados });
  } catch (err) {
    console.error('[RESULTADO]', err.message);
    res.status(400).json({ ok: false, erro: err.message });
  }
});

// ─── Sync da API externa ───────────────────────────────────────────────────

function extrairRodadas(payload) {
  const candidatos = [
    payload,
    payload?.data,
    payload?.results,
    payload?.rounds,
    payload?.history,
    payload?.items,
    payload?.data?.results,
    payload?.data?.rounds,
    payload?.data?.history,
    payload?.data?.items
  ];

  const lista = candidatos.find(Array.isArray);
  if (!lista) return [];

  return lista
    .map(item => {
      const data = item?.data && typeof item.data === 'object' ? item.data : item;
      const result = data?.result && typeof data.result === 'object' ? data.result : data;

      return {
        rodada_id:
          data?.id ??
          data?.roundId ??
          data?.round_id ??
          item?.id ??
          item?.roundId ??
          item?.gameId,
        resultado:
          result?.outcome ??
          result?.resultado ??
          result?.winner ??
          data?.outcome ??
          data?.resultado ??
          data?.winner ??
          data?.result ??
          item?.result ??
          item?.winner ??
          item?.color,
        ocorrido_em:
          data?.settledAt ??
          data?.finishedAt ??
          data?.endedAt ??
          data?.timestamp ??
          data?.createdAt ??
          item?.settledAt ??
          item?.timestamp
      };
    })
    .filter(item => normalizarId(item.rodada_id) && normalizarResultado(item.resultado));
}

async function sincronizarFonte() {
  if (!API_BACBO || sincronizando) return;
  sincronizando = true;

  try {
    const response = await axios.get(API_BACBO, {
      timeout: 12000,
      headers: {
        Accept: 'application/json',
        'User-Agent': 'CapungaDados/2.0'
      }
    });

    const rodadas = extrairRodadas(response.data);
    if (!rodadas.length) {
      console.warn('[FONTE] Nenhuma rodada reconhecida. Verifica o formato da API.');
      lastError = 'api_sem_rodadas';
      return;
    }

    // Processa da mais antiga para a mais recente
    for (const rodada of rodadas.reverse()) {
      await processarResultado(rodada.rodada_id, rodada.resultado, rodada.ocorrido_em);
    }

    lastSyncAt = new Date().toISOString();
    lastError = null;
  } catch (err) {
    console.error('[SYNC]', err.response?.status || '', err.message);
    lastError = `sync: ${err.message}`;
  } finally {
    sincronizando = false;
  }
}

// ─── Arranque ──────────────────────────────────────────────────────────────

async function iniciar() {
  await criarTabelas();

  const server = app.listen(PORT, '0.0.0.0', () => {
    console.log(`[HTTP] CAPUNGA DADOS ativo na porta ${PORT}`);
  });

  if (API_BACBO) {
    console.log('[FONTE] A sincronizar com', API_BACBO.slice(0, 60) + '...');
    await sincronizarFonte();
    timerSync = setInterval(sincronizarFonte, INTERVALO_SYNC_MS);
  } else {
    console.warn('[FONTE] API_BACBO ausente. Usa POST /resultado com x-ingest-token.');
  }

  // Limpeza periódica de sinais expirados
  setInterval(() => {
    expirarSinaisAntigos().catch(err => console.error('[EXPIRA]', err.message));
  }, 60000).unref();

  let encerrando = false;
  async function encerrar() {
    if (encerrando) return;
    encerrando = true;
    console.log('[SHUTDOWN] A encerrar...');
    if (timerSync) clearInterval(timerSync);
    server.close(async () => {
      try { await pool.end(); } finally { process.exit(0); }
    });
    setTimeout(() => process.exit(1), 8000).unref();
  }

  process.on('SIGTERM', encerrar);
  process.on('SIGINT', encerrar);
}

iniciar().catch(err => {
  console.error('[FATAL]', err);
  process.exit(1);
});
