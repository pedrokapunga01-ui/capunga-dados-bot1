'use strict';

/**
 * CAPUNGA DADOS — BAC BO
 * Servidor com validação estatística e Gale máximo 2.
 *
 * Variáveis necessárias:
 * DATABASE_URL
 * BOT_TOKEN
 * CHAT_ID
 * INGEST_TOKEN
 *
 * Opcionais:
 * API_BACBO
 * PORT
 * MIN_AMOSTRA
 * MIN_ACERTO
 * INTERVALO_SYNC_MS
 * MIN_INTERVAL_SINAL_MS
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
const BOT_TOKEN = process.env.BOT_TOKEN || '';
const CHAT_ID = process.env.CHAT_ID || '';
const INGEST_TOKEN = process.env.INGEST_TOKEN;

const API_BACBO = process.env.API_BACBO || '';
const MAX_GALE = 2;

const MIN_AMOSTRA = Math.max(
  100,
  Number(process.env.MIN_AMOSTRA || 500)
);

const MIN_ACERTO = Math.min(
  100,
  Math.max(0, Number(process.env.MIN_ACERTO || 55))
);

const INTERVALO_SYNC_MS = Math.max(
  5000,
  Number(process.env.INTERVALO_SYNC_MS || 10000)
);

const MIN_INTERVAL_SINAL_MS = Math.max(
  120000,
  Number(process.env.MIN_INTERVAL_SINAL_MS || 120000)
);

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
  ssl: process.env.DATABASE_SSL === 'false'
    ? false
    : { rejectUnauthorized: false },
  max: 5,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000
});

let sincronizando = false;
let ultimaTentativaSinal = 0;
let timerSync = null;

function normalizarResultado(valor) {
  const v = String(valor || '').trim().toLowerCase();

  if (['red', 'vermelho', 'r'].includes(v)) return 'RED';
  if (['blue', 'azul', 'b'].includes(v)) return 'BLUE';
  if (['tie', 'empate', 'draw', 't'].includes(v)) return 'TIE';

  return null;
}

function normalizarId(valor) {
  if (valor === undefined || valor === null) return null;

  const id = String(valor).trim();

  return id ? id.slice(0, 180) : null;
}

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
      { timeout: 15000 }
    );

    return response.data?.ok === true;
  } catch (err) {
    console.error(
      '[TELEGRAM]',
      err.response?.data || err.message
    );

    return false;
  }
}

async function criarTabelas() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS bacbo_resultados (
      id BIGSERIAL PRIMARY KEY,
      rodada_id TEXT UNIQUE NOT NULL,
      resultado TEXT NOT NULL
        CHECK (resultado IN ('RED', 'BLUE', 'TIE')),
      ocorrido_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_bacbo_resultados_data
    ON bacbo_resultados (id DESC)
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS bacbo_testes (
      id BIGSERIAL PRIMARY KEY,
      rodada_id TEXT UNIQUE NOT NULL,
      previsao TEXT NOT NULL
        CHECK (previsao IN ('RED', 'BLUE')),
      estado TEXT NOT NULL DEFAULT 'PENDENTE'
        CHECK (estado IN ('PENDENTE', 'GREEN', 'LOSS')),
      resultado TEXT,
      criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      resolvido_em TIMESTAMPTZ
    )
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_bacbo_testes_estado
    ON bacbo_testes (estado, id DESC)
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS sinais (
      id BIGSERIAL PRIMARY KEY,
      rodada_origem TEXT NOT NULL,
      previsao TEXT NOT NULL
        CHECK (previsao IN ('RED', 'BLUE')),
      estado TEXT NOT NULL DEFAULT 'PENDENTE'
        CHECK (
          estado IN ('PENDENTE', 'GREEN', 'LOSS', 'CANCELADO')
        ),
      gale INTEGER NOT NULL DEFAULT 0,
      resultado_final TEXT,
      taxa_teste NUMERIC(6,2),
      amostra_teste INTEGER,
      criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      resolvido_em TIMESTAMPTZ
    )
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_sinais_estado
    ON sinais (estado, id DESC)
  `);

  console.log('[DB] Tabelas verificadas.');
}

async function obterUltimosResultados(limite = 12) {
  const { rows } = await pool.query(
    `SELECT rodada_id, resultado, ocorrido_em
     FROM bacbo_resultados
     ORDER BY id DESC
     LIMIT $1`,
    [limite]
  );

  return rows.reverse();
}

/**
 * Filtro experimental de desequilíbrio recente.
 * Não é um modelo preditivo validado: resultados de Bac Bo podem ser
 * independentes e padrões passados não garantem vantagem futura.
 * Exige uma janela decisiva altamente desequilibrada; caso contrário abstém-se.
 */
function gerarPrevisao(resultados) {
  const decisivos = resultados
    .map(item => item.resultado)
    .filter(item => item === 'RED' || item === 'BLUE');

  if (decisivos.length < 12) return null;

  const janela = decisivos.slice(-12);
  const reds = janela.filter(item => item === 'RED').length;
  const blues = janela.length - reds;

  // Segue o lado mais frequente apenas como hipótese experimental.
  // A alta seletividade reduz volume; não aumenta a certeza do próximo resultado.
  if (reds >= 10) return 'RED';
  if (blues >= 10) return 'BLUE';
  return null;
}

async function obterSinalPendente() {
  const { rows } = await pool.query(`
    SELECT *
    FROM sinais
    WHERE estado = 'PENDENTE'
    ORDER BY id ASC
    LIMIT 1
  `);

  return rows[0] || null;
}

async function resolverTestes(resultadoAtual, rodadaAtualId) {
  const { rows } = await pool.query(`
    SELECT id, rodada_id, previsao
    FROM bacbo_testes
    WHERE estado = 'PENDENTE'
    ORDER BY id ASC
  `);

  for (const teste of rows) {
    // Nunca avaliar uma previsão usando o próprio resultado
    // que originou a previsão.
    if (teste.rodada_id === rodadaAtualId) continue;

    const green =
      resultadoAtual === 'TIE' ||
      resultadoAtual === teste.previsao;

    await pool.query(
      `UPDATE bacbo_testes
       SET estado = $1,
           resultado = $2,
           resolvido_em = NOW()
       WHERE id = $3 AND estado = 'PENDENTE'`,
      [
        green ? 'GREEN' : 'LOSS',
        resultadoAtual,
        teste.id
      ]
    );
  }
}

async function obterEstatisticasTeste() {
  const { rows } = await pool.query(`
    SELECT estado
    FROM bacbo_testes
    WHERE estado IN ('GREEN', 'LOSS')
    ORDER BY resolvido_em DESC, id DESC
    LIMIT 100
  `);

  const amostra = rows.length;
  const greens = rows.filter(r => r.estado === 'GREEN').length;
  const taxa = amostra ? greens * 100 / amostra : 0;

  const recentes = rows.slice(0, 50);
  const greensRecentes = recentes.filter(
    r => r.estado === 'GREEN'
  ).length;

  const taxaRecente = recentes.length
    ? greensRecentes * 100 / recentes.length
    : 0;

  return {
    amostra,
    greens,
    losses: amostra - greens,
    taxa: Number(taxa.toFixed(2)),
    amostra50: recentes.length,
    taxa50: Number(taxaRecente.toFixed(2)),
    autorizado:
      amostra >= MIN_AMOSTRA &&
      recentes.length >= 50 &&
      taxa >= MIN_ACERTO &&
      taxaRecente >= MIN_ACERTO
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

async function resolverSinal(resultado, rodadaId) {
  const sinal = await obterSinalPendente();

  if (!sinal) return;

  // Não contar a rodada que originou o sinal como resultado.
  if (sinal.rodada_origem === rodadaId) return;

  // Empate protegido: fecha como GREEN.
  if (resultado === 'TIE') {
    await pool.query(
      `UPDATE sinais
       SET estado = 'GREEN',
           resultado_final = 'TIE',
           resolvido_em = NOW()
       WHERE id = $1 AND estado = 'PENDENTE'`,
      [sinal.id]
    );

    await telegram(
      `🟢 GREEN — EMPATE PROTEGIDO\n` +
      `Sinal #${sinal.id}\n` +
      `Gale utilizado: ${sinal.gale}`
    );

    return;
  }

  if (resultado === sinal.previsao) {
    await pool.query(
      `UPDATE sinais
       SET estado = 'GREEN',
           resultado_final = $2,
           resolvido_em = NOW()
       WHERE id = $1 AND estado = 'PENDENTE'`,
      [sinal.id, resultado]
    );

    await telegram(
      `🟢 GREEN — CAPUNGA DADOS\n` +
      `Sinal #${sinal.id}\n` +
      `Previsão: ${sinal.previsao}\n` +
      `Resultado: ${resultado}\n` +
      `Gale utilizado: ${sinal.gale}`
    );

    return;
  }

  if (Number(sinal.gale) < MAX_GALE) {
    const novoGale = Number(sinal.gale) + 1;

    await pool.query(
      `UPDATE sinais SET gale = $2
       WHERE id = $1 AND estado = 'PENDENTE'`,
      [sinal.id, novoGale]
    );

    await telegram(
      `⚠️ GALE ${novoGale}\n` +
      `Sinal #${sinal.id}\n` +
      `Manter previsão: ${sinal.previsao}\n` +
      `Resultado observado: ${resultado}\n` +
      `Recuperação não garantida.`
    );

    return;
  }

  await pool.query(
    `UPDATE sinais
     SET estado = 'LOSS',
         resultado_final = $2,
         resolvido_em = NOW()
     WHERE id = $1 AND estado = 'PENDENTE'`,
    [sinal.id, resultado]
  );

  await telegram(
    `🔴 LOSS — GALE MÁXIMO ATINGIDO\n` +
    `Sinal #${sinal.id}\n` +
    `Previsão: ${sinal.previsao}\n` +
    `Resultado: ${resultado}\n` +
    `Gales utilizados: ${MAX_GALE}`
  );
}

async function tentarEnviarSinal(rodadaId, previsao) {
  if (!previsao) return;

  if (await obterSinalPendente()) return;

  if (Date.now() - ultimaTentativaSinal < MIN_INTERVAL_SINAL_MS) {
    return;
  }

  const stats = await obterEstatisticasTeste();

  if (!stats.autorizado) {
    console.log(
      '[FILTRO] Sinal bloqueado:',
      JSON.stringify(stats)
    );
    return;
  }

  const insert = await pool.query(
    `INSERT INTO sinais
       (rodada_origem, previsao, taxa_teste, amostra_teste)
     VALUES ($1, $2, $3, $4)
     RETURNING id`,
    [rodadaId, previsao, stats.taxa, stats.amostra]
  );

  const sinal = insert.rows[0];

  // Evita tentativas em sequência, mesmo se o Telegram falhar.
  ultimaTentativaSinal = Date.now();

  const enviado = await telegram(
    `🎲 CAPUNGA DADOS — BAC BO | EXPERIMENTAL\n\n` +
    `📢 HIPÓTESE DE SINAL (não é garantia)\n` +
    `Previsão: ${previsao}\n` +
    `Gale máximo: ${MAX_GALE}\n` +
    `Empate: protegido\n\n` +
    `📊 Testes resolvidos: ${stats.amostra}\n` +
    `Taxa histórica observada: ${stats.taxa}% (n=${stats.amostra})\n` +
    `Últimos 50 testes: ${stats.taxa50}%\n\n` +
    `⚠️ Frequência histórica não é probabilidade nem garantia do próximo resultado.\n` +
    `ID do sinal: ${sinal.id}`
  );

  if (!enviado) {
    console.error('[TELEGRAM] Falha no envio do sinal:', sinal.id);
  }
}

async function processarResultado(rodadaId, resultado, ocorridoEm) {
  const id = normalizarId(rodadaId);
  const res = normalizarResultado(resultado);

  if (!id || !res) {
    throw new Error('rodada_id ou resultado inválido.');
  }

  const insert = await pool.query(
    `INSERT INTO bacbo_resultados
       (rodada_id, resultado, ocorrido_em)
     VALUES ($1, $2, COALESCE($3::timestamptz, NOW()))
     ON CONFLICT (rodada_id) DO NOTHING
     RETURNING id`,
    [id, res, ocorridoEm || null]
  );

  if (!insert.rowCount) {
    return { duplicado: true, rodada_id: id };
  }

  // Primeiro avalia as previsões anteriores.
  await resolverTestes(res, id);
  await resolverSinal(res, id);

  // Só depois cria a previsão para futuras rodadas.
  const historico = await obterUltimosResultados(12);
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

function autenticarEntrada(req, res, next) {
  if (req.get('x-ingest-token') !== INGEST_TOKEN) {
    return res.status(401).json({
      ok: false,
      erro: 'Token inválido.'
    });
  }

  next();
}

app.get('/', (_req, res) => {
  res.json({
    nome: 'CAPUNGA DADOS — BAC BO | experimental',
    aviso: 'Sem garantia de acerto; jogo aleatório envolve risco de perda.',
    estado: 'online',
    gale_maximo: MAX_GALE,
    garantia_100: false
  });
});

app.get('/health', async (_req, res) => {
  try {
    await pool.query('SELECT 1');
    res.json({ ok: true, database: 'connected' });
  } catch (err) {
    res.status(503).json({
      ok: false,
      database: 'error',
      erro: err.message
    });
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
        COUNT(*) FILTER (WHERE estado = 'PENDENTE')::int AS pendentes
      FROM sinais
    `);

    const { rows: rodadas } = await pool.query(`
      SELECT COUNT(*)::int AS total
      FROM bacbo_resultados
    `);

    res.json({
      ok: true,
      estrategia_autorizada: stats.autorizado,
      aviso: 'Sinais experimentais; não representam probabilidade calibrada nem garantia de acerto.',
      criterios: {
        amostra_minima: MIN_AMOSTRA,
        taxa_minima: MIN_ACERTO,
        gale_maximo: MAX_GALE
      },
      testes: stats,
      sinais: totais[0],
      sinal_pendente: pendente,
      rodadas_registadas: rodadas[0].total
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

    res.json({
      ok: true,
      testes,
      sinais_por_estado: rows
    });
  } catch (err) {
    res.status(500).json({ ok: false, erro: err.message });
  }
});

app.post('/resultado', autenticarEntrada, async (req, res) => {
  try {
    const { rodada_id, resultado, ocorrido_em } = req.body || {};

    const dados = await processarResultado(
      rodada_id,
      resultado,
      ocorrido_em
    );

    res.json({ ok: true, ...dados });
  } catch (err) {
    console.error('[RESULTADO]', err.message);

    res.status(400).json({
      ok: false,
      erro: err.message
    });
  }
});

/**
 * Extrator genérico.
 * Confirma a estrutura da API antes de depender dela.
 */
function extrairRodadas(payload) {
  const candidatos = [
    payload,
    payload?.data,
    payload?.results,
    payload?.rounds,
    payload?.history,
    payload?.data?.results,
    payload?.data?.rounds,
    payload?.data?.history
  ];

  const lista = candidatos.find(Array.isArray);

  if (!lista) return [];

  return lista.map(item => ({
    rodada_id:
      item?.roundId ??
      item?.round_id ??
      item?.id ??
      item?.gameId ??
      item?.game_id,
    resultado:
      item?.result ??
      item?.winner ??
      item?.outcome ??
      item?.color ??
      item?.winningSide,
    ocorrido_em:
      item?.timestamp ??
      item?.createdAt ??
      item?.created_at
  })).filter(item =>
    normalizarId(item.rodada_id) &&
    normalizarResultado(item.resultado)
  );
}

async function sincronizarFonte() {
  if (!API_BACBO || sincronizando) return;

  sincronizando = true;

  try {
    const response = await axios.get(API_BACBO, {
      timeout: 15000,
      headers: { Accept: 'application/json' }
    });

    const rodadas = extrairRodadas(response.data);

    if (!rodadas.length) {
      console.warn(
        '[FONTE] Nenhuma rodada reconhecida. ' +
        'Verifica o formato real da API.'
      );
      return;
    }

    // Processa da mais antiga para a mais recente quando
    // a API entrega a lista em ordem decrescente.
    for (const rodada of rodadas.reverse()) {
      await processarResultado(
        rodada.rodada_id,
        rodada.resultado,
        rodada.ocorrido_em
      );
    }
  } catch (err) {
    console.error(
      '[SYNC]',
      err.response?.status || '',
      err.message
    );
  } finally {
    sincronizando = false;
  }
}

async function iniciar() {
  await criarTabelas();

  const server = app.listen(PORT, () => {
    console.log(`[HTTP] Ativo na porta ${PORT}`);
  });

  if (API_BACBO) {
    await sincronizarFonte();
    timerSync = setInterval(sincronizarFonte, INTERVALO_SYNC_MS);
  } else {
    console.warn(
      '[FONTE] API_BACBO ausente. ' +
      'Aguarda resultados em POST /resultado. Sem fonte não há análise nem sinais.'
    );
  }

  let encerrando = false;

  async function encerrar() {
    if (encerrando) return;
    encerrando = true;

    console.log('[SHUTDOWN] A encerrar o servidor.');

    if (timerSync) clearInterval(timerSync);

    server.close(async () => {
      try {
        await pool.end();
      } finally {
        process.exit(0);
      }
    });

    setTimeout(() => process.exit(1), 10000).unref();
  }

  process.on('SIGTERM', encerrar);
  process.on('SIGINT', encerrar);
}

iniciar().catch(err => {
  console.error('[FATAL]', err);
  process.exit(1);
});