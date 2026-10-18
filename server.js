
const express = require("express");
const cors = require("cors");
const axios = require("axios");
const { Pool } = require("pg");

const app = express();
const PORT = process.env.PORT || 3000;

const API_BACBO =
  "https://api-cs.casino.org/svc-evolution-game-events/api/bacbo";

const pool = process.env.DATABASE_URL
  ? new Pool({
      connectionString: process.env.DATABASE_URL,
      ssl: { rejectUnauthorized: false },
      connectionTimeoutMillis: 10000,
      idleTimeoutMillis: 30000,
      max: 5
    })
  : null;

app.use(cors());
app.use(express.json());

const http = axios.create({
  timeout: 20000,
  headers: {
    "User-Agent": "Mozilla/5.0 (compatible; CapungaDadosBot/1.0)",
    Accept: "application/json"
  }
});

function horaAngola(data = new Date()) {
  return new Intl.DateTimeFormat("pt-AO", {
    timeZone: "Africa/Luanda",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false
  }).format(new Date(data));
}

function traduzirResultado(outcome) {
  const mapa = {
    BankerWon: "Banca",
    PlayerWon: "Jogador",
    Tie: "Empate"
  };

  return mapa[outcome] || outcome || "Desconhecido";
}

function normalizarRodada(item) {
  const dados = item.data || {};
  const resultado = dados.result || {};
  const jogador = resultado.playerDice || {};
  const banca = resultado.bankerDice || {};

  return {
    id: String(item.id || dados.id || ""),
    estado: dados.status || "Desconhecido",
    inicioUTC: dados.startedAt || null,
    fimUTC: dados.settledAt || null,
    horaAngola: horaAngola(dados.settledAt || new Date()),
    resultadoOriginal: resultado.outcome || null,
    resultado: traduzirResultado(resultado.outcome),
    multiplicador: resultado.multiplier ?? null,
    jogador: {
      dado1: jogador.first ?? null,
      dado2: jogador.second ?? null,
      total: jogador.score ?? null
    },
    banca: {
      dado1: banca.first ?? null,
      dado2: banca.second ?? null,
      total: banca.score ?? null
    }
  };
}

async function obterResultados() {
  const resposta = await http.get(API_BACBO);
  if (!Array.isArray(resposta.data)) {
    throw new Error("A API não devolveu uma lista.");
  }

  return resposta.data
    .filter(item => item.data && item.data.result)
    .map(normalizarRodada);
}

async function prepararBaseDados() {
  if (!pool) {
    throw new Error("A variável DATABASE_URL não está configurada.");
  }

  await pool.query(`
    CREATE TABLE IF NOT EXISTS bacbo_resultados (
      id TEXT PRIMARY KEY,
      resultado TEXT NOT NULL,
      resultado_original TEXT,
      inicio_utc TIMESTAMPTZ,
      fim_utc TIMESTAMPTZ,
      jogador_total INTEGER,
      banca_total INTEGER,
      multiplicador NUMERIC,
      criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS sinais (
      id BIGSERIAL PRIMARY KEY,
      criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      dia_angola DATE NOT NULL DEFAULT
        (timezone('Africa/Luanda', NOW()))::date,
      cor TEXT NOT NULL,
      estado TEXT NOT NULL DEFAULT 'PENDENTE',
      gale INTEGER NOT NULL DEFAULT 0,
      resultado_rodada_id TEXT,
      resolvido_em TIMESTAMPTZ,
      observacoes TEXT
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS estatisticas_diarias (
      dia_angola DATE PRIMARY KEY,
      total_sinais INTEGER NOT NULL DEFAULT 0,
      green INTEGER NOT NULL DEFAULT 0,
      loss INTEGER NOT NULL DEFAULT 0,
      gs INTEGER NOT NULL DEFAULT 0,
      g1 INTEGER NOT NULL DEFAULT 0,
      g2 INTEGER NOT NULL DEFAULT 0,
      ge INTEGER NOT NULL DEFAULT 0,
      ge1 INTEGER NOT NULL DEFAULT 0,
      ge2 INTEGER NOT NULL DEFAULT 0,
      empates INTEGER NOT NULL DEFAULT 0,
      atualizada_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  return true;
}

async function guardarResultados(resultados) {
  if (!pool) {
    throw new Error("DATABASE_URL não está configurada.");
  }

  let guardados = 0;

  for (const r of resultados) {
    if (!r.id || !r.resultadoOriginal) continue;

    const resultado = await pool.query(
      `INSERT INTO bacbo_resultados
       (id, resultado, resultado_original, inicio_utc, fim_utc,
        jogador_total, banca_total, multiplicador)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
       ON CONFLICT (id) DO NOTHING`,
      [
        r.id,
        r.resultado,
        r.resultadoOriginal,
        r.inicioUTC,
        r.fimUTC,
        r.jogador.total,
        r.banca.total,
        r.multiplicador
      ]
    );

    guardados += resultado.rowCount;
  }

  return guardados;
}

app.get("/", (req, res) => {
  res.json({
    nome: "Capunga Dados Bot",
    estado: "online",
    fonte: "CasinoScores",
    baseDadosConfigurada: Boolean(pool),
    rotas: [
      "/health",
      "/diagnostico",
      "/resultados",
      "/db/teste",
      "/db/preparar",
      "/db/sincronizar",
      "/telegram/teste",
      "/estatisticas"
    ]
  });
});

app.get("/health", (req, res) => {
  res.json({
    sucesso: true,
    estado: "online",
    horaAngola: horaAngola()
  });
});

app.get("/diagnostico", async (req, res) => {
  try {
    const resultados = await obterResultados();

    res.json({
      sucesso: true,
      fonte: "CasinoScores",
      quantidadeRecebida: resultados.length,
      primeiroResultado: resultados[0] || null,
      horaConsultaAngola: horaAngola()
    });
  } catch (erro) {
    res.status(502).json({
      sucesso: false,
      erro: erro.message
    });
  }
});

app.get("/resultados", async (req, res) => {
  try {
    const resultados = await obterResultados();
    const limite = Math.min(
      Math.max(parseInt(req.query.limite, 10) || 10, 1),
      100
    );

    res.json({
      sucesso: true,
      fonte: "CasinoScores",
      quantidade: resultados.length,
      horaConsultaAngola: horaAngola(),
      resultados: resultados.slice(0, limite)
    });
  } catch (erro) {
    res.status(502).json({
      sucesso: false,
      erro: erro.message
    });
  }
});

app.get("/db/teste", async (req, res) => {
  if (!pool) {
    return res.status(500).json({
      sucesso: false,
      erro: "DATABASE_URL não está configurada no Render."
    });
  }

  try {
    const resultado = await pool.query(
      "SELECT NOW() AS hora_servidor, current_database() AS base"
    );

    res.json({
      sucesso: true,
      mensagem: "Ligação PostgreSQL estabelecida.",
      base: resultado.rows[0].base,
      horaServidorUTC: resultado.rows[0].hora_servidor
    });
  } catch (erro) {
    res.status(502).json({
      sucesso: false,
      erro: erro.message
    });
  }
});

app.get("/db/preparar", async (req, res) => {
  try {
    await prepararBaseDados();

    res.json({
      sucesso: true,
      mensagem: "Tabelas preparadas para resultados, sinais e estatísticas."
    });
  } catch (erro) {
    res.status(502).json({
      sucesso: false,
      erro: erro.message
    });
  }
});

app.get("/db/sincronizar", async (req, res) => {
  try {
    const resultados = await obterResultados();
    const guardados = await guardarResultados(resultados);

    res.json({
      sucesso: true,
      resultadosRecebidos: resultados.length,
      novosResultadosGuardados: guardados,
      horaAngola: horaAngola()
    });
  } catch (erro) {
    res.status(502).json({
      sucesso: false,
      erro: erro.message
    });
  }
});

app.get("/estatisticas", async (req, res) => {
  if (!pool) {
    return res.status(500).json({
      sucesso: false,
      erro: "DATABASE_URL não está configurada."
    });
  }

  try {
    const hoje = await pool.query(`
      SELECT *
      FROM estatisticas_diarias
      WHERE dia_angola = (timezone('Africa/Luanda', NOW()))::date
    `);

    const historico = await pool.query(`
      SELECT dia_angola, total_sinais, green, loss, gs, g1, g2,
             ge, ge1, ge2, empates
      FROM estatisticas_diarias
      ORDER BY dia_angola DESC
      LIMIT 30
    `);

    res.json({
      sucesso: true,
      hoje: hoje.rows[0] || null,
      historico: historico.rows
    });
  } catch (erro) {
    res.status(502).json({
      sucesso: false,
      erro: erro.message
    });
  }
});

app.get("/telegram/teste", async (req, res) => {
  const token = process.env.BOT_TOKEN;
  const chatId = process.env.CHAT_ID;

  if (!token || !chatId) {
    return res.status(500).json({
      sucesso: false,
      erro: "Configura BOT_TOKEN e CHAT_ID no Render."
    });
  }

  try {
    const resposta = await axios.post(
      `https://api.telegram.org/bot${token}/sendMessage`,
      {
        chat_id: chatId,
        text: "✅ Capunga Dados Bot: teste do Telegram concluído."
      },
      { timeout: 15000 }
    );

    res.json({ sucesso: resposta.data.ok });
  } catch (erro) {
    res.status(502).json({
      sucesso: false,
      erro: erro.response?.data?.description || erro.message
    });
  }
});

app.use((req, res) => {
  res.status(404).json({
    sucesso: false,
    erro: "Rota não encontrada",
    caminho: req.path
  });
});

app.listen(PORT, "0.0.0.0", () => {
  console.log(`Capunga Dados Bot ativo na porta ${PORT}`);
});

process.on("SIGTERM", async () => {
  if (pool) await pool.end().catch(() => {});
  process.exit(0);
});
    
