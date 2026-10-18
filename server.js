JavaScript

const express = require("express");
const cors = require("cors");
const axios = require("axios");
const { Pool } = require("pg");

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

const pool = process.env.DATABASE_URL
  ? new Pool({
      connectionString: process.env.DATABASE_URL,
      ssl: { rejectUnauthorized: false },
      connectionTimeoutMillis: 10000,
      idleTimeoutMillis: 30000,
      max: 5
    })
  : null;

const API_BACBO =
  "https://api-cs.casino.org/svc-evolution-game-events/api/bacbo";

function horaAngola(data = new Date()) {
  return new Intl.DateTimeFormat("pt-PT", {
    timeZone: "Africa/Luanda",
    dateStyle: "short",
    timeStyle: "medium",
    hour12: false
  }).format(data);
}

function traduzirResultado(resultado) {
  const valor = String(resultado || "").toLowerCase();

  if (valor.includes("banker")) return "Banca";
  if (valor.includes("player")) return "Jogador";
  if (valor.includes("tie")) return "Empate";

  return resultado || "Desconhecido";
}

function normalizarRodada(item) {
  const d = item?.data || item || {};
  const r = d.result || {};

  return {
    id: String(item?.id || d.id || ""),
    estado: d.status || "Desconhecido",
    inicio: d.startedAt || null,
    fim: d.settledAt || null,
    resultado: traduzirResultado(r.outcome),
    pontosJogador: r.playerDice?.score ?? null,
    pontosBanca: r.bankerDice?.score ?? null,
    multiplicador: r.multiplier ?? null
  };
}

async function obterResultados() {
  const resposta = await axios.get(API_BACBO, {
    timeout: 15000,
    headers: {
      "User-Agent": "Dados-AO-Bot/1.0",
      Accept: "application/json"
    }
  });

  const dados = resposta.data;
  const lista = Array.isArray(dados)
    ? dados
    : Array.isArray(dados?.data)
      ? dados.data
      : Array.isArray(dados?.results)
        ? dados.results
        : [];

  return lista
    .map(normalizarRodada)
    .filter(r => r.id && r.resultado !== "Desconhecido");
}

async function prepararBaseDados() {
  if (!pool) {
    throw new Error("DATABASE_URL não está configurada no Render.");
  }

  await pool.query(`
    CREATE TABLE IF NOT EXISTS bacbo_resultados (
      id TEXT PRIMARY KEY,
      resultado TEXT NOT NULL,
      estado TEXT,
      inicio TIMESTAMPTZ,
      fim TIMESTAMPTZ,
      pontos_jogador INTEGER,
      pontos_banca INTEGER,
      multiplicador NUMERIC,
      guardado_em TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS sinais (
      id BIGSERIAL PRIMARY KEY,
      criado_em TIMESTAMPTZ DEFAULT NOW(),
      dia_angola DATE NOT NULL DEFAULT
        (NOW() AT TIME ZONE 'Africa/Luanda')::date,
      cor TEXT NOT NULL,
      estado TEXT NOT NULL DEFAULT 'PENDENTE',
      gale INTEGER NOT NULL DEFAULT 0,
      resultado_id TEXT,
      resolvido_em TIMESTAMPTZ,
      observacoes TEXT
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS estatisticas_diarias (
      dia_angola DATE PRIMARY KEY DEFAULT
        (NOW() AT TIME ZONE 'Africa/Luanda')::date,
      total_sinais INTEGER NOT NULL DEFAULT 0,
      green INTEGER NOT NULL DEFAULT 0,
      loss INTEGER NOT NULL DEFAULT 0,
      g1 INTEGER NOT NULL DEFAULT 0,
      g2 INTEGER NOT NULL DEFAULT 0,
      empates INTEGER NOT NULL DEFAULT 0,
      atualizada_em TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  return true;
}

async function enviarTelegram(mensagem) {
  const token = process.env.BOT_TOKEN;
  const chatId = process.env.CHAT_ID;

  if (!token || !chatId) {
    throw new Error("BOT_TOKEN ou CHAT_ID não configurado no Render.");
  }

  const url = `https://api.telegram.org/bot${token}/sendMessage`;

  const resposta = await axios.post(
    url,
    {
      chat_id: chatId,
      text: mensagem
    },
    { timeout: 15000 }
  );

  return resposta.data;
}

app.get("/", (req, res) => {
  res.json({
    nome: "Dados AO Bot",
    online: true,
    horaAngola: horaAngola(),
    rotas: [
      "/health",
      "/diagnostico",
      "/db/teste",
      "/db/preparar",
      "/db/sincronizar",
      "/resultados",
      "/estatisticas",
      "/telegram/teste"
    ]
  });
});

app.get("/health", (req, res) => {
  res.json({
    sucesso: true,
    online: true,
    horaAngola: horaAngola()
  });
});

app.get("/diagnostico", (req, res) => {
  res.json({
    servidor: "online",
    baseConfigurada: Boolean(process.env.DATABASE_URL),
    telegramConfigurado: Boolean(
      process.env.BOT_TOKEN && process.env.CHAT_ID
    ),
    horaAngola: horaAngola()
  });
});

app.get("/db/teste", async (req, res) => {
  if (!pool) {
    return res.status(503).json({
      sucesso: false,
      erro: "DATABASE_URL não está configurada no Render."
    });
  }

  try {
    const resultado = await pool.query(`
      SELECT NOW() AS hora_servidor,
             CURRENT_DATE AS dia_servidor
    `);

    res.json({
      sucesso: true,
      mensagem: "Ligação PostgreSQL estabelecida.",
      horaAngola: horaAngola(),
      baseDados: "conectada",
      teste: resultado.rows[0]
    });
  } catch (erro) {
    res.status(500).json({
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
      mensagem: "Tabelas da base de dados preparadas."
    });
  } catch (erro) {
    res.status(500).json({
      sucesso: false,
      erro: erro.message
    });
  }
});

app.get("/db/sincronizar", async (req, res) => {
  try {
    await prepararBaseDados();

    const resultados = await obterResultados();
    let guardados = 0;

    for (const r of resultados) {
      const insercao = await pool.query(
        `INSERT INTO bacbo_resultados
          (id, resultado, estado, inicio, fim,
           pontos_jogador, pontos_banca, multiplicador)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
         ON CONFLICT (id) DO NOTHING
         RETURNING id`,
        [
          r.id,
          r.resultado,
          r.estado,
          r.inicio,
          r.fim,
          r.pontosJogador,
          r.pontosBanca,
          r.multiplicador
        ]
      );

      guardados += insercao.rowCount;
    }

    res.json({
      sucesso: true,
      recebidos: resultados.length,
      novosGuardados: guardados,
      mensagem: "Sincronização concluída. Esta rota guarda resultados, não gera sinais."
    });
  } catch (erro) {
    res.status(500).json({
      sucesso: false,
      erro: erro.message
    });
  }
});

app.get("/resultados", async (req, res) => {
  if (!pool) {
    return res.status(503).json({
      sucesso: false,
      erro: "Base de dados não configurada."
    });
  }

  try {
    const limite = Math.min(
      Math.max(parseInt(req.query.limite, 10) || 10, 1),
      100
    );

    const resultado = await pool.query(
      `SELECT id, resultado, estado, inicio, fim,
              pontos_jogador, pontos_banca, multiplicador
       FROM bacbo_resultados
       ORDER BY fim DESC NULLS LAST
       LIMIT $1`,
      [limite]
    );

    res.json({
      sucesso: true,
      total: resultado.rowCount,
      resultados: resultado.rows
    });
  } catch (erro) {
    res.status(500).json({
      sucesso: false,
      erro: erro.message
    });
  }
});

app.get("/estatisticas", async (req, res) => {
  if (!pool) {
    return res.status(503).json({
      sucesso: false,
      erro: "Base de dados não configurada."
    });
  }

  try {
    const hoje = await pool.query(`
      SELECT dia_angola, total_sinais, green, loss,
             g1, g2, empates, atualizada_em
      FROM estatisticas_diarias
      WHERE dia_angola =
        (NOW() AT TIME ZONE 'Africa/Luanda')::date
    `);

    const ultimosDias = await pool.query(`
      SELECT dia_angola, total_sinais, green, loss,
             g1, g2, empates
      FROM estatisticas_diarias
      ORDER BY dia_angola DESC
      LIMIT 30
    `);

    res.json({
      sucesso: true,
      horaAngola: horaAngola(),
      hoje: hoje.rows[0] || {
        mensagem: "Ainda não existem estatísticas registadas para hoje."
      },
      ultimos30Dias: ultimosDias.rows
    });
  } catch (erro) {
    res.status(500).json({
      sucesso: false,
      erro: erro.message
    });
  }
});

app.get("/telegram/teste", async (req, res) => {
  try {
    await enviarTelegram(
      "✅ Dados AO Bot: teste de ligação ao Telegram.\nHora de Angola: " +
      horaAngola()
    );

    res.json({
      sucesso: true,
      mensagem: "Mensagem de teste enviada ao Telegram."
    });
  } catch (erro) {
    res.status(500).json({
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

app.listen(PORT, () => {
  console.log(`Dados AO Bot ativo na porta ${PORT}`);
  console.log(`Hora de Angola: ${horaAngola()}`);
});

process.on("SIGTERM", async () => {
  if (pool) await pool.end();
  process.exit(0);
});
  
