
const express = require("express");
const cors = require("cors");
const axios = require("axios");

const app = express();
const PORT = process.env.PORT || 3000;

const API_BACBO =
  "https://api-cs.casino.org/svc-evolution-game-events/api/bacbo";

const CASINOSCORES_URL =
  "https://www.casino.org/casinoscores/pt-br/bac-bo/";

app.use(cors());
app.use(express.json());

const http = axios.create({
  timeout: 20000,
  headers: {
    "User-Agent": "Mozilla/5.0 (compatible; DadosAOBot/1.0)",
    "Accept": "application/json, text/html, */*",
    "Cache-Control": "no-cache"
  }
});

function horaAngola(data) {
  if (!data) return null;

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
  const resultados = {
    BankerWon: "Banca",
    PlayerWon: "Jogador",
    Tie: "Empate"
  };

  return resultados[outcome] || outcome || "Desconhecido";
}

function normalizarRodada(item) {
  const dados = item.data || {};
  const resultado = dados.result || {};
  const jogador = resultado.playerDice || {};
  const banca = resultado.bankerDice || {};

  return {
    id: item.id || dados.id,
    estado: dados.status || "Desconhecido",
    inicioUTC: dados.startedAt || null,
    fimUTC: dados.settledAt || null,
    horaAngola: horaAngola(dados.settledAt),
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
  const dados = resposta.data;

  if (!Array.isArray(dados)) {
    throw new Error("A API não devolveu uma lista de resultados.");
  }

  return dados
    .filter(item => item.data && item.data.result)
    .map(normalizarRodada);
}

app.get("/", (req, res) => {
  res.json({
    nome: "Dados AO Bot",
    estado: "online",
    fonte: "CasinoScores",
    rotas: [
      "/health",
      "/teste-casinoscores",
      "/resultados",
      "/diagnostico",
      "/diagnostico-scripts",
      "/telegram/teste",
      "/telegram/resultados"
    ]
  });
});

app.get("/health", (req, res) => {
  res.json({
    sucesso: true,
    estado: "online",
    horaAngola: horaAngola(new Date())
  });
});

app.get("/teste-casinoscores", async (req, res) => {
  try {
    const resposta = await http.get(CASINOSCORES_URL);

    res.json({
      sucesso: true,
      fonte: "CasinoScores",
      statusHTTP: resposta.status,
      tamanhoHTML: resposta.data.length,
      contemBacBo: resposta.data.toLowerCase().includes("bac-bo"),
      contemHistorico:
        resposta.data.toLowerCase().includes("history")
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
      horaConsultaAngola: horaAngola(new Date()),
      resultados: resultados.slice(0, limite)
    });
  } catch (erro) {
    res.status(502).json({
      sucesso: false,
      fonte: "CasinoScores",
      erro: erro.message
    });
  }
});

app.get("/diagnostico", async (req, res) => {
  try {
    const resposta = await http.get(API_BACBO);
    const dados = resposta.data;
    const primeiro = Array.isArray(dados) ? dados[0] : null;

    res.json({
      sucesso: true,
      statusHTTP: resposta.status,
      formatoLista: Array.isArray(dados),
      quantidadeRecebida: Array.isArray(dados) ? dados.length : 0,
      primeiroResultado: primeiro
        ? normalizarRodada(primeiro)
        : null,
      horaConsultaAngola: horaAngola(new Date())
    });
  } catch (erro) {
    res.status(502).json({
      sucesso: false,
      erro: erro.message
    });
  }
});

app.get("/diagnostico-scripts", async (req, res) => {
  try {
    const resposta = await http.get(CASINOSCORES_URL);
    const html = resposta.data;
    const scripts = [
      ...html.matchAll(/<script[^>]+src=["']([^"']+)["']/gi)
    ]
      .map(match => match[1])
      .filter(src => src.includes(".js"))
      .slice(0, 30);

    const encontrados = [];

    for (const src of scripts) {
      try {
        const url = new URL(src, CASINOSCORES_URL).href;
        const script = await http.get(url);
        const texto = String(script.data);
        const pistas = [
          "latest_rolls",
          "spinHistory",
          "HistoryTableBacBo",
          "api-cs.casino.org/svc-evolution-game-events/api/bacbo",
          "api.casinoscores.com/ws"
        ].filter(pista => texto.includes(pista));

        if (pistas.length) {
          encontrados.push({
            ficheiro: url.split("/").pop(),
            pistas
          });
        }
      } catch (_) {
        // Ignora scripts que não possam ser consultados.
      }
    }

    res.json({
      sucesso: true,
      scriptsAnalisados: scripts.length,
      encontrados
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
      erro: "Configura BOT_TOKEN e CHAT_ID nas variáveis do Render."
    });
  }

  try {
    const resposta = await axios.post(
      `https://api.telegram.org/bot${token}/sendMessage`,
      {
        chat_id: chatId,
        text: "✅ Dados AO Bot: ligação ao Telegram testada!"
      },
      { timeout: 15000 }
    );

    res.json({
      sucesso: resposta.data.ok,
      mensagem: "Teste do Telegram enviado."
    });
  } catch (erro) {
    res.status(502).json({
      sucesso: false,
      erro: erro.response?.data?.description || erro.message
    });
  }
});

app.get("/telegram/resultados", async (req, res) => {
  const token = process.env.BOT_TOKEN;
  const chatId = process.env.CHAT_ID;

  if (!token || !chatId) {
    return res.status(500).json({
      sucesso: false,
      erro: "Configura BOT_TOKEN e CHAT_ID no Render."
    });
  }

  try {
    const resultados = await obterResultados();

    if (!resultados.length) {
      return res.status(502).json({
        sucesso: false,
        erro: "A API não devolveu resultados."
      });
    }

    const lista = resultados.slice(0, 10).map((r, i) => {
      const emoji =
        r.resultado === "Banca" ? "🔴" :
        r.resultado === "Jogador" ? "🔵" : "🟡";

      return `${i + 1}. ${emoji} ${r.resultado} | ${r.horaAngola}`;
    });

    const mensagem =
      "🎲 DADOS AO BOT — BAC BO\n\n" +
      lista.join("\n") +
      "\n\nFonte: CasinoScores\n" +
      "Resultados históricos, não previsões.";

    const resposta = await axios.post(
      `https://api.telegram.org/bot${token}/sendMessage`,
      {
        chat_id: chatId,
        text: mensagem
      },
      { timeout: 15000 }
    );

    res.json({
      sucesso: resposta.data.ok,
      resultadosEnviados: Math.min(resultados.length, 10)
    });
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
  console.log(`Dados AO Bot ativo na porta ${PORT}`);
});
    
