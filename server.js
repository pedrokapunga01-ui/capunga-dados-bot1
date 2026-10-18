
const express = require("express");
const cors = require("cors");
const axios = require("axios");

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3000;
const CASINOSCORES_URL =
  "https://www.casino.org/casinoscores/pt-br/bac-bo/";

async function obterHTML() {
  const resposta = await axios.get(CASINOSCORES_URL, {
    timeout: 20000,
    headers: {
      "User-Agent": "Mozilla/5.0",
      "Accept": "text/html"
    }
  });

  return typeof resposta.data === "string"
    ? resposta.data
    : JSON.stringify(resposta.data);
}

function limparHTML(html) {
  return html
    .replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, " ")
    .replace(/<style\b[^>]*>[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/\s+/g, " ")
    .trim();
}

app.get("/", (req, res) => {
  res.json({
    bot: "Dados AO Bot",
    status: "online",
    fonte: "CasinoScores",
    mensagem: "Servidor ativo."
  });
});

app.get("/health", (req, res) => {
  res.json({ status: "ok" });
});

// Teste de acesso ao CasinoScores
app.get("/teste-casinoscores", async (req, res) => {
  try {
    const html = await obterHTML();

    res.json({
      fonte: "CasinoScores",
      sucesso: true,
      statusHTTP: 200,
      tamanhoHTML: html.length,
      contemBacBo: html.toLowerCase().includes("bac bo"),
      contemHistorico:
        html.toLowerCase().includes("histórico de lançamentos"),
      aviso: "A página foi carregada; os resultados reais ainda precisam de validação."
    });
  } catch (error) {
    res.status(502).json({
      fonte: "CasinoScores",
      sucesso: false,
      erro: error.response
        ? "HTTP " + error.response.status
        : error.message
    });
  }
});

// Teste inicial de extração de texto
app.get("/resultados", async (req, res) => {
  try {
    const html = await obterHTML();
    const texto = limparHTML(html);
    const posicao = texto.toLowerCase().indexOf("histórico de lançamentos");

    res.json({
      fonte: "CasinoScores",
      sucesso: true,
      encontrouHistorico: posicao !== -1,
      trecho: posicao >= 0
        ? texto.substring(posicao, posicao + 3000)
        : texto.substring(0, 3000),
      aviso: "Este teste não garante que as rodadas estejam presentes no HTML."
    });
  } catch (error) {
    res.status(502).json({
      fonte: "CasinoScores",
      sucesso: false,
      erro: error.response
        ? "HTTP " + error.response.status
        : error.message
    });
  }
});

// Diagnóstico dos scripts e pistas de dados
app.get("/diagnostico", async (req, res) => {
  try {
    const html = await obterHTML();

    const scripts = [
      ...html.matchAll(
        /<script\b[^>]*\bsrc=["']([^"']+)["'][^>]*>/gi
      )
    ].map(match => match[1]);

    const termos = [
      "latest rolls",
      "latestrolls",
      "api/",
      "graphql",
      "results",
      "history",
      "rolls"
    ];

    const pistas = {};
    const htmlMinusculo = html.toLowerCase();

    for (const termo of termos) {
      const posicao = htmlMinusculo.indexOf(termo);

      pistas[termo] = posicao >= 0
        ? html.substring(
            Math.max(0, posicao - 150),
            posicao + 350
          )
        : null;
    }

    res.json({
      fonte: "CasinoScores",
      sucesso: true,
      quantidadeScripts: scripts.length,
      scripts: scripts.slice(0, 30),
      pistas,
      aviso: "As pistas não confirmam, por si só, uma API de resultados."
    });
  } catch (error) {
    res.status(502).json({
      fonte: "CasinoScores",
      sucesso: false,
      erro: error.response
        ? "HTTP " + error.response.status
        : error.message
    });
  }
});

// Teste de ligação ao Telegram
app.get("/telegram/teste", async (req, res) => {
  const token = process.env.BOT_TOKEN;
  const chatId = process.env.CHAT_ID;

  if (!token || !chatId) {
    return res.status(500).json({
      enviado: false,
      erro: "BOT_TOKEN ou CHAT_ID não configurado no Render."
    });
  }

  try {
    await axios.post(
      `https://api.telegram.org/bot${token}/sendMessage`,
      {
        chat_id: chatId,
        text: "✅ Dados AO Bot: ligação ao Telegram testada!"
      },
      { timeout: 15000 }
    );

    res.json({
      enviado: true,
      mensagem: "Verifica o teu Telegram."
    });
  } catch (error) {
    res.status(502).json({
      enviado: false,
      erro: error.response?.data?.description ||
        "Falha no envio para o Telegram."
    });
  }
});

app.use((req, res) => {
  res.status(404).json({
    erro: "Rota não encontrada",
    rotaSolicitada: req.path,
    rotasDisponiveis: [
      "/",
      "/health",
      "/teste-casinoscores",
      "/resultados",
      "/diagnostico",
      "/telegram/teste"
    ]
  });
});

app.listen(PORT, "0.0.0.0", () => {
  console.log(`Dados AO Bot ativo na porta ${PORT}`);
});
    
