
const express = require("express");
const cors = require("cors");
const axios = require("axios");

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3000;

const CASINOSCORES_URL =
  "https://www.casino.org/casinoscores/pt-br/bac-bo/";

app.get("/", (req, res) => {
  res.json({
    bot: "Dados AO Bot",
    status: "online",
    fonte: "CasinoScores",
    mensagem: "Servidor ativo. Captura de resultados por testar."
  });
});

app.get("/health", (req, res) => {
  res.json({ status: "ok" });
});

// Testa se o servidor consegue aceder ao CasinoScores.
app.get("/teste-casinoscores", async (req, res) => {
  try {
    const resposta = await axios.get(CASINOSCORES_URL, {
      timeout: 20000,
      headers: {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/html"
      }
    });

    const html =
      typeof resposta.data === "string"
        ? resposta.data
        : JSON.stringify(resposta.data);

    res.json({
      fonte: "CasinoScores",
      sucesso: true,
      statusHTTP: resposta.status,
      tamanhoHTML: html.length,
      contemHistorico: html.includes("Histórico de Lançamentos"),
      contemBacBo: html.toLowerCase().includes("bac bo"),
      aviso: "O acesso à página foi testado. Os resultados reais ainda precisam de ser extraídos."
    });
  } catch (error) {
    res.status(502).json({
      fonte: "CasinoScores",
      sucesso: false,
      erro: error.response
        ? "O CasinoScores respondeu com HTTP " + error.response.status
        : error.message
    });
  }
});

// Teste de ligação ao Telegram.
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

app.listen(PORT, "0.0.0.0", () => {
  console.log(`Dados AO Bot ativo na porta ${PORT}`);
});
