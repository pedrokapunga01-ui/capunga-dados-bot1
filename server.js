
const express = require("express");
const cors = require("cors");
const axios = require("axios");

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3000;
const BETMIND_URL = "https://betmind.org";

app.get("/", (req, res) => {
  res.json({
    bot: "Dados AO Bot",
    status: "online",
    fonte: "BetMind",
    aviso: "Resultados dependem do acesso à API."
  });
});

app.get("/health", (req, res) => {
  res.json({ status: "ok" });
});

async function consultarBetMind(caminho) {
  const chave = process.env.BETMIND_API_KEY;

  if (!chave) {
    throw new Error(
      "Falta configurar BETMIND_API_KEY nas variáveis do Render."
    );
  }

  const resposta = await axios.get(
    `${BETMIND_URL}${caminho}`,
    {
      headers: {
        "Content-Type": "application/json",
        "X-API-Key": chave
      },
      timeout: 15000
    }
  );

  return resposta.data;
}

app.get("/resultados", async (req, res) => {
  try {
    const dados = await consultarBetMind(
      "/api/results?game=bacbo&limit=10&offset=0"
    );

    res.json({
      fonte: "BetMind",
      sucesso: true,
      dados
    });
  } catch (error) {
    const status = error.response?.status || 502;

    res.status(status).json({
      fonte: "BetMind",
      sucesso: false,
      erro:
        error.response?.data?.message ||
        error.response?.data?.error ||
        error.message
    });
  }
});

app.get("/ultimo", async (req, res) => {
  try {
    const dados = await consultarBetMind(
      "/api/latest?game=bacbo"
    );

    res.json({
      fonte: "BetMind",
      sucesso: true,
      dados
    });
  } catch (error) {
    res.status(error.response?.status || 502).json({
      fonte: "BetMind",
      sucesso: false,
      erro:
        error.response?.data?.message ||
        error.response?.data?.error ||
        error.message
    });
  }
});

app.get("/telegram/teste", async (req, res) => {
  const token = process.env.BOT_TOKEN;
  const chatId = process.env.CHAT_ID;

  if (!token || !chatId) {
    return res.status(500).json({
      erro: "BOT_TOKEN ou CHAT_ID não configurado."
    });
  }

  try {
    await axios.post(
      `https://api.telegram.org/bot${token}/sendMessage`,
      {
        chat_id: chatId,
        text: "✅ Dados AO Bot: ligação ao Telegram testada com sucesso!"
      }
    );

    res.json({
      enviado: true,
      mensagem: "Verifica o teu Telegram."
    });
  } catch (error) {
    res.status(500).json({
      enviado: false,
      erro: error.response?.data?.description ||
        "Falha no envio para o Telegram."
    });
  }
});

app.listen(PORT, "0.0.0.0", () => {
  console.log(`Dados AO Bot ativo na porta ${PORT}`);
});
      
