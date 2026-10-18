
const express = require("express");
const cors = require("cors");
const axios = require("axios");

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3000;

app.get("/", (req, res) => {
  res.json({
    bot: "Dados AO Bot",
    status: "online",
    fonte_de_resultados: "por configurar"
  });
});

app.get("/health", (req, res) => {
  res.json({ status: "ok" });
});

app.get("/resultados", (req, res) => {
  res.json({
    resultados: [],
    mensagem: "Aguardando uma fonte real de resultados Bac Bo."
  });
});

app.post("/telegram/teste", async (req, res) => {
  const token = process.env.BOT_TOKEN;
  const chatId = process.env.CHAT_ID;

  if (!token || !chatId) {
    return res.status(500).json({
      erro: "Configura BOT_TOKEN e CHAT_ID nas variáveis de ambiente."
    });
  }

  try {
    await axios.post(
      `https://api.telegram.org/bot${token}/sendMessage`,
      {
        chat_id: chatId,
        text: "✅ Dados AO Bot: ligação ao Telegram testada."
      }
    );

    res.json({ enviado: true });
  } catch (error) {
    res.status(500).json({
      erro: "Não foi possível enviar a mensagem ao Telegram."
    });
  }
});

app.listen(PORT, "0.0.0.0", () => {
  console.log(`Dados AO Bot ativo na porta ${PORT}`);
});
