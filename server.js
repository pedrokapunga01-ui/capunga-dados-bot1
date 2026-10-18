
const express = require("express");
const cors = require("cors");
const axios = require("axios");

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3000;
const BASE_URL = "https://www.casino.org";
const CASINOSCORES_URL =
  BASE_URL + "/casinoscores/pt-br/bac-bo/";

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
    .replace(/\s+/g, " ")
    .trim();
}

app.get("/", (req, res) => {
  res.json({
    bot: "Dados AO Bot",
    status: "online",
    fonte: "CasinoScores"
  });
});

app.get("/health", (req, res) => {
  res.json({ status: "ok" });
});

// Testar acesso ao CasinoScores
app.get("/teste-casinoscores", async (req, res) => {
  try {
    const html = await obterHTML();

    res.json({
      sucesso: true,
      statusHTTP: 200,
      tamanhoHTML: html.length,
      contemBacBo: html.toLowerCase().includes("bac bo"),
      contemHistorico:
        html.toLowerCase().includes("spin-history-section")
    });
  } catch (error) {
    res.status(502).json({
      sucesso: false,
      erro: error.response
        ? "HTTP " + error.response.status
        : error.message
    });
  }
});

// Testar extração de texto
app.get("/resultados", async (req, res) => {
  try {
    const html = await obterHTML();
    const texto = limparHTML(html);
    const pos = texto.toLowerCase().indexOf(
      "histórico de lançamentos"
    );

    res.json({
      fonte: "CasinoScores",
      sucesso: true,
      encontrouHistorico: pos !== -1,
      trecho: pos >= 0
        ? texto.substring(pos, pos + 3000)
        : texto.substring(0, 3000),
      aviso: "Ainda não confirma a extração das rodadas reais."
    });
  } catch (error) {
    res.status(502).json({
      sucesso: false,
      erro: error.message
    });
  }
});

// Diagnóstico inicial da página
app.get("/diagnostico", async (req, res) => {
  try {
    const html = await obterHTML();

    const scripts = [
      ...html.matchAll(
        /<script\b[^>]*\bsrc=["']([^"']+)["'][^>]*>/gi
      )
    ].map(m => m[1]);

    const termos = [
      "spin-history-section",
      "latest_rolls",
      "api/",
      "graphql",
      "results",
      "history",
      "rolls"
    ];

    const pistas = {};
    const lower = html.toLowerCase();

    for (const termo of termos) {
      const pos = lower.indexOf(termo);
      pistas[termo] = pos >= 0
        ? html.substring(Math.max(0, pos - 100), pos + 300)
        : null;
    }

    res.json({
      sucesso: true,
      quantidadeScripts: scripts.length,
      scripts: scripts.slice(0, 30),
      pistas
    });
  } catch (error) {
    res.status(502).json({
      sucesso: false,
      erro: error.message
    });
  }
});

// NOVO: analisar os ficheiros JavaScript da página
app.get("/diagnostico-scripts", async (req, res) => {
  try {
    const html = await obterHTML();

    const scripts = [
      ...html.matchAll(
        /<script\b[^>]*\bsrc=["']([^"']+)["'][^>]*>/gi
      )
    ].map(m => m[1]);

    const ficheiros = scripts
      .filter(src => src.includes("/_next/static/"))
      .slice(0, 30);

    const resultados = await Promise.all(
      ficheiros.map(async src => {
        try {
          const url = new URL(src, BASE_URL).href;

          const resposta = await axios.get(url, {
            timeout: 15000,
            headers: {
              "User-Agent": "Mozilla/5.0"
            }
          });

          const js = typeof resposta.data === "string"
            ? resposta.data
            : JSON.stringify(resposta.data);

          const padroes = [
            /https?:\/\/[^"'`\s<>]+/gi,
            /["'`](?:\/[^"'`\s]{1,180})["'`]/g,
            /(?:fetch|axios)\s*\([^)]{1,250}\)/gi,
            /(?:graphql|api|endpoint|latest_rolls|spin-history|results|history|rolls)[^"'`\s]{0,100}/gi
          ];

          const encontrados = [];

          for (const padrao of padroes) {
            const matches = js.match(padrao) || [];

            for (const item of matches) {
              if (
                /api|graphql|result|history|roll|spin|game|casino/i
                  .test(item)
              ) {
                encontrados.push(item.substring(0, 250));
              }

              if (encontrados.length >= 20) break;
            }

            if (encontrados.length >= 20) break;
          }

          return {
            ficheiro: src,
            sucesso: true,
            tamanho: js.length,
            pistas: [...new Set(encontrados)].slice(0, 20)
          };
        } catch (error) {
          return {
            ficheiro: src,
            sucesso: false,
            erro: error.response
              ? "HTTP " + error.response.status
              : error.message
          };
        }
      })
    );

    res.json({
      fonte: "CasinoScores",
      sucesso: true,
      scriptsEncontrados: ficheiros.length,
      scriptsAnalisados: resultados.length,
      resultados,
      aviso:
        "As pistas são candidatas a investigação; nenhuma API é considerada confirmada sem testar a resposta."
    });
  } catch (error) {
    res.status(502).json({
      sucesso: false,
      erro: error.response
        ? "HTTP " + error.response.status
        : error.message
    });
  }
});

// Testar ligação ao Telegram
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
    rotaSolicitada: req.path
  });
});

app.listen(PORT, "0.0.0.0", () => {
  console.log(`Dados AO Bot ativo na porta ${PORT}`);
});
  
