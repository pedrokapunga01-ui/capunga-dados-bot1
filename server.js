
const express = require("express");
const cors = require("cors");
const axios = require("axios");
const { Pool } = require("pg");

const app = express();
const PORT = process.env.PORT || 3000;
const INTERVALO_SINCRONIZACAO = 10000;
const MAX_GALE = 2;
const SINAIS_ATIVOS = process.env.SINAIS_ATIVOS === "true";

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

let sincronizacaoEmCurso = false;
let ultimaSincronizacao = null;
let ultimoErroSincronizacao = null;

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
    .filter(
      r => r.id && r.resultado !== "Desconhecido"
    );
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
        ((NOW() AT TIME ZONE 'Africa/Luanda')::date),
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
        ((NOW() AT TIME ZONE 'Africa/Luanda')::date),
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

  const resposta = await axios.post(
    `https://api.telegram.org/bot${token}/sendMessage`,
    {
      chat_id: chatId,
      text: mensagem
    },
    { timeout: 15000 }
  );

  return resposta.data;
}

async function atualizarEstatistica(campo, incremento = 1, dia = null) {
  const camposPermitidos = [
    "total_sinais", "green", "loss", "g1", "g2", "empates"
  ];

  if (!camposPermitidos.includes(campo)) return;

  const data = dia || null;

  await pool.query(
    `INSERT INTO estatisticas_diarias
       (dia_angola, ${campo}, atualizada_em)
     VALUES (
       COALESCE($1::date, (NOW() AT TIME ZONE 'Africa/Luanda')::date),
       $2, NOW()
     )
     ON CONFLICT (dia_angola)
     DO UPDATE SET
       ${campo} = estatisticas_diarias.${campo} + EXCLUDED.${campo},
       atualizada_em = NOW()`,
    [data, incremento]
  );
}

async function resolverSinalComRodada(rodada) {
  const resultadoSinal = await pool.query(
    `SELECT *
     FROM sinais
     WHERE estado = 'PENDENTE'
       AND resultado_id IS DISTINCT FROM $1
     ORDER BY id ASC
     LIMIT 1`,
    [rodada.id]
  );

  if (!resultadoSinal.rowCount) return;

  const sinal = resultadoSinal.rows[0];

  if (rodada.resultado === "Empate") {
    await atualizarEstatistica("empates", 1, sinal.dia_angola);

    await enviarTelegram(
      `🟡 DADOS AO BOT\n` +
      `Resultado: EMPATE\n` +
      `Sinal #${sinal.id} protegido.\n` +
      `A gale não avança.\n` +
      `A aguardar nova rodada.`
    ).catch(err => console.error("Telegram:", err.message));

    return;
  }

  if (rodada.resultado === sinal.cor) {
    const estadoFinal =
      sinal.gale === 0 ? "GREEN" :
      sinal.gale === 1 ? "G1" : "G2";

    await pool.query(
      `UPDATE sinais
       SET estado = $1, resolvido_em = NOW(),
           observacoes = COALESCE(observacoes, '')
             || $2
       WHERE id = $3`,
      [
        estadoFinal,
        ` | Resultado confirmado: ${rodada.resultado}`,
        sinal.id
      ]
    );

    const campo =
      estadoFinal === "GREEN" ? "green" : estadoFinal.toLowerCase();

    await atualizarEstatistica(campo, 1, sinal.dia_angola);

    await enviarTelegram(
      `✅ DADOS AO BOT\n` +
      `Sinal #${sinal.id}: ${estadoFinal}\n` +
      `Resultado: ${rodada.resultado}\n` +
      `Hora Angola: ${horaAngola()}`
    ).catch(err => console.error("Telegram:", err.message));

    return;
  }

  if (sinal.gale < MAX_GALE) {
    const proximaGale = sinal.gale + 1;

    await pool.query(
      `UPDATE sinais
       SET gale = $1,
           observacoes = COALESCE(observacoes, '') || $2
       WHERE id = $3`,
      [
        proximaGale,
        ` | Divergência na gale ${sinal.gale}: ${rodada.resultado}`,
        sinal.id
      ]
    );

    await enviarTelegram(
      `⚠️ DADOS AO BOT\n` +
      `Sinal #${sinal.id}\n` +
      `Resultado: ${rodada.resultado}\n` +
      `A avançar para G${proximaGale}.\n` +
      `Cor original: ${sinal.cor}`
    ).catch(err => console.error("Telegram:", err.message));

    return;
  }

  await pool.query(
    `UPDATE sinais
     SET estado = 'LOSS',
         resolvido_em = NOW(),
         observacoes = COALESCE(observacoes, '') || $1
     WHERE id = $2`,
    [
      ` | LOSS após G2; último resultado: ${rodada.resultado}`,
      sinal.id
    ]
  );

  await atualizarEstatistica("loss", 1, sinal.dia_angola);

  await enviarTelegram(
    `❌ DADOS AO BOT\n` +
    `Sinal #${sinal.id}: LOSS\n` +
    `Gale máximo atingido.\n` +
    `Hora Angola: ${horaAngola()}`
  ).catch(err => console.error("Telegram:", err.message));
}

/*
 Estratégia experimental:
 identifica três resultados iguais consecutivos,
 precedidos por um resultado diferente.
 Não existe garantia de previsão ou de lucro.
*/
async function tentarGerarSinal(rodadaAtual) {
  if (!SINAIS_ATIVOS) return;
  if (!["Banca", "Jogador"].includes(rodadaAtual.resultado)) return;

  const pendente = await pool.query(
    `SELECT id FROM sinais
     WHERE estado = 'PENDENTE'
     LIMIT 1`
  );

  if (pendente.rowCount) return;

  const ultimos = await pool.query(`
    SELECT id, resultado
    FROM bacbo_resultados
    WHERE resultado IN ('Banca', 'Jogador', 'Empate')
    ORDER BY COALESCE(fim, inicio) DESC NULLS LAST,
             guardado_em DESC
    LIMIT 4
  `);

  if (ultimos.rowCount < 4) return;

  const [a, b, c, d] = ultimos.rows;

  if (
    a.resultado === "Empate" ||
    b.resultado === "Empate" ||
    c.resultado === "Empate" ||
    d.resultado === "Empate"
  ) return;

  // a é o mais recente; b e c devem igualar a;
  // d deve ser diferente.
  if (
    a.resultado !== b.resultado ||
    b.resultado !== c.resultado ||
    c.resultado === d.resultado
  ) return;

  const cor = a.resultado === "Banca" ? "Jogador" : "Banca";

  const criado = await pool.query(
    `INSERT INTO sinais
       (cor, estado, gale, resultado_id, observacoes)
     VALUES ($1, 'PENDENTE', 0, $2, $3)
     RETURNING id, cor, criado_em`,
    [
      cor,
      rodadaAtual.id,
      "Estratégia experimental: sequência de três resultados iguais."
    ]
  );

  const sinal = criado.rows[0];

  await atualizarEstatistica("total_sinais", 1);

  try {
    await enviarTelegram(
      `🎯 DADOS AO BOT — NOVO SINAL\n\n` +
      `Sinal: #${sinal.id}\n` +
      `Mesa: Bac bo\n` +
      `Entrada experimental: ${sinal.cor}\n` +
      `Proteção: Empate não avança a gale\n` +
      `Gale máximo: G2\n` +
      `Hora Angola: ${horaAngola()}\n\n` +
      `⚠️ Sinal experimental, sem garantia de acerto.`
    );
  } catch (erro) {
    console.error("Falha ao enviar sinal:", erro.message);
  }
}

async function processarNovaRodada(rodada) {
  await resolverSinalComRodada(rodada);
  await tentarGerarSinal(rodada);
}

async function sincronizarResultados() {
  if (!pool) {
    throw new Error("Base de dados não configurada.");
  }

  await prepararBaseDados();

  const resultados = await obterResultados();

  // Processar do mais antigo para o mais recente.
  resultados.sort((a, b) => {
    const dataA = new Date(a.fim || a.inicio || 0).getTime();
    const dataB = new Date(b.fim || b.inicio || 0).getTime();
    return dataA - dataB;
  });

  let novosGuardados = 0;

  for (const r of resultados) {
    const insercao = await pool.query(
      `INSERT INTO bacbo_resultados
        (
          id, resultado, estado, inicio, fim,
          pontos_jogador, pontos_banca, multiplicador
        )
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

    if (insercao.rowCount > 0) {
      novosGuardados++;
      await processarNovaRodada(r);
    }
  }

  ultimaSincronizacao = horaAngola();
  ultimoErroSincronizacao = null;

  return {
    recebidos: resultados.length,
    novosGuardados
  };
}

async function sincronizarAutomaticamente() {
  if (!pool || sincronizacaoEmCurso) return;

  sincronizacaoEmCurso = true;

  try {
    const resultado = await sincronizarResultados();

    if (resultado.novosGuardados > 0) {
      console.log(`Novos resultados: ${resultado.novosGuardados}`);
    }
  } catch (erro) {
    ultimoErroSincronizacao =
      erro.response?.data?.message ||
      erro.response?.statusText ||
      erro.message;

    console.error("Erro de sincronização:", ultimoErroSincronizacao);
  } finally {
    sincronizacaoEmCurso = false;
  }
}

app.get("/", (req, res) => {
  res.json({
    nome: "Dados AO Bot",
    online: true,
    sinaisAtivos: SINAIS_ATIVOS,
    horaAngola: horaAngola(),
    rotas: [
      "/health",
      "/diagnostico",
      "/db/teste",
      "/db/preparar",
      "/db/sincronizar",
      "/resultados",
      "/sinais",
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
    sinaisAtivos: SINAIS_ATIVOS,
    sincronizacaoEmCurso,
    ultimaSincronizacao,
    erroSincronizacao: ultimoErroSincronizacao,
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
    const resultado = await pool.query(
      "SELECT NOW() AS hora_servidor"
    );

    res.json({
      sucesso: true,
      mensagem: "Ligação PostgreSQL estabelecida.",
      horaAngola: horaAngola(),
      teste: resultado.rows[0]
    });
  } catch (erro) {
    res.status(500).json({ sucesso: false, erro: erro.message });
  }
});

app.get("/db/preparar", async (req, res) => {
  try {
    await prepararBaseDados();
    res.json({ sucesso: true, mensagem: "Tabelas preparadas." });
  } catch (erro) {
    res.status(500).json({ sucesso: false, erro: erro.message });
  }
});

app.get("/db/sincronizar", async (req, res) => {
  if (sincronizacaoEmCurso) {
    return res.status(409).json({
      sucesso: false,
      mensagem: "Já existe uma sincronização em curso."
    });
  }

  sincronizacaoEmCurso = true;

  try {
    const resultado = await sincronizarResultados();

    res.json({
      sucesso: true,
      ...resultado,
      ultimaSincronizacao,
      sinaisAtivos: SINAIS_ATIVOS
    });
  } catch (erro) {
    res.status(500).json({
      sucesso: false,
      erro: erro.response?.data?.message || erro.message
    });
  } finally {
    sincronizacaoEmCurso = false;
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
    res.status(500).json({ sucesso: false, erro: erro.message });
  }
});

app.get("/sinais", async (req, res) => {
  if (!pool) {
    return res.status(503).json({
      sucesso: false,
      erro: "Base de dados não configurada."
    });
  }

  try {
    const limite = Math.min(
      Math.max(parseInt(req.query.limite, 10) || 20, 1),
      100
    );

    const resultado = await pool.query(
      `SELECT id, criado_em, dia_angola, cor, estado,
              gale, resultado_id, resolvido_em, observacoes
       FROM sinais
       ORDER BY id DESC
       LIMIT $1`,
      [limite]
    );

    res.json({
      sucesso: true,
      sinaisAtivos: SINAIS_ATIVOS,
      total: resultado.rowCount,
      sinais: resultado.rows
    });
  } catch (erro) {
    res.status(500).json({ sucesso: false, erro: erro.message });
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
        mensagem: "Ainda não existem estatísticas para hoje."
      },
      ultimos30Dias: ultimosDias.rows
    });
  } catch (erro) {
    res.status(500).json({ sucesso: false, erro: erro.message });
  }
});

app.get("/telegram/teste", async (req, res) => {
  try {
    await enviarTelegram(
      "✅ Dados AO Bot: teste de ligação ao Telegram.\n" +
      "Hora de Angola: " + horaAngola()
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

const servidor = app.listen(PORT, () => {
  console.log(`Dados AO Bot ativo na porta ${PORT}`);
  console.log(`Hora de Angola: ${horaAngola()}`);
  console.log(`Sinais automáticos: ${SINAIS_ATIVOS ? "ATIVOS" : "DESATIVADOS"}`);

  setTimeout(() => {
    sincronizarAutomaticamente();
    setInterval(sincronizarAutomaticamente, INTERVALO_SINCRONIZACAO);
  }, 3000);
});

process.on("SIGTERM", async () => {
  servidor.close();

  if (pool) {
    await pool.end();
  }

  process.exit(0);
});
    
