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
app.use(express.json({ limit: "100kb" }));

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
let sincronizacaoInicialConcluida = false;
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

  if (valor.includes("banker") || valor === "banca") {
    return "Banca";
  }

  if (valor.includes("player") || valor === "jogador") {
    return "Jogador";
  }

  if (
    valor.includes("tie") ||
    valor.includes("draw") ||
    valor === "empate"
  ) {
    return "Empate";
  }

  return "Desconhecido";
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
    .filter(r =>
      r.id &&
      ["Banca", "Jogador", "Empate"].includes(r.resultado) &&
      String(r.estado).toLowerCase() === "resolved"
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

  await pool.query(`
    CREATE TABLE IF NOT EXISTS testes_sinais (
      id BIGSERIAL PRIMARY KEY,
      criado_em TIMESTAMPTZ DEFAULT NOW(),
      cenario TEXT NOT NULL,
      resultado TEXT NOT NULL,
      passou BOOLEAN NOT NULL
    )
  `);
}

async function enviarTelegram(mensagem) {
  const token = process.env.BOT_TOKEN;
  const chatId = process.env.CHAT_ID;

  if (!token || !chatId) {
    throw new Error("BOT_TOKEN ou CHAT_ID não configurado no Render.");
  }

  const resposta = await axios.post(
    `https://api.telegram.org/bot${token}/sendMessage`,
    { chat_id: chatId, text: mensagem },
    { timeout: 15000 }
  );

  if (!resposta.data?.ok) {
    throw new Error("O Telegram não confirmou o envio.");
  }

  return resposta.data;
}

async function atualizarEstatistica(campo, incremento = 1, dia = null) {
  const permitidos = [
    "total_sinais", "green", "loss", "g1", "g2", "empates"
  ];

  if (!permitidos.includes(campo)) {
    throw new Error("Campo estatístico inválido.");
  }

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
    [dia, incremento]
  );
}

function avaliarPasso(sinal, resultado) {
  if (resultado === "Empate") {
    return {
      estado: "PENDENTE",
      gale: sinal.gale,
      empateProtegido: true
    };
  }

  if (resultado === sinal.cor) {
    return {
      estado:
        sinal.gale === 0 ? "GREEN" :
        sinal.gale === 1 ? "G1" : "G2",
      gale: sinal.gale,
      empateProtegido: false
    };
  }

  if (sinal.gale < MAX_GALE) {
    return {
      estado: "PENDENTE",
      gale: sinal.gale + 1,
      empateProtegido: false
    };
  }

  return {
    estado: "LOSS",
    gale: sinal.gale,
    empateProtegido: false
  };
}

function executarTestesLocais() {
  const testes = [
    {
      cenario: "GREEN",
      sinal: { cor: "Banca", gale: 0 },
      sequencia: ["Banca"],
      esperado: "GREEN"
    },
    {
      cenario: "G1",
      sinal: { cor: "Banca", gale: 0 },
      sequencia: ["Jogador", "Banca"],
      esperado: "G1"
    },
    {
      cenario: "G2",
      sinal: { cor: "Banca", gale: 0 },
      sequencia: ["Jogador", "Jogador", "Banca"],
      esperado: "G2"
    },
    {
      cenario: "LOSS",
      sinal: { cor: "Banca", gale: 0 },
      sequencia: ["Jogador", "Jogador", "Jogador"],
      esperado: "LOSS"
    },
    {
      cenario: "EMPATE_PROTEGIDO",
      sinal: { cor: "Banca", gale: 0 },
      sequencia: ["Empate", "Banca"],
      esperado: "GREEN"
    }
  ];

  return testes.map(teste => {
    const sinal = { ...teste.sinal };
    let estadoFinal = "PENDENTE";
    let empateProtegido = false;

    for (const resultado of teste.sequencia) {
      const avaliacao = avaliarPasso(sinal, resultado);
      sinal.gale = avaliacao.gale;
      empateProtegido ||= avaliacao.empateProtegido;

      if (avaliacao.estado !== "PENDENTE") {
        estadoFinal = avaliacao.estado;
        break;
      }
    }

    return {
      cenario: teste.cenario,
      esperado: teste.esperado,
      resultado: estadoFinal,
      empateProtegido,
      passou: estadoFinal === teste.esperado
    };
  });
}

async function guardarTeste(cenario, resultado, passou) {
  await pool.query(
    `INSERT INTO testes_sinais (cenario, resultado, passou)
     VALUES ($1, $2, $3)`,
    [cenario, resultado, passou]
  );
}

async function resolverSinalComRodada(rodada) {
  const consulta = await pool.query(
    `SELECT *
     FROM sinais
     WHERE estado = 'PENDENTE'
     ORDER BY id ASC
     LIMIT 1`
  );

  if (!consulta.rowCount) return;

  const sinal = consulta.rows[0];

  if (sinal.resultado_id === rodada.id) return;

  const avaliacao = avaliarPasso(sinal, rodada.resultado);

  if (avaliacao.empateProtegido) {
    await pool.query(
      `UPDATE sinais
       SET resultado_id = $1,
           observacoes = COALESCE(observacoes, '') ||
             ' | Empate protegido; gale mantida.'
       WHERE id = $2 AND estado = 'PENDENTE'`,
      [rodada.id, sinal.id]
    );

    await atualizarEstatistica("empates", 1, sinal.dia_angola);
    return;
  }

  if (avaliacao.estado === "PENDENTE") {
    await pool.query(
      `UPDATE sinais
       SET gale = $1,
           resultado_id = $2,
           observacoes = COALESCE(observacoes, '') || $3
       WHERE id = $4 AND estado = 'PENDENTE'`,
      [
        avaliacao.gale,
        rodada.id,
        ` | Resultado: ${rodada.resultado}; próxima gale: G${avaliacao.gale}.`,
        sinal.id
      ]
    );

    await enviarTelegram(
      `⚠️ Dados AO Bot — Sinal #${sinal.id}\n` +
      `Resultado: ${rodada.resultado}\n` +
      `Próxima etapa: G${avaliacao.gale}`
    ).catch(err => console.error("Erro Telegram:", err.message));

    return;
  }

  const atualizacao = await pool.query(
    `UPDATE sinais
     SET estado = $1,
         resultado_id = $2,
         resolvido_em = NOW(),
         observacoes = COALESCE(observacoes, '') || $3
     WHERE id = $4 AND estado = 'PENDENTE'
     RETURNING id`,
    [
      avaliacao.estado,
      rodada.id,
      ` | Fechado com ${rodada.resultado}.`,
      sinal.id
    ]
  );

  if (!atualizacao.rowCount) return;

  const campo = avaliacao.estado === "LOSS"
    ? "loss"
    : avaliacao.estado.toLowerCase();

  await atualizarEstatistica(campo, 1, sinal.dia_angola);

  await enviarTelegram(
    `${avaliacao.estado === "LOSS" ? "❌" : "✅"} Dados AO Bot\n` +
    `Sinal #${sinal.id}: ${avaliacao.estado}\n` +
    `Resultado: ${rodada.resultado}\n` +
    `Hora Angola: ${horaAngola()}`
  ).catch(err => console.error("Erro Telegram:", err.message));
}

/*
 * Filtro experimental seletivo para procurar GS.
 * Exige quatro resultados iguais consecutivos e um resultado
 * diferente antes dessa sequência. Não é garantia de previsão.
 */
async function tentarGerarSinal(rodada) {
  if (!SINAIS_ATIVOS) return;
  if (!["Banca", "Jogador"].includes(rodada.resultado)) return;

  const pendente = await pool.query(
    `SELECT id FROM sinais
     WHERE estado = 'PENDENTE'
     LIMIT 1`
  );

  if (pendente.rowCount) return;

  const consulta = await pool.query(`
    SELECT id, resultado
    FROM bacbo_resultados
    WHERE resultado IN ('Banca', 'Jogador', 'Empate')
    ORDER BY COALESCE(fim, inicio) DESC NULLS LAST,
             guardado_em DESC
    LIMIT 5
  `);

  if (consulta.rowCount < 5) return;

  const [a, b, c, d, e] = consulta.rows;

  if ([a, b, c, d, e].some(r => r.resultado === "Empate")) return;

  const quatroIguais =
    a.resultado === b.resultado &&
    b.resultado === c.resultado &&
    c.resultado === d.resultado;

  const anteriorDiferente = d.resultado !== e.resultado;

  if (!quatroIguais || !anteriorDiferente) return;

  const cor = a.resultado;

  const criado = await pool.query(
    `INSERT INTO sinais
       (cor, estado, gale, resultado_id, observacoes)
     VALUES ($1, 'PENDENTE', 0, $2, $3)
     RETURNING id, cor`,
    [
      cor,
      rodada.id,
      "Filtro experimental de sequência; sem garantia de acerto ou lucro."
    ]
  );

  await atualizarEstatistica("total_sinais");

  const sinal = criado.rows[0];

  await enviarTelegram(
    `🎯 DADOS AO BOT — SINAL EXPERIMENTAL\n\n` +
    `Sinal #${sinal.id}\n` +
    `Entrada: ${sinal.cor}\n` +
    `Objetivo: GREEN de primeira\n` +
    `Proteção: empate não avança a gale\n` +
    `Máximo: G2\n` +
    `Hora Angola: ${horaAngola()}\n\n` +
    `Regra experimental; sem garantia de acerto ou lucro.`
  ).catch(err => console.error("Erro Telegram:", err.message));
}

async function sincronizarResultados() {
  if (!pool) throw new Error("Base de dados não configurada.");

  await prepararBaseDados();

  const resultados = await obterResultados();

  resultados.sort((a, b) =>
    new Date(a.fim || a.inicio || 0).getTime() -
    new Date(b.fim || b.inicio || 0).getTime()
  );

  /*
   * Na primeira sincronização, apenas carrega o histórico.
   * Não gera nem resolve sinais com rondas antigas.
   */
  const primeiraSincronizacao = !sincronizacaoInicialConcluida;
  let novosGuardados = 0;

  for (const rodada of resultados) {
    const insercao = await pool.query(
      `INSERT INTO bacbo_resultados
       (id, resultado, estado, inicio, fim,
        pontos_jogador, pontos_banca, multiplicador)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
       ON CONFLICT (id) DO NOTHING
       RETURNING id`,
      [
        rodada.id,
        rodada.resultado,
        rodada.estado,
        rodada.inicio,
        rodada.fim,
        rodada.pontosJogador,
        rodada.pontosBanca,
        rodada.multiplicador
      ]
    );

    if (insercao.rowCount) {
      novosGuardados++;

      if (!primeiraSincronizacao) {
        await resolverSinalComRodada(rodada);
        await tentarGerarSinal(rodada);
      }
    }
  }

  sincronizacaoInicialConcluida = true;
  ultimaSincronizacao = horaAngola();
  ultimoErroSincronizacao = null;

  return {
    recebidos: resultados.length,
    novosGuardados,
    primeiraSincronizacao,
    historicoProcessado: !primeiraSincronizacao
  };
}

async function sincronizarAutomaticamente() {
  if (!pool || sincronizacaoEmCurso) return;

  sincronizacaoEmCurso = true;

  try {
    await sincronizarResultados();
  } catch (erro) {
    ultimoErroSincronizacao =
      erro.response?.data?.message || erro.message;
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
      "/health", "/diagnostico", "/db/teste", "/db/preparar",
      "/db/sincronizar", "/resultados", "/sinais",
      "/estatisticas", "/testes", "/telegram/teste"
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
    sincronizacaoInicialConcluida,
    ultimaSincronizacao,
    erroSincronizacao: ultimoErroSincronizacao,
    horaAngola: horaAngola()
  });
});

app.get("/db/teste", async (req, res) => {
  if (!pool) {
    return res.status(503).json({
      sucesso: false,
      erro: "DATABASE_URL não está configurada."
    });
  }

  try {
    const resultado = await pool.query("SELECT NOW() AS hora_servidor");

    res.json({
      sucesso: true,
      mensagem: "Ligação PostgreSQL estabelecida.",
      teste: resultado.rows[0],
      horaAngola: horaAngola()
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
      mensagem: "Sincronização já em curso."
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
       ORDER BY COALESCE(fim, inicio) DESC NULLS LAST,
                guardado_em DESC
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
      `SELECT id, criado_em, dia_angola, cor, estado, gale,
              resultado_id, resolvido_em, observacoes
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
      SELECT dia_angola, total_sinais, green, loss, g1, g2,
             empates, atualizada_em
      FROM estatisticas_diarias
      WHERE dia_angola =
        (NOW() AT TIME ZONE 'Africa/Luanda')::date
    `);

    const dias = await pool.query(`
      SELECT dia_angola, total_sinais, green, loss, g1, g2, empates
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
      ultimos30Dias: dias.rows
    });
  } catch (erro) {
    res.status(500).json({ sucesso: false, erro: erro.message });
  }
});

app.get("/testes", (req, res) => {
  const resultados = executarTestesLocais();

  res.json({
    sucesso: resultados.every(t => t.passou),
    modo: "simulação isolada",
    sinaisReaisCriados: false,
    mensagensTelegramEnviadas: false,
    resultados
  });
});

app.post("/testes/guardar", async (req, res) => {
  if (!pool) {
    return res.status(503).json({
      sucesso: false,
      erro: "Base de dados não configurada."
    });
  }

  try {
    await prepararBaseDados();
    const resultados = executarTestesLocais();

    for (const teste of resultados) {
      await guardarTeste(teste.cenario, teste.resultado, teste.passou);
    }

    res.json({
      sucesso: resultados.every(t => t.passou),
      guardados: resultados.length,
      resultados
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
  console.log(`Hora Angola: ${horaAngola()}`);
  console.log(
    `Sinais automáticos: ${SINAIS_ATIVOS ? "ATIVOS" : "DESATIVADOS"}`
  );

  setTimeout(() => {
    sincronizarAutomaticamente();
    setInterval(
      sincronizarAutomaticamente,
      INTERVALO_SINCRONIZACAO
    );
  }, 3000);
});

process.on("SIGTERM", async () => {
  servidor.close();

  if (pool) await pool.end();

  process.exit(0);
});
