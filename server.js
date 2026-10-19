/**
 * Capunga Dados 🎲🇺🇸 — Server final
 *
 * Variáveis de ambiente:
 *  DATABASE_URL, BOT_TOKEN, CHAT_ID           (obrigatórias)
 *  SINAIS_ATIVOS=true                         (liga os sinais)
 *  ADMIN_TOKEN=uma_senha_forte                (para /db/* e /telegram/teste)
 *  MAX_REDS_DIA=3                             (stop diário; 0 desliga)
 *  STREAK_MIN=4                               (nº de cores iguais para entrar)
 *  EMPATE_PROTEGIDO=true                      (empate mantém a etapa)
 *  INTERVALO_MINIMO_MS=180000
 *  SINAL_EXPIRA_MIN=12
 *  PLACAR_DESDE=2026-10-19T00:00:00Z          (ignora sinais de teste antigos)
 *  BOT_CARD_IMAGE_URL=https://...             (imagem opcional antes da entrada)
 *  MOSTRAR_RISCO=true                         (linha de risco na entrada)
 *  MESA_NOME="Bac Bo - Evolution"
 *  BOTAO_URL / BOTAO_TEXTO                    (botão opcional na entrada)
 *  PLACAR_FIXO=true                           (placar do dia fixado e editado)
 *  RESUMO_DIARIO=true                         (resumo do dia anterior)
 *  MSG_ANALISANDO=true                        (aviso "analisando")
 *
 * AVISO: nenhuma regra de entrada altera a probabilidade de uma rodada de
 * dados. O placar é o que realmente acontece; não existe garantia.
 */

"use strict";

const crypto = require("crypto");
const express = require("express");
const cors = require("cors");
const axios = require("axios");
const { Pool } = require("pg");

// ======================================================================
// 1. CONFIGURAÇÃO
// ======================================================================

const PORT = process.env.PORT || 3000;

function envBool(nome, padrao) {
  const v = process.env[nome];
  if (v === undefined || v === "") return padrao;
  return String(v).toLowerCase() === "true";
}

function envNum(nome, padrao) {
  const n = Number(process.env[nome]);
  return Number.isFinite(n) && process.env[nome] !== "" &&
    process.env[nome] !== undefined ? n : padrao;
}

const CFG = {
  nomeBot: process.env.NOME_BOT || "Capunga Dados🎲🇺🇸",
  intervaloSync: 10000,
  intervaloMinimoSinais: envNum("INTERVALO_MINIMO_MS", 180000),
  maxGale: 2,
  multiplicadorGale: 2,
  streakMin: Math.max(2, envNum("STREAK_MIN", 4)),
  empateProtegido: envBool("EMPATE_PROTEGIDO", true),
  sinaisAtivos: envBool("SINAIS_ATIVOS", false),
  maxRedsDia: envNum("MAX_REDS_DIA", 3),
  expiraMin: envNum("SINAL_EXPIRA_MIN", 12),
  janelaRecenteMs: envNum("JANELA_RECENTE_MS", 240000),
  placarDesde: process.env.PLACAR_DESDE || null,
  mostrarRisco: envBool("MOSTRAR_RISCO", true),
  imagem: process.env.BOT_CARD_IMAGE_URL || null,
  amostraMinima: 30,
  mesa: process.env.MESA_NOME || "Bac Bo - Evolution",
  botaoUrl: process.env.BOTAO_URL || null,
  botaoTexto: process.env.BOTAO_TEXTO || "🎰 Ir para a mesa",
  placarFixo: envBool("PLACAR_FIXO", true),
  resumoDiario: envBool("RESUMO_DIARIO", true),
  analisando: envBool("MSG_ANALISANDO", true),
  analisandoIntervaloMs: envNum("ANALISANDO_INTERVALO_MS", 120000),
  lockId: 774411
};

const API_BACBO =
  "https://api-cs.casino.org/svc-evolution-game-events/api/bacbo";

const EMOJI = { Jogador: "🔵", Banca: "🔴", Empate: "🟡" };
const ROTULO = { Jogador: "JOGADOR", Banca: "BANCA", Empate: "EMPATE" };

const pool = process.env.DATABASE_URL
  ? new Pool({
      connectionString: process.env.DATABASE_URL,
      ssl: { rejectUnauthorized: false },
      connectionTimeoutMillis: 10000,
      idleTimeoutMillis: 30000,
      max: 5
    })
  : null;

const estado = {
  sincronizacaoEmCurso: false,
  sincronizacaoInicialConcluida: false,
  tabelasPreparadas: false,
  ultimaSincronizacao: null,
  ultimoErro: null,
  pausaAvisadaNoDia: null,
  ultimoAnalisandoMs: 0,
  timer: null
};

// ======================================================================
// 2. UTILITÁRIOS
// ======================================================================

const dormir = ms => new Promise(r => setTimeout(r, ms));

function horaAngola(data = new Date()) {
  return new Intl.DateTimeFormat("pt-PT", {
    timeZone: "Africa/Luanda",
    dateStyle: "short",
    timeStyle: "medium",
    hour12: false
  }).format(data);
}

function diaAngola(data = new Date()) {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: "Africa/Luanda"
  }).format(data);
}

function traduzirResultado(resultado) {
  const valor = String(resultado || "").toLowerCase();
  if (valor.includes("banker") || valor === "banca") return "Banca";
  if (valor.includes("player") || valor === "jogador") return "Jogador";
  if (valor.includes("tie") || valor.includes("draw") || valor === "empate") {
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

function instanteRodada(rodada) {
  const t = rodada.fim || rodada.inicio;
  const ms = t ? new Date(t).getTime() : NaN;
  return Number.isFinite(ms) ? ms : null;
}

function rodadaRecente(rodada, primeiraSincronizacao) {
  const ms = instanteRodada(rodada);
  if (ms === null) return !primeiraSincronizacao;
  return Date.now() - ms <= CFG.janelaRecenteMs;
}

function fmtPct(valor) {
  return valor === null || valor === undefined
    ? "—"
    : `${valor.toFixed(1)}%`;
}

function fmtUn(valor) {
  const arred = Math.round(valor * 100) / 100;
  return `${arred > 0 ? "+" : ""}${arred}`;
}

// ======================================================================
// 3. REGRAS PURAS (testáveis sem banco)
// ======================================================================

/**
 * Entrada: as últimas `streakMin` rodadas decisivas (sem empate) têm a
 * mesma cor e a anterior a elas é diferente. Retorna a cor ou null.
 * `resultadosDesc` vem do mais recente para o mais antigo.
 */
function detectarEntrada(resultadosDesc, streakMin = CFG.streakMin) {
  const decisivos = resultadosDesc.filter(r => r === "Banca" || r === "Jogador");
  if (decisivos.length < streakMin + 1) return null;

  const cor = decisivos[0];
  for (let i = 1; i < streakMin; i++) {
    if (decisivos[i] !== cor) return null;
  }
  return decisivos[streakMin] !== cor ? cor : null;
}

/** Pré-alerta: falta 1 rodada igual para a regra de entrada disparar. */
function detectarPreAlerta(resultadosDesc, streakMin = CFG.streakMin) {
  const alvo = streakMin - 1;
  const d = resultadosDesc.filter(r => r === "Banca" || r === "Jogador");
  if (d.length < alvo + 1) return null;
  for (let i = 1; i < alvo; i++) {
    if (d[i] !== d[0]) return null;
  }
  return d[alvo] !== d[0] ? d[0] : null;
}

/** Sequência atual e recorde de greens (do mais antigo ao mais recente). */
function sequenciaGreens(estadosAsc) {
  let atual = 0;
  let recorde = 0;
  for (const e of estadosAsc) {
    if (e === "LOSS") {
      atual = 0;
    } else if (e === "GREEN" || e === "G1" || e === "G2") {
      atual++;
      if (atual > recorde) recorde = atual;
    }
  }
  return { atual, recorde };
}

/** Taxas teóricas exatas desta estratégia (referência para o placar real). */
function taxaTeorica(empateProtegido = CFG.empateProtegido, maxGale = CFG.maxGale) {
  const pTie = 146 / 1296;
  const pWin = empateProtegido ? 0.5 : (1 - pTie) / 2;
  const q = 1 - pWin;
  const pRed = Math.pow(q, maxGale + 1);

  let ev = pRed * lucroPorDesfecho("LOSS");
  for (let k = 0; k <= maxGale; k++) {
    ev += Math.pow(q, k) * pWin * lucroPorDesfecho(estadoDoGale(k));
  }

  return {
    empateProtegido,
    verdeDireto: +(100 * pWin).toFixed(2),
    verdeAteLimite: +(100 * (1 - pRed)).toFixed(2),
    red: +(100 * pRed).toFixed(2),
    esperancaPorSinalUn: +ev.toFixed(4),
    leitura: "Percentagens acima da teórica por poucos sinais são sorte. " +
             "Com o RED custando 7x o green, o saldo esperado nunca é positivo."
  };
}

function estadoDoGale(gale) {
  return gale === 0 ? "GREEN" : gale === 1 ? "G1" : "G2";
}

/**
 * Avalia um passo do sinal:
 *  - PENDENTE com `empate: true`  => empate protegido, etapa mantida
 *  - PENDENTE com gale+1          => errou, próxima etapa
 *  - GREEN | G1 | G2              => acertou
 *  - LOSS                         => errou após o gale limite
 */
function avaliarPasso(sinal, resultado, empateProtegido = CFG.empateProtegido) {
  const gale = Number(sinal.gale);

  if (resultado === "Empate" && empateProtegido) {
    return { estado: "PENDENTE", gale, empate: true };
  }

  if (resultado === sinal.cor) {
    return { estado: estadoDoGale(gale), gale, empate: false };
  }

  if (gale < CFG.maxGale) {
    return { estado: "PENDENTE", gale: gale + 1, empate: false };
  }

  return { estado: "LOSS", gale, empate: false };
}

/** Lucro líquido, em unidades da aposta base, de cada desfecho. */
function lucroPorDesfecho(estadoFinal, mult = CFG.multiplicadorGale,
                          maxGale = CFG.maxGale) {
  const stake = k => Math.pow(mult, k);
  const gastoAte = k => {
    let s = 0;
    for (let j = 0; j < k; j++) s += stake(j);
    return s;
  };

  if (estadoFinal === "GREEN") return stake(0);
  if (estadoFinal === "G1") return stake(1) - gastoAte(1);
  if (estadoFinal === "G2") return stake(2) - gastoAte(2);
  if (estadoFinal === "LOSS") return -gastoAte(maxGale + 1);
  return 0;
}

function calcularUnidades(c) {
  return (
    c.g0 * lucroPorDesfecho("GREEN") +
    c.g1 * lucroPorDesfecho("G1") +
    c.g2 * lucroPorDesfecho("G2") +
    c.loss * lucroPorDesfecho("LOSS")
  );
}

function resumirPlacar(c) {
  const greens = c.g0 + c.g1 + c.g2;
  const total = greens + c.loss;
  return {
    g0: c.g0,
    g1: c.g1,
    g2: c.g2,
    loss: c.loss,
    empates: c.empates || 0,
    greens,
    total,
    pct: total ? (100 * greens) / total : null,
    pctDireto: total ? (100 * c.g0) / total : null,
    unidades: calcularUnidades(c)
  };
}

// ======================================================================
// 4. MENSAGENS DO TELEGRAM (HTML)
// ======================================================================

const LINHA = "━━━━━━━━━━━━━━━━━━";

function cab() {
  return `🎲 <b>${CFG.nomeBot}</b>\n${LINHA}`;
}

function blocoDesempenho(hoje, geral) {
  const linhas = [
    "📊 <b>Desempenho real do robô</b>",
    `📅 Hoje: ✅ ${hoje.greens} · 🟡 ${hoje.empates} · ❌ ${hoje.loss} ` +
      `(${fmtPct(hoje.pct)})`,
    `🏆 Geral: ✅ ${geral.greens} · ❌ ${geral.loss} ` +
      `(${fmtPct(geral.pct)}) em ${geral.total} sinais`,
    `🎯 Green de primeira: ${fmtPct(geral.pctDireto)} ` +
      `· G1: ${geral.g1} · G2: ${geral.g2}`,
    `🔥 Sequência: ${geral.seqAtual || 0} greens ` +
      `(recorde ${geral.recorde || 0})`,
    `💰 Saldo: hoje ${fmtUn(hoje.unidades)} un · ` +
      `geral ${fmtUn(geral.unidades)} un`
  ];

  if (geral.total < CFG.amostraMinima) {
    linhas.push("ℹ️ Amostra pequena: a percentagem oscila bastante.");
  }
  return linhas.join("\n");
}

function rodape() {
  return `${LINHA}\n🔞 Jogue com responsabilidade. Sem garantia de lucro.`;
}

function msgEntrada(sinal, hoje, geral) {
  const e = EMOJI[sinal.cor];
  const m = CFG.multiplicadorGale;
  const linhas = [
    cab(),
    `🚨 <b>SINAL CONFIRMADO #${sinal.id}</b> 🚨`,
    `🖥️ Mesa: ${CFG.mesa}`,
    "",
    `🎯 Entrada: ${e} <b>${ROTULO[sinal.cor]}</b>`,
    `🔁 Cobertura: até <b>G${CFG.maxGale}</b> ` +
      `(${CFG.maxGale + 1} tentativas · G1 ${m}x · G2 ${m * m}x)`,
    CFG.empateProtegido
      ? "🟡 Empate: mantém a etapa"
      : "🟡 Empate: conta como erro da etapa",
    `🕒 Angola: ${horaAngola()}`,
    "",
    blocoDesempenho(hoje, geral)
  ];

  if (CFG.mostrarRisco) {
    linhas.push(
      "",
      `⚠️ Risco: green +${lucroPorDesfecho("GREEN")} un · ` +
        `RED ${lucroPorDesfecho("LOSS")} un`
    );
  }

  linhas.push(rodape());
  return linhas.join("\n");
}

function msgEmpate(sinal) {
  return [
    cab(),
    `🟡 <b>EMPATE</b> — etapa mantida (Sinal #${sinal.id})`,
    `🔁 Repita ${EMOJI[sinal.cor]} <b>${ROTULO[sinal.cor]}</b>` +
      ` (${sinal.gale === 0 ? "entrada" : "G" + sinal.gale})`
  ].join("\n");
}

function msgGale(sinal, galeNovo, saiu) {
  const e = EMOJI[sinal.cor];
  const titulo = galeNovo === 1
    ? "⚠️ <b>NÃO BATEU — VAMOS PARA O 1º GALE (G1)!</b> 🔁"
    : "🚨 <b>SEGUNDO GALE (G2) — ÚLTIMA TENTATIVA!</b> 🚨";

  return [
    cab(),
    titulo,
    `Sinal #${sinal.id} · saiu ${EMOJI[saiu]} ${saiu}`,
    "",
    `🎯 Repita: ${e} <b>${ROTULO[sinal.cor]}</b> (G${galeNovo})`,
    `🕒 Angola: ${horaAngola()}`
  ].join("\n");
}

function msgGreen(sinal, estadoFinal, hoje, geral) {
  const titulos = {
    GREEN: "✅✅✅ <b>GREEN DE PRIMEIRA!</b> 🎉🔥",
    G1: "✅✅ <b>GREEN NO G1!</b> 🎯💰",
    G2: "✅ <b>GREEN NO G2!</b> 😮‍💨🙌"
  };

  const extra = geral.seqAtual >= 3
    ? [`🔥 <b>${geral.seqAtual} GREENS SEGUIDOS!</b>`]
    : [];

  return [
    cab(),
    titulos[estadoFinal],
    ...extra,
    `Sinal #${sinal.id} · ${EMOJI[sinal.cor]} ${ROTULO[sinal.cor]} ` +
      `· lucro ${fmtUn(lucroPorDesfecho(estadoFinal))} un`,
    "",
    blocoDesempenho(hoje, geral),
    rodape()
  ].join("\n");
}

function msgLoss(sinal, hoje, geral) {
  return [
    cab(),
    "❌ <b>RED</b> — o sinal não bateu até o G2 😔",
    `Sinal #${sinal.id} · ${EMOJI[sinal.cor]} ${ROTULO[sinal.cor]} ` +
      `· ${fmtUn(lucroPorDesfecho("LOSS"))} un`,
    "",
    blocoDesempenho(hoje, geral),
    "🧠 Respeite o stop e não tente recuperar de uma vez.",
    rodape()
  ].join("\n");
}

function msgPausa(hoje) {
  return [
    cab(),
    "⛔ <b>PAUSA DE SEGURANÇA</b>",
    `Atingimos ${hoje.loss} RED(s) hoje (limite ${CFG.maxRedsDia}).`,
    "Novos sinais voltam amanhã (hora de Angola). 🌅",
    "",
    blocoDesempenho(hoje, hoje),
    rodape()
  ].join("\n");
}

function msgExpirado(sinal) {
  return [
    cab(),
    `⏱️ <b>Sinal #${sinal.id} anulado</b> — a mesa ficou sem resultados.`,
    "Não conta no placar."
  ].join("\n");
}

function msgAnalisando() {
  return [
    cab(),
    "🔎 <b>ANALISANDO A MESA...</b> 🔎",
    `🖥️ Mesa: ${CFG.mesa}`,
    "⏳ Possível entrada em instantes.",
    "🚫 Só entre depois do sinal confirmado."
  ].join("\n");
}

function msgPlacarDia(hoje, geral, titulo = "PLACAR DO DIA") {
  return [
    `🏆 <b>${titulo}</b> 🏆`,
    `✅ Greens: ${hoje.greens} ` +
      `(direto ${hoje.g0} · G1 ${hoje.g1} · G2 ${hoje.g2})`,
    `🟡 Empates: ${hoje.empates}`,
    `❌ Loss: ${hoje.loss}`,
    `🎯 Assertividade: ${fmtPct(hoje.pct)}`,
    `🔥 Sequência: ${geral.seqAtual || 0} (recorde ${geral.recorde || 0})`,
    `💰 Saldo: ${fmtUn(hoje.unidades)} un`,
    `🕒 ${horaAngola()}`
  ].join("\n");
}
PARTE 2/3 (Telegram, base de dados, fluxo do sinal, sincronização). Cole logo a seguir à Parte 1:
// ======================================================================
// 5. TELEGRAM
// ======================================================================

async function chamarTelegram(metodo, corpo, timeout) {
  const token = process.env.BOT_TOKEN;
  if (!token || !process.env.CHAT_ID) {
    throw new Error("BOT_TOKEN ou CHAT_ID não configurado.");
  }

  for (let tentativa = 1; tentativa <= 3; tentativa++) {
    try {
      const resp = await axios.post(
        `https://api.telegram.org/bot${token}/${metodo}`,
        { chat_id: process.env.CHAT_ID, ...corpo },
        { timeout }
      );
      if (!resp.data?.ok) throw new Error("Telegram não confirmou o envio.");
      return resp.data;
    } catch (erro) {
      if (tentativa === 3) throw erro;
      const espera = erro.response?.data?.parameters?.retry_after;
      await dormir(espera ? espera * 1000 : 1000 * tentativa);
    }
  }
  return null;
}

function enviarTexto(texto, extra = {}) {
  return chamarTelegram(
    "sendMessage",
    { text: texto, parse_mode: "HTML", disable_web_page_preview: true, ...extra },
    15000
  );
}

async function editarTexto(messageId, texto) {
  try {
    return await chamarTelegram(
      "editMessageText",
      { message_id: messageId, text: texto, parse_mode: "HTML" },
      15000
    );
  } catch (erro) {
    const desc = erro.response?.data?.description || erro.message;
    if (/not modified/i.test(desc)) return null;
    throw erro;
  }
}

function fixarMensagem(messageId) {
  return chamarTelegram(
    "pinChatMessage",
    { message_id: messageId, disable_notification: true },
    15000
  );
}

function tecladoMesa() {
  if (!CFG.botaoUrl) return {};
  return {
    reply_markup: {
      inline_keyboard: [[{ text: CFG.botaoTexto, url: CFG.botaoUrl }]]
    }
  };
}

function enviarFoto(url) {
  return chamarTelegram("sendPhoto", { photo: url }, 20000);
}

async function enviarMensagens(mensagens) {
  for (const m of mensagens) {
    try {
      if (m.foto && CFG.imagem) await enviarFoto(CFG.imagem);
      await enviarTexto(m.texto, m.botao ? tecladoMesa() : {});
    } catch (erro) {
      console.error("[TELEGRAM] Erro:", erro.response?.data?.description || erro.message);
    }
    await dormir(350);
  }
}

// ======================================================================
// 6. BASE DE DADOS
// ======================================================================

async function prepararBaseDados(db) {
  await db.query(`
    CREATE TABLE IF NOT EXISTS bacbo_resultados (
      id TEXT PRIMARY KEY,
      seq BIGSERIAL,
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

  await db.query(`ALTER TABLE bacbo_resultados ADD COLUMN IF NOT EXISTS seq BIGSERIAL`);
  await db.query(`CREATE INDEX IF NOT EXISTS idx_bacbo_seq ON bacbo_resultados (seq DESC)`);

  await db.query(`
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

  await db.query(`ALTER TABLE sinais ADD COLUMN IF NOT EXISTS origem_seq BIGINT`);
  await db.query(`ALTER TABLE sinais ADD COLUMN IF NOT EXISTS origem_em TIMESTAMPTZ`);
  await db.query(`ALTER TABLE sinais ADD COLUMN IF NOT EXISTS empates INTEGER NOT NULL DEFAULT 0`);
  await db.query(`
    CREATE TABLE IF NOT EXISTS bot_estado (
      chave TEXT PRIMARY KEY,
      valor TEXT
    )
  `);
  await db.query(`CREATE INDEX IF NOT EXISTS idx_sinais_estado ON sinais (estado)`);

  // Sinais pendentes antigos (sem rodada de origem) são anulados uma vez.
  await db.query(`
    UPDATE sinais
    SET estado = 'ANULADO',
        resolvido_em = NOW(),
        observacoes = COALESCE(observacoes, '') || ' | Anulado na migração.'
    WHERE estado = 'PENDENTE' AND origem_seq IS NULL
  `);

  estado.tabelasPreparadas = true;
}

async function obterPlacar(db, dia = null) {
  const params = [];
  const cond = ["estado IN ('GREEN','G1','G2','LOSS')"];

  if (CFG.placarDesde) {
    params.push(CFG.placarDesde);
    cond.push(`criado_em >= $${params.length}::timestamptz`);
  }

  if (dia) {
    params.push(dia);
    cond.push(`dia_angola = $${params.length}::date`);
  }

  const r = await db.query(
    `SELECT
       COUNT(*) FILTER (WHERE estado = 'GREEN') AS g0,
       COUNT(*) FILTER (WHERE estado = 'G1')    AS g1,
       COUNT(*) FILTER (WHERE estado = 'G2')    AS g2,
       COUNT(*) FILTER (WHERE estado =
