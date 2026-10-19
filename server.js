"use strict";

/**
 * Capunga Dados 🎲🇺🇸 — Servidor com sincronização reforçada
 *
 * Variáveis obrigatórias:
 * DATABASE_URL, BOT_TOKEN, CHAT_ID
 *
 * Variáveis opcionais:
 * SINAIS_ATIVOS=true
 * ADMIN_TOKEN=senha-forte
 * STREAK_MIN=4
 * EMPATE_PROTEGIDO=true
 * MAX_REDS_DIA=3
 * INTERVALO_MINIMO_MS=180000
 * INTERVALO_SYNC_MS=10000
 * SINAL_EXPIRA_MIN=12
 * API_BACBO=https://api-cs.casino.org/svc-evolution-game-events/api/bacbo
 */

const express = require("express");
const cors = require("cors");
const axios = require("axios");
const { Pool } = require("pg");
const crypto = require("crypto");

const PORT = Number(process.env.PORT || 10000);

const CFG = {
  nomeBot: process.env.NOME_BOT || "Capunga Dados 🎲🇺🇸",
  api: process.env.API_BACBO ||
    "https://api-cs.casino.org/svc-evolution-game-events/api/bacbo",
  intervaloSync: Math.max(
    5000,
    Number(process.env.INTERVALO_SYNC_MS || 10000)
  ),
  intervaloMinimoSinais: Math.max(
    0,
    Number(process.env.INTERVALO_MINIMO_MS || 180000)
  ),
  streakMin: Math.max(
    2,
    Math.min(12, Number(process.env.STREAK_MIN || 4))
  ),
  maxGale: 2,
  empateProtegido:
    String(process.env.EMPATE_PROTEGIDO ?? "true").toLowerCase() === "true",
  sinaisAtivos:
    String(process.env.SINAIS_ATIVOS ?? "false").toLowerCase() === "true",
  maxRedsDia: Math.max(0, Number(process.env.MAX_REDS_DIA ?? 3)),
  expiraMin: Math.max(1, Number(process.env.SINAL_EXPIRA_MIN || 12)),
  adminToken: process.env.ADMIN_TOKEN || null,
  mesa: process.env.MESA_NOME || "Bac Bo - Evolution",
  botaoUrl: process.env.BOTAO_URL || null,
  botaoTexto: process.env.BOTAO_TEXTO || "🎰 Ir para a mesa",
};

const EMOJI = {
  Jogador: "🔵",
  Banca: "🔴",
  Empate: "🟡",
};

const ROTULO = {
  Jogador: "JOGADOR",
  Banca: "BANCA",
  Empate: "EMPATE",
};

const app = express();
app.disable("x-powered-by");
app.use(cors());
app.use(express.json({ limit: "100kb" }));

const pool = process.env.DATABASE_URL
  ? new Pool({
      connectionString: process.env.DATABASE_URL,
      ssl: process.env.PGSSLMODE === "disable"
        ? false
        : { rejectUnauthorized: false },
      connectionTimeoutMillis: 10000,
      idleTimeoutMillis: 30000,
      max: 5,
    })
  : null;

const runtime = {
  syncBusy: false,
  tablesReady: false,
  initialSyncDone: false,
  lastSync: null,
  lastError: null,
  lastNewResultAt: null,
  lastNewRoundId: null,
  lastApiCount: 0,
  lastApiNewestAt: null,
  lastDuplicateCount: 0,
  lastSignalAt: null,
  timer: null,
  shuttingDown: false,
};

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

function angolaDate(date = new Date()) {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Africa/Luanda",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(date);

  const p = Object.fromEntries(parts.map(x => [x.type, x.value]));
  return `${p.year}-${p.month}-${p.day}`;
}

function angolaTime(date = new Date()) {
  return new Intl.DateTimeFormat("pt-PT", {
    timeZone: "Africa/Luanda",
    dateStyle: "short",
    timeStyle: "medium",
    hour12: false,
  }).format(date);
}

function normalizeOutcome(value) {
  if (value && typeof value === "object") {
    value =
      value.outcome ??
      value.resultado ??
      value.result ??
      value.winner ??
      value.color ??
      value.colour;
  }

  const s = String(value ?? "").trim().toLowerCase();

  if (
    s === "red" ||
    s === "banca" ||
    s === "banker" ||
    s.includes("banker") ||
    s === "b"
  ) return "Banca";

  if (
    s === "blue" ||
    s === "jogador" ||
    s === "player" ||
    s.includes("player") ||
    s === "p"
  ) return "Jogador";

  if (
    s === "tie" ||
    s === "draw" ||
    s === "empate" ||
    s.includes("tie") ||
    s.includes("draw")
  ) return "Empate";

  return "Desconhecido";
}

function firstValue(...values) {
  return values.find(v => v !== undefined && v !== null && v !== "");
}

function numericOrNull(value) {
  if (value === undefined || value === null || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function normalizeRound(item) {
  if (!item || typeof item !== "object") {
    return {
      id: "",
      estado: "Desconhecido",
      inicio: null,
      fim: null,
      resultado: "Desconhecido",
      pontosJogador: null,
      pontosBanca: null,
      multiplicador: null,
      resolvida: false,
    };
  }

  const d =
    item.data && typeof item.data === "object" && !Array.isArray(item.data)
      ? item.data
      : item.round && typeof item.round === "object"
        ? item.round
        : item;

  const rawResult = firstValue(d.result, item.result);
  const r =
    rawResult && typeof rawResult === "object"
      ? rawResult
      : {};

  const rawOutcome = firstValue(
    item.resultado,
    d.resultado,
    r.outcome,
    r.resultado,
    r.winner,
    r.color,
    r.colour,
    typeof rawResult === "string" ? rawResult : null,
    d.outcome,
    item.outcome,
    d.winner,
    item.winner
  );

  const estado = String(firstValue(
    d.status,
    item.status,
    d.estado,
    item.estado,
    "Desconhecido"
  ));

  const status = estado.trim().toLowerCase();

  const inicio = firstValue(
    d.startedAt,
    item.startedAt,
    d.startTime,
    item.startTime,
    d.inicio,
    item.inicio
  ) || null;

  const fim = firstValue(
    d.settledAt,
    item.settledAt,
    d.finishedAt,
    item.finishedAt,
    d.endTime,
    item.endTime,
    d.fim,
    item.fim
  ) || null;

  const playerDice = firstValue(
    r.playerDice,
    d.playerDice,
    item.playerDice
  );

  const bankerDice = firstValue(
    r.bankerDice,
    d.bankerDice,
    item.bankerDice
  );

  const pontosJogador = numericOrNull(firstValue(
    r.playerScore,
    r.playerPoints,
    playerDice?.score,
    d.pontos_jogador,
    item.pontos_jogador,
    d.playerScore,
    item.playerScore,
    d.playerPoints,
    item.playerPoints
  ));

  const pontosBanca = numericOrNull(firstValue(
    r.bankerScore,
    r.bankerPoints,
    bankerDice?.score,
    d.pontos_banca,
    item.pontos_banca,
    d.bankerScore,
    item.bankerScore,
    d.bankerPoints,
    item.bankerPoints
  ));

  const id = String(firstValue(
    item.id,
    d.id,
    item.roundId,
    d.roundId,
    item.gameId,
    d.gameId
  ) || "");

  const resolvida =
    ["resolved", "settled", "finished", "complete", "completed",
     "resolvido", "resolvida", "concluido", "concluído"]
      .includes(status) ||
    Boolean(fim);

  return {
    id,
    estado,
    inicio,
    fim,
    resultado: normalizeOutcome(rawOutcome),
    pontosJogador,
    pontosBanca,
    multiplicador: firstValue(
      r.multiplier,
      d.multiplier,
      item.multiplier,
      d.multiplicador,
      item.multiplicador
    ) == null
      ? null
      : String(firstValue(
          r.multiplier,
          d.multiplier,
          item.multiplier,
          d.multiplicador,
          item.multiplicador
        )),
    resolvida,
  };
}

function roundTimeMs(r) {
  const n = Date.parse(r.fim || r.inicio || "");
  return Number.isFinite(n) ? n : 0;
}

/**
 * Aceita os formatos mais comuns de resposta da API.
 * Inclui o formato em português "resultados".
 */
function extractItems(body) {
  if (Array.isArray(body)) return body;
  if (!body || typeof body !== "object") return [];

  const containers = [
    body,
    body.data,
    body.payload,
    body.response,
  ].filter(x => x && typeof x === "object");

  const keys = [
    "resultados",
    "results",
    "items",
    "rounds",
    "events",
    "games",
    "data",
  ];

  for (const container of containers) {
    if (Array.isArray(container)) return container;

    for (const key of keys) {
      if (Array.isArray(container[key])) return container[key];
    }
  }

  return [];
}

function detectEntry(resultsDesc, streakMin = CFG.streakMin) {
  const decisive = resultsDesc.filter(
    x => x === "Banca" || x === "Jogador"
  );

  if (decisive.length < streakMin + 1) return null;

  const color = decisive[0];

  for (let i = 1; i < streakMin; i++) {
    if (decisive[i] !== color) return null;
  }

  return decisive[streakMin] !== color ? color : null;
}

function assessStep(signal, result) {
  const gale = Number(signal.gale || 0);

  if (result === "Empate" && CFG.empateProtegido) {
    return { estado: "PENDENTE", gale, empate: true };
  }

  if (result === signal.cor) {
    return {
      estado: gale === 0 ? "GREEN" : gale === 1 ? "G1" : "G2",
      gale,
      empate: false,
    };
  }

  if (gale < CFG.maxGale) {
    return { estado: "PENDENTE", gale: gale + 1, empate: false };
  }

  return { estado: "LOSS", gale, empate: false };
}

async function telegram(method, payload, timeout = 15000) {
  if (!process.env.BOT_TOKEN || !process.env.CHAT_ID) {
    throw new Error("BOT_TOKEN ou CHAT_ID não configurado.");
  }

  let lastError;

  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const response = await axios.post(
        `https://api.telegram.org/bot${process.env.BOT_TOKEN}/${method}`,
        { chat_id: process.env.CHAT_ID, ...payload },
        { timeout }
      );

      if (!response.data?.ok) {
        throw new Error(
          response.data?.description || "Telegram não confirmou o envio."
        );
      }

      return response.data.result;
    } catch (error) {
      lastError = error;

      if (attempt < 3) {
        const retry = Number(
          error.response?.data?.parameters?.retry_after || attempt
        );
        await sleep(Math.min(retry * 1000, 10000));
      }
    }
  }

  throw lastError;
}

function telegramKeyboard() {
  if (!CFG.botaoUrl || !/^https?:\/\//i.test(CFG.botaoUrl)) return {};

  return {
    reply_markup: {
      inline_keyboard: [[
        { text: CFG.botaoTexto, url: CFG.botaoUrl },
      ]],
    },
  };
}

async function sendMessage(text, options = {}) {
  return telegram("sendMessage", {
    text,
    parse_mode: "HTML",
    disable_web_page_preview: true,
    ...options,
  });
}

function header() {
  return `🎲 <b>${CFG.nomeBot}</b>\n━━━━━━━━━━━━━━━━━━`;
}

function footer() {
  return "━━━━━━━━━━━━━━━━━━\n⚠️ Joga com responsabilidade. Sem garantia de lucro.";
}

function entryMessage(s) {
  return [
    header(),
    `🚨 <b>SINAL CONFIRMADO #${s.id}</b>`,
    `🖥️ Mesa: ${CFG.mesa}`,
    "",
    `🎯 Entrada: ${EMOJI[s.cor]} <b>${ROTULO[s.cor]}</b>`,
    "🔁 Proteção: entrada + G1 + G2",
    CFG.empateProtegido
      ? "🟡 Empate: etapa mantida"
      : "🟡 Empate: conta como erro",
    `🕒 Hora de Angola: ${angolaTime()}`,
    "",
    "Sinal experimental. Não existe garantia de acerto.",
    footer(),
  ].join("\n");
}

function stepMessage(s, result, newGale) {
  if (result === "Empate" && CFG.empateProtegido) {
    return [
      header(),
      `🟡 <b>EMPATE — etapa mantida</b> (Sinal #${s.id})`,
      `🔁 Mantém ${EMOJI[s.cor]} <b>${ROTULO[s.cor]}</b>`,
      footer(),
    ].join("\n");
  }

  return [
    header(),
    newGale === 1
      ? "⚠️ <b>Não bateu — G1</b>"
      : "🚨 <b>G2 — última tentativa</b>",
    `Sinal #${s.id} · resultado: ${EMOJI[result] || "⚪"} ${result}`,
    `🎯 Entrada: ${EMOJI[s.cor]} <b>${ROTULO[s.cor]}</b>`,
    `🕒 ${angolaTime()}`,
    footer(),
  ].join("\n");
}

function finalMessage(s, state) {
  return [
    header(),
    state === "LOSS"
      ? "❌ <b>LOSS — não acertou até G2</b>"
      : `✅ <b>${state === "GREEN" ? "GREEN DE PRIMEIRA" : `GREEN NO ${state}`}!</b>`,
    `Sinal #${s.id} · ${EMOJI[s.cor]} ${ROTULO[s.cor]}`,
    `🕒 ${angolaTime()}`,
    footer(),
  ].join("\n");
}

/**
 * Migra as tabelas sem apagar o histórico existente.
 */
async function ensureSchema(db = pool) {
  if (!db) throw new Error("DATABASE_URL não configurada.");

  await db.query(`
    CREATE TABLE IF NOT EXISTS bacbo_resultados (
      id TEXT PRIMARY KEY,
      seq BIGSERIAL UNIQUE,
      resultado TEXT NOT NULL,
      estado TEXT,
      inicio TIMESTAMPTZ,
      fim TIMESTAMPTZ,
      pontos_jogador INTEGER,
      pontos_banca INTEGER,
      multiplicador TEXT,
      guardado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await db.query(`
    ALTER TABLE bacbo_resultados
    ADD COLUMN IF NOT EXISTS seq BIGSERIAL
  `);

  await db.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_bacbo_resultados_seq
    ON bacbo_resultados(seq)
  `);

  await db.query(`
    CREATE TABLE IF NOT EXISTS sinais (
      id BIGSERIAL PRIMARY KEY,
      criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      dia_angola DATE NOT NULL,
      cor TEXT NOT NULL,
      estado TEXT NOT NULL DEFAULT 'PENDENTE',
      gale INTEGER NOT NULL DEFAULT 0,
      resultado_id TEXT,
      resolvido_em TIMESTAMPTZ,
      observacoes TEXT,
      empates INTEGER NOT NULL DEFAULT 0
    )
  `);

  await db.query(`
    ALTER TABLE sinais
    ADD COLUMN IF NOT EXISTS empates INTEGER NOT NULL DEFAULT 0
  `);

  await db.query(`
    CREATE INDEX IF NOT EXISTS idx_resultados_fim
    ON bacbo_resultados(fim DESC)
  `);

  await db.query(`
    CREATE INDEX IF NOT EXISTS idx_sinais_estado
    ON sinais(estado, criado_em DESC)
  `);

  await db.query(`
    CREATE INDEX IF NOT EXISTS idx_sinais_dia
    ON sinais(dia_angola, estado)
  `);

  runtime.tablesReady = true;
}

async function fetchRounds() {
  const response = await axios.get(CFG.api, {
    timeout: 20000,
    headers: {
      Accept: "application/json",
      "User-Agent": "CapungaDadosBot/2.0",
    },
  });

  const body = response.data;
  const items = extractItems(body);

  if (!items.length) {
    console.error("[API BACBO] Estrutura não reconhecida:", JSON.stringify({
      tipo: Array.isArray(body) ? "array" : typeof body,
      chaves: body && typeof body === "object"
        ? Object.keys(body)
        : [],
    }));
    throw new Error("A API não devolveu uma lista de rodadas reconhecida.");
  }

  const normalized = items
    .map(normalizeRound)
    .filter(r =>
      r.id &&
      r.resolvida &&
      ["Banca", "Jogador", "Empate"].includes(r.resultado)
    );

  const unique = new Map();
  for (const r of normalized) unique.set(r.id, r);

  const rounds = [...unique.values()]
    .sort((a, b) => roundTimeMs(a) - roundTimeMs(b));

  runtime.lastApiCount = rounds.length;
  runtime.lastApiNewestAt = rounds.length
    ? new Date(roundTimeMs(rounds[rounds.length - 1])).toISOString()
    : null;

  console.log("[API BACBO]", JSON.stringify({
    itensRecebidos: items.length,
    rodadasValidas: rounds.length,
    rodadaMaisRecente: rounds.length
      ? rounds[rounds.length - 1].id
      : null,
    horaMaisRecente: runtime.lastApiNewestAt,
  }));

  if (!rounds.length) {
    throw new Error("A API respondeu, mas nenhuma rodada resolvida foi reconhecida.");
  }

  return rounds;
}

async function notifySafe(text) {
  try {
    await sendMessage(text, telegramKeyboard());
  } catch (e) {
    console.error("[TELEGRAM] Falha ao notificar:", e.message);
  }
}

async function getStats(db = pool, day = null) {
  const params = [];
  let where = "";

  if (day) {
    params.push(day);
    where = `WHERE dia_angola = $${params.length}::date`;
  }

  const q = await db.query(`
    SELECT
      COUNT(*) FILTER (
        WHERE estado IN ('GREEN','G1','G2')
      )::int AS greens,
      COUNT(*) FILTER (WHERE estado = 'LOSS')::int AS losses,
      COUNT(*) FILTER (WHERE estado = 'ANULADO')::int AS anulados,
      COUNT(*) FILTER (WHERE estado = 'PENDENTE')::int AS pendentes,
      COUNT(*)::int AS total
    FROM sinais
    ${where}
  `, params);

  const row = q.rows[0] || {};
  const greens = Number(row.greens || 0);
  const losses = Number(row.losses || 0);
  const total = greens + losses;

  return {
    greens,
    losses,
    anulados: Number(row.anulados || 0),
    pendentes: Number(row.pendentes || 0),
    total: Number(row.total || 0),
    aproveitamento: total
      ? Number((greens / total * 100).toFixed(2))
      : 0,
  };
}

async function maybeCreateSignal(round, seq, allowProcessing) {
  if (!CFG.sinaisAtivos || !allowProcessing) return;

  const today = angolaDate();
  const stats = await getStats(pool, today);

  if (CFG.maxRedsDia > 0 && stats.losses >= CFG.maxRedsDia) {
    console.log("[SINAL] Limite diário de losses atingido.");
    return;
  }

  const pending = await pool.query(`
    SELECT id
    FROM sinais
    WHERE estado = 'PENDENTE'
    LIMIT 1
  `);

  if (pending.rowCount) return;

  // Consulta o último sinal guardado, inclusive após reinícios do servidor.
  if (CFG.intervaloMinimoSinais > 0) {
    const last = await pool.query(`
      SELECT criado_em
      FROM sinais
      ORDER BY criado_em DESC
      LIMIT 1
    `);

    if (last.rowCount) {
      const lastTime = Date.parse(last.rows[0].criado_em);
      if (
        Number.isFinite(lastTime) &&
        Date.now() - lastTime < CFG.intervaloMinimoSinais
      ) {
        return;
      }
    }
  }

  const recent = await pool.query(`
    SELECT resultado
    FROM bacbo_resultados
    WHERE seq <= $1
    ORDER BY seq DESC
    LIMIT 30
  `, [seq]);

  const color = detectEntry(
    recent.rows.map(r => r.resultado),
    CFG.streakMin
  );

  if (!color) return;

  const created = await pool.query(`
    INSERT INTO sinais (dia_angola, cor, estado, gale, resultado_id, observacoes)
    VALUES ($1, $2, 'PENDENTE', 0, $3, $4)
    RETURNING *
  `, [
    today,
    color,
    round.id,
    "Regra experimental; sem garantia de acerto ou lucro.",
  ]);

  const signal = created.rows[0];
  runtime.lastSignalAt = signal.criado_em;

  console.log("[SINAL] Criado:", JSON.stringify({
    id: signal.id,
    cor: signal.cor,
    rodada: round.id,
  }));

  await notifySafe(entryMessage(signal));
}

async function resolvePending(round) {
  const pending = await pool.query(`
    SELECT *
    FROM sinais
    WHERE estado = 'PENDENTE'
    ORDER BY criado_em ASC
    LIMIT 1
  `);

  if (!pending.rowCount) return;

  const signal = pending.rows[0];

  // A rodada que originou o sinal não pode resolver o próprio sinal.
  if (signal.resultado_id === round.id) return;

  const assessment = assessStep(signal, round.resultado);

  if (assessment.empate) {
    await pool.query(`
      UPDATE sinais
      SET empates = empates + 1,
          resultado_id = $1,
          observacoes = COALESCE(observacoes, '') || ' | Empate protegido; etapa mantida.'
      WHERE id = $2
    `, [round.id, signal.id]);

    await notifySafe(stepMessage(signal, round.resultado, assessment.gale));
    return;
  }

  if (assessment.estado === "PENDENTE") {
    await pool.query(`
      UPDATE sinais
      SET gale = $1,
          resultado_id = $2,
          observacoes = COALESCE(observacoes, '') || $3
      WHERE id = $4
    `, [
      assessment.gale,
      round.id,
      ` | Resultado: ${round.resultado}; próxima etapa G${assessment.gale}.`,
      signal.id,
    ]);

    await notifySafe(stepMessage(signal, round.resultado, assessment.gale));
    return;
  }

  await pool.query(`
    UPDATE sinais
    SET estado = $1,
        gale = $2,
        resultado_id = $3,
        resolvido_em = NOW(),
        observacoes = COALESCE(observacoes, '') || $4
    WHERE id = $5
  `, [
    assessment.estado,
    assessment.gale,
    round.id,
    ` | Fechado com ${round.resultado}.`,
    signal.id,
  ]);

  await notifySafe(finalMessage(signal, assessment.estado));
}

/**
 * Sincronização:
 * - primeira instalação sem histórico: guarda o histórico sem gerar sinais retroativos;
 * - base já existente: processa as rodadas novas, mesmo após reiniciar o serviço;
 * - IDs repetidos: ignorados sem duplicar linhas.
 */
async function syncOnce() {
  if (!pool) throw new Error("DATABASE_URL não configurada.");

  if (runtime.syncBusy) {
    return { sucesso: true, ignorado: true, motivo: "sincronização já em curso" };
  }

  runtime.syncBusy = true;

  try {
    await ensureSchema();

    const before = await pool.query(`
      SELECT COUNT(*)::int AS total
      FROM bacbo_resultados
    `);

    const hadHistory = Number(before.rows[0]?.total || 0) > 0;
    const rounds = await fetchRounds();
    const firstSync = !runtime.initialSyncDone;

    // Só ignora a geração retroativa quando a base estava vazia.
    const allowProcessing = runtime.initialSyncDone || hadHistory;

    let newCount = 0;
    let duplicateCount = 0;
    let evaluated = 0;
    let lastNew = null;

    for (const r of rounds) {
      const inserted = await pool.query(`
        INSERT INTO bacbo_resultados (
          id, resultado, estado, inicio, fim,
          pontos_jogador, pontos_banca, multiplicador
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
        ON CONFLICT (id) DO NOTHING
        RETURNING seq
      `, [
        r.id,
        r.resultado,
        r.estado,
        r.inicio,
        r.fim,
        r.pontosJogador,
        r.pontosBanca,
        r.multiplicador,
      ]);

      if (!inserted.rowCount) {
        duplicateCount++;
        continue;
      }

      newCount++;
      lastNew = r;

      if (allowProcessing) {
        await resolvePending(r);
        await maybeCreateSignal(r, inserted.rows[0].seq, true);
        evaluated++;
      }
    }

    runtime.initialSyncDone = true;
    runtime.lastSync = new Date().toISOString();
    runtime.lastError = null;
    runtime.lastDuplicateCount = duplicateCount;

    if (lastNew) {
      runtime.lastNewResultAt = runtime.lastSync;
      runtime.lastNewRoundId = lastNew.id;
    }

    const summary = {
      recebidos: rounds.length,
      novosGuardados: newCount,
      duplicados: duplicateCount,
      sinaisAvaliados: evaluated,
      primeiraSincronizacao: firstSync,
      historicoPreexistente: hadHistory,
      ultimaRodadaNova: lastNew?.id || null,
      horaAPI: runtime.lastApiNewestAt,
      horaAngola: angolaTime(),
    };

    console.log("[SINCRONIZACAO]", JSON.stringify(summary));
    return { sucesso: true, ...summary };
  } finally {
    runtime.syncBusy = false;
  }
}

async function expireStaleSignals() {
  if (!pool || CFG.expiraMin <= 0) return;

  const cutoff = new Date(
    Date.now() - CFG.expiraMin * 60 * 1000
  ).toISOString();

  const expired = await pool.query(`
    UPDATE sinais
    SET estado = 'ANULADO',
        resolvido_em = NOW(),
        observacoes = COALESCE(observacoes, '') ||
          ' | Anulado: expirou sem resolução.'
    WHERE estado = 'PENDENTE'
      AND criado_em < $1::timestamptz
    RETURNING id, cor
  `, [cutoff]);

  for (const s of expired.rows) {
    await notifySafe(
      `${header()}\n⏱️ <b>Sinal #${s.id} anulado por expiração.</b>\n` +
      `Não foi resolvido dentro de ${CFG.expiraMin} minutos.\n${footer()}`
    );
  }
}

async function syncLoop() {
  if (runtime.shuttingDown || runtime.syncBusy) return;

  try {
    await syncOnce();
    await expireStaleSignals();
  } catch (e) {
    runtime.lastError = e.message;
    console.error("[SINCRONIZACAO] Erro:", e.stack || e.message);
  }
}

function adminOnly(req, res, next) {
  if (!CFG.adminToken) {
    return res.status(503).json({
      sucesso: false,
      erro: "ADMIN_TOKEN não está configurado no Render.",
    });
  }

  const supplied = String(
    req.get("x-admin-token") ||
    req.query.token ||
    req.body?.adminToken ||
    ""
  );

  const a = Buffer.from(supplied);
  const b = Buffer.from(CFG.adminToken);

  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) {
    return res.status(401).json({
      sucesso: false,
      erro: "Token administrativo inválido.",
    });
  }

  next();
}

app.get("/", (_req, res) => {
  res.type("html").send(`<!doctype html>
<html lang="pt">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${CFG.nomeBot}</title>
</head>
<body style="font:16px system-ui;max-width:720px;margin:40px auto;padding:0 18px">
<h1>🎲 ${CFG.nomeBot}</h1>
<p>Servidor de acompanhamento experimental do Bac Bo.</p>
<p>
<a href="/health">Health</a> ·
<a href="/diagnostico">Diagnóstico</a> ·
<a href="/resultados">Resultados</a> ·
<a href="/sinais">Sinais</a> ·
<a href="/estatisticas">Estatísticas</a> ·
<a href="/testes">Testes</a>
</p>
<p>Não há garantia de acerto ou lucro. Joga com responsabilidade.</p>
</body>
</html>`);
});

app.get("/health", (_req, res) => {
  res.json({
    sucesso: true,
    online: true,
    nome: CFG.nomeBot,
    horaAngola: angolaTime(),
  });
});

app.get("/diagnostico", (_req, res) => {
  res.json({
    servidor: "online",
    baseConfigurada: Boolean(pool),
    telegramConfigurado: Boolean(process.env.BOT_TOKEN && process.env.CHAT_ID),
    sinaisAtivos: CFG.sinaisAtivos,
    tabelasPreparadas: runtime.tablesReady,
    sincronizacaoEmCurso: runtime.syncBusy,
    sincronizacaoInicialConcluida: runtime.initialSyncDone,
    ultimaSincronizacao: runtime.lastSync
      ? angolaTime(new Date(runtime.lastSync))
      : null,
    ultimaRodadaNova: runtime.lastNewResultAt
      ? angolaTime(new Date(runtime.lastNewResultAt))
      : null,
    ultimoIdNovo: runtime.lastNewRoundId,
    ultimaHoraDaAPI: runtime.lastApiNewestAt,
    quantidadeAPI: runtime.lastApiCount,
    duplicadosUltimoCiclo: runtime.lastDuplicateCount,
    ultimoSinal: runtime.lastSignalAt
      ? angolaTime(new Date(runtime.lastSignalAt))
      : null,
    erroSincronizacao: runtime.lastError,
    horaAngola: angolaTime(),
    configuracao: {
      streakMin: CFG.streakMin,
      maxGale: CFG.maxGale,
      empateProtegido: CFG.empateProtegido,
      maxRedsDia: CFG.maxRedsDia,
      intervaloMinimoMs: CFG.intervaloMinimoSinais,
      intervaloSyncMs: CFG.intervaloSync,
    },
  });
});

app.get("/resultados", async (_req, res) => {
  if (!pool) {
    return res.status(503).json({
      sucesso: false,
      erro: "DATABASE_URL não configurada.",
    });
  }

  try {
    await ensureSchema();

    const r = await pool.query(`
      SELECT id, resultado, estado, inicio, fim,
             pontos_jogador, pontos_banca, multiplicador, guardado_em
      FROM bacbo_resultados
      ORDER BY seq DESC
      LIMIT 50
    `);

    res.json({
      sucesso: true,
      total: r.rowCount,
      resultados: r.rows,
    });
  } catch (e) {
    res.status(500).json({ sucesso: false, erro: e.message });
  }
});

app.get("/sinais", async (_req, res) => {
  if (!pool) {
    return res.status(503).json({
      sucesso: false,
      erro: "DATABASE_URL não configurada.",
    });
  }

  try {
    await ensureSchema();

    const r = await pool.query(`
      SELECT id, criado_em, dia_angola, cor, estado,
             gale, resultado_id, resolvido_em, observacoes, empates
      FROM sinais
      ORDER BY id DESC
      LIMIT 100
    `);

    res.json({
      sucesso: true,
      total: r.rowCount,
      sinais: r.rows,
    });
  } catch (e) {
    res.status(500).json({ sucesso: false, erro: e.message });
  }
});

app.get("/estatisticas", async (_req, res) => {
  if (!pool) {
    return res.status(503).json({
      sucesso: false,
      erro: "DATABASE_URL não configurada.",
    });
  }

  try {
    await ensureSchema();

    const [hoje, geral] = await Promise.all([
      getStats(pool, angolaDate()),
      getStats(pool),
    ]);

    res.json({
      sucesso: true,
      diaAngola: angolaDate(),
      hoje,
      geral,
      nota: "Estatística histórica; não prevê resultados futuros.",
    });
  } catch (e) {
    res.status(500).json({ sucesso: false, erro: e.message });
  }
});

app.get("/testes", (_req, res) => {
  const cases = [
    ["GREEN direto", { cor: "Banca", gale: 0 }, "Banca", "GREEN"],
    ["Avança G1", { cor: "Banca", gale: 0 }, "Jogador", "PENDENTE/1"],
    ["GREEN G1", { cor: "Banca", gale: 1 }, "Banca", "G1"],
    ["Avança G2", { cor: "Banca", gale: 1 }, "Jogador", "PENDENTE/2"],
    ["GREEN G2", { cor: "Banca", gale: 2 }, "Banca", "G2"],
    ["LOSS", { cor: "Banca", gale: 2 }, "Jogador", "LOSS"],
    [
      "Empate protegido",
      { cor: "Banca", gale: 1 },
      "Empate",
      CFG.empateProtegido ? "PENDENTE/1" : "PENDENTE/2",
    ],
  ].map(([nome, sinal, resultado, esperado]) => {
    const got = assessStep(sinal, resultado);
    const obtido = got.estado === "PENDENTE"
      ? `PENDENTE/${got.gale}`
      : got.estado;

    return { nome, esperado, obtido, passou: esperado === obtido };
  });

  const entrada = detectEntry(
    ["Banca", "Banca", "Banca", "Banca", "Jogador"],
    4
  );

  res.json({
    sucesso: cases.every(c => c.passou) && entrada === "Banca",
    testesPassaram: cases.filter(c => c.passou).length,
    totalTestes: cases.length,
    entradaExemplo: entrada,
    testes: cases,
  });
});

app.get("/db/teste", adminOnly, async (_req, res) => {
  try {
    await ensureSchema();

    const r = await pool.query(
      "SELECT NOW() AS agora, current_database() AS base"
    );

    res.json({
      sucesso: true,
      base: r.rows[0].base,
      horaServidor: r.rows[0].agora,
      colunaSeqPreparada: true,
      colunaEmpatesPreparada: true,
    });
  } catch (e) {
    res.status(500).json({ sucesso: false, erro: e.message });
  }
});

app.post("/db/preparar", adminOnly, async (_req, res) => {
  try {
    await ensureSchema();
    res.json({
      sucesso: true,
      tabelasPreparadas: true,
      colunaSeqPreparada: true,
      colunaEmpatesPreparada: true,
    });
  } catch (e) {
    res.status(500).json({ sucesso: false, erro: e.message });
  }
});

app.post("/db/sincronizar", adminOnly, async (_req, res) => {
  try {
    res.json(await syncOnce());
  } catch (e) {
    runtime.lastError = e.message;
    res.status(500).json({ sucesso: false, erro: e.message });
  }
});

app.post("/telegram/teste", adminOnly, async (_req, res) => {
  try {
    await sendMessage(
      `${header()}\n✅ <b>Teste do Telegram concluído.</b>\n` +
      `🕒 Angola: ${angolaTime()}\n${footer()}`,
      telegramKeyboard()
    );

    res.json({ sucesso: true, mensagem: "Teste enviado ao Telegram." });
  } catch (e) {
    res.status(500).json({
      sucesso: false,
      erro: e.response?.data?.description || e.message,
    });
  }
});

app.use((err, _req, res, _next) => {
  console.error("[HTTP]", err.message);
  res.status(400).json({ sucesso: false, erro: "Pedido inválido." });
});

const server = app.listen(PORT, () => {
  console.log(`${CFG.nomeBot} ativo na porta ${PORT}`);
  console.log(`Hora Angola: ${angolaTime()}`);
  console.log(`Sinais automáticos: ${CFG.sinaisAtivos ? "ATIVOS" : "DESATIVADOS"}`);

  if (!pool) {
    console.error("DATABASE_URL não configurada.");
    return;
  }

  syncLoop();
  runtime.timer = setInterval(syncLoop, CFG.intervaloSync);
  runtime.timer.unref?.();
});

async function shutdown(signal) {
  if (runtime.shuttingDown) return;

  runtime.shuttingDown = true;
  console.log(`A encerrar (${signal})...`);

  if (runtime.timer) clearInterval(runtime.timer);
  server.close(() => {});

  try {
    if (pool) await pool.end();
  } catch (e) {
    console.error("Erro ao fechar PostgreSQL:", e.message);
  }

  process.exit(0);
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

process.on("unhandledRejection", err => {
  runtime.lastError = err?.message || String(err);
  console.error("[UNHANDLED REJECTION]", err);
});
