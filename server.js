/**
 * Capunga Dados 🎲🇺🇸 — servidor consolidado
 * Node.js + Express + PostgreSQL + Telegram.
 *
 * Variáveis obrigatórias: DATABASE_URL, BOT_TOKEN, CHAT_ID
 * SINAIS_ATIVOS=true|false (padrão false)
 * ADMIN_TOKEN=senha-forte (protege rotas administrativas)
 * STREAK_MIN=4 | EMPATE_PROTEGIDO=true | MAX_REDS_DIA=3
 * INTERVALO_MINIMO_MS=180000 | SINAL_EXPIRA_MIN=12
 * PLACAR_DESDE=ISO opcional | MESA_NOME="Bac Bo - Evolution"
 * BOT_CARD_IMAGE_URL opcional | BOTAO_URL opcional | BOTAO_TEXTO opcional
 *
 * Nota: resultados passados não preveem resultados futuros. Sem garantia de lucro.
 */
"use strict";

const express = require("express");
const cors = require("cors");
const axios = require("axios");
const { Pool } = require("pg");
const crypto = require("crypto");

const PORT = Number(process.env.PORT || 10000);
const CFG = {
  nomeBot: process.env.NOME_BOT || "Capunga Dados🎲🇺🇸",
  intervaloSync: Math.max(5000, Number(process.env.INTERVALO_SYNC_MS || 10000)),
  intervaloMinimoSinais: Math.max(0, Number(process.env.INTERVALO_MINIMO_MS || 180000)),
  streakMin: Math.max(2, Math.min(12, Number(process.env.STREAK_MIN || 4))),
  maxGale: 2,
  empateProtegido: String(process.env.EMPATE_PROTEGIDO ?? "true").toLowerCase() === "true",
  sinaisAtivos: String(process.env.SINAIS_ATIVOS ?? "false").toLowerCase() === "true",
  maxRedsDia: Math.max(0, Number(process.env.MAX_REDS_DIA ?? 3)),
  expiraMin: Math.max(1, Number(process.env.SINAL_EXPIRA_MIN || 12)),
  janelaRecenteMs: Math.max(60000, Number(process.env.JANELA_RECENTE_MS || 240000)),
  placarDesde: process.env.PLACAR_DESDE || null,
  mostrarRisco: String(process.env.MOSTRAR_RISCO ?? "true").toLowerCase() === "true",
  imagem: process.env.BOT_CARD_IMAGE_URL || null,
  mesa: process.env.MESA_NOME || "Bac Bo - Evolution",
  botaoUrl: process.env.BOTAO_URL || null,
  botaoTexto: process.env.BOTAO_TEXTO || "🎰 Ir para a mesa",
  adminToken: process.env.ADMIN_TOKEN || null,
  api: process.env.API_BACBO || "https://api-cs.casino.org/svc-evolution-game-events/api/bacbo",
};

const EMOJI = { Jogador: "🔵", Banca: "🔴", Empate: "🟡" };
const ROTULO = { Jogador: "JOGADOR", Banca: "BANCA", Empate: "EMPATE" };
const app = express();
app.disable("x-powered-by");
app.use(cors());
app.use(express.json({ limit: "100kb" }));
app.get("/favicon.ico", (_req, res) => res.status(204).end());

const pool = process.env.DATABASE_URL ? new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.PGSSLMODE === "disable" ? false : { rejectUnauthorized: false },
  connectionTimeoutMillis: 10000,
  idleTimeoutMillis: 30000,
  max: 5,
}) : null;

const runtime = {
  syncBusy: false, tablesReady: false, initialSyncDone: false,
  lastSync: null, lastError: null, lastNewResultAt: null,
  lastSignalAt: null, lastAnalyzingAt: 0, timer: null, shuttingDown: false,
};

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

function angolaDate(date = new Date()) {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Africa/Luanda", year: "numeric", month: "2-digit", day: "2-digit",
  }).formatToParts(date);
  const p = Object.fromEntries(parts.map(x => [x.type, x.value]));
  return `${p.year}-${p.month}-${p.day}`;
}

function angolaTime(date = new Date()) {
  return new Intl.DateTimeFormat("pt-PT", {
    timeZone: "Africa/Luanda", dateStyle: "short", timeStyle: "medium", hour12: false,
  }).format(date);
}

function parseBool(value, fallback = false) {
  if (value == null || value === "") return fallback;
  return String(value).toLowerCase() === "true";
}

function normalizeOutcome(value) {
  const s = String(value ?? "").trim().toLowerCase();
  if (s.includes("banker") || s === "banca" || s === "red") return "Banca";
  if (s.includes("player") || s === "jogador" || s === "blue") return "Jogador";
  if (s.includes("tie") || s.includes("draw") || s === "empate") return "Empate";
  return "Desconhecido";
}

function normalizeRound(item) {
  const d = item?.data || item || {};
  const r = d.result || item?.result || {};
  const outcome = normalizeOutcome(r.outcome ?? d.outcome ?? item?.outcome);
  const status = String(d.status ?? item?.status ?? "").toLowerCase();

  return {
    id: String(item?.id ?? d.id ?? ""),
    estado: d.status ?? item?.status ?? "Desconhecido",
    inicio: d.startedAt ?? item?.startedAt ?? null,
    fim: d.settledAt ?? item?.settledAt ?? null,
    resultado: outcome,
    pontosJogador: r.playerDice?.score ?? d.playerDice?.score ?? null,
    pontosBanca: r.bankerDice?.score ?? d.bankerDice?.score ?? null,
    multiplicador: r.multiplier ?? null,
    resolvida: status === "resolved" || status === "settled" ||
      Boolean(d.settledAt ?? item?.settledAt),
  };
}

function roundTimeMs(r) {
  const n = Date.parse(r.fim || r.inicio || "");
  return Number.isFinite(n) ? n : 0;
}

function fmtPct(n) {
  return n == null || !Number.isFinite(Number(n)) ? "—" : `${Number(n).toFixed(1)}%`;
}

function fmtUnits(n) {
  const v = Math.round(Number(n || 0) * 100) / 100;
  return `${v > 0 ? "+" : ""}${v.toFixed(2)}`;
}

function profitFor(state) {
  if (state === "GREEN") return 1;
  if (state === "G1") return 1;
  if (state === "G2") return 1;
  if (state === "LOSS") return -7;
  return 0;
}

function stateForGale(gale) {
  return gale === 0 ? "GREEN" : gale === 1 ? "G1" : "G2";
}

function detectEntry(resultsDesc, streakMin = CFG.streakMin) {
  const decisive = resultsDesc.filter(x => x === "Banca" || x === "Jogador");
  if (decisive.length < streakMin + 1) return null;
  const color = decisive[0];
  for (let i = 1; i < streakMin; i++) {
    if (decisive[i] !== color) return null;
  }
  return decisive[streakMin] !== color ? color : null;
}

function detectPreAlert(resultsDesc, streakMin = CFG.streakMin) {
  const target = streakMin - 1;
  const d = resultsDesc.filter(x => x === "Banca" || x === "Jogador");
  if (d.length < target + 1) return null;
  for (let i = 1; i < target; i++) {
    if (d[i] !== d[0]) return null;
  }
  return d[target] !== d[0] ? d[0] : null;
}

function assessStep(signal, result) {
  const gale = Number(signal.gale || 0);
  if (result === "Empate" && CFG.empateProtegido) {
    return { estado: "PENDENTE", gale, empate: true };
  }
  if (result === signal.cor) {
    return { estado: stateForGale(gale), gale, empate: false };
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
        throw new Error(response.data?.description || "Telegram não confirmou o envio.");
      }

      return response.data.result;
    } catch (error) {
      lastError = error;
      if (attempt < 3) {
        const retry = Number(error.response?.data?.parameters?.retry_after || attempt);
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
      inline_keyboard: [[{ text: CFG.botaoTexto, url: CFG.botaoUrl }]],
    },
  };
}

async function sendMessage(text, options = {}) {
  return telegram("sendMessage", {
    text,
    parse_mode: "HTML",
    disable_web_page_preview: true,
    ...options,
  }, 15000);
}

async function sendSignalMessage(text, options = {}) {
  if (CFG.imagem && options.includePhoto) {
    try {
      await telegram("sendPhoto", {
        photo: CFG.imagem,
        caption: text,
        parse_mode: "HTML",
        ...telegramKeyboard(),
      }, 20000);
      return;
    } catch (e) {
      console.error("[TELEGRAM] Foto falhou; envio texto:", e.message);
    }
  }

  const extra = options.button ? telegramKeyboard() : {};
  return sendMessage(text, extra);
}

function header() {
  return `🎲 <b>${CFG.nomeBot}</b>\n━━━━━━━━━━━━━━━━━━`;
}

function footer() {
  return "━━━━━━━━━━━━━━━━━━\n🔞 Joga com responsabilidade. Sem garantia de lucro.";
}

function entryMessage(s) {
  const lines = [
    header(),
    `🚨 <b>SINAL CONFIRMADO #${s.id}</b> 🚨`,
    `🖥️ Mesa: ${CFG.mesa}`,
    "",
    `🎯 Entrada: ${EMOJI[s.cor]} <b>${ROTULO[s.cor]}</b>`,
    `🔁 Proteção: até <b>G${CFG.maxGale}</b> (entrada + 2 tentativas)`,
    CFG.empateProtegido
      ? "🟡 Empate: mantém a etapa"
      : "🟡 Empate: conta como erro",
    `🕒 Hora de Angola: ${angolaTime()}`,
  ];

  if (CFG.mostrarRisco) {
    lines.push(
      "",
      "⚠️ Risco: uma sequência de perdas pode superar vários greens. Não há garantia de acerto."
    );
  }

  lines.push(
    "",
    "Sinal experimental; confirma sempre a rodada e não arrisques dinheiro necessário.",
    footer()
  );

  return lines.join("\n");
}

function stepMessage(s, result, newGale) {
  if (result === "Empate" && CFG.empateProtegido) {
    return [
      header(),
      `🟡 <b>EMPATE — etapa mantida</b> (Sinal #${s.id})`,
      `🔁 Repete ${EMOJI[s.cor]} <b>${ROTULO[s.cor]}</b> (${s.gale === 0 ? "entrada" : `G${s.gale}`})`,
      footer(),
    ].join("\n");
  }

  return [
    header(),
    newGale === 1 ? "⚠️ <b>Não bateu — G1</b>" : "🚨 <b>G2 — última tentativa</b>",
    `Sinal #${s.id} · resultado: ${EMOJI[result] || "⚪"} ${result}`,
    `🎯 Repete ${EMOJI[s.cor]} <b>${ROTULO[s.cor]}</b> (G${newGale})`,
    `🕒 ${angolaTime()}`,
    footer(),
  ].join("\n");
}

function finalMessage(s, state) {
  const lines = [
    header(),
    state === "LOSS"
      ? "❌ <b>LOSS — não acertou até G2</b>"
      : `✅ <b>${state === "GREEN" ? "GREEN DE PRIMEIRA" : `GREEN NO ${state}`}!</b>`,
    `Sinal #${s.id} · ${EMOJI[s.cor]} ${ROTULO[s.cor]}`,
    `🕒 ${angolaTime()}`,
    "Sinal experimental; resultados anteriores não garantem os próximos.",
    footer(),
  ];
  return lines.join("\n");
}

async function ensureSchema(db = pool) {
  if (!db) throw new Error("DATABASE_URL não configurada.");

  await db.query(`CREATE TABLE IF NOT EXISTS bacbo_resultados (
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
  )`);

  await db.query(`CREATE TABLE IF NOT EXISTS sinais (
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
  )`);

  await db.query(`CREATE INDEX IF NOT EXISTS idx_resultados_fim
    ON bacbo_resultados (fim DESC)`);

  await db.query(`CREATE INDEX IF NOT EXISTS idx_sinais_estado
    ON sinais (estado, criado_em DESC)`);

  await db.query(`CREATE INDEX IF NOT EXISTS idx_sinais_dia
    ON sinais (dia_angola, estado)`);

  runtime.tablesReady = true;
}

async function fetchRounds() {
  const response = await axios.get(CFG.api, {
    timeout: 20000,
    headers: {
      Accept: "application/json",
      "User-Agent": "CapungaDadosBot/1.0",
    },
  });

  const body = response.data;
  let items = [];

  if (Array.isArray(body)) items = body;
  else if (Array.isArray(body?.data)) items = body.data;
  else if (Array.isArray(body?.results)) items = body.results;
  else if (Array.isArray(body?.items)) items = body.items;
  else if (Array.isArray(body?.data?.results)) items = body.data.results;

  return items
    .map(normalizeRound)
    .filter(r => r.id && r.resolvida &&
      ["Banca", "Jogador", "Empate"].includes(r.resultado))
    .sort((a, b) => roundTimeMs(a) - roundTimeMs(b));
}

async function notifySafe(text) {
  try {
    await sendMessage(text);
  } catch (e) {
    console.error("[TELEGRAM] Falha ao notificar:", e.message);
  }
}

async function getStats(db = pool, day = null) {
  const params = [];
  let where = "WHERE estado <> 'PENDENTE'";

  if (day) {
    params.push(day);
    where += ` AND dia_angola = $${params.length}::date`;
  }

  const q = await db.query(`
    SELECT
      COUNT(*) FILTER (WHERE estado IN ('GREEN','G1','G2'))::int AS greens,
      COUNT(*) FILTER (WHERE estado = 'LOSS')::int AS losses,
      COUNT(*) FILTER (WHERE estado = 'ANULADO')::int AS anulados,
      COUNT(*) FILTER (WHERE estado = 'PENDENTE')::int AS pendentes,
      COUNT(*)::int AS total
    FROM sinais ${where}`, params);

  const row = q.rows[0] || {};
  const greens = Number(row.greens || 0);
  const losses = Number(row.losses || 0);
  const total = greens + losses;

  return {
    greens,
    losses,
    anulados: Number(row.anulados || 0),
    pendentes: Number(row.pendentes || 0),
    total,
    aproveitamento: total ? Number((greens / total * 100).toFixed(2)) : 0,
  };
}

async function maybeCreateSignal(round, seq, isStartup = false) {
  if (!CFG.sinaisAtivos || !runtime.initialSyncDone || isStartup) return;

  const today = angolaDate();
  const stats = await getStats(pool, today);

  if (CFG.maxRedsDia > 0 && stats.losses >= CFG.maxRedsDia) {
    return;
  }

  if (runtime.lastSignalAt &&
      Date.now() - Date.parse(runtime.lastSignalAt) < CFG.intervaloMinimoSinais) {
    return;
  }

  const pending = await pool.query(
    "SELECT id FROM sinais WHERE estado='PENDENTE' LIMIT 1"
  );
  if (pending.rowCount) return;

  const recent = await pool.query(
    `SELECT resultado FROM bacbo_resultados
     WHERE seq <= $1
     ORDER BY seq DESC LIMIT 30`,
    [seq]
  );

  const results = recent.rows.map(r => r.resultado);
  const color = detectEntry(results, CFG.streakMin);
  if (!color) return;

  const created = await pool.query(
    `INSERT INTO sinais (dia_angola, cor, estado, gale, resultado_id)
     VALUES ($1, $2, 'PENDENTE', 0, $3)
     RETURNING *`,
    [today, color, round.id]
  );

  const signal = created.rows[0];
  runtime.lastSignalAt = new Date().toISOString();

  await notifySafe(entryMessage(signal));
}

async function resolvePending(round, seq) {
  const pending = await pool.query(
    `SELECT * FROM sinais
     WHERE estado='PENDENTE'
     ORDER BY criado_em ASC LIMIT 1`
  );

  if (!pending.rowCount) return;

  const signal = pending.rows[0];

  if (signal.resultado_id === round.id) return;

  const assessment = assessStep(signal, round.resultado);

  if (assessment.empate) {
    await pool.query(
      `UPDATE sinais
       SET empates=empates+1, resultado_id=$1,
           observacoes=COALESCE(observacoes,'') || ' | Empate protegido.'
       WHERE id=$2`,
      [round.id, signal.id]
    );

    await notifySafe(stepMessage(signal, round.resultado, assessment.gale));
    return;
  }

  if (assessment.estado === "PENDENTE") {
    await pool.query(
      `UPDATE sinais
       SET gale=$1, resultado_id=$2
       WHERE id=$3`,
      [assessment.gale, round.id, signal.id]
    );

    await notifySafe(stepMessage(signal, round.resultado, assessment.gale));
    return;
  }

  await pool.query(
    `UPDATE sinais
     SET estado=$1, gale=$2, resultado_id=$3, resolvido_em=NOW()
     WHERE id=$4`,
    [assessment.estado, assessment.gale, round.id, signal.id]
  );

  await notifySafe(finalMessage(signal, assessment.estado));
}

async function syncOnce() {
  if (!pool) throw new Error("DATABASE_URL não configurada.");
  if (runtime.syncBusy) {
    return { ignorado: true, motivo: "sincronização já em curso" };
  }

  runtime.syncBusy = true;

  try {
    await ensureSchema();

    const rounds = await fetchRounds();
    if (!rounds.length) {
      throw new Error("A API não devolveu rodadas resolvidas reconhecíveis.");
    }

    const firstSync = !runtime.initialSyncDone;
    let newCount = 0;
    let evaluated = 0;
    let lastNew = null;

    for (const r of rounds) {
      const inserted = await pool.query(
        `INSERT INTO bacbo_resultados
         (id, resultado, estado, inicio, fim, pontos_jogador, pontos_banca, multiplicador)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
         ON CONFLICT (id) DO NOTHING RETURNING seq`,
        [
          r.id, r.resultado, r.estado, r.inicio, r.fim,
          r.pontosJogador, r.pontosBanca, r.multiplicador,
        ]
      );

      if (!inserted.rowCount) continue;

      newCount++;
      lastNew = { round: r, seq: inserted.rows[0].seq };

      if (runtime.initialSyncDone) {
        await resolvePending(r, inserted.rows[0].seq);
        evaluated++;
        await maybeCreateSignal(r, inserted.rows[0].seq, false);
      }
    }

    runtime.initialSyncDone = true;
    runtime.lastSync = new Date().toISOString();
    runtime.lastError = null;

    if (lastNew) runtime.lastNewResultAt = runtime.lastSync;

    console.log("[SINCRONIZACAO]", JSON.stringify({
      recebidos: rounds.length,
      novosGuardados: newCount,
      sinaisAvaliados: evaluated,
      primeiraSincronizacao: firstSync,
      ultimaRodadaNova: lastNew?.round?.id || null,
      horaAngola: angolaTime(),
    }));

    return {
      sucesso: true,
      recebidos: rounds.length,
      novosGuardados: newCount,
      sinaisAvaliados: evaluated,
      primeiraSincronizacao: firstSync,
    };
  } finally {
    runtime.syncBusy = false;
  }
}

async function expireStaleSignals() {
  if (!pool || CFG.expiraMin <= 0) return;

  const cutoff = new Date(Date.now() - CFG.expiraMin * 60 * 1000).toISOString();

  const expired = await pool.query(
    `UPDATE sinais
     SET estado='ANULADO', resolvido_em=NOW(),
         observacoes=COALESCE(observacoes,'') || ' | Anulado: expirou sem resolução.'
     WHERE estado='PENDENTE' AND criado_em < $1::timestamptz
     RETURNING id, cor`,
    [cutoff]
  );

  for (const s of expired.rows) {
    await notifySafe(
      `${header()}\n⏱️ <b>Sinal #${s.id} anulado por expiração.</b>\n` +
      `Não foi resolvido dentro de ${CFG.expiraMin} minutos e não entra no placar.\n${footer()}`
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
    console.error("[SINCRONIZACAO]", e.stack || e.message);
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
    req.get("x-admin-token") || req.query.token || req.body?.adminToken || ""
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

app.get("/", (_req, res) => res.type("html").send(
  `<!doctype html><html lang="pt"><meta charset="utf-8">` +
  `<meta name="viewport" content="width=device-width">` +
  `<title>${CFG.nomeBot}</title>` +
  `<body style="font:16px system-ui;max-width:720px;margin:40px auto;padding:0 18px">` +
  `<h1>🎲 ${CFG.nomeBot}</h1>` +
  `<p>Servidor de acompanhamento experimental do Bac Bo.</p>` +
  `<p>Estado: <a href="/health">health</a> · ` +
  `<a href="/diagnostico">diagnóstico</a> · ` +
  `<a href="/resultados">resultados</a> · ` +
  `<a href="/sinais">sinais</a> · ` +
  `<a href="/estatisticas">estatísticas</a> · ` +
  `<a href="/testes">testes</a></p>` +
  `<p>Não há garantia de acerto ou lucro. Joga com responsabilidade.</p>` +
  `</body></html>`
));

app.get("/health", (_req, res) => res.json({
  sucesso: true,
  online: true,
  nome: CFG.nomeBot,
  horaAngola: angolaTime(),
}));

app.get("/diagnostico", (_req, res) => res.json({
  servidor: "online",
  baseConfigurada: Boolean(pool),
  telegramConfigurado: Boolean(process.env.BOT_TOKEN && process.env.CHAT_ID),
  sinaisAtivos: CFG.sinaisAtivos,
  tabelasPreparadas: runtime.tablesReady,
  sincronizacaoEmCurso: runtime.syncBusy,
  sincronizacaoInicialConcluida: runtime.initialSyncDone,
  ultimaSincronizacao: runtime.lastSync ? angolaTime(new Date(runtime.lastSync)) : null,
  ultimaRodadaNova: runtime.lastNewResultAt ? angolaTime(new Date(runtime.lastNewResultAt)) : null,
  ultimoSinal: runtime.lastSignalAt ? angolaTime(new Date(runtime.lastSignalAt)) : null,
  erroSincronizacao: runtime.lastError,
  horaAngola: angolaTime(),
  configuracao: {
    streakMin: CFG.streakMin,
    maxGale: CFG.maxGale,
    empateProtegido: CFG.empateProtegido,
    maxRedsDia: CFG.maxRedsDia,
    intervaloMinimoMs: CFG.intervaloMinimoSinais,
  },
}));

app.get("/resultados", async (_req, res) => {
  if (!pool) {
    return res.status(503).json({
      sucesso: false,
      erro: "DATABASE_URL não configurada.",
    });
  }

  try {
    await ensureSchema();
    const r = await pool.query(
      `SELECT id, resultado, estado, inicio, fim, pontos_jogador,
              pontos_banca, multiplicador, guardado_em
       FROM bacbo_resultados ORDER BY seq DESC LIMIT 50`
    );
    res.json({ sucesso: true, total: r.rowCount, resultados: r.rows });
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
    const r = await pool.query(
      `SELECT id, criado_em, dia_angola, cor, estado, gale,
              resultado_id, resolvido_em, observacoes, empates
       FROM sinais ORDER BY id DESC LIMIT 100`
    );
    res.json({ sucesso: true, total: r.rowCount, sinais: r.rows });
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
    { nome: "GREEN direto", sinal: { cor: "Banca", gale: 0 }, resultado: "Banca", esperado: "GREEN" },
    { nome: "Avança G1", sinal: { cor: "Banca", gale: 0 }, resultado: "Jogador", esperado: "PENDENTE/1" },
    { nome: "GREEN G1", sinal: { cor: "Banca", gale: 1 }, resultado: "Banca", esperado: "G1" },
    { nome: "Avança G2", sinal: { cor: "Banca", gale: 1 }, resultado: "Jogador", esperado: "PENDENTE/2" },
    { nome: "GREEN G2", sinal: { cor: "Banca", gale: 2 }, resultado: "Banca", esperado: "G2" },
    { nome: "LOSS", sinal: { cor: "Banca", gale: 2 }, resultado: "Jogador", esperado: "LOSS" },
    {
      nome: "Empate protegido",
      sinal: { cor: "Banca", gale: 1 },
      resultado: "Empate",
      esperado: CFG.empateProtegido ? "PENDENTE/1" : "PENDENTE/2",
    },
  ].map(c => {
    const got = assessStep(c.sinal, c.resultado);
    const actual = got.estado === "PENDENTE" ? `PENDENTE/${got.gale}` : got.estado;
    return {
      nome: c.nome,
      esperado: c.esperado,
      obtido: actual,
      passou: actual === c.esperado,
    };
  });

  const entrada = detectEntry(["Banca", "Banca", "Banca", "Banca", "Jogador"], 4);

  res.json({
    sucesso: cases.every(c => c.passou) && entrada === "Banca",
    testesPassaram: cases.filter(c => c.passou).length,
    totalTestes: cases.length,
    entradaExemplo: entrada,
    testes: cases,
  });
});

app.get("/db/teste", adminOnly, async (_req, res) => {
  if (!pool) {
    return res.status(503).json({
      sucesso: false,
      erro: "DATABASE_URL não configurada.",
    });
  }

  try {
    const r = await pool.query("SELECT NOW() AS agora, current_database() AS base");
    res.json({
      sucesso: true,
      base: r.rows[0].base,
      horaServidor: r.rows[0].agora,
    });
  } catch (e) {
    res.status(500).json({ sucesso: false, erro: e.message });
  }
});

app.post("/db/preparar", adminOnly, async (_req, res) => {
  try {
    await ensureSchema();
    res.json({ sucesso: true, tabelasPreparadas: true });
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
      `🕒 Angola: ${angolaTime()}\n${footer()}`
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
  } else {
    syncLoop();
    runtime.timer = setInterval(syncLoop, CFG.intervaloSync);
    runtime.timer.unref?.();
  }
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
