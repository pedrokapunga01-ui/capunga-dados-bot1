'use strict';

/**
 * CAPUNGA DADOS — BAC BO
 * Node.js + Express + PostgreSQL + Telegram
 *
 * Variáveis no Render (obrigatórias):
 *   DATABASE_URL
 *   BOT_TOKEN
 *   CHAT_ID
 *   ADMIN_TOKEN
 *
 * Variáveis opcionais (defaults):
 *   SINAIS_ATIVOS=true
 *   STREAK_MIN=3
 *   MAX_GALE=2
 *   EMPATE_PROTEGIDO=true
 *   MAX_REDS_DIA=8
 *   INTERVALO_MINIMO_MS=45000
 *   SINAL_EXPIRA_MIN=10
 *   INTERVALO_SYNC_MS=8000
 *
 * Cores Telegram:
 *   Banca  = Vermelho 🔴
 *   Jogador = Azul 🔵
 *
 * Lógica de entrada (CORRIGIDA):
 *   - Dispara quando os últimos STREAK_MIN resultados são da MESMA cor
 *   - (antes exigia streak EXACTO e bloqueava sequências longas)
 *   - Gale até 2 | Empate protegido | Placar após cada resultado
 */

const express = require('express');
const cors = require('cors');
const axios = require('axios');
const { Pool } = require('pg');

const app = express();

app.disable('x-powered-by');
app.use(cors());
app.use(express.json({ limit: '1mb' }));

const PORT = Number(process.env.PORT || 10000);

const DATABASE_URL = process.env.DATABASE_URL || '';
const BOT_TOKEN = process.env.BOT_TOKEN || '';
const CHAT_ID = process.env.CHAT_ID || '';
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || '';

const API_BACBO =
  process.env.API_BACBO ||
  'https://api-cs.casino.org/svc-evolution-game-events/api/bacbo';

function boolEnv(value, fallback = false) {
  if (value === undefined || value === '') return fallback;
  return ['true', '1', 'yes', 'sim', 'on'].includes(
    String(value).trim().toLowerCase()
  );
}

function intEnv(name, fallback, min, max) {
  const n = Number.parseInt(process.env[name], 10);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, n));
}

const CONFIG = {
  sinaisAtivos: boolEnv(process.env.SINAIS_ATIVOS, true),
  streakMin: intEnv('STREAK_MIN', 3, 2, 12),
  maxGale: intEnv('MAX_GALE', 2, 0, 3),
  empateProtegido: boolEnv(process.env.EMPATE_PROTEGIDO, true),
  maxRedsDia: intEnv('MAX_REDS_DIA', 8, 1, 50),
  intervaloMinimoMs: intEnv('INTERVALO_MINIMO_MS', 45000, 15000, 3600000),
  sinalExpiraMin: intEnv('SINAL_EXPIRA_MIN', 10, 3, 60),
  intervaloSyncMs: intEnv('INTERVALO_SYNC_MS', 8000, 5000, 60000)
};

const pool = DATABASE_URL
  ? new Pool({
      connectionString: DATABASE_URL,
      ssl: /localhost|127\\.0\\.0\\.1/i.test(DATABASE_URL)
        ? false
        : { rejectUnauthorized: false },
      max: 5,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 15000
    })
  : null;

const runtime = {
  iniciadoEm: new Date().toISOString(),
  sincronizacaoEmCurso: false,
  sincronizacaoInicialConcluida: false,
  ultimaSincronizacao: null,
  ultimaRodadaNova: null,
  ultimoIdNovo: null,
  ultimaHoraDaAPI: null,
  quantidadeAPI: 0,
  itensRecebidosAPI: 0,
  duplicadosUltimoCiclo: 0,
  novosUltimoCiclo: 0,
  erroSincronizacao: null,
  ultimoSinal: null,
  ultimaMensagemTelegram: null,
  ultimoErroTelegram: null,
  ultimoHTTPAPI: null,
  ultimoErroAPI: null,
  ultimaRodadaDB: null,
  ultimoIdDB: null,
  ciclosSemNovos: 0,
  motivoSemSinal: null
};

let schemaPromise = null;
let server = null;
let syncTimer = null;

function log(tag, data) {
  console.log(`[${tag}]`, typeof data === 'string' ? data : JSON.stringify(data));
}

function horaAngola() {
  return new Intl.DateTimeFormat('pt-PT', {
    timeZone: 'Africa/Luanda',
    dateStyle: 'short',
    timeStyle: 'medium',
    hour12: false
  }).format(new Date());
}

/** Banca = Vermelho 🔴 | Jogador = Azul 🔵 */
function corDisplay(cor) {
  if (cor === 'Banca') return '🔴 Vermelho 🔴';
  if (cor === 'Jogador') return '🔵 Azul 🔵';
  return String(cor || '');
}

function normalizeOutcome(value) {
  if (value && typeof value === 'object') {
    value =
      value.outcome ??
      value.resultado ??
      value.winner ??
      value.name;
  }

  const s = String(value ?? '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .trim()
    .toLowerCase()
    .replace(/[\\s_-]+/g, '');

  if (
    [
      'player',
      'jogador',
      'playerwin',
      'playerwins',
      'playerwon',
      'playerwinner'
    ].includes(s)
  ) {
    return 'Jogador';
  }

  if (
    [
      'banker',
      'banca',
      'bankerwin',
      'bankerwins',
      'bankerwon',
      'bankerwinner',
      'red',
      'vermelho'
    ].includes(s)
  ) {
    return 'Banca';
  }

  if (['tie', 'draw', 'empate', 'equal', 'push', 'tied'].includes(s)) {
    return 'Empate';
  }

  return null;
}

function validDate(value) {
  if (!value) return null;
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
}

function extractItems(body) {
  if (Array.isArray(body)) return body;
  if (!body || typeof body !== 'object') return [];

  const candidates = [
    body.data,
    body.results,
    body.items,
    body.resultados,
    body.rounds,
    body.events,
    body.data?.results,
    body.data?.items,
    body.data?.resultados,
    body.data?.rounds,
    body.data?.events,
    body.data?.data
  ];

  return candidates.find(Array.isArray) || [];
}

function normalizeRound(item) {
  const d = item?.data || item?.round || item || {};

  const r =
    d.result && typeof d.result === 'object'
      ? d.result
      : item?.result && typeof item.result === 'object'
        ? item.result
        : {};

  const status = String(
    d.status ?? d.estado ?? item?.status ?? item?.estado ?? ''
  ).toLowerCase();

  const inicio = validDate(
    d.startedAt ??
      d.startTime ??
      d.started_at ??
      d.inicio ??
      item?.startedAt ??
      item?.inicio
  );

  const fim = validDate(
    d.settledAt ??
      d.endTime ??
      d.endedAt ??
      d.finishedAt ??
      d.fim ??
      item?.settledAt ??
      item?.fim
  );

  const rawOutcome =
    r.outcome ??
    r.resultado ??
    r.winner ??
    d.outcome ??
    d.resultado ??
    d.winner ??
    item?.outcome ??
    item?.resultado ??
    item?.winner ??
    (typeof d.result === 'string' ? d.result : null) ??
    (typeof item?.result === 'string' ? item.result : null);

  const resultado = normalizeOutcome(rawOutcome);

  const resolvida =
    [
      'resolved',
      'settled',
      'finished',
      'complete',
      'completed',
      'resolvido',
      'concluido',
      'finalizado'
    ].some((word) => status.includes(word)) || Boolean(fim);

  const id = String(
    d.id ??
      d.roundId ??
      d.round_id ??
      d.eventId ??
      item?.id ??
      item?.roundId ??
      item?.round_id ??
      item?.eventId ??
      ''
  ).trim();

  if (!id || !resultado || !resolvida || !fim) return null;

  const scoreJogador = Number(
    r.playerDice?.score ??
      d.playerDice?.score ??
      d.playerScore ??
      d.player_score ??
      d.pontos_jogador
  );

  const scoreBanca = Number(
    r.bankerDice?.score ??
      d.bankerDice?.score ??
      d.bankerScore ??
      d.banker_score ??
      d.pontos_banca
  );

  const multiplicador =
    r.multiplier ?? d.multiplier ?? d.multiplicador ?? null;

  return {
    id,
    resultado,
    estado: 'Resolved',
    inicio: inicio || fim,
    fim,
    pontos_jogador: Number.isFinite(scoreJogador) ? scoreJogador : null,
    pontos_banca: Number.isFinite(scoreBanca) ? scoreBanca : null,
    multiplicador: multiplicador == null ? null : String(multiplicador)
  };
}

async function telegramSend(texto) {
  if (!BOT_TOKEN || !CHAT_ID) {
    runtime.ultimoErroTelegram = 'BOT_TOKEN ou CHAT_ID não configurado';
    return false;
  }

  try {
    const response = await axios.post(
      `https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`,
      {
        chat_id: CHAT_ID,
        text: texto,
        disable_web_page_preview: true
      },
      { timeout: 15000 }
    );

    if (response.data?.ok === true) {
      runtime.ultimaMensagemTelegram = new Date().toISOString();
      runtime.ultimoErroTelegram = null;
      return true;
    }

    runtime.ultimoErroTelegram = JSON.stringify(response.data);
    return false;
  } catch (error) {
    runtime.ultimoErroTelegram = error.response?.data
      ? JSON.stringify(error.response.data)
      : error.message;
    console.error('[TELEGRAM]', runtime.ultimoErroTelegram);
    return false;
  }
}

async function ensureSchema() {
  if (!pool) throw new Error('DATABASE_URL não configurada');
  if (schemaPromise) return schemaPromise;

  schemaPromise = (async () => {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS bacbo_resultados (
        id TEXT PRIMARY KEY,
        resultado TEXT NOT NULL,
        estado TEXT NOT NULL DEFAULT 'Resolved',
        inicio TIMESTAMPTZ,
        fim TIMESTAMPTZ,
        pontos_jogador INTEGER,
        pontos_banca INTEGER,
        multiplicador TEXT,
        guardado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        seq BIGSERIAL
      )
    `);

    await pool.query(`
      ALTER TABLE bacbo_resultados
      ADD COLUMN IF NOT EXISTS seq BIGSERIAL
    `);

    await pool.query(`
      ALTER TABLE bacbo_resultados
      ADD COLUMN IF NOT EXISTS guardado_em TIMESTAMPTZ
      NOT NULL DEFAULT NOW()
    `);

    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_bacbo_fim
      ON bacbo_resultados (fim DESC)
    `);

    await pool.query(`
      CREATE TABLE IF NOT EXISTS sinais (
        id BIGSERIAL PRIMARY KEY,
        criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        cor TEXT NOT NULL,
        estado TEXT NOT NULL DEFAULT 'PENDENTE',
        gale INTEGER NOT NULL DEFAULT 0,
        resultado_id TEXT,
        resultado_final TEXT,
        empates INTEGER NOT NULL DEFAULT 0
      )
    `);

    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_sinais_estado
      ON sinais (estado)
    `);

    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_sinais_criado
      ON sinais (criado_em DESC)
    `);

    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_sinais_loss_dia
      ON sinais (criado_em)
      WHERE estado = 'LOSS'
    `);

    log('DB', { tabelasPreparadas: true });
  })();

  try {
    await schemaPromise;
  } catch (error) {
    schemaPromise = null;
    throw error;
  }
}

async function fetchRounds() {
  try {
    const response = await axios.get(API_BACBO, {
      timeout: 20000,
      headers: {
        Accept: 'application/json',
        'User-Agent': 'CapungaDadosBot/1.0'
      }
    });

    runtime.ultimoHTTPAPI = response.status;
    runtime.ultimoErroAPI = null;

    const items = extractItems(response.data);
    const unique = new Map();

    for (const item of items) {
      const round = normalizeRound(item);
      if (round) unique.set(round.id, round);
    }

    const rounds = [...unique.values()].sort(
      (a, b) => new Date(a.fim) - new Date(b.fim)
    );

    runtime.itensRecebidosAPI = items.length;
    runtime.quantidadeAPI = rounds.length;
    runtime.ultimaHoraDaAPI = rounds.at(-1)?.fim || null;

    if (items.length && !rounds.length) {
      log('AVISO API', {
        mensagem: 'API respondeu sem rodadas válidas',
        exemplo: items[0]
      });
    }

    return rounds;
  } catch (error) {
    runtime.ultimoErroAPI = error.message || String(error);
    runtime.ultimoHTTPAPI = error.response?.status || null;
    throw error;
  }
}

async function insertRound(round) {
  const result = await pool.query(
    `
    INSERT INTO bacbo_resultados (
      id, resultado, estado, inicio, fim,
      pontos_jogador, pontos_banca, multiplicador
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
    ON CONFLICT (id) DO NOTHING
    RETURNING id
  `,
    [
      round.id,
      round.resultado,
      round.estado,
      round.inicio,
      round.fim,
      round.pontos_jogador,
      round.pontos_banca,
      round.multiplicador
    ]
  );

  return result.rows.length > 0;
}

async function getLatestFromDB() {
  const result = await pool.query(`
    SELECT id, resultado, fim
    FROM bacbo_resultados
    ORDER BY fim DESC NULLS LAST, seq DESC
    LIMIT 1
  `);
  return result.rows[0] || null;
}

async function getRecentOutcomes(limit = 30) {
  const result = await pool.query(
    `
    SELECT id, resultado, fim
    FROM bacbo_resultados
    WHERE resultado IN ('Jogador', 'Banca')
    ORDER BY fim DESC NULLS LAST, seq DESC
    LIMIT $1
  `,
    [limit]
  );
  return result.rows;
}

/**
 * CORRIGIDO: dispara com streak de PELO MENOS streakMin da mesma cor.
 * Antes exigia streak EXACTO → sequências longas silenciavam o bot.
 * rows[0] = mais recente.
 */
function detectEntry(rows) {
  const min = CONFIG.streakMin;
  if (!rows || rows.length < min) return null;

  const color = rows[0].resultado;
  if (!['Jogador', 'Banca'].includes(color)) return null;

  for (let i = 0; i < min; i++) {
    if (rows[i].resultado !== color) return null;
  }

  return color;
}

async function redsToday() {
  const result = await pool.query(`
    SELECT COUNT(*)::int AS total
    FROM sinais
    WHERE estado = 'LOSS'
      AND criado_em >= date_trunc('day', NOW() AT TIME ZONE 'Africa/Luanda')
  `);
  return result.rows[0]?.total || 0;
}

async function getPlacarDia() {
  const result = await pool.query(`
    SELECT
      COUNT(*) FILTER (WHERE estado = 'GREEN')::int AS green,
      COUNT(*) FILTER (WHERE estado = 'G1')::int AS g1,
      COUNT(*) FILTER (WHERE estado = 'G2')::int AS g2,
      COUNT(*) FILTER (WHERE estado = 'LOSS')::int AS loss,
      COALESCE(SUM(empates), 0)::int AS empates
    FROM sinais
    WHERE criado_em >= date_trunc('day', NOW() AT TIME ZONE 'Africa/Luanda')
      AND estado IN ('GREEN', 'G1', 'G2', 'LOSS')
  `);

  const s = result.rows[0] || {
    green: 0,
    g1: 0,
    g2: 0,
    loss: 0,
    empates: 0
  };

  const acertos = (s.green || 0) + (s.g1 || 0) + (s.g2 || 0);
  const finalizados = acertos + (s.loss || 0);
  const percent =
    finalizados > 0
      ? Number(((acertos / finalizados) * 100).toFixed(1))
      : 100;

  const recent = await pool.query(`
    SELECT estado
    FROM sinais
    WHERE criado_em >= date_trunc('day', NOW() AT TIME ZONE 'Africa/Luanda')
      AND estado IN ('GREEN', 'G1', 'G2', 'LOSS')
    ORDER BY criado_em DESC, id DESC
    LIMIT 50
  `);

  let greensSeguidos = 0;
  for (const row of recent.rows) {
    if (row.estado === 'GREEN' || row.estado === 'G1' || row.estado === 'G2') {
      greensSeguidos++;
    } else {
      break;
    }
  }

  return {
    green: s.green || 0,
    g1: s.g1 || 0,
    g2: s.g2 || 0,
    empates: s.empates || 0,
    loss: s.loss || 0,
    percent,
    greensSeguidos
  };
}

function formatPlacar(p) {
  return [
    '',
    '━━━━━━━━━━━━━━━━━━━━',
    '🎲🇺🇸 CAPUNGA BOT 🇺🇸🎲',
    '📊 PLACAR DO DIA 📊',
    '━━━━━━━━━━━━━━━━━━━━',
    `🟩 GREEN (sem gale): ${p.green}`,
    `🟨 G1: ${p.g1}`,
    `🟧 G2: ${p.g2}`,
    `🟡 Empates: ${p.empates}`,
    `🟥 LOSS: ${p.loss}`,
    `📈 Percentagem: ${p.percent}%`,
    `🔥 Greens seguidos: ${p.greensSeguidos}`,
    '━━━━━━━━━━━━━━━━━━━━'
  ].join('\n');
}

async function getPendingSignal() {
  const result = await pool.query(`
    SELECT *
    FROM sinais
    WHERE estado = 'PENDENTE'
    ORDER BY criado_em ASC, id ASC
    LIMIT 1
  `);
  return result.rows[0] || null;
}

async function expirePendingSignal(signal) {
  if (!signal) return false;

  const age = Date.now() - new Date(signal.criado_em).getTime();
  if (age < CONFIG.sinalExpiraMin * 60000) return false;

  await pool.query(
    `
    UPDATE sinais SET estado = 'CANCELADO'
    WHERE id = $1 AND estado = 'PENDENTE'
  `,
    [signal.id]
  );

  await telegramSend(
    [
      '⌛ SINAL EXPIRADO',
      `Sinal #${signal.id}`,
      `🕒 Hora Angola: ${horaAngola()}`
    ].join('\n')
  );

  log('SINAL EXPIRADO', { id: signal.id });
  return true;
}

function msgNovoSinal(color) {
  return [
    '🎲🇺🇸 CAPUNGA DADOS 🇺🇸🎲',
    '⚠️🚨 Entrada Confirmada 🚨⚠️',
    `Cor: ${corDisplay(color)}`,
    '🪬 Proteção: 🟡 Empate 🟡',
    `🐓 Gale Máximo: ${CONFIG.maxGale}`,
    '🧠 Faça Gestão de Banca 🧠',
    `🕒 ${horaAngola()}`
  ].join('\n');
}

function msgResultadoGreen(signal, state) {
  let titulo;
  if (state === 'GREEN') {
    titulo = '🟩🏆 GREEEEN 🏆🟩\n✨ Sem Gale ✨';
  } else if (state === 'G1') {
    titulo = '🟨🏆 GREEN G1 🏆🟨\n🐓 Gale 1';
  } else {
    titulo = '🟧🏆 GREEN G2 🏆🟧\n🐓 Gale 2';
  }

  return [
    '🎲🇺🇸 CAPUNGA DADOS 🇺🇸🎲',
    '📋 RESULTADO DO SINAL',
    titulo,
    `🎯 Entrada: ${corDisplay(signal.cor)}`,
    `📌 Sinal #${signal.id}`,
    `🕒 ${horaAngola()}`
  ].join('\n');
}

function msgResultadoLoss(signal) {
  return [
    '🎲🇺🇸 CAPUNGA DADOS 🇺🇸🎲',
    '📋 RESULTADO DO SINAL',
    '🟥❌ LOSS FINAL ❌🟥',
    `🎯 Entrada: ${corDisplay(signal.cor)}`,
    `🐓 Gale usado: ${signal.gale || 0}/${CONFIG.maxGale}`,
    `📌 Sinal #${signal.id}`,
    `🕒 ${horaAngola()}`
  ].join('\n');
}

function msgGale(signal, nextGale) {
  return [
    '🎲🇺🇸 CAPUNGA DADOS 🇺🇸🎲',
    `🐓 GALE ${nextGale} 🐓`,
    `🎯 Mantém: ${corDisplay(signal.cor)}`,
    `📌 Sinal #${signal.id}`,
    `🕒 ${horaAngola()}`
  ].join('\n');
}

function msgEmpate(signal) {
  return [
    '🎲🇺🇸 CAPUNGA DADOS 🇺🇸🎲',
    '🟡🤝 EMPATE PROTEGIDO 🤝🟡',
    `📌 Sinal #${signal.id} continua pendente`,
    `🎯 Entrada: ${corDisplay(signal.cor)}`,
    `🕒 ${horaAngola()}`
  ].join('\n');
}

async function maybeCreateSignal(round) {
  if (!CONFIG.sinaisAtivos) {
    runtime.motivoSemSinal = 'sinais_desativados';
    return;
  }
  if (!['Jogador', 'Banca'].includes(round.resultado)) {
    runtime.motivoSemSinal = 'rodada_empate_ou_invalida';
    return;
  }

  const pending = await getPendingSignal();
  if (pending) {
    if (await expirePendingSignal(pending)) {
      // expirou → pode criar novo
    } else {
      runtime.motivoSemSinal = `pendente_#${pending.id}`;
      return;
    }
  }

  const reds = await redsToday();
  if (reds >= CONFIG.maxRedsDia) {
    runtime.motivoSemSinal = `limite_reds_${reds}`;
    log('LIMITE REDS', { reds, max: CONFIG.maxRedsDia });
    return;
  }

  const lastResult = await pool.query(`
    SELECT criado_em FROM sinais
    ORDER BY criado_em DESC, id DESC LIMIT 1
  `);
  const last = lastResult.rows[0];
  if (
    last?.criado_em &&
    Date.now() - new Date(last.criado_em).getTime() < CONFIG.intervaloMinimoMs
  ) {
    runtime.motivoSemSinal = 'intervalo_minimo';
    return;
  }

  const rows = await getRecentOutcomes(CONFIG.streakMin + 5);
  const color = detectEntry(rows);
  if (!color) {
    const recent = rows
      .slice(0, CONFIG.streakMin)
      .map((r) => r.resultado)
      .join(',');
    runtime.motivoSemSinal = `sem_streak_${CONFIG.streakMin}[${recent}]`;
    return;
  }

  const jaUsada = await pool.query(
    `SELECT 1 FROM sinais WHERE resultado_id = $1 LIMIT 1`,
    [round.id]
  );
  if (jaUsada.rows.length > 0) {
    runtime.motivoSemSinal = 'rodada_ja_usada';
    return;
  }

  const result = await pool.query(
    `
    INSERT INTO sinais (cor, estado, gale, resultado_id, empates)
    VALUES ($1, 'PENDENTE', 0, $2, 0)
    RETURNING *
  `,
    [color, round.id]
  );

  const signal = result.rows[0];

  runtime.ultimoSinal = {
    id: signal.id,
    cor: signal.cor,
    estado: signal.estado,
    criado_em: signal.criado_em
  };
  runtime.motivoSemSinal = null;

  await telegramSend(msgNovoSinal(color));
  log('SINAL CRIADO', { id: signal.id, cor: color, roundId: round.id });
}

async function resolvePending(round) {
  const signal = await getPendingSignal();
  if (!signal) return;

  if (String(signal.resultado_id) === String(round.id)) return;

  if (await expirePendingSignal(signal)) return;

  if (round.resultado === 'Empate') {
    if (!CONFIG.empateProtegido) return;

    await pool.query(
      `
      UPDATE sinais
      SET empates = COALESCE(empates, 0) + 1
      WHERE id = $1 AND estado = 'PENDENTE'
    `,
      [signal.id]
    );

    await telegramSend(msgEmpate(signal));
    return;
  }

  if (round.resultado === signal.cor) {
    const galeNum = Number(signal.gale || 0);
    let state = 'GREEN';
    if (galeNum === 1) state = 'G1';
    else if (galeNum >= 2) state = 'G2';

    await pool.query(
      `
      UPDATE sinais
      SET estado = $1, resultado_final = $2
      WHERE id = $3 AND estado = 'PENDENTE'
    `,
      [state, round.resultado, signal.id]
    );

    const placar = await getPlacarDia();
    await telegramSend(msgResultadoGreen(signal, state) + formatPlacar(placar));
    log('SINAL RESOLVIDO', { id: signal.id, estado: state });
    return;
  }

  const nextGale = Number(signal.gale || 0) + 1;

  if (nextGale <= CONFIG.maxGale) {
    await pool.query(
      `
      UPDATE sinais SET gale = $1
      WHERE id = $2 AND estado = 'PENDENTE'
    `,
      [nextGale, signal.id]
    );

    await telegramSend(msgGale(signal, nextGale));
    log('GALE', { id: signal.id, gale: nextGale });
    return;
  }

  await pool.query(
    `
    UPDATE sinais
    SET estado = 'LOSS', resultado_final = $1
    WHERE id = $2 AND estado = 'PENDENTE'
  `,
    [round.resultado, signal.id]
  );

  const placar = await getPlacarDia();
  await telegramSend(msgResultadoLoss(signal) + formatPlacar(placar));
  log('SINAL LOSS', { id: signal.id });
}

async function syncOnce() {
  if (!pool) throw new Error('DATABASE_URL não configurada');

  if (runtime.sincronizacaoEmCurso) {
    return { sucesso: false, ignorada: true };
  }

  runtime.sincronizacaoEmCurso = true;

  try {
    await ensureSchema();

    const latestDB = await getLatestFromDB();
    runtime.ultimaRodadaDB = latestDB?.fim || null;
    runtime.ultimoIdDB = latestDB?.id || null;

    const count = await pool.query(
      'SELECT COUNT(*)::int AS total FROM bacbo_resultados'
    );
    const hadHistory = count.rows[0].total > 0;

    const rounds = await fetchRounds();

    let inserted = 0;
    let duplicates = 0;
    let evaluated = 0;
    let lastNew = null;

    for (const round of rounds) {
      const saved = await insertRound(round);

      if (!saved) {
        duplicates++;
        continue;
      }

      inserted++;
      lastNew = round;

      if (hadHistory) {
        await resolvePending(round);
        await maybeCreateSignal(round);
        evaluated++;
      }
    }

    if (inserted === 0 && hadHistory) {
      const pending = await getPendingSignal();
      if (pending) await expirePendingSignal(pending);
    }

    runtime.sincronizacaoInicialConcluida = true;
    runtime.ultimaSincronizacao = new Date().toISOString();
    runtime.duplicadosUltimoCiclo = duplicates;
    runtime.novosUltimoCiclo = inserted;
    runtime.erroSincronizacao = null;

    if (lastNew) {
      runtime.ultimaRodadaNova = new Date().toISOString();
      runtime.ultimoIdNovo = lastNew.id;
      runtime.ciclosSemNovos = 0;
    } else {
      runtime.ciclosSemNovos = (runtime.ciclosSemNovos || 0) + 1;
    }

    const summary = {
      recebidos: rounds.length,
      novosGuardados: inserted,
      duplicados: duplicates,
      sinaisAvaliados: evaluated,
      historicoPreexistente: hadHistory,
      ultimoIdDB: runtime.ultimoIdDB,
      ultimoIdAPI: rounds.at(-1)?.id || null,
      motivoSemSinal: runtime.motivoSemSinal,
      ciclosSemNovos: runtime.ciclosSemNovos,
      horaAngola: horaAngola()
    };

    if (inserted > 0 || runtime.ciclosSemNovos % 8 === 0) {
      log('SINCRONIZACAO', summary);
    }

    return { sucesso: true, ...summary };
  } catch (error) {
    runtime.erroSincronizacao = error.message || String(error);
    console.error('[ERRO SINCRONIZACAO]', error.message);
    return {
      sucesso: false,
      erro: runtime.erroSincronizacao
    };
  } finally {
    runtime.sincronizacaoEmCurso = false;
  }
}

function requireAdmin(req, res, next) {
  if (!ADMIN_TOKEN) {
    return res.status(503).json({
      sucesso: false,
      erro: 'ADMIN_TOKEN não configurado no Render'
    });
  }

  const token = req.get('x-admin-token');
  if (token !== ADMIN_TOKEN) {
    return res.status(401).json({
      sucesso: false,
      erro: 'Token administrativo inválido'
    });
  }

  next();
}

app.get('/', (req, res) => {
  res.json({
    nome: 'Capunga Dados',
    estado: 'online',
    horaAngola: horaAngola(),
    sinaisAtivos: CONFIG.sinaisAtivos,
    rotas: [
      '/health',
      '/diagnostico',
      '/resultados',
      '/sinais',
      '/estatisticas'
    ]
  });
});

app.get('/health', (req, res) => {
  res.json({
    sucesso: true,
    estado: 'online',
    baseConfigurada: Boolean(pool),
    horaAngola: horaAngola(),
    ultimaSincronizacao: runtime.ultimaSincronizacao,
    novosUltimoCiclo: runtime.novosUltimoCiclo,
    ciclosSemNovos: runtime.ciclosSemNovos,
    motivoSemSinal: runtime.motivoSemSinal,
    erroSincronizacao: runtime.erroSincronizacao
  });
});

app.get('/diagnostico', async (req, res) => {
  let ultimoResultadoDB = null;
  let totalResultados = 0;
  let totalSinais = 0;
  let pendentes = 0;
  let recentes = [];

  try {
    if (pool) {
      await ensureSchema();
      const r1 = await pool.query(`
        SELECT id, resultado, fim FROM bacbo_resultados
        ORDER BY fim DESC NULLS LAST, seq DESC LIMIT 1
      `);
      ultimoResultadoDB = r1.rows[0] || null;

      const r2 = await pool.query(
        'SELECT COUNT(*)::int AS total FROM bacbo_resultados'
      );
      totalResultados = r2.rows[0].total;

      const r3 = await pool.query(
        'SELECT COUNT(*)::int AS total FROM sinais'
      );
      totalSinais = r3.rows[0].total;

      const r4 = await pool.query(
        `SELECT COUNT(*)::int AS total FROM sinais WHERE estado = 'PENDENTE'`
      );
      pendentes = r4.rows[0].total;

      const r5 = await pool.query(`
        SELECT resultado FROM bacbo_resultados
        WHERE resultado IN ('Jogador', 'Banca')
        ORDER BY fim DESC NULLS LAST, seq DESC
        LIMIT 8
      `);
      recentes = r5.rows.map((x) => x.resultado);
    }
  } catch (e) {}

  res.json({
    sucesso: true,
    nome: 'Capunga Dados',
    iniciadoEm: runtime.iniciadoEm,
    horaAngola: horaAngola(),
    baseConfigurada: Boolean(pool),
    telegramConfigurado: Boolean(BOT_TOKEN && CHAT_ID),
    api: API_BACBO,
    httpAPI: runtime.ultimoHTTPAPI,
    erroAPI: runtime.ultimoErroAPI,
    itensRecebidosAPI: runtime.itensRecebidosAPI,
    rodadasValidasAPI: runtime.quantidadeAPI,
    ultimaHoraDaAPI: runtime.ultimaHoraDaAPI,
    ultimaSincronizacao: runtime.ultimaSincronizacao,
    ultimaRodadaNova: runtime.ultimaRodadaNova,
    ultimoIdNovo: runtime.ultimoIdNovo,
    novosUltimoCiclo: runtime.novosUltimoCiclo,
    duplicadosUltimoCiclo: runtime.duplicadosUltimoCiclo,
    ciclosSemNovos: runtime.ciclosSemNovos,
    motivoSemSinal: runtime.motivoSemSinal,
    sincronizacaoEmCurso: runtime.sincronizacaoEmCurso,
    erroSincronizacao: runtime.erroSincronizacao,
    ultimoErroTelegram: runtime.ultimoErroTelegram,
    ultimaMensagemTelegram: runtime.ultimaMensagemTelegram,
    ultimoSinal: runtime.ultimoSinal,
    ultimosResultados: recentes,
    baseDados: {
      totalResultados,
      totalSinais,
      pendentes,
      ultimoResultadoDB
    },
    configuracao: {
      sinaisAtivos: CONFIG.sinaisAtivos,
      streakMin: CONFIG.streakMin,
      maxGale: CONFIG.maxGale,
      empateProtegido: CONFIG.empateProtegido,
      maxRedsDia: CONFIG.maxRedsDia,
      intervaloMinimoMs: CONFIG.intervaloMinimoMs,
      sinalExpiraMin: CONFIG.sinalExpiraMin,
      intervaloSyncMs: CONFIG.intervaloSyncMs
    }
  });
});

app.get('/resultados', async (req, res) => {
  try {
    await ensureSchema();
    const limit = Math.min(
      Math.max(Number.parseInt(req.query.limit, 10) || 50, 1),
      500
    );

    const result = await pool.query(
      `
      SELECT id, resultado, estado, inicio, fim,
             pontos_jogador, pontos_banca, multiplicador, guardado_em
      FROM bacbo_resultados
      ORDER BY fim DESC NULLS LAST, seq DESC
      LIMIT $1
    `,
      [limit]
    );

    res.json({
      sucesso: true,
      total: result.rows.length,
      resultados: result.rows
    });
  } catch (error) {
    res.status(500).json({ sucesso: false, erro: error.message });
  }
});

app.get('/sinais', async (req, res) => {
  try {
    await ensureSchema();
    const limit = Math.min(
      Math.max(Number.parseInt(req.query.limit, 10) || 50, 1),
      500
    );

    const result = await pool.query(
      `
      SELECT * FROM sinais
      ORDER BY criado_em DESC, id DESC
      LIMIT $1
    `,
      [limit]
    );

    res.json({
      sucesso: true,
      total: result.rows.length,
      sinais: result.rows
    });
  } catch (error) {
    res.status(500).json({ sucesso: false, erro: error.message });
  }
});

app.get('/estatisticas', async (req, res) => {
  try {
    await ensureSchema();

    const result = await pool.query(`
      SELECT
        COUNT(*)::int AS total,
        COUNT(*) FILTER (WHERE estado = 'GREEN')::int AS green,
        COUNT(*) FILTER (WHERE estado = 'G1')::int AS g1,
        COUNT(*) FILTER (WHERE estado = 'G2')::int AS g2,
        COUNT(*) FILTER (WHERE estado = 'LOSS')::int AS loss,
        COUNT(*) FILTER (WHERE estado = 'PENDENTE')::int AS pendentes,
        COUNT(*) FILTER (WHERE estado = 'CANCELADO')::int AS cancelados
      FROM sinais
    `);

    const stats = result.rows[0];
    const acertos =
      (stats.green || 0) + (stats.g1 || 0) + (stats.g2 || 0);
    const finalizados = acertos + (stats.loss || 0);
    const taxa =
      finalizados > 0
        ? Number(((acertos / finalizados) * 100).toFixed(1))
        : null;

    let placarDia = null;
    try {
      placarDia = await getPlacarDia();
    } catch (_) {}

    res.json({
      sucesso: true,
      estatisticas: {
        ...stats,
        finalizados,
        acertos,
        taxaAcertoPercent: taxa
      },
      placarDia,
      horaAngola: horaAngola()
    });
  } catch (error) {
    res.status(500).json({ sucesso: false, erro: error.message });
  }
});

app.post('/db/preparar', requireAdmin, async (req, res) => {
  try {
    await ensureSchema();
    res.json({ sucesso: true, mensagem: 'Tabelas preparadas' });
  } catch (error) {
    res.status(500).json({ sucesso: false, erro: error.message });
  }
});

app.post('/db/sincronizar', requireAdmin, async (req, res) => {
  try {
    const result = await syncOnce();
    res.status(result.sucesso ? 200 : 500).json(result);
  } catch (error) {
    res.status(500).json({
      sucesso: false,
      erro: error.message || String(error)
    });
  }
});

app.post('/telegram/teste', requireAdmin, async (req, res) => {
  const enviado = await telegramSend(
    [
      '🎲🇺🇸 CAPUNGA DADOS 🇺🇸🎲',
      '✅ Teste do bot concluído com sucesso!',
      `🕒 Hora Angola: ${horaAngola()}`
    ].join('\n')
  );

  res.status(enviado ? 200 : 502).json({
    sucesso: enviado,
    erro: runtime.ultimoErroTelegram
  });
});

app.use((req, res) => {
  res.status(404).json({
    sucesso: false,
    erro: 'Rota não encontrada',
    caminho: req.path
  });
});

async function start() {
  if (!DATABASE_URL) {
    throw new Error('DATABASE_URL não configurada no Render');
  }

  await ensureSchema();

  server = app.listen(PORT, '0.0.0.0', () => {
    console.log('======================================');
    console.log('CAPUNGA DADOS — SERVIDOR INICIADO');
    console.log(`Porta: ${PORT}`);
    console.log(`Hora Angola: ${horaAngola()}`);
    console.log(`Sinais automáticos: ${CONFIG.sinaisAtivos}`);
    console.log(`Streak mínimo: ${CONFIG.streakMin}`);
    console.log(`Gale máximo: ${CONFIG.maxGale}`);
    console.log(`Intervalo mínimo sinais: ${CONFIG.intervaloMinimoMs}ms`);
    console.log(`Intervalo sync: ${CONFIG.intervaloSyncMs}ms`);
    console.log('======================================');
  });

  const initial = await syncOnce();
  if (!initial.sucesso) {
    console.error('[PRIMEIRA SINCRONIZAÇÃO]', initial.erro);
  } else {
    log('PRIMEIRA SYNC', {
      recebidos: initial.recebidos,
      novos: initial.novosGuardados,
      duplicados: initial.duplicados
    });
  }

  syncTimer = setInterval(async () => {
    if (runtime.sincronizacaoEmCurso) return;
    try {
      await syncOnce();
    } catch (err) {
      console.error('[SYNC INTERVAL]', err.message);
    }
  }, CONFIG.intervaloSyncMs);

  if (typeof syncTimer.unref === 'function') syncTimer.unref();
}

process.on('unhandledRejection', (reason) => {
  console.error('[UNHANDLED REJECTION]', reason);
});

process.on('uncaughtException', (error) => {
  console.error('[UNCAUGHT EXCEPTION]', error);
});

async function shutdown() {
  if (syncTimer) clearInterval(syncTimer);
  if (server) {
    await new Promise((resolve) => server.close(resolve));
  }
  if (pool) await pool.end();
  process.exit(0);
}

process.on('SIGTERM', () => {
  shutdown().catch((error) => {
    console.error('[SHUTDOWN]', error);
    process.exit(1);
  });
});

process.on('SIGINT', () => {
  shutdown().catch((error) => {
    console.error('[SHUTDOWN]', error);
    process.exit(1);
  });
});

start().catch((error) => {
  console.error('[ERRO FATAL]', error);
  process.exit(1);
});