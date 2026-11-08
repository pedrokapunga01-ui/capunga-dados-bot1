'use strict';

/**
 * CAPUNGA DADOS — UNIFICADO (v3.3 Final)
 * 
 * Apenas 4 variáveis obrigatórias:
 * DATABASE_URL, BOT_TOKEN, CHAT_ID, ADMIN_TOKEN
 * 
 * - Sem limite de reds
 * - Sem confiança
 * - Sinais 24h
 * - Reset automático no start + 00h Angola
 */

const express = require('express');
const cors = require('cors');
const axios = require('axios');
const crypto = require('node:crypto');
const { Pool } = require('pg');

const app = express();
app.disable('x-powered-by');
app.use(cors());
app.use(express.json({ limit: '1mb' }));

// ====================== CONFIGURAÇÃO ======================
const PORT = Number(process.env.PORT || 10000);
const DATABASE_URL = process.env.DATABASE_URL || '';
const BOT_TOKEN = process.env.BOT_TOKEN || '';
const CHAT_ID = process.env.CHAT_ID || '';
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || '';

const API_BACBO = 'https://api-cs.casino.org/svc-evolution-game-events/api/bacbo?page=0&size=50&sort=data.settledAt,desc&duration=120&wheelResults=PlayerWon,BankerWon,Tie';

const CONFIG = {
  sinaisAtivos: true,
  streakMin: 3,
  maxGale: 2,
  empateProtegido: true,
  intervaloMinimoMs: 35000,
  sinalExpiraMin: 8,
  intervaloSyncMs: 6000
};

// ====================== BANCO ======================
const isLocalDb = /localhost|127\.0\.0\.1/i.test(DATABASE_URL);
const pool = DATABASE_URL
  ? new Pool({
      connectionString: DATABASE_URL,
      ssl: isLocalDb ? false : { rejectUnauthorized: false },
      max: 5,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 15000
    })
  : null;

const runtime = {
  iniciadoEm: new Date().toISOString(),
  sincronizacaoEmCurso: false,
  ultimaSincronizacao: null,
  ultimaRodadaNova: null,
  novosUltimoCiclo: 0,
  duplicadosUltimoCiclo: 0,
  erroSincronizacao: null,
  ultimoSinal: null,
  ultimaEstrategia: null,
  motivoSemSinal: null,
  ciclosSemNovos: 0,
  ultimaMensagemTelegram: null,
  ultimoErroTelegram: null,
  ultimoLogAPI: null
};

let schemaPromise = null;
let syncTimer = null;
let server = null;
let ultimoResetDia = null;

// ====================== HELPERS ======================
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

function corDisplay(cor) {
  if (cor === 'Banca') return '🔴 VERMELHO 🔴';
  if (cor === 'Jogador') return '🔵 AZUL 🔵';
  return String(cor || '');
}

function oposta(cor) {
  if (cor === 'Banca') return 'Jogador';
  if (cor === 'Jogador') return 'Banca';
  return null;
}

function normalizeOutcome(value) {
  if (value && typeof value === 'object') {
    value = value.outcome ?? value.resultado ?? value.winner ?? value.name;
  }
  const s = String(value ?? '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .trim()
    .toLowerCase()
    .replace(/[\s_-]+/g, '');

  if (['player', 'jogador', 'playerwin', 'playerwins', 'playerwon', 'blue', 'p'].includes(s)) return 'Jogador';
  if (['banker', 'banca', 'bankerwin', 'bankerwins', 'bankerwon', 'red', 'b'].includes(s)) return 'Banca';
  if (['tie', 'draw', 'empate', 'equal', 'push', 'tied', 'yellow', 'e'].includes(s)) return 'Empate';
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
    body, body.data, body.results, body.items, body.resultados, body.rounds, body.events, body.content,
    body.data?.results, body.data?.items, body.data?.resultados, body.data?.rounds, body.data?.events, body.data?.data, body.data?.content
  ];
  
  for (const c of candidates) {
    if (Array.isArray(c) && c.length > 0) return c;
  }
  return [];
}

function resultSignature(r) {
  return crypto.createHash('sha256').update(JSON.stringify({
    outcome: r.resultado,
    settledAt: r.fim,
    playerScore: r.pontos_jogador,
    bankerScore: r.pontos_banca
  })).digest('hex');
}

// ====================== TELEGRAM ======================
async function telegramSend(texto) {
  if (!BOT_TOKEN || !CHAT_ID) {
    runtime.ultimoErroTelegram = 'BOT_TOKEN ou CHAT_ID não configurado';
    return false;
  }
  try {
    const response = await axios.post(
      `https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`,
      { chat_id: CHAT_ID, text: texto, disable_web_page_preview: true },
      { timeout: 12000 }
    );
    if (response.data?.ok === true) {
      runtime.ultimaMensagemTelegram = new Date().toISOString();
      runtime.ultimoErroTelegram = null;
      return true;
    }
    runtime.ultimoErroTelegram = JSON.stringify(response.data);
    return false;
  } catch (error) {
    runtime.ultimoErroTelegram = error.message;
    return false;
  }
}

// ====================== SCHEMA ======================
async function ensureSchema() {
  if (!pool) throw new Error('DATABASE_URL não configurada');
  if (schemaPromise) return schemaPromise;

  schemaPromise = (async () => {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS mesa_resultados (
        source_key TEXT PRIMARY KEY,
        source_id TEXT,
        resultado TEXT NOT NULL,
        settled_at TIMESTAMPTZ,
        player_score INTEGER,
        banker_score INTEGER,
        multiplier TEXT,
        id_reliable BOOLEAN NOT NULL DEFAULT FALSE,
        source TEXT,
        observado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_mesa_settled ON mesa_resultados (settled_at DESC NULLS LAST, observado_em DESC)`);

    await pool.query(`
      CREATE TABLE IF NOT EXISTS sinais (
        id BIGSERIAL PRIMARY KEY,
        criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        cor TEXT NOT NULL,
        estado TEXT NOT NULL DEFAULT 'PENDENTE',
        gale INTEGER NOT NULL DEFAULT 0,
        resultado_id TEXT,
        resultado_final TEXT,
        empates INTEGER NOT NULL DEFAULT 0,
        estrategia TEXT,
        nome_estrategia TEXT
      )
    `);
    await pool.query(`ALTER TABLE sinais ADD COLUMN IF NOT EXISTS nome_estrategia TEXT`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_sinais_estado ON sinais (estado)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_sinais_criado ON sinais (criado_em DESC)`);

    log('DB', 'Schema pronto');
  })();

  try { await schemaPromise; } catch (e) { schemaPromise = null; throw e; }
}

// ====================== COLETA ======================
function normalizeRound(item) {
  try {
    const d = item?.data || item?.round || item || {};
    const r = (d.result && typeof d.result === 'object') ? d.result :
              (item?.result && typeof item.result === 'object') ? item.result : {};

    const rawOutcome =
      r.outcome ?? r.resultado ?? r.winner ??
      d.outcome ?? d.resultado ?? d.winner ??
      item?.outcome ?? item?.resultado ?? item?.winner ??
      (typeof d.result === 'string' ? d.result : null) ??
      (typeof item?.result === 'string' ? item.result : null);

    const resultado = normalizeOutcome(rawOutcome);
    if (!resultado) return null;

    const id = String(
      d.id ?? d.roundId ?? d.round_id ?? d.eventId ?? d.gameId ??
      item?.id ?? item?.roundId ?? item?.round_id ?? item?.eventId ?? ''
    ).trim() || null;

    const fim = validDate(
      d.settledAt ?? d.settled_at ?? d.endTime ?? d.endedAt ?? d.finishedAt ??
      d.fim ?? item?.settledAt ?? item?.settled_at ?? item?.finishedAt ?? item?.fim
    ) || new Date().toISOString();

    const scoreJogador = Number(
      r.playerDice?.score ?? d.playerDice?.score ??
      d.playerScore ?? d.player_score ?? d.pontos_jogador ??
      r.player?.score ?? d.player?.score
    );

    const scoreBanca = Number(
      r.bankerDice?.score ?? d.bankerDice?.score ??
      d.bankerScore ?? d.banker_score ?? d.pontos_banca ??
      r.banker?.score ?? d.banker?.score
    );

    const round = {
      source_id: id,
      resultado,
      fim,
      pontos_jogador: Number.isFinite(scoreJogador) ? scoreJogador : null,
      pontos_banca: Number.isFinite(scoreBanca) ? scoreBanca : null,
      multiplicador: r.multiplier ?? d.multiplier ?? null
    };

    round.source_key = id ? `id:\( {id}` : `hash: \){resultSignature(round)}`;
    return round;
  } catch (e) {
    return null;
  }
}

async function fetchRounds() {
  try {
    const response = await axios.get(API_BACBO, {
      timeout: 15000,
      headers: {
        Accept: 'application/json',
        'User-Agent': 'CapungaDados/3.3',
        'Cache-Control': 'no-cache',
        Pragma: 'no-cache'
      }
    });

    const items = extractItems(response.data);

    if (!items.length) {
      runtime.ultimoLogAPI = { recebidos: 0, msg: 'API retornou lista vazia' };
      log('API', 'Lista vazia');
      return [];
    }

    const unique = new Map();
    let normalizados = 0;

    for (const item of items) {
      const round = normalizeRound(item);
      if (round) {
        unique.set(round.source_key, round);
        normalizados++;
      }
    }

    const rounds = [...unique.values()].sort(
      (a, b) => new Date(a.fim) - new Date(b.fim)
    );

    const maisRecente = rounds.length ? rounds[rounds.length - 1].fim : null;

    runtime.ultimoLogAPI = {
      recebidos: items.length,
      normalizados,
      unicos: rounds.length,
      maisRecente
    };

    log('API', runtime.ultimoLogAPI);

    return rounds;
  } catch (error) {
    runtime.ultimoLogAPI = { erro: error.message };
    log('API ERRO', error.message);
    throw error;
  }
}

async function insertRound(round) {
  const result = await pool.query(
    `INSERT INTO mesa_resultados (
      source_key, source_id, resultado, settled_at,
      player_score, banker_score, multiplier, id_reliable, source
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
    ON CONFLICT (source_key) DO NOTHING
    RETURNING source_key`,
    [
      round.source_key,
      round.source_id,
      round.resultado,
      round.fim,
      round.pontos_jogador,
      round.pontos_banca,
      round.multiplicador,
      Boolean(round.source_id),
      'api'
    ]
  );
  return result.rows.length > 0;
}

async function getRecentOutcomes(limit = 40) {
  const result = await pool.query(
    `SELECT source_id as id, resultado, settled_at as fim,
            player_score as pontos_jogador, banker_score as pontos_banca
     FROM mesa_resultados
     WHERE resultado IN ('Jogador', 'Banca')
     ORDER BY settled_at DESC NULLS LAST, observado_em DESC
     LIMIT $1`,
    [limit]
  );
  return result.rows;
}

// ====================== ESTRATÉGIAS ======================
function streakLen(rows) {
  if (!rows.length) return 0;
  const c = rows[0].resultado;
  if (!['Jogador', 'Banca'].includes(c)) return 0;
  let n = 0;
  for (const r of rows) {
    if (r.resultado === c) n++;
    else break;
  }
  return n;
}

function stratRepeticao(rows) {
  const n = streakLen(rows);
  if (n < CONFIG.streakMin) return null;
  const cor = rows[0].resultado;
  const prioridade = Math.min(92, 60 + n * 8);
  return { cor, estrategia: 'REPETICAO', nome: `Repetição x${n}`, emoji: '🔁', prioridade };
}

function stratQuebra(rows) {
  const n = streakLen(rows);
  if (n < CONFIG.streakMin + 1) return null;
  const cor = oposta(rows[0].resultado);
  if (!cor) return null;
  const prioridade = Math.min(90, 62 + n * 7);
  return { cor, estrategia: 'QUEBRA', nome: `Quebra x${n}`, emoji: '💥', prioridade };
}

function stratDoisDois(rows) {
  if (rows.length < 4) return null;
  const r = rows.map(x => x.resultado);

  if (r[0] === r[1] && r[2] === r[3] && r[0] !== r[2]) {
    return { cor: r[0], estrategia: 'DOIS_DOIS', nome: '2+2 (bloco)', emoji: '♟️', prioridade: 86 };
  }

  if (rows.length >= 3 && r[0] !== r[1] && r[1] === r[2] && ['Jogador', 'Banca'].includes(r[0])) {
    return { cor: r[0], estrategia: 'DOIS_DOIS', nome: '2+2 (completa)', emoji: '♟️', prioridade: 83 };
  }
  return null;
}

function stratSurf(rows) {
  if (rows.length < 4) return null;
  const a = rows[0].resultado;
  const b = rows[1].resultado;
  if (!['Jogador', 'Banca'].includes(a) || !['Jogador', 'Banca'].includes(b) || a === b) return null;

  for (let i = 0; i < 4; i++) {
    if (rows[i]?.resultado !== (i % 2 === 0 ? a : b)) return null;
  }

  return { cor: b, estrategia: 'SURF', nome: 'Surf (alternado)', emoji: '🌊', prioridade: 84 };
}

function stratForca(rows) {
  const sample = rows.slice(0, 12).filter(r => ['Jogador', 'Banca'].includes(r.resultado));
  if (sample.length < 9) return null;

  let j = 0, b = 0;
  for (const r of sample) r.resultado === 'Jogador' ? j++ : b++;
  const total = j + b;
  const pctB = b / total;
  const pctJ = j / total;

  if (pctB >= 0.68) {
    return { cor: 'Banca', estrategia: 'FORCA', nome: `Força Banca ${Math.round(pctB * 100)}%`, emoji: '💪', prioridade: 82 };
  }
  if (pctJ >= 0.68) {
    return { cor: 'Jogador', estrategia: 'FORCA', nome: `Força Jogador ${Math.round(pctJ * 100)}%`, emoji: '💪', prioridade: 82 };
  }
  return null;
}

function stratNumerica(rows) {
  if (rows.length < 2) return null;
  const last = rows[0];
  const pj = Number(last.pontos_jogador);
  const pb = Number(last.pontos_banca);
  if (!Number.isFinite(pj) || !Number.isFinite(pb)) return null;

  if (last.resultado === 'Jogador' && pj >= 10 && pj > pb) {
    return { cor: 'Jogador', estrategia: 'NUMERICA', nome: `Numérica Alta J(${pj})`, emoji: '🔢', prioridade: 81 };
  }
  if (last.resultado === 'Banca' && pb >= 10 && pb > pj) {
    return { cor: 'Banca', estrategia: 'NUMERICA', nome: `Numérica Alta B(${pb})`, emoji: '🔢', prioridade: 81 };
  }
  return null;
}

function detectBestEntry(rows) {
  if (!rows || rows.length < 3) return null;

  const candidatos = [
    stratRepeticao(rows),
    stratQuebra(rows),
    stratDoisDois(rows),
    stratSurf(rows),
    stratForca(rows),
    stratNumerica(rows)
  ].filter(Boolean);

  if (!candidatos.length) return null;

  const ordem = { REPETICAO: 6, QUEBRA: 5, DOIS_DOIS: 4, SURF: 3, FORCA: 2, NUMERICA: 1 };

  candidatos.sort((a, b) => {
    if (b.prioridade !== a.prioridade) return b.prioridade - a.prioridade;
    return (ordem[b.estrategia] || 0) - (ordem[a.estrategia] || 0);
  });

  return candidatos[0];
}

// ====================== SINAIS ======================
async function getPlacarDia() {
  const r = await pool.query(`
    SELECT
      COUNT(*) FILTER (WHERE estado = 'GREEN')::int AS green,
      COUNT(*) FILTER (WHERE estado = 'G1')::int AS g1,
      COUNT(*) FILTER (WHERE estado = 'G2')::int AS g2,
      COUNT(*) FILTER (WHERE estado = 'LOSS')::int AS loss,
      COALESCE(SUM(empates),0)::int AS empates
    FROM sinais
    WHERE criado_em >= date_trunc('day', NOW() AT TIME ZONE 'Africa/Luanda')
      AND estado IN ('GREEN','G1','G2','LOSS')
  `);
  const s = r.rows[0] || { green: 0, g1: 0, g2: 0, loss: 0, empates: 0 };
  const acertos = (s.green || 0) + (s.g1 || 0) + (s.g2 || 0);
  const finalizados = acertos + (s.loss || 0);
  const percent = finalizados > 0 ? Number(((acertos / finalizados) * 100).toFixed(1)) : 100;
  return { ...s, percent };
}

function formatPlacar(p) {
  return [
    '',
    '════════════════════',
    '🏆 CAPUNGA PLACAR 🏆',
    '════════════════════',
    `✅ GREEN: ${p.green}   |  ⚡ G1: ${p.g1}`,
    `🔥 G2: ${p.g2}      |  🟡 Empates: ${p.empates}`,
    `❌ LOSS: ${p.loss}`,
    `📊 Assertividade: ${p.percent}%`,
    '════════════════════'
  ].join('\n');
}

async function getPendingSignal() {
  const r = await pool.query(`SELECT * FROM sinais WHERE estado = 'PENDENTE' ORDER BY criado_em ASC LIMIT 1`);
  return r.rows[0] || null;
}

async function expirePendingSignal(signal) {
  if (!signal) return false;
  const age = Date.now() - new Date(signal.criado_em).getTime();
  if (age < CONFIG.sinalExpiraMin * 60000) return false;

  await pool.query(`UPDATE sinais SET estado = 'CANCELADO' WHERE id = $1 AND estado = 'PENDENTE'`, [signal.id]);
  await telegramSend(`⏰ Sinal #${signal.id} expirado\n🕒 ${horaAngola()}`);
  return true;
}

function msgNovoSinal(entry) {
  return [
    '🎲🇺🇸 CAPUNGA DADOS 🇺🇸🎲',
    '━━━━━━━━━━━━━━━━━━━━',
    '🚨⚡ ENTRADA CONFIRMADA ⚡🚨',
    '━━━━━━━━━━━━━━━━━━━━',
    `🎯 Cor: ${corDisplay(entry.cor)}`,
    `${entry.emoji} Estratégia: ${entry.nome}`,
    '🛡️ Proteção: 🟡 EMPATE',
    `🐓 Gale máximo: ${CONFIG.maxGale}`,
    '💎 Gestão de banca obrigatória',
    `🕒 ${horaAngola()}`,
    '━━━━━━━━━━━━━━━━━━━━'
  ].join('\n');
}

function msgResultadoGreen(signal, state) {
  const titulo = state === 'GREEN' ? '✅🏆 GREEENNN 🏆✅\n✨ Sem Gale ✨' :
                 state === 'G1'   ? '⚡🏆 GREEN G1 🏆⚡\n🐓 Gale 1' :
                                   '🔥🏆 GREEN G2 🏆🔥\n🐓 Gale 2';
  return [
    '🎲🇺🇸 CAPUNGA DADOS 🇺🇸🎲',
    '📋 RESULTADO DO SINAL',
    titulo,
    `🎯 Entrada: ${corDisplay(signal.cor)}`,
    `🔢 Sinal #${signal.id}`,
    `🕒 ${horaAngola()}`
  ].join('\n');
}

function msgResultadoLoss(signal) {
  return [
    '🎲🇺🇸 CAPUNGA DADOS 🇺🇸🎲',
    '📋 RESULTADO DO SINAL',
    '❌💔 LOSS FINAL 💔❌',
    `🎯 Entrada: ${corDisplay(signal.cor)}`,
    `🐓 Gale: \( {signal.gale || 0}/ \){CONFIG.maxGale}`,
    `🔢 Sinal #${signal.id}`,
    `🕒 ${horaAngola()}`
  ].join('\n');
}

function msgGale(signal, nextGale) {
  return [
    '🎲🇺🇸 CAPUNGA DADOS 🇺🇸🎲',
    `🐓⚡ GALE ${nextGale} ⚡🐓`,
    `🎯 Mantém: ${corDisplay(signal.cor)}`,
    `🔢 Sinal #${signal.id}`,
    `🕒 ${horaAngola()}`
  ].join('\n');
}

function msgEmpate(signal) {
  return [
    '🎲🇺🇸 CAPUNGA DADOS 🇺🇸🎲',
    '🟡🤝 EMPATE PROTEGIDO 🤝🟡',
    `🔢 Sinal #${signal.id} segue pendente`,
    `🎯 ${corDisplay(signal.cor)}`,
    `🕒 ${horaAngola()}`
  ].join('\n');
}

async function maybeCreateSignal(round) {
  if (!CONFIG.sinaisAtivos) {
    runtime.motivoSemSinal = 'sinais_desativados';
    return;
  }
  if (!['Jogador', 'Banca'].includes(round.resultado)) {
    runtime.motivoSemSinal = 'rodada_empate';
    return;
  }

  const pending = await getPendingSignal();
  if (pending) {
    if (!(await expirePendingSignal(pending))) {
      runtime.motivoSemSinal = `pendente_#${pending.id}`;
      return;
    }
  }

  const last = (await pool.query(`SELECT criado_em FROM sinais ORDER BY criado_em DESC LIMIT 1`)).rows[0];
  if (last?.criado_em && Date.now() - new Date(last.criado_em).getTime() < CONFIG.intervaloMinimoMs) {
    runtime.motivoSemSinal = 'intervalo_minimo';
    return;
  }

  const rows = await getRecentOutcomes(30);
  const entry = detectBestEntry(rows);

  if (!entry) {
    runtime.motivoSemSinal = 'sem_setup';
    return;
  }

  const jaUsada = await pool.query(`SELECT 1 FROM sinais WHERE resultado_id = $1 LIMIT 1`, [round.source_id || round.source_key]);
  if (jaUsada.rows.length > 0) {
    runtime.motivoSemSinal = 'rodada_ja_usada';
    return;
  }

  const result = await pool.query(
    `INSERT INTO sinais (cor, estado, gale, resultado_id, empates, estrategia, nome_estrategia)
     VALUES ($1, 'PENDENTE', 0, $2, 0, $3, $4) RETURNING *`,
    [entry.cor, round.source_id || round.source_key, entry.estrategia, entry.nome]
  );

  const signal = result.rows[0];
  runtime.ultimoSinal = signal;
  runtime.ultimaEstrategia = entry;
  runtime.motivoSemSinal = null;

  await telegramSend(msgNovoSinal(entry));
  log('SINAL', { id: signal.id, cor: entry.cor, estrategia: entry.estrategia });
}

async function resolvePending(round) {
  const signal = await getPendingSignal();
  if (!signal) return;
  if (String(signal.resultado_id) === String(round.source_id || round.source_key)) return;
  if (await expirePendingSignal(signal)) return;

  if (round.resultado === 'Empate') {
    if (!CONFIG.empateProtegido) return;
    await pool.query(`UPDATE sinais SET empates = COALESCE(empates,0) + 1 WHERE id = $1 AND estado = 'PENDENTE'`, [signal.id]);
    await telegramSend(msgEmpate(signal));
    return;
  }

  if (round.resultado === signal.cor) {
    const galeNum = Number(signal.gale || 0);
    const state = galeNum === 0 ? 'GREEN' : galeNum === 1 ? 'G1' : 'G2';
    await pool.query(`UPDATE sinais SET estado = $1, resultado_final = $2 WHERE id = $3 AND estado = 'PENDENTE'`, [state, round.resultado, signal.id]);
    const placar = await getPlacarDia();
    await telegramSend(msgResultadoGreen(signal, state) + formatPlacar(placar));
    return;
  }

  const nextGale = Number(signal.gale || 0) + 1;
  if (nextGale <= CONFIG.maxGale) {
    await pool.query(`UPDATE sinais SET gale = $1 WHERE id = $2 AND estado = 'PENDENTE'`, [nextGale, signal.id]);
    await telegramSend(msgGale(signal, nextGale));
    return;
  }

  await pool.query(`UPDATE sinais SET estado = 'LOSS', resultado_final = $1 WHERE id = $2 AND estado = 'PENDENTE'`, [round.resultado, signal.id]);
  const placar = await getPlacarDia();
  await telegramSend(msgResultadoLoss(signal) + formatPlacar(placar));
}

// ====================== RESET ======================
async function verificarResetDiario() {
  const agora = new Date().toLocaleString('en-CA', { timeZone: 'Africa/Luanda', hour12: false });
  const [dataAtual, horaCompleta] = agora.split(' ');
  const hora = horaCompleta.slice(0, 5);

  if (hora >= '00:00' && hora <= '00:04') {
    if (ultimoResetDia !== dataAtual) {
      await pool.query(`DELETE FROM sinais`);
      ultimoResetDia = dataAtual;
      log('RESET', `Histórico limpo às 00h Angola (${dataAtual})`);
      await telegramSend(`🧹 Histórico de sinais resetado\n🕛 00:00 Angola\n📅 ${dataAtual}`);
    }
  }
}

// ====================== SYNC ======================
async function syncOnce() {
  if (!pool) throw new Error('DATABASE_URL não configurada');
  if (runtime.sincronizacaoEmCurso) return { sucesso: false, ignorada: true };

  runtime.sincronizacaoEmCurso = true;

  try {
    await ensureSchema();
    const rounds = await fetchRounds();

    let inserted = 0;
    let duplicates = 0;

    for (const round of rounds) {
      const saved = await insertRound(round);
      if (!saved) {
        duplicates++;
        continue;
      }
      inserted++;

      await resolvePending(round);
      await maybeCreateSignal(round);
    }

    runtime.ultimaSincronizacao = new Date().toISOString();
    runtime.novosUltimoCiclo = inserted;
    runtime.duplicadosUltimoCiclo = duplicates;
    runtime.erroSincronizacao = null;

    if (inserted > 0) {
      runtime.ultimaRodadaNova = new Date().toISOString();
      runtime.ciclosSemNovos = 0;
      log('NOVAS RODADAS', { quantidade: inserted });
    } else {
      runtime.ciclosSemNovos = (runtime.ciclosSemNovos || 0) + 1;
    }

    if (inserted === 0) {
      log('SYNC', {
        novos: 0,
        duplicados: duplicates,
        ciclosSemNovos: runtime.ciclosSemNovos,
        motivo: runtime.motivoSemSinal,
        api: runtime.ultimoLogAPI
      });
    }

    return { sucesso: true, novos: inserted, duplicados: duplicates };
  } catch (error) {
    runtime.erroSincronizacao = error.message;
    console.error('[ERRO SYNC]', error.message);
    return { sucesso: false, erro: error.message };
  } finally {
    runtime.sincronizacaoEmCurso = false;
  }
}

// ====================== ROTAS ======================
function requireAdmin(req, res, next) {
  if (!ADMIN_TOKEN) return res.status(503).json({ sucesso: false, erro: 'ADMIN_TOKEN não configurado' });
  if (req.get('x-admin-token') !== ADMIN_TOKEN) return res.status(401).json({ sucesso: false, erro: 'Token inválido' });
  next();
}

app.get('/', (req, res) => {
  res.json({
    nome: 'Capunga Dados — Unificado v3.3',
    estado: 'online',
    horaAngola: horaAngola(),
    api: 'api-cs.casino.org (robusta)',
    estrategias: ['REPETICAO', 'QUEBRA', 'DOIS_DOIS', 'SURF', 'FORCA', 'NUMERICA']
  });
});

app.get('/health', (req, res) => {
  res.json({
    sucesso: true,
    estado: 'online',
    horaAngola: horaAngola(),
    ultimaSincronizacao: runtime.ultimaSincronizacao,
    novosUltimoCiclo: runtime.novosUltimoCiclo,
    ciclosSemNovos: runtime.ciclosSemNovos,
    motivoSemSinal: runtime.motivoSemSinal,
    ultimoLogAPI: runtime.ultimoLogAPI,
    ultimoSinal: runtime.ultimoSinal ? {
      id: runtime.ultimoSinal.id,
      cor: runtime.ultimoSinal.cor
    } : null
  });
});

app.get('/diagnostico', async (req, res) => {
  let totalResultados = 0, totalSinais = 0, pendentes = 0, recentes = [];
  try {
    await ensureSchema();
    totalResultados = (await pool.query('SELECT COUNT(*)::int AS t FROM mesa_resultados')).rows[0].t;
    totalSinais = (await pool.query('SELECT COUNT(*)::int AS t FROM sinais')).rows[0].t;
    pendentes = (await pool.query(`SELECT COUNT(*)::int AS t FROM sinais WHERE estado = 'PENDENTE'`)).rows[0].t;
    recentes = (await pool.query(`
      SELECT resultado, player_score, banker_score, settled_at
      FROM mesa_resultados
      ORDER BY settled_at DESC NULLS LAST LIMIT 15
    `)).rows;
  } catch (_) {}

  res.json({
    sucesso: true,
    runtime,
    configuracao: CONFIG,
    baseDados: { totalResultados, totalSinais, pendentes },
    ultimosResultados: recentes
  });
});

app.get('/resultados', async (req, res) => {
  try {
    await ensureSchema();
    const limit = Math.min(Math.max(Number(req.query.limit) || 40, 1), 200);
    const r = await pool.query(`SELECT * FROM mesa_resultados ORDER BY settled_at DESC NULLS LAST LIMIT $1`, [limit]);
    res.json({ sucesso: true, total: r.rows.length, resultados: r.rows });
  } catch (e) {
    res.status(500).json({ sucesso: false, erro: e.message });
  }
});

app.get('/sinais', async (req, res) => {
  try {
    await ensureSchema();
    const limit = Math.min(Math.max(Number(req.query.limit) || 40, 1), 200);
    const r = await pool.query(`SELECT * FROM sinais ORDER BY criado_em DESC LIMIT $1`, [limit]);
    res.json({ sucesso: true, total: r.rows.length, sinais: r.rows });
  } catch (e) {
    res.status(500).json({ sucesso: false, erro: e.message });
  }
});

app.post('/admin/sync', requireAdmin, async (req, res) => {
  const result = await syncOnce();
  res.status(result.sucesso ? 200 : 500).json(result);
});

app.post('/telegram/teste', requireAdmin, async (req, res) => {
  const ok = await telegramSend(`🎲 CAPUNGA DADOS — Teste de conexão OK\n🕒 ${horaAngola()}`);
  res.status(ok ? 200 : 502).json({ sucesso: ok, erro: runtime.ultimoErroTelegram });
});

// ====================== START ======================
async function start() {
  if (!DATABASE_URL) throw new Error('DATABASE_URL não configurada');
  if (!BOT_TOKEN || !CHAT_ID) console.warn('⚠️  BOT_TOKEN ou CHAT_ID não configurado');

  await ensureSchema();

  // Reset automático no start (novo deploy)
  await pool.query(`DELETE FROM sinais`);
  log('RESET', 'Histórico de sinais limpo no start do servidor');

  server = app.listen(PORT, '0.0.0.0', () => {
    console.log('==============================================');
    console.log('   CAPUNGA DADOS — UNIFICADO v3.3');
    console.log(`   Porta: ${PORT}`);
    console.log(`   Angola: ${horaAngola()}`);
    console.log(`   Sync a cada: ${CONFIG.intervaloSyncMs / 1000}s`);
    console.log('   Sem limite de reds | Sinais 24h');
    console.log('==============================================');
  });

  await syncOnce();

  syncTimer = setInterval(async () => {
    if (runtime.sincronizacaoEmCurso) return;
    try {
      await syncOnce();
      await verificarResetDiario();
    } catch (err) {
      console.error('[SYNC LOOP]', err.message);
    }
  }, CONFIG.intervaloSyncMs);
}

process.on('SIGTERM', async () => {
  if (syncTimer) clearInterval(syncTimer);
  if (server) server.close();
  if (pool) await pool.end();
  process.exit(0);
});

process.on('SIGINT', async () => {
  if (syncTimer) clearInterval(syncTimer);
  if (server) server.close();
  if (pool) await pool.end();
  process.exit(0);
});

start().catch(err => {
  console.error('[FATAL]', err);
  process.exit(1);
});