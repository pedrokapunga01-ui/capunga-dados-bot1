'use strict';

/**
 * CAPUNGA DADOS — BAC BO · SERVER FINAL COMPLETO
 *
 * Análise multi-camada:
 *  - Curto prazo (últimas 3–8)
 *  - Médio (10–20)
 *  - Longo (50–100) tendência e frequência
 *  - Várias lógicas + confirmação cruzada
 *
 * Empate = GREEN e fecha o sinal.
 * Mensagens SEM nome de estratégia.
 *
 * Env: DATABASE_URL, BOT_TOKEN, CHAT_ID, ADMIN_TOKEN
 * Opcional: SINAIS_ATIVOS MAX_GALE EMPATE_PROTEGIDO MAX_REDS_DIA
 *   INTERVALO_MINIMO_MS SINAL_EXPIRA_MIN INTERVALO_SYNC_MS
 *   KEEP_ALIVE_MS KEEP_ALIVE_URL CONSENSO_MIN MIN_PRIORIDADE
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

function boolEnv(v, fb = false) {
  if (v === undefined || v === '') return fb;
  return ['true', '1', 'yes', 'sim', 'on'].includes(String(v).trim().toLowerCase());
}
function intEnv(name, fb, min, max) {
  const n = Number.parseInt(process.env[name], 10);
  if (!Number.isFinite(n)) return fb;
  return Math.max(min, Math.min(max, n));
}

const CONFIG = {
  sinaisAtivos: boolEnv(process.env.SINAIS_ATIVOS, true),
  maxGale: intEnv('MAX_GALE', 2, 0, 3),
  empateProtegido: boolEnv(process.env.EMPATE_PROTEGIDO, true),
  maxRedsDia: intEnv('MAX_REDS_DIA', 12, 1, 50),
  // ~45s → permite 20+ sinais/hora quando a mesa confirma
  intervaloMinimoMs: intEnv('INTERVALO_MINIMO_MS', 45000, 25000, 600000),
  sinalExpiraMin: intEnv('SINAL_EXPIRA_MIN', 8, 4, 30),
  intervaloSyncMs: intEnv('INTERVALO_SYNC_MS', 8000, 5000, 60000),
  keepAliveMs: intEnv('KEEP_ALIVE_MS', 240000, 60000, 900000),
  consensoMin: intEnv('CONSENSO_MIN', 1, 1, 5),
  minPrioridade: intEnv('MIN_PRIORIDADE', 62, 0, 100),
  keepAliveUrl: process.env.KEEP_ALIVE_URL || process.env.RENDER_EXTERNAL_URL || ''
};

/* ===================== ANÁLISE DE MESA ===================== */

const OPOSTO = { Banca: 'Jogador', Jogador: 'Banca' };

function decisivos(hist) {
  return hist.filter((r) => r === 'Banca' || r === 'Jogador');
}

function sequencia(dec) {
  if (!dec.length) return { cor: null, tamanho: 0 };
  let n = 1;
  while (n < dec.length && dec[n] === dec[0]) n++;
  return { cor: dec[0], tamanho: n };
}

function contar(dec, n) {
  const s = dec.slice(0, n);
  const banca = s.filter((x) => x === 'Banca').length;
  return { n: s.length, banca, jogador: s.length - banca, pctBanca: s.length ? banca / s.length : 0.5 };
}

function zProp(k, n, p0 = 0.5) {
  return n ? (k / n - p0) / Math.sqrt((p0 * (1 - p0)) / n) : 0;
}

function taxaAlternancia(dec, n) {
  const s = dec.slice(0, n);
  if (s.length < 2) return 0.5;
  let troca = 0;
  for (let i = 1; i < s.length; i++) if (s[i] !== s[i - 1]) troca++;
  return troca / (s.length - 1);
}

/** Tendência e frequência em várias janelas (curto → longo). */
function analisarMesa(hist) {
  const dec = decisivos(hist);
  const seq = sequencia(dec);
  const c10 = contar(dec, 10);
  const c20 = contar(dec, 20);
  const c50 = contar(dec, 50);
  const c100 = contar(dec, 100);
  const alt20 = taxaAlternancia(dec, 20);
  const alt50 = taxaAlternancia(dec, 50);

  let viesLongo = null; // cor dominante no longo prazo
  if (c50.n >= 40) {
    const z = zProp(c50.banca, c50.n);
    if (Math.abs(z) >= 1.5) viesLongo = z > 0 ? 'Banca' : 'Jogador';
  }
  if (!viesLongo && c100.n >= 60) {
    const z = zProp(c100.banca, c100.n);
    if (Math.abs(z) >= 1.4) viesLongo = z > 0 ? 'Banca' : 'Jogador';
  }

  let viesMedio = null;
  if (c20.n >= 15) {
    if (c20.pctBanca >= 0.6) viesMedio = 'Banca';
    else if (c20.pctBanca <= 0.4) viesMedio = 'Jogador';
  }

  // Mesa “quente” = pouca alternância (streaks) vs “fria” = muita alternância
  const modo =
    alt20 >= 0.65 ? 'alternante' : alt20 <= 0.35 ? 'streak' : 'misto';

  return {
    dec,
    seq,
    c10,
    c20,
    c50,
    c100,
    alt20,
    alt50,
    viesLongo,
    viesMedio,
    modo,
    amostra: dec.length
  };
}

/**
 * Bónus de confirmação: alinha padrão curto com tendência média/longa.
 * +pontos se a cor proposta vai a favor do fluxo da mesa.
 */
function bonusConfirmacao(cor, mesa) {
  let bonus = 0;
  if (mesa.viesMedio === cor) bonus += 8;
  if (mesa.viesLongo === cor) bonus += 10;
  if (mesa.viesMedio && mesa.viesMedio !== cor) bonus -= 6;
  if (mesa.viesLongo && mesa.viesLongo !== cor) bonus -= 4;

  // Em modo streak, favorece continuação da sequência
  if (mesa.modo === 'streak' && mesa.seq.cor === cor && mesa.seq.tamanho >= 2) bonus += 6;
  // Em modo alternante, favorece a oposta da última
  if (mesa.modo === 'alternante' && mesa.seq.cor && OPOSTO[mesa.seq.cor] === cor) bonus += 6;

  // Frequência recente (10) alinhada
  if (mesa.c10.n >= 8) {
    const dom10 = mesa.c10.banca > mesa.c10.jogador ? 'Banca' : 'Jogador';
    if (dom10 === cor && Math.abs(mesa.c10.banca - mesa.c10.jogador) >= 2) bonus += 5;
  }
  return bonus;
}

/* ===================== LÓGICAS / PADRÕES ===================== */

const LOGICAS = [
  {
    id: 'STREAK3',
    prioridade: 70,
    avaliar(dec) {
      const s = sequencia(dec);
      if (s.tamanho >= 3 && s.tamanho <= 4) return { cor: s.cor };
      return null;
    }
  },
  {
    id: 'STREAK5_CONT',
    prioridade: 78,
    avaliar(dec) {
      const s = sequencia(dec);
      if (s.tamanho >= 5 && s.tamanho < 7) return { cor: s.cor };
      return null;
    }
  },
  {
    id: 'QUEBRA7',
    prioridade: 84,
    avaliar(dec) {
      const s = sequencia(dec);
      if (s.tamanho >= 7) return { cor: OPOSTO[s.cor] };
      return null;
    }
  },
  {
    id: 'PAD22',
    prioridade: 88,
    avaliar(dec) {
      if (dec.length < 4) return null;
      const [a, b, c, d] = dec;
      if (a === b && c === d && a !== c) return { cor: a };
      return null;
    }
  },
  {
    id: 'PAD22_COMPLETA',
    prioridade: 74,
    avaliar(dec) {
      // Após 2 iguais + 1 da outra → completa o par da nova
      if (dec.length < 3) return null;
      const [a, b, c] = dec;
      if (a !== b && b === c) return { cor: a };
      return null;
    }
  },
  {
    id: 'ALT4',
    prioridade: 80,
    avaliar(dec) {
      const s = dec.slice(0, 4);
      if (s.length < 4) return null;
      for (let i = 1; i < 4; i++) if (s[i] === s[i - 1]) return null;
      return { cor: OPOSTO[s[0]] };
    }
  },
  {
    id: 'ALT6',
    prioridade: 86,
    avaliar(dec) {
      const s = dec.slice(0, 6);
      if (s.length < 6) return null;
      for (let i = 1; i < 6; i++) if (s[i] === s[i - 1]) return null;
      return { cor: OPOSTO[s[0]] };
    }
  },
  {
    id: 'MAIORIA12',
    prioridade: 72,
    avaliar(dec) {
      const c = contar(dec, 12);
      if (c.n < 12 || Math.abs(c.banca - c.jogador) < 4) return null;
      const cor = c.banca > c.jogador ? 'Banca' : 'Jogador';
      const s = sequencia(dec);
      if (s.cor && s.cor !== cor && s.tamanho >= 3) return null;
      return { cor };
    }
  },
  {
    id: 'MAIORIA20',
    prioridade: 68,
    avaliar(dec) {
      const c = contar(dec, 20);
      if (c.n < 20 || Math.abs(c.banca - c.jogador) < 6) return null;
      return { cor: c.banca > c.jogador ? 'Banca' : 'Jogador' };
    }
  },
  {
    id: 'FLUXO50',
    prioridade: 66,
    avaliar(dec, mesa) {
      if (!mesa.viesLongo) return null;
      if (mesa.c50.n < 40) return null;
      const s = sequencia(dec);
      // Só entra a favor do vies longo se a sequência actual não for fortemente contrária
      if (s.cor && s.cor !== mesa.viesLongo && s.tamanho >= 4) return null;
      return { cor: mesa.viesLongo };
    }
  },
  {
    id: 'DUPLO_BLOCO',
    prioridade: 90,
    avaliar(dec) {
      if (dec.length < 6) return null;
      const [a, b, c, d, e, f] = dec;
      if (a === b && e === f && a === e && c === d && a !== c) return { cor: a };
      return null;
    }
  },
  {
    id: 'PRESSAO_RECENTE',
    prioridade: 76,
    avaliar(dec) {
      // 3 das últimas 4 na mesma cor
      if (dec.length < 4) return null;
      const s = dec.slice(0, 4);
      const b = s.filter((x) => x === 'Banca').length;
      if (b >= 3) return { cor: 'Banca' };
      if (b <= 1) return { cor: 'Jogador' };
      return null;
    }
  }
];

/**
 * Decide entrada:
 * 1) Avalia todas as lógicas no histórico longo
 * 2) Aplica bónus de tendência/frequência da mesa
 * 3) Escolhe cor com mais peso; exige score mínimo
 */
function decidir(hist, opcoes = {}) {
  const consensoMin = Math.max(1, Number(opcoes.consensoMin) || 1);
  const minP = Number(opcoes.minPrioridade) || 62;
  const mesa = analisarMesa(hist);
  const dec = mesa.dec;

  if (dec.length < 8) return null;

  const candidatos = [];
  for (const L of LOGICAS) {
    const r = L.avaliar(dec, mesa);
    if (!r || !r.cor) continue;
    const bonus = bonusConfirmacao(r.cor, mesa);
    const score = L.prioridade + bonus;
    if (score < minP) continue;
    candidatos.push({
      id: L.id,
      cor: r.cor,
      score,
      prioridade: L.prioridade,
      bonus
    });
  }

  if (!candidatos.length) return null;

  const votos = { Banca: 0, Jogador: 0 };
  const peso = { Banca: 0, Jogador: 0 };
  for (const c of candidatos) {
    votos[c.cor]++;
    peso[c.cor] += c.score;
  }

  let cor = null;
  if (peso.Banca > peso.Jogador) cor = 'Banca';
  else if (peso.Jogador > peso.Banca) cor = 'Jogador';
  else return null;

  if (votos[cor] < consensoMin) return null;

  const apoio = candidatos
    .filter((c) => c.cor === cor)
    .sort((a, b) => b.score - a.score);
  const top = apoio[0];
  const confianca = Math.min(99, Math.round(top.score));

  // Confirmação extra: se só 1 lógica e bónus de mesa negativo forte, rejeita
  if (apoio.length === 1 && top.bonus <= -6) return null;

  return {
    cor,
    confianca,
    votos: votos[cor],
    totalLogicas: candidatos.length,
    // interno (logs) — NÃO vai para o Telegram
    _ids: apoio.map((c) => c.id).join('+'),
    mesa: {
      modo: mesa.modo,
      viesMedio: mesa.viesMedio,
      viesLongo: mesa.viesLongo,
      alt20: Math.round(mesa.alt20 * 100),
      amostra: mesa.amostra
    }
  };
}

/* ===================== APP / DB / TELEGRAM ===================== */

const pool = DATABASE_URL
  ? new Pool({
      connectionString: DATABASE_URL,
      ssl: /localhost|127\.0\.0\.1/i.test(DATABASE_URL)
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
  ciclosSemNovos: 0,
  motivoSemSinal: null,
  ultimaEstrategia: null,
  rodadasDesdeSinal: 0,
  publicUrl: CONFIG.keepAliveUrl || null,
  ultimoKeepAlive: null,
  ultimoRelatorioDia: null
};

let schemaPromise = null;
let server = null;
let syncTimer = null;
let keepAliveTimer = null;
let midnightTimer = null;

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
  if (cor === 'Banca') return '🔴 Vermelho 🔴';
  if (cor === 'Jogador') return '🔵 Azul 🔵';
  return String(cor || '');
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
  if (['player', 'jogador', 'playerwin', 'playerwins', 'playerwon', 'playerwinner'].includes(s))
    return 'Jogador';
  if (['banker', 'banca', 'bankerwin', 'bankerwins', 'bankerwon', 'bankerwinner', 'red', 'vermelho'].includes(s))
    return 'Banca';
  if (['tie', 'draw', 'empate', 'equal', 'push', 'tied'].includes(s)) return 'Empate';
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
    body.data, body.results, body.items, body.resultados, body.rounds, body.events,
    body.data?.results, body.data?.items, body.data?.resultados, body.data?.rounds,
    body.data?.events, body.data?.data
  ];
  return candidates.find(Array.isArray) || [];
}

function normalizeRound(item) {
  const d = item?.data || item?.round || item || {};
  const r =
    d.result && typeof d.result === 'object' ? d.result
      : item?.result && typeof item.result === 'object' ? item.result : {};
  const status = String(d.status ?? d.estado ?? item?.status ?? item?.estado ?? '').toLowerCase();
  const inicio = validDate(d.startedAt ?? d.startTime ?? d.started_at ?? d.inicio ?? item?.startedAt ?? item?.inicio);
  const fim = validDate(d.settledAt ?? d.endTime ?? d.endedAt ?? d.finishedAt ?? d.fim ?? item?.settledAt ?? item?.fim);
  const rawOutcome =
    r.outcome ?? r.resultado ?? r.winner ?? d.outcome ?? d.resultado ?? d.winner ??
    item?.outcome ?? item?.resultado ?? item?.winner ??
    (typeof d.result === 'string' ? d.result : null) ??
    (typeof item?.result === 'string' ? item.result : null);
  const resultado = normalizeOutcome(rawOutcome);
  const resolvida =
    ['resolved', 'settled', 'finished', 'complete', 'completed', 'resolvido', 'concluido', 'finalizado']
      .some((w) => status.includes(w)) || Boolean(fim);
  const id = String(
    d.id ?? d.roundId ?? d.round_id ?? d.eventId ??
    item?.id ?? item?.roundId ?? item?.round_id ?? item?.eventId ?? ''
  ).trim();
  if (!id || !resultado || !resolvida || !fim) return null;
  const scoreJogador = Number(r.playerDice?.score ?? d.playerDice?.score ?? d.playerScore ?? d.player_score ?? d.pontos_jogador);
  const scoreBanca = Number(r.bankerDice?.score ?? d.bankerDice?.score ?? d.bankerScore ?? d.banker_score ?? d.pontos_banca);
  const multiplicador = r.multiplier ?? d.multiplier ?? d.multiplicador ?? null;
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
      { chat_id: CHAT_ID, text: texto, disable_web_page_preview: true },
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
    await pool.query(`ALTER TABLE bacbo_resultados ADD COLUMN IF NOT EXISTS seq BIGSERIAL`);
    await pool.query(`ALTER TABLE bacbo_resultados ADD COLUMN IF NOT EXISTS guardado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_bacbo_fim ON bacbo_resultados (fim DESC)`);
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
        estrategia TEXT
      )
    `);
    await pool.query(`ALTER TABLE sinais ADD COLUMN IF NOT EXISTS estrategia TEXT`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_sinais_estado ON sinais (estado)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_sinais_criado ON sinais (criado_em DESC)`);
    log('DB', { tabelasPreparadas: true });
  })();
  try {
    await schemaPromise;
  } catch (e) {
    schemaPromise = null;
    throw e;
  }
}

async function fetchRounds() {
  try {
    const response = await axios.get(API_BACBO, {
      timeout: 20000,
      headers: { Accept: 'application/json', 'User-Agent': 'CapungaDadosBot/4.0' }
    });
    runtime.ultimoHTTPAPI = response.status;
    runtime.ultimoErroAPI = null;
    const items = extractItems(response.data);
    const unique = new Map();
    for (const item of items) {
      const round = normalizeRound(item);
      if (round) unique.set(round.id, round);
    }
    const rounds = [...unique.values()].sort((a, b) => new Date(a.fim) - new Date(b.fim));
    runtime.itensRecebidosAPI = items.length;
    runtime.quantidadeAPI = rounds.length;
    runtime.ultimaHoraDaAPI = rounds.at(-1)?.fim || null;
    return rounds;
  } catch (error) {
    runtime.ultimoErroAPI = error.message || String(error);
    runtime.ultimoHTTPAPI = error.response?.status || null;
    throw error;
  }
}

async function insertRound(round) {
  const result = await pool.query(
    `INSERT INTO bacbo_resultados
      (id, resultado, estado, inicio, fim, pontos_jogador, pontos_banca, multiplicador)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
     ON CONFLICT (id) DO NOTHING RETURNING id`,
    [round.id, round.resultado, round.estado, round.inicio, round.fim,
     round.pontos_jogador, round.pontos_banca, round.multiplicador]
  );
  return result.rows.length > 0;
}

async function getRecentCores(limit = 150) {
  const result = await pool.query(
    `SELECT resultado FROM bacbo_resultados
     ORDER BY fim DESC NULLS LAST, seq DESC LIMIT $1`,
    [limit]
  );
  return result.rows.map((r) => r.resultado);
}

async function redsToday() {
  const result = await pool.query(`
    SELECT COUNT(*)::int AS total FROM sinais
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
  const s = result.rows[0] || { green: 0, g1: 0, g2: 0, loss: 0, empates: 0 };
  const acertos = (s.green || 0) + (s.g1 || 0) + (s.g2 || 0);
  const finalizados = acertos + (s.loss || 0);
  const percent = finalizados > 0 ? Number(((acertos / finalizados) * 100).toFixed(1)) : 100;
  const recent = await pool.query(`
    SELECT estado FROM sinais
    WHERE criado_em >= date_trunc('day', NOW() AT TIME ZONE 'Africa/Luanda')
      AND estado IN ('GREEN', 'G1', 'G2', 'LOSS')
    ORDER BY criado_em DESC, id DESC LIMIT 50
  `);
  let greensSeguidos = 0;
  for (const row of recent.rows) {
    if (['GREEN', 'G1', 'G2'].includes(row.estado)) greensSeguidos++;
    else break;
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
    `🟩 GREEN: ${p.green}   🟨 G1: ${p.g1}   🟧 G2: ${p.g2}`,
    `🟡 Empates: ${p.empates}`,
    `🟥 LOSS: ${p.loss}`,
    `📈 Percentagem: ${p.percent}%`,
    `🔥 Greens seguidos: ${p.greensSeguidos}`,
    '━━━━━━━━━━━━━━━━━━━━'
  ].join('\n');
}

async function getPendingSignal() {
  const result = await pool.query(`
    SELECT * FROM sinais WHERE estado = 'PENDENTE'
    ORDER BY criado_em ASC, id ASC LIMIT 1
  `);
  return result.rows[0] || null;
}

async function expirePendingSignal(signal) {
  if (!signal) return false;
  const ageMs = Date.now() - new Date(signal.criado_em).getTime();
  const limite = CONFIG.sinalExpiraMin * 60000;
  if (ageMs < limite) return false;
  if (runtime.rodadasDesdeSinal < 2 && ageMs < limite * 1.5) return false;

  await pool.query(
    `UPDATE sinais SET estado = 'CANCELADO' WHERE id = $1 AND estado = 'PENDENTE'`,
    [signal.id]
  );
  await telegramSend(
    ['⌛ SINAL EXPIRADO', `Sinal #${signal.id}`, `🕒 ${horaAngola()}`].join('\n')
  );
  log('SINAL EXPIRADO', { id: signal.id });
  runtime.rodadasDesdeSinal = 0;
  return true;
}

/* Mensagens — SEM nome de estratégia */

function msgNovoSinal(entry) {
  return [
    '🎲🇺🇸 CAPUNGA DADOS 🇺🇸🎲',
    '⚠️🚨 Entrada Confirmada 🚨⚠️',
    `Cor: ${corDisplay(entry.cor)}`,
    '🪬 Proteção: 🟡 Empate = GREEN 🟡',
    `🐓 Gale Máximo: ${CONFIG.maxGale}`,
    '🧠 Faça Gestão de Banca 🧠',
    `🕒 ${horaAngola()}`
  ].join('\n');
}

function msgResultadoGreen(signal, state, porEmpate) {
  let titulo;
  if (porEmpate) {
    titulo = '🟡🏆 GREEN (EMPATE) 🏆🟡\n✨ Sinal fechado ✨';
  } else if (state === 'GREEN') {
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
    `🐓 Gale: ${signal.gale || 0}/${CONFIG.maxGale}`,
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

async function maybeCreateSignal(round) {
  if (!CONFIG.sinaisAtivos) {
    runtime.motivoSemSinal = 'sinais_desativados';
    return;
  }
  if (!['Jogador', 'Banca'].includes(round.resultado)) {
    runtime.motivoSemSinal = 'rodada_empate';
    return;
  }

  let pending = await getPendingSignal();
  if (pending) {
    if (await expirePendingSignal(pending)) pending = null;
    else {
      runtime.motivoSemSinal = `pendente_#${pending.id}`;
      return;
    }
  }

  const reds = await redsToday();
  if (reds >= CONFIG.maxRedsDia) {
    runtime.motivoSemSinal = `limite_reds_${reds}`;
    return;
  }

  const lastResult = await pool.query(
    `SELECT criado_em FROM sinais ORDER BY criado_em DESC, id DESC LIMIT 1`
  );
  const last = lastResult.rows[0];
  if (
    last?.criado_em &&
    Date.now() - new Date(last.criado_em).getTime() < CONFIG.intervaloMinimoMs
  ) {
    runtime.motivoSemSinal = 'intervalo_minimo';
    return;
  }

  // Histórico longo: até 150 resultados (curto + médio + longo)
  const hist = await getRecentCores(150);
  const entry = decidir(hist, {
    consensoMin: CONFIG.consensoMin,
    minPrioridade: CONFIG.minPrioridade
  });

  if (!entry) {
    runtime.motivoSemSinal = `sem_setup[n=${hist.length}]`;
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
    `INSERT INTO sinais (cor, estado, gale, resultado_id, empates, estrategia)
     VALUES ($1, 'PENDENTE', 0, $2, 0, $3) RETURNING *`,
    [entry.cor, round.id, entry._ids || null]
  );
  const signal = result.rows[0];

  runtime.ultimoSinal = {
    id: signal.id,
    cor: signal.cor,
    estado: signal.estado,
    criado_em: signal.criado_em,
    confianca: entry.confianca
  };
  runtime.ultimaEstrategia = {
    confianca: entry.confianca,
    votos: entry.votos,
    mesa: entry.mesa,
    ids: entry._ids
  };
  runtime.motivoSemSinal = null;
  runtime.rodadasDesdeSinal = 0;

  await telegramSend(msgNovoSinal(entry));
  log('SINAL CRIADO', {
    id: signal.id,
    cor: entry.cor,
    confianca: entry.confianca,
    ids: entry._ids,
    mesa: entry.mesa
  });
}

/** Empate = GREEN e fecha. */
async function resolvePending(round) {
  const signal = await getPendingSignal();
  if (!signal) return;
  if (String(signal.resultado_id) === String(round.id)) return;

  runtime.rodadasDesdeSinal = (runtime.rodadasDesdeSinal || 0) + 1;

  if (round.resultado === 'Empate') {
    if (!CONFIG.empateProtegido) return;
    const galeNum = Number(signal.gale || 0);
    let state = 'GREEN';
    if (galeNum === 1) state = 'G1';
    else if (galeNum >= 2) state = 'G2';
    await pool.query(
      `UPDATE sinais SET estado = $1, resultado_final = 'Empate',
        empates = COALESCE(empates, 0) + 1
       WHERE id = $2 AND estado = 'PENDENTE'`,
      [state, signal.id]
    );
    const placar = await getPlacarDia();
    await telegramSend(msgResultadoGreen(signal, state, true) + formatPlacar(placar));
    log('SINAL GREEN EMPATE', { id: signal.id });
    runtime.rodadasDesdeSinal = 0;
    return;
  }

  if (round.resultado === signal.cor) {
    const galeNum = Number(signal.gale || 0);
    let state = 'GREEN';
    if (galeNum === 1) state = 'G1';
    else if (galeNum >= 2) state = 'G2';
    await pool.query(
      `UPDATE sinais SET estado = $1, resultado_final = $2
       WHERE id = $3 AND estado = 'PENDENTE'`,
      [state, round.resultado, signal.id]
    );
    const placar = await getPlacarDia();
    await telegramSend(msgResultadoGreen(signal, state, false) + formatPlacar(placar));
    log('SINAL RESOLVIDO', { id: signal.id, estado: state });
    runtime.rodadasDesdeSinal = 0;
    return;
  }

  const nextGale = Number(signal.gale || 0) + 1;
  if (nextGale <= CONFIG.maxGale) {
    await pool.query(
      `UPDATE sinais SET gale = $1 WHERE id = $2 AND estado = 'PENDENTE'`,
      [nextGale, signal.id]
    );
    await telegramSend(msgGale(signal, nextGale));
    log('GALE', { id: signal.id, gale: nextGale });
    return;
  }

  await pool.query(
    `UPDATE sinais SET estado = 'LOSS', resultado_final = $1
     WHERE id = $2 AND estado = 'PENDENTE'`,
    [round.resultado, signal.id]
  );
  const placar = await getPlacarDia();
  await telegramSend(msgResultadoLoss(signal) + formatPlacar(placar));
  log('SINAL LOSS', { id: signal.id });
  runtime.rodadasDesdeSinal = 0;
}

async function syncOnce() {
  if (!pool) throw new Error('DATABASE_URL não configurada');
  if (runtime.sincronizacaoEmCurso) return { sucesso: false, ignorada: true };
  runtime.sincronizacaoEmCurso = true;

  try {
    await ensureSchema();
    const count = await pool.query('SELECT COUNT(*)::int AS total FROM bacbo_resultados');
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
      novos: inserted,
      duplicados: duplicates,
      avaliados: evaluated,
      motivoSemSinal: runtime.motivoSemSinal,
      horaAngola: horaAngola()
    };
    if (inserted > 0 || runtime.ciclosSemNovos % 8 === 0) log('SYNC', summary);
    return { sucesso: true, ...summary };
  } catch (error) {
    runtime.erroSincronizacao = error.message || String(error);
    console.error('[ERRO SYNC]', error.message);
    return { sucesso: false, erro: runtime.erroSincronizacao };
  } finally {
    runtime.sincronizacaoEmCurso = false;
  }
}

async function keepAliveTick() {
  try {
    let url = runtime.publicUrl || CONFIG.keepAliveUrl;
    if (!url && process.env.RENDER_EXTERNAL_URL) {
      url = process.env.RENDER_EXTERNAL_URL;
    }
    if (!url) return;
    const base = url.replace(/\/$/, '');
    const target = base.includes('/health') ? base : `${base}/health`;
    await axios.get(target, { timeout: 10000 });
    runtime.ultimoKeepAlive = new Date().toISOString();
    runtime.publicUrl = base;
  } catch (_) {}
}

async function maybeMidnightReport() {
  try {
    const parts = new Intl.DateTimeFormat('en-GB', {
      timeZone: 'Africa/Luanda',
      hour: '2-digit',
      minute: '2-digit',
      day: '2-digit',
      month: '2-digit',
      year: 'numeric',
      hour12: false
    }).formatToParts(new Date());
    const get = (t) => parts.find((p) => p.type === t)?.value;
    const hour = get('hour');
    const minute = get('minute');
    const dayKey = `${get('year')}-${get('month')}-${get('day')}`;
    if (hour === '00' && Number(minute) < 2) {
      if (runtime.ultimoRelatorioDia === dayKey) return;
      runtime.ultimoRelatorioDia = dayKey;
      const placar = await getPlacarDia();
      await telegramSend(
        [
          '🌙 RELATÓRIO DE MEIA-NOITE',
          `🕒 ${horaAngola()}`,
          formatPlacar(placar)
        ].join('\n')
      );
    }
  } catch (_) {}
}

function requireAdmin(req, res, next) {
  if (!ADMIN_TOKEN) {
    return res.status(503).json({ sucesso: false, erro: 'ADMIN_TOKEN não configurado' });
  }
  if (req.get('x-admin-token') !== ADMIN_TOKEN) {
    return res.status(401).json({ sucesso: false, erro: 'Token inválido' });
  }
  next();
}

app.get('/', (req, res) => {
  res.json({
    nome: 'Capunga Dados',
    versao: 'final-completo',
    estado: 'online',
    horaAngola: horaAngola(),
    empateRegra: 'GREEN e fecha',
    rotas: ['/health', '/diagnostico', '/resultados', '/sinais', '/estatisticas']
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
    motivoSemSinal: runtime.motivoSemSinal
  });
});

app.get('/diagnostico', async (req, res) => {
  let totalResultados = 0;
  let totalSinais = 0;
  let pendentes = 0;
  let recentes = [];
  let ultimo = null;
  let mesa = null;
  try {
    if (pool) {
      await ensureSchema();
      totalResultados = (await pool.query('SELECT COUNT(*)::int AS t FROM bacbo_resultados')).rows[0].t;
      totalSinais = (await pool.query('SELECT COUNT(*)::int AS t FROM sinais')).rows[0].t;
      pendentes = (await pool.query(`SELECT COUNT(*)::int AS t FROM sinais WHERE estado = 'PENDENTE'`)).rows[0].t;
      recentes = (
        await pool.query(
          `SELECT resultado FROM bacbo_resultados ORDER BY fim DESC NULLS LAST, seq DESC LIMIT 20`
        )
      ).rows.map((r) => r.resultado);
      ultimo = (
        await pool.query(
          `SELECT id, resultado, fim FROM bacbo_resultados ORDER BY fim DESC NULLS LAST, seq DESC LIMIT 1`
        )
      ).rows[0];
      const hist = await getRecentCores(150);
      mesa = analisarMesa(hist);
      mesa = {
        amostra: mesa.amostra,
        modo: mesa.modo,
        viesMedio: mesa.viesMedio,
        viesLongo: mesa.viesLongo,
        alt20: Math.round(mesa.alt20 * 100),
        seq: mesa.seq,
        c10: mesa.c10,
        c20: mesa.c20,
        c50: { n: mesa.c50.n, banca: mesa.c50.banca, jogador: mesa.c50.jogador }
      };
    }
  } catch (_) {}

  res.json({
    sucesso: true,
    nome: 'Capunga Dados Final Completo',
    iniciadoEm: runtime.iniciadoEm,
    horaAngola: horaAngola(),
    telegramConfigurado: Boolean(BOT_TOKEN && CHAT_ID),
    httpAPI: runtime.ultimoHTTPAPI,
    erroAPI: runtime.ultimoErroAPI,
    ultimaSincronizacao: runtime.ultimaSincronizacao,
    novosUltimoCiclo: runtime.novosUltimoCiclo,
    ciclosSemNovos: runtime.ciclosSemNovos,
    motivoSemSinal: runtime.motivoSemSinal,
    ultimoSinal: runtime.ultimoSinal,
    ultimaAnalise: runtime.ultimaEstrategia,
    ultimosResultados: recentes,
    analiseMesa: mesa,
    baseDados: { totalResultados, totalSinais, pendentes, ultimo },
    configuracao: CONFIG,
    regraEmpate: 'Empate = GREEN e fecha',
    mensagens: 'Sem nome de estratégia no Telegram'
  });
});

app.get('/resultados', async (req, res) => {
  try {
    await ensureSchema();
    const limit = Math.min(Math.max(Number.parseInt(req.query.limit, 10) || 50, 1), 500);
    const result = await pool.query(
      `SELECT id, resultado, estado, inicio, fim, pontos_jogador, pontos_banca, multiplicador
       FROM bacbo_resultados ORDER BY fim DESC NULLS LAST, seq DESC LIMIT $1`,
      [limit]
    );
    res.json({ sucesso: true, total: result.rows.length, resultados: result.rows });
  } catch (error) {
    res.status(500).json({ sucesso: false, erro: error.message });
  }
});

app.get('/sinais', async (req, res) => {
  try {
    await ensureSchema();
    const limit = Math.min(Math.max(Number.parseInt(req.query.limit, 10) || 50, 1), 500);
    const result = await pool.query(
      `SELECT id, criado_em, cor, estado, gale, resultado_final, empates
       FROM sinais ORDER BY criado_em DESC, id DESC LIMIT $1`,
      [limit]
    );
    res.json({ sucesso: true, total: result.rows.length, sinais: result.rows });
  } catch (error) {
    res.status(500).json({ sucesso: false, erro: error.message });
  }
});

app.get('/estatisticas', async (req, res) => {
  try {
    await ensureSchema();
    const result = await pool.query(`
      SELECT COUNT(*)::int AS total,
        COUNT(*) FILTER (WHERE estado = 'GREEN')::int AS green,
        COUNT(*) FILTER (WHERE estado = 'G1')::int AS g1,
        COUNT(*) FILTER (WHERE estado = 'G2')::int AS g2,
        COUNT(*) FILTER (WHERE estado = 'LOSS')::int AS loss,
        COUNT(*) FILTER (WHERE estado = 'PENDENTE')::int AS pendentes,
        COUNT(*) FILTER (WHERE estado = 'CANCELADO')::int AS cancelados
      FROM sinais
    `);
    const stats = result.rows[0];
    const acertos = (stats.green || 0) + (stats.g1 || 0) + (stats.g2 || 0);
    const finalizados = acertos + (stats.loss || 0);
    const taxa = finalizados > 0 ? Number(((acertos / finalizados) * 100).toFixed(1)) : null;
    let placarDia = null;
    try {
      placarDia = await getPlacarDia();
    } catch (_) {}
    res.json({
      sucesso: true,
      estatisticas: { ...stats, finalizados, acertos, taxaAcertoPercent: taxa },
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
    res.json({ sucesso: true, mensagem: 'OK' });
  } catch (error) {
    res.status(500).json({ sucesso: false, erro: error.message });
  }
});

app.post('/db/sincronizar', requireAdmin, async (req, res) => {
  try {
    const result = await syncOnce();
    res.status(result.sucesso ? 200 : 500).json(result);
  } catch (error) {
    res.status(500).json({ sucesso: false, erro: error.message });
  }
});

app.post('/telegram/teste', requireAdmin, async (req, res) => {
  const enviado = await telegramSend(
    [
      '🎲🇺🇸 CAPUNGA DADOS 🇺🇸🎲',
      '✅ Teste OK — versão final completa',
      '🟡 Empate = GREEN | Análise multi-camada',
      `🕒 ${horaAngola()}`
    ].join('\n')
  );
  res.status(enviado ? 200 : 502).json({ sucesso: enviado, erro: runtime.ultimoErroTelegram });
});

app.use((req, res) => {
  res.status(404).json({ sucesso: false, erro: 'Rota não encontrada', caminho: req.path });
});

async function start() {
  if (!DATABASE_URL) throw new Error('DATABASE_URL não configurada no Render');
  await ensureSchema();

  server = app.listen(PORT, '0.0.0.0', () => {
    console.log('======================================');
    console.log('CAPUNGA DADOS — FINAL COMPLETO');
    console.log(`Porta: ${PORT} | ${horaAngola()}`);
    console.log(`Consenso: ${CONFIG.consensoMin} | Min score: ${CONFIG.minPrioridade}`);
    console.log(`Intervalo: ${CONFIG.intervaloMinimoMs}ms | Empate=GREEN`);
    console.log('Análise: curto + médio + longo + frequência');
    console.log('======================================');
  });

  const initial = await syncOnce();
  if (!initial.sucesso) console.error('[PRIMEIRA SYNC]', initial.erro);
  else log('PRIMEIRA SYNC', { recebidos: initial.recebidos, novos: initial.novos });

  syncTimer = setInterval(async () => {
    if (runtime.sincronizacaoEmCurso) return;
    try {
      await syncOnce();
    } catch (err) {
      console.error('[SYNC]', err.message);
    }
  }, CONFIG.intervaloSyncMs);
  if (typeof syncTimer.unref === 'function') syncTimer.unref();

  keepAliveTimer = setInterval(() => {
    keepAliveTick().catch(() => {});
  }, CONFIG.keepAliveMs);
  if (typeof keepAliveTimer.unref === 'function') keepAliveTimer.unref();
  setTimeout(() => keepAliveTick().catch(() => {}), 30000);

  midnightTimer = setInterval(() => {
    maybeMidnightReport().catch(() => {});
  }, 60000);
  if (typeof midnightTimer.unref === 'function') midnightTimer.unref();
}

process.on('unhandledRejection', (r) => console.error('[UNHANDLED]', r));
process.on('uncaughtException', (e) => console.error('[UNCAUGHT]', e));

async function shutdown() {
  if (syncTimer) clearInterval(syncTimer);
  if (keepAliveTimer) clearInterval(keepAliveTimer);
  if (midnightTimer) clearInterval(midnightTimer);
  if (server) await new Promise((r) => server.close(r));
  if (pool) await pool.end();
  process.exit(0);
}
process.on('SIGTERM', () => shutdown().catch(() => process.exit(1)));
process.on('SIGINT', () => shutdown().catch(() => process.exit(1)));

start().catch((e) => {
  console.error('[FATAL]', e);
  process.exit(1);
});
