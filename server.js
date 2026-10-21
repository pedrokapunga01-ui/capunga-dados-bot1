'use strict';

/**
 * BAC BO TELEGRAM — V4 · GREEN/LOSS DIRETO · KEEP-ALIVE ROBUSTO
 *
 * Melhorias:
 * - health check interno + múltiplos candidatos de keep-alive
 * - tolerância a Render / instâncias suspensas
 * - mensagem de análise única e reutilização
 * - não força sinal por silêncio
 * - GREEN e LOSS em 1 rodada, sem Gale
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
  modo: 'DIRETO_SEM_GALE',
  empateProtegido: boolEnv(process.env.EMPATE_PROTEGIDO, true),
  intervaloSyncMs: intEnv('INTERVALO_SYNC_MS', 8000, 5000, 60000),
  keepAliveMs: intEnv('KEEP_ALIVE_MS', 60000, 30000, 900000),
  consensoMin: intEnv('CONSENSO_MIN', 2, 2, 5),
  minPrioridade: intEnv('MIN_PRIORIDADE', 88, 80, 98),
  intervaloOportunidadeMs: {
    elite: intEnv('INTERVALO_ELITE_MS', 120000, 60000, 300000),
    forte: intEnv('INTERVALO_FORTE_MS', 300000, 120000, 600000),
    normal: intEnv('INTERVALO_NORMAL_MS', 600000, 300000, 1800000)
  },
  sinalExpiraMin: intEnv('SINAL_EXPIRA_MIN', 6, 3, 20),
  keepAliveUrl: process.env.KEEP_ALIVE_URL || process.env.RENDER_EXTERNAL_URL || '',
  keepAliveUrls: (process.env.KEEP_ALIVE_URLS || '')
    .split(',')
    .map((x) => x.trim())
    .filter(Boolean)
};

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
  ultimaAnalise: null,
  rodadasDesdeSinal: 0,
  publicUrl: CONFIG.keepAliveUrl || null,
  ultimoKeepAlive: null,
  ultimoSinalEm: null,
  ultimaAnaliseMsgId: null,
  ultimaAnaliseEdicao: 0,
  erroApagarAnalise: null,
  ultimoKeepAliveUrl: null
};

let schemaPromise = null;
let server = null;
let syncTimer = null;
let keepAliveTimer = null;
let lockClient = null;

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

function scoreOf(value) {
  const n = Number(value?.score ?? value?.total ?? value);
  return Number.isInteger(n) && n >= 2 && n <= 12 ? n : null;
}

function normalizeRound(item) {
  const d = item?.data || item?.round || item || {};
  const r =
    d.result && typeof d.result === 'object'
      ? d.result
      : item?.result && typeof item.result === 'object'
        ? item.result
        : {};

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

  return {
    id,
    resultado,
    estado: 'Resolved',
    inicio: inicio || fim,
    fim,
    pontos_jogador: scoreOf(r.playerDice ?? r.player ?? d.playerDice ?? d.playerScore ?? d.pontosJogador),
    pontos_banca: scoreOf(r.bankerDice ?? r.banker ?? d.bankerDice ?? d.bankerScore ?? d.pontosBanca),
    multiplicador: r.multiplier ?? d.multiplier ?? null
  };
}

/* ===================== ANÁLISE AVANÇADA ===================== */

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
  return {
    n: s.length,
    banca,
    jogador: s.length - banca,
    pctBanca: s.length ? banca / s.length : 0.5
  };
}

function zProp(k, n, p0 = 0.5) {
  return n ? (k / n - p0) / Math.sqrt((p0 * (1 - p0)) / n) : 0;
}

function taxaAlt(dec, n) {
  const s = dec.slice(0, n);
  if (s.length < 2) return 0.5;
  let t = 0;
  for (let i = 1; i < s.length; i++) if (s[i] !== s[i - 1]) t++;
  return t / (s.length - 1);
}

function analisarMesa(hist) {
  const dec = decisivos(hist);
  const seq = sequencia(dec);
  const c8 = contar(dec, 8);
  const c12 = contar(dec, 12);
  const c20 = contar(dec, 20);
  const c50 = contar(dec, 50);
  const c100 = contar(dec, 100);
  const alt12 = taxaAlt(dec, 12);
  const alt20 = taxaAlt(dec, 20);

  let viesMedio = null;
  if (c20.n >= 16) {
    if (c20.pctBanca >= 0.6) viesMedio = 'Banca';
    else if (c20.pctBanca <= 0.4) viesMedio = 'Jogador';
  }

  let viesLongo = null;
  if (c50.n >= 40) {
    const z = zProp(c50.banca, c50.n);
    if (Math.abs(z) >= 1.5) viesLongo = z > 0 ? 'Banca' : 'Jogador';
  }
  if (!viesLongo && c100.n >= 70) {
    const z = zProp(c100.banca, c100.n);
    if (Math.abs(z) >= 1.4) viesLongo = z > 0 ? 'Banca' : 'Jogador';
  }

  const modo = alt20 >= 0.6 ? 'alternante' : alt20 <= 0.35 ? 'streak' : 'misto';

  return {
    dec,
    seq,
    c8,
    c12,
    c20,
    c50,
    c100,
    alt12,
    alt20,
    viesMedio,
    viesLongo,
    modo,
    amostra: dec.length
  };
}

function bonusMesa(cor, mesa) {
  let b = 0;
  if (mesa.viesMedio === cor) b += 10;
  if (mesa.viesLongo === cor) b += 12;
  if (mesa.viesMedio && mesa.viesMedio !== cor) b -= 8;
  if (mesa.viesLongo && mesa.viesLongo !== cor) b -= 5;
  if (mesa.modo === 'streak' && mesa.seq.cor === cor && mesa.seq.tamanho >= 3) b += 8;
  if (mesa.modo === 'alternante' && mesa.seq.cor && OPOSTO[mesa.seq.cor] === cor) b += 8;
  if (mesa.c12.n >= 10) {
    const dom = mesa.c12.banca > mesa.c12.jogador ? 'Banca' : 'Jogador';
    if (dom === cor && Math.abs(mesa.c12.banca - mesa.c12.jogador) >= 4) b += 7;
  }
  if (mesa.alt12 <= 0.3 && mesa.seq.cor === cor) b += 5;
  return b;
}

const LOGICAS = [
  {
    id: 'SURF4',
    p: 92,
    avaliar(dec) {
      const s = sequencia(dec);
      if (s.tamanho === 4 && dec.length > 4 && dec[4] !== s.cor) return { cor: s.cor };
      return null;
    }
  },
  {
    id: 'PAD22',
    p: 94,
    avaliar(dec) {
      if (dec.length < 4) return null;
      const [a, b, c, d] = dec;
      if (a === b && c === d && a !== c) return { cor: a };
      return null;
    }
  },
  {
    id: 'DUPLO22',
    p: 96,
    avaliar(dec) {
      if (dec.length < 6) return null;
      const [a, b, c, d, e, f] = dec;
      if (a === b && e === f && a === e && c === d && a !== c) return { cor: a };
      return null;
    }
  },
  {
    id: 'ALT5',
    p: 90,
    avaliar(dec) {
      const s = dec.slice(0, 5);
      if (s.length < 5) return null;
      for (let i = 1; i < 5; i++) if (s[i] === s[i - 1]) return null;
      return { cor: OPOSTO[s[0]] };
    }
  },
  {
    id: 'ALT6',
    p: 93,
    avaliar(dec) {
      const s = dec.slice(0, 6);
      if (s.length < 6) return null;
      for (let i = 1; i < 6; i++) if (s[i] === s[i - 1]) return null;
      return { cor: OPOSTO[s[0]] };
    }
  },
  {
    id: 'STREAK4',
    p: 85,
    avaliar(dec) {
      const s = sequencia(dec);
      if (s.tamanho === 4 || s.tamanho === 5) return { cor: s.cor };
      return null;
    }
  },
  {
    id: 'QUEBRA7',
    p: 88,
    avaliar(dec) {
      const s = sequencia(dec);
      if (s.tamanho >= 7) return { cor: OPOSTO[s.cor] };
      return null;
    }
  },
  {
    id: 'MAIORIA12',
    p: 82,
    avaliar(dec) {
      const c = contar(dec, 12);
      if (c.n < 12 || Math.abs(c.banca - c.jogador) < 5) return null;
      const cor = c.banca > c.jogador ? 'Banca' : 'Jogador';
      const s = sequencia(dec);
      if (s.cor && s.cor !== cor && s.tamanho >= 2) return null;
      return { cor };
    }
  },
  {
    id: 'PRESSAO5',
    p: 84,
    avaliar(dec) {
      if (dec.length < 5) return null;
      const s = dec.slice(0, 5);
      const b = s.filter((x) => x === 'Banca').length;
      if (b >= 4) return { cor: 'Banca' };
      if (b <= 1) return { cor: 'Jogador' };
      return null;
    }
  },
  {
    id: 'FLUXO50',
    p: 80,
    avaliar(dec, mesa) {
      if (!mesa.viesLongo || mesa.c50.n < 45) return null;
      const s = sequencia(dec);
      if (s.cor && s.cor !== mesa.viesLongo && s.tamanho >= 3) return null;
      return { cor: mesa.viesLongo };
    }
  },
  {
    id: 'FLEX_STREAK3',
    p: 70,
    flexOnly: true,
    avaliar(dec) {
      const s = sequencia(dec);
      if (s.tamanho >= 3) return { cor: s.cor };
      return null;
    }
  },
  {
    id: 'FLEX_PRESSAO3',
    p: 68,
    flexOnly: true,
    avaliar(dec) {
      if (dec.length < 3) return null;
      const s = dec.slice(0, 3);
      if (s[0] === s[1] && s[1] === s[2]) return { cor: s[0] };
      const b = s.filter((x) => x === 'Banca').length;
      if (b === 3) return { cor: 'Banca' };
      if (b === 0) return { cor: 'Jogador' };
      return null;
    }
  }
];

function decidir(hist, opcoes = {}) {
  const minP = Number(opcoes.minPrioridade) || 88;
  const consensoMin = Math.max(1, Number(opcoes.consensoMin) || 2);
  const flexivel = Boolean(opcoes.flexivel);
  const limiar = flexivel ? Math.max(62, minP - 16) : minP;
  const cons = flexivel ? 1 : consensoMin;

  const mesa = analisarMesa(hist);
  const dec = mesa.dec;
  if (dec.length < 10 && !flexivel) return null;
  if (dec.length < 6) return null;

  const candidatos = [];
  for (const L of LOGICAS) {
    if (L.flexOnly && !flexivel) continue;
    const r = L.avaliar(dec, mesa);
    if (!r || !r.cor) continue;
    const bonus = bonusMesa(r.cor, mesa);
    const score = L.p + bonus;
    if (score < limiar) continue;
    candidatos.push({ id: L.id, cor: r.cor, score, bonus });
  }
  if (!candidatos.length) return null;

  const peso = { Banca: 0, Jogador: 0 };
  const votos = { Banca: 0, Jogador: 0 };
  for (const c of candidatos) {
    peso[c.cor] += c.score;
    votos[c.cor]++;
  }

  let cor = null;
  if (peso.Banca > peso.Jogador) cor = 'Banca';
  else if (peso.Jogador > peso.Banca) cor = 'Jogador';
  else return null;

  if (votos[cor] < cons) return null;

  const apoio = candidatos.filter((c) => c.cor === cor).sort((a, b) => b.score - a.score);
  const top = apoio[0];

  if (!flexivel && apoio.length === 1 && top.bonus < 0) return null;

  return {
    cor,
    confianca: Math.min(99, Math.round(top.score)),
    flexivel,
    votos: votos[cor],
    _ids: apoio.map((c) => c.id).join('+'),
    mesa: {
      modo: mesa.modo,
      viesMedio: mesa.viesMedio,
      viesLongo: mesa.viesLongo,
      amostra: mesa.amostra
    }
  };
}

/* ===================== DB / TELEGRAM / APP ===================== */

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
        resultado_id TEXT,
        resultado_final TEXT,
        empates INTEGER NOT NULL DEFAULT 0,
        estrategia TEXT,
        resultado_rodada_id TEXT
      )
    `);

    await pool.query(`ALTER TABLE sinais ADD COLUMN IF NOT EXISTS estrategia TEXT`);
    await pool.query(`ALTER TABLE sinais ADD COLUMN IF NOT EXISTS resultado_rodada_id TEXT`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_sinais_estado ON sinais (estado)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_sinais_criado ON sinais (criado_em DESC)`);

    await pool.query(`
      CREATE TABLE IF NOT EXISTS bacbo_bot_state (
        chave TEXT PRIMARY KEY,
        valor TEXT,
        atualizado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);

    const res = await pool.query(
      `SELECT valor FROM bacbo_bot_state WHERE chave = 'analysis_message_id' LIMIT 1`
    );
    const saved = Number(res.rows[0]?.valor);
    if (Number.isSafeInteger(saved) && saved > 0) {
      runtime.ultimaAnaliseMsgId = saved;
    }

    log('DB', { ok: true, analysisMessageRecovered: Boolean(runtime.ultimaAnaliseMsgId) });
  })();

  try {
    await schemaPromise;
  } catch (e) {
    schemaPromise = null;
    throw e;
  }
}

async function persistAnalysisMessageId(messageId) {
  runtime.ultimaAnaliseMsgId = messageId || null;
  if (!pool) return;
  try {
    await pool.query(
      `INSERT INTO bacbo_bot_state (chave, valor, atualizado_em)
       VALUES ('analysis_message_id', $1, NOW())
       ON CONFLICT (chave) DO UPDATE SET valor = EXCLUDED.valor, atualizado_em = NOW()`,
      [messageId ? String(messageId) : null]
    );
  } catch (error) {
    console.error('[TELEGRAM_STATE]', error.message);
  }
}

async function telegramDelete(messageId) {
  if (!BOT_TOKEN || !CHAT_ID || !messageId || messageId === true) return false;
  try {
    const response = await axios.post(
      `https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`,
      { chat_id: CHAT_ID, message_id: messageId },
      { timeout: 10000 }
    );
    return response.data?.ok === true;
  } catch (error) {
    const description = String(error.response?.data?.description || '').toLowerCase();
    if (description.includes('message to delete not found')) return true;
    runtime.ultimoErroTelegram = error.response?.data ? JSON.stringify(error.response.data) : error.message;
    return false;
  }
}

async function limparMensagemAnalise(tentativas = 3) {
  const id = runtime.ultimaAnaliseMsgId;
  if (!id) return true;

  for (let tentativa = 1; tentativa <= tentativas; tentativa++) {
    if (await telegramDelete(id)) {
      runtime.ultimaAnaliseEdicao = 0;
      runtime.erroApagarAnalise = null;
      await persistAnalysisMessageId(null);
      return true;
    }

    if (tentativa < tentativas) {
      await new Promise((resolve) => setTimeout(resolve, 300 * tentativa));
    }
  }

  runtime.erroApagarAnalise = 'Telegram não confirmou a remoção após 3 tentativas';
  console.error('[TELEGRAM]', runtime.erroApagarAnalise, 'ID mantido para nova tentativa:', id);
  return false;
}

async function telegramSend(texto, opcoes = {}) {
  if (!BOT_TOKEN || !CHAT_ID) {
    runtime.ultimoErroTelegram = 'BOT_TOKEN ou CHAT_ID em falta';
    return null;
  }

  if (!opcoes.mensagemAnalise && runtime.ultimaAnaliseMsgId) {
    await limparMensagemAnalise(3);
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
      return response.data.result?.message_id ?? true;
    }

    runtime.ultimoErroTelegram = JSON.stringify(response.data);
    return null;
  } catch (error) {
    runtime.ultimoErroTelegram = error.response?.data
      ? JSON.stringify(error.response.data)
      : error.message;
    console.error('[TELEGRAM]', runtime.ultimoErroTelegram);
    return null;
  }
}

async function telegramEdit(messageId, texto) {
  if (!BOT_TOKEN || !CHAT_ID || !messageId) return false;
  try {
    const response = await axios.post(
      `https://api.telegram.org/bot${BOT_TOKEN}/editMessageText`,
      { chat_id: CHAT_ID, message_id: messageId, text: texto, disable_web_page_preview: true },
      { timeout: 10000 }
    );
    if (response.data?.ok === true) return true;
    return String(response.data?.description || '').toLowerCase().includes('message is not modified');
  } catch (error) {
    const description = String(error.response?.data?.description || '').toLowerCase();
    if (description.includes('message is not modified')) return true;
    runtime.ultimoErroTelegram = error.response?.data ? JSON.stringify(error.response.data) : error.message;
    return false;
  }
}

async function estadoAnalise(ativo) {
  if (!ativo) {
    await limparMensagemAnalise(3);
    return;
  }

  const texto = '◌ RADAR BAC BO · EM LEITURA\nA aguardar consenso e oportunidade. Sem previsão garantida.';
  const agora = Date.now();

  if (runtime.ultimaAnaliseMsgId) {
    if (agora - runtime.ultimaAnaliseEdicao < 60_000) return;
    if (await telegramEdit(runtime.ultimaAnaliseMsgId, texto)) {
      runtime.ultimaAnaliseEdicao = agora;
      return;
    }
    if (!(await limparMensagemAnalise(3))) return;
  }

  const id = await telegramSend(texto, { mensagemAnalise: true });
  if (id && id !== true) {
    await persistAnalysisMessageId(id);
    runtime.ultimaAnaliseEdicao = agora;
  }
}

async function fetchRounds() {
  try {
    const response = await axios.get(API_BACBO, {
      timeout: 20000,
      headers: { Accept: 'application/json', 'User-Agent': 'BacBoDirect/4.0' }
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
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (id) DO NOTHING RETURNING id`,
    [round.id, round.resultado, round.estado, round.inicio, round.fim,
      round.pontos_jogador, round.pontos_banca, round.multiplicador]
  );

  if (result.rows.length > 0) return true;

  await pool.query(
    `UPDATE bacbo_resultados SET
       pontos_jogador = COALESCE(pontos_jogador, $2),
       pontos_banca = COALESCE(pontos_banca, $3),
       multiplicador = COALESCE(multiplicador, $4)
     WHERE id = $1`,
    [round.id, round.pontos_jogador, round.pontos_banca, round.multiplicador]
  );

  return false;
}

async function getRecentCores(limit = 150) {
  const result = await pool.query(
    `SELECT resultado FROM bacbo_resultados
     ORDER BY fim DESC NULLS LAST, seq DESC LIMIT $1`,
    [limit]
  );
  return result.rows.map((r) => r.resultado);
}

async function getPlacarDia() {
  const result = await pool.query(`
    SELECT
      COUNT(*) FILTER (WHERE estado = 'GREEN')::int AS greens,
      COUNT(*) FILTER (WHERE estado = 'LOSS')::int AS loss,
      COALESCE(SUM(empates), 0)::int AS empates
    FROM sinais
    WHERE criado_em >= ((date_trunc('day', NOW() AT TIME ZONE 'Africa/Luanda')) AT TIME ZONE 'Africa/Luanda')
      AND estado IN ('GREEN', 'LOSS')
  `);

  const s = result.rows[0] || { greens: 0, loss: 0, empates: 0 };
  const greensTotal = s.greens || 0;
  const finalizados = greensTotal + (s.loss || 0);
  const percent = finalizados > 0 ? Number(((greensTotal / finalizados) * 100).toFixed(1)) : null;

  return {
    greensTotal,
    empates: s.empates || 0,
    loss: s.loss || 0,
    percent
  };
}

function formatPlacar(p) {
  return [
    '╭── ◇ RADAR · BALANÇO DO DIA ──╮',
    `│ 🟢 GREEN direto: ${p.greensTotal}`,
    `│ 🔻 LOSS direto: ${p.loss}`,
    `│ 🟡 Empates: ${p.empates}`,
    `│ ◈ Aproveitamento: ${p.percent === null ? '—' : `${p.percent}%`}`,
    '╰────────────────────────────╯'
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
  if (runtime.rodadasDesdeSinal < 2 && ageMs < limite * 1.3) return false;

  await pool.query(
    `UPDATE sinais SET estado = 'CANCELADO' WHERE id = $1 AND estado = 'PENDENTE'`,
    [signal.id]
  );
  await telegramSend(`⏳ Entrada #${signal.id} cancelada: não chegou uma rodada dentro do prazo.`);
  log('EXPIRADO', { id: signal.id });
  runtime.rodadasDesdeSinal = 0;
  return true;
}

function msgNovoSinal(entry) {
  const color = entry.cor === 'Jogador' ? '🔷 AZUL · JOGADOR' : '🔶 LARANJA · BANCA';
  const tier = entry.confianca >= 96 && entry.votos >= 3 ? '✦ JANELA ELITE'
    : entry.confianca >= 92 ? '◈ JANELA FORTE' : '◇ OPORTUNIDADE';

  return [
    '╭━━ ◉ RADAR BAC BO ━━╮',
    `│ ${tier}`,
    `│ ENTRADA · ${color}`,
    `│ Força do padrão: ${entry.confianca}/100 · ${entry.votos} votos`,
    '│ Direto · 1 rodada · sem gale',
    '│ Score técnico, não probabilidade.',
    '╰━━━━━━━━━━━━━━━━━━╯'
  ].join('\n');
}

function msgResultado(state, round, porEmpate) {
  const empateGreen = state === 'GREEN' && (porEmpate || round.resultado === 'Empate');
  const headline = state === 'LOSS' ? '🔻 FECHO DIRETO · LOSS'
    : empateGreen ? '🟡 FECHO DIRETO · GREEN POR EMPATE' : '🟢 FECHO DIRETO · GREEN';

  const resultado = round.resultado === 'Empate'
    ? '◎ Resultado: EMPATE'
    : `✧ Venceu: ${round.resultado === 'Banca' ? '🟠 BANCA' : '🔹 JOGADOR'}`;

  const points = round.pontos_jogador !== null && round.pontos_banca !== null
    ? `\n▸ Jogador ${round.pontos_jogador}  │  Banca ${round.pontos_banca}`
    : '';

  return `${headline}\n${resultado}${points}`;
}

function intervaloPara(entry) {
  if (entry.confianca >= 96 && entry.votos >= 3) return CONFIG.intervaloOportunidadeMs.elite;
  if (entry.confianca >= 92 && entry.votos >= 2) return CONFIG.intervaloOportunidadeMs.forte;
  return CONFIG.intervaloOportunidadeMs.normal;
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
    runtime.motivoSemSinal = `pendente_#${pending.id}`;
    return;
  }

  const hist = await getRecentCores(150);
  const entry = decidir(hist, {
    minPrioridade: CONFIG.minPrioridade,
    consensoMin: CONFIG.consensoMin
  });

  if (!entry) {
    runtime.motivoSemSinal = `oportunidade_rejeitada[n=${hist.length}]`;
    runtime.ultimaAnalise = { amostra: hist.length, decisao: 'sem_consenso_ou_qualidade' };
    return;
  }

  const last = await pool.query(`SELECT criado_em FROM sinais ORDER BY criado_em DESC, id DESC LIMIT 1`);
  const lastAt = last.rows[0]?.criado_em ? new Date(last.rows[0].criado_em).getTime() : 0;
  const pause = intervaloPara(entry);
  if (lastAt && Date.now() - lastAt < pause) {
    runtime.motivoSemSinal = 'aguardando_intervalo_da_oportunidade';
    runtime.ultimaAnalise = { scoreInterno: entry.confianca, votos: entry.votos, pausaMs: pause };
    return;
  }

  const used = await pool.query(`SELECT 1 FROM sinais WHERE resultado_id = $1 LIMIT 1`, [round.id]);
  if (used.rows.length) {
    runtime.motivoSemSinal = 'rodada_ja_usada';
    return;
  }

  const result = await pool.query(
    `INSERT INTO sinais (cor, estado, resultado_id, empates, estrategia)
     VALUES ($1, 'PENDENTE', $2, 0, $3) RETURNING *`,
    [entry.cor, round.id, entry._ids || null]
  );

  const signal = result.rows[0];
  runtime.ultimoSinal = { id: signal.id, cor: signal.cor, criado_em: signal.criado_em };
  runtime.ultimoSinalEm = new Date().toISOString();
  runtime.ultimaAnalise = {
    scoreInterno: entry.confianca,
    votos: entry.votos,
    flexivel: false,
    mesa: entry.mesa,
    logicas: entry._ids,
    intervaloAplicadoMs: pause
  };
  runtime.motivoSemSinal = null;
  runtime.rodadasDesdeSinal = 0;

  await estadoAnalise(false);
  await telegramSend(msgNovoSinal(entry));
  log('SINAL_DIRETO', {
    id: signal.id,
    cor: entry.cor,
    scoreInterno: entry.confianca,
    votos: entry.votos,
    intervaloMs: pause
  });
}

async function resolvePending(round) {
  const signal = await getPendingSignal();
  if (!signal || String(signal.resultado_id) === String(round.id)) return;

  const empate = round.resultado === 'Empate' && CONFIG.empateProtegido;
  const green = empate || round.resultado === signal.cor;
  const state = green ? 'GREEN' : 'LOSS';

  await pool.query(
    `UPDATE sinais SET estado = $1, resultado_final = $2, resultado_rodada_id = $3,
       empates = COALESCE(empates,0) + $4
     WHERE id = $5 AND estado = 'PENDENTE'`,
    [state, round.resultado, round.id, empate ? 1 : 0, signal.id]
  );

  const placar = await getPlacarDia();
  await telegramSend(msgResultado(state, round, empate) + '\n\n' + formatPlacar(placar));
  log(green ? 'GREEN_DIRETO' : 'LOSS_DIRETO', {
    id: signal.id,
    corSinal: signal.cor,
    resultado: round.resultado,
    empate,
    rodadaId: round.id
  });

  runtime.rodadasDesdeSinal = 0;
}

async function syncOnce() {
  if (!pool) throw new Error('DATABASE_URL não configurada');
  if (runtime.sincronizacaoEmCurso) return { sucesso: false, ignorada: true };
  runtime.sincronizacaoEmCurso = true;

  try {
    await ensureSchema();
    if (!(await getPendingSignal())) await estadoAnalise(true);

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

function getKeepAliveTargets() {
  const list = new Set();

  const direct = CONFIG.keepAliveUrl || process.env.RENDER_EXTERNAL_URL || '';
  if (direct) list.add(direct);

  for (const url of CONFIG.keepAliveUrls || []) {
    if (url) list.add(url);
  }

  if (process.env.RENDER_EXTERNAL_URL) list.add(process.env.RENDER_EXTERNAL_URL);
  if (process.env.PUBLIC_URL) list.add(process.env.PUBLIC_URL);

  return [...list].map((u) => {
    const base = String(u).trim().replace(/\/$/, '');
    return base ? (base.includes('/health') ? base : `${base}/health`) : '';
  }).filter(Boolean);
}

async function keepAliveTick() {
  const targets = getKeepAliveTargets();
  if (!targets.length) return;

  let ok = false;
  for (const target of targets) {
    try {
      await axios.get(target, { timeout: 10000 });
      runtime.ultimoKeepAlive = new Date().toISOString();
      runtime.ultimoKeepAliveUrl = target;
      runtime.publicUrl = target.replace(/\/health$/, '');
      ok = true;
      break;
    } catch (_) {
      // tenta próximo alvo
    }
  }

  if (!ok) {
    runtime.ultimoKeepAlive = null;
  }
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
    nome: 'Bac Bo Telegram',
    versao: '4.0-sinal-direto',
    modo: 'DIRETO_SEM_GALE',
    estado: 'online',
    horaAngola: horaAngola()
  });
});

app.get('/health', (req, res) => {
  const ok = Boolean(
    DATABASE_URL &&
    BOT_TOKEN &&
    CHAT_ID &&
    runtime.ultimaSincronizacao &&
    !runtime.erroSincronizacao &&
    !runtime.ultimoErroAPI &&
    !runtime.ultimoErroTelegram
  );

  res.status(ok ? 200 : 503).json({
    sucesso: ok,
    estado: ok ? 'online' : 'degradado',
    horaAngola: horaAngola(),
    ultimaSincronizacao: runtime.ultimaSincronizacao,
    ultimaRodadaNova: runtime.ultimaRodadaNova || null,
    erroAPI: runtime.ultimoErroAPI,
    erroTelegram: runtime.ultimoErroTelegram,
    erroApagarAnalise: runtime.erroApagarAnalise,
    mensagemAnaliseAtiva: Boolean(runtime.ultimaAnaliseMsgId),
    motivoSemSinal: runtime.motivoSemSinal,
    ultimoKeepAlive: runtime.ultimoKeepAlive,
    ultimoKeepAliveUrl: runtime.ultimoKeepAliveUrl
  });
});

app.get('/diagnostico', async (req, res) => {
  let totalResultados = 0;
  let totalSinais = 0;
  let pendentes = 0;
  let recentes = [];
  let mesa = null;

  try {
    if (pool) {
      await ensureSchema();
      totalResultados = (await pool.query('SELECT COUNT(*)::int AS t FROM bacbo_resultados')).rows[0].t;
      totalSinais = (await pool.query('SELECT COUNT(*)::int AS t FROM sinais')).rows[0].t;
      pendentes = (await pool.query(`SELECT COUNT(*)::int AS t FROM sinais WHERE estado = 'PENDENTE'`)).rows[0].t;
      recentes = (
        await pool.query(
          `SELECT resultado FROM bacbo_resultados ORDER BY fim DESC NULLS LAST, seq DESC LIMIT 15`
        )
      ).rows.map((r) => r.resultado);

      const hist = await getRecentCores(150);
      const m = analisarMesa(hist);
      mesa = {
        amostra: m.amostra,
        modo: m.modo,
        viesMedio: m.viesMedio,
        viesLongo: m.viesLongo,
        seq: m.seq
      };
    }
  } catch (_) {}

  res.json({
    sucesso: true,
    versao: '4.0-sinal-direto',
    modo: 'DIRETO_SEM_GALE',
    horaAngola: horaAngola(),
    telegramConfigurado: Boolean(BOT_TOKEN && CHAT_ID),
    httpAPI: runtime.ultimoHTTPAPI,
    erroAPI: runtime.ultimoErroAPI,
    ultimaSincronizacao: runtime.ultimaSincronizacao,
    novosUltimoCiclo: runtime.novosUltimoCiclo,
    ciclosSemNovos: runtime.ciclosSemNovos,
    motivoSemSinal: runtime.motivoSemSinal,
    ultimoSinal: runtime.ultimoSinal,
    ultimaAnalise: runtime.ultimaAnalise,
    ultimosResultados: recentes,
    analiseMesa: mesa,
    baseDados: { totalResultados, totalSinais, pendentes },
    configuracao: CONFIG
  });
});

app.get('/resultados', async (req, res) => {
  try {
    await ensureSchema();
    const limit = Math.min(Math.max(Number.parseInt(req.query.limit, 10) || 40, 1), 300);
    const result = await pool.query(
      `SELECT id, resultado, fim FROM bacbo_resultados
       ORDER BY fim DESC NULLS LAST, seq DESC LIMIT $1`,
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
    const limit = Math.min(Math.max(Number.parseInt(req.query.limit, 10) || 40, 1), 300);
    const result = await pool.query(
      `SELECT id, criado_em, cor, estado, resultado_final, empates
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
    const placarDia = await getPlacarDia();
    res.json({ sucesso: true, placarDia, horaAngola: horaAngola() });
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
  const mid = await telegramSend(
    ['◉ RADAR BAC BO', '◎ Teste de ligação · SINAL DIRETO'].join('\n')
  );
  res.status(mid ? 200 : 502).json({ sucesso: Boolean(mid), erro: runtime.ultimoErroTelegram });
});

app.use((req, res) => {
  res.status(404).json({ sucesso: false, erro: 'Rota não encontrada' });
});

async function start() {
  if (!DATABASE_URL) throw new Error('DATABASE_URL não configurada');
  await ensureSchema();

  lockClient = await pool.connect();
  const lock = await lockClient.query('SELECT pg_try_advisory_lock($1) AS locked', [731905221]);
  if (!lock.rows[0]?.locked) {
    lockClient.release();
    lockClient = null;
    throw new Error('Já existe outra instância do bot com esta base de dados; evitando sinais duplicados.');
  }

  server = app.listen(PORT, '0.0.0.0', () => {
    console.log('======================================');
    console.log('◉ RADAR BAC BO — GREEN OU LOSS DIRETO');
    console.log(`Porta ${PORT} | ${horaAngola()}`);
    console.log(`Cadência por oportunidade: elite ${CONFIG.intervaloOportunidadeMs.elite}ms / forte ${CONFIG.intervaloOportunidadeMs.forte}ms / normal ${CONFIG.intervaloOportunidadeMs.normal}ms`);
    console.log(`Consenso ${CONFIG.consensoMin} | score interno mínimo ${CONFIG.minPrioridade} | sem tentativas adicionais`);
    console.log('======================================');
  });

  const initial = await syncOnce();
  if (!initial.sucesso) console.error('[PRIMEIRA SYNC]', initial.erro);
  else log('PRIMEIRA SYNC', initial);

  syncTimer = setInterval(async () => {
    if (runtime.sincronizacaoEmCurso) return;
    try {
      await syncOnce();
    } catch (err) {
      console.error('[SYNC]', err.message);
    }
  }, CONFIG.intervaloSyncMs);

  if (typeof syncTimer.unref === 'function') syncTimer.unref();

  keepAliveTimer = setInterval(() => keepAliveTick().catch(() => {}), CONFIG.keepAliveMs);
  if (typeof keepAliveTimer.unref === 'function') keepAliveTimer.unref();

  setTimeout(() => keepAliveTick().catch(() => {}), 15000);
  setTimeout(() => keepAliveTick().catch(() => {}), 30000);
}

process.on('unhandledRejection', (r) => {
  console.error('[UNHANDLED] Processo será encerrado para reinício supervisionado.', r);
  process.exit(1);
});

process.on('uncaughtException', (e) => {
  console.error('[UNCAUGHT] Processo será encerrado para reinício supervisionado.', e);
  process.exit(1);
});

async function shutdown() {
  if (syncTimer) clearInterval(syncTimer);
  if (keepAliveTimer) clearInterval(keepAliveTimer);

  if (server) {
    await new Promise((r) => server.close(r));
  }

  if (lockClient) {
    try {
      await lockClient.query('SELECT pg_advisory_unlock($1)', [731905221]);
    } finally {
      lockClient.release();
      lockClient = null;
    }
  }

  if (pool) await pool.end();
  process.exit(0);
}

process.on('SIGTERM', () => shutdown().catch(() => process.exit(1)));
process.on('SIGINT', () => shutdown().catch(() => process.exit(1)));

start().catch((e) => {
  console.error('[FATAL]', e);
  process.exit(1);
});