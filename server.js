'use strict';

/**
 * CAPUNGA DADOS — UNIFICADO (v3.7)
 *
 * Variáveis obrigatórias: DATABASE_URL, BOT_TOKEN, CHAT_ID, ADMIN_TOKEN
 * Opcional: RENDER_EXTERNAL_URL (Render define sozinho) -> auto-ping anti-sleep
 *
 * v3.7: liderança por lock no Postgres (impede 2 instâncias), alerta de instância estranha,
 *       resolução de sinais atômica e em ordem, só 1 PENDENTE (índice único), Telegram com retry.
 * v3.6: confiança do sinal (80-95%) na mensagem, índice multifatorial (computeConfidence).
 * v3.5: sinais nunca expiram nem ficam cancelados; só fecham em GREEN/G1/G2/LOSS.
 *
 * Correções v3.4:
 * - Chave de deduplicação corrigida (template literal)
 * - Limpeza de chaves corrompidas no start
 * - Baseline: no 1º sync só guarda histórico, não gera sinais com rodadas antigas
 * - Sinal gerado 1x por ciclo, com base na rodada mais recente
 * - Reset diário às 00h Angola corrigido
 * - Diagnóstico de atraso da API (atrasoMin)
 */

const express = require('express');
const cors = require('cors');
const axios = require('axios');
const crypto = require('node:crypto');
const { Pool, Client } = require('pg');

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
const SELF_URL = process.env.RENDER_EXTERNAL_URL || '';
const INSTANCIA = crypto.randomBytes(3).toString('hex');
const LOCK_KEY = 727255;
// RESET_RODADAS=true na Render também apaga o histórico de rodadas no deploy (o bot recarrega 50 da API)
const RESET_RODADAS = String(process.env.RESET_RODADAS || '').toLowerCase() === 'true';

const API_BACBO =
  'https://api-cs.casino.org/svc-evolution-game-events/api/bacbo?page=0&size=50&sort=data.settledAt,desc&duration=120&wheelResults=PlayerWon,BankerWon,Tie';

const CONFIG = {
  sinaisAtivos: true,
  streakMin: 3,
  maxGale: 2,
  empateProtegido: true,
  intervaloMinimoMs: 35000,
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
  instancia: INSTANCIA,
  lider: false,
  alertaOutraInstancia: null,
  baselineFeito: false,
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
let pingTimer = null;
let server = null;
let ultimoResetDia = null;

// ====================== HELPERS ======================
function log(tag, data) {
  console.log(`[${tag}]`, typeof data === 'string' ? data : JSON.stringify(data));
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

function horaAngola() {
  return new Intl.DateTimeFormat('pt-PT', {
    timeZone: 'Africa/Luanda',
    dateStyle: 'short',
    timeStyle: 'medium',
    hour12: false
  }).format(new Date());
}

// Retorna { data: 'YYYY-MM-DD', hora: 'HH:MM' } no fuso de Angola
function agoraAngola() {
  const s = new Date().toLocaleString('sv-SE', { timeZone: 'Africa/Luanda' }); // "2026-11-08 12:34:16"
  const [data, h] = s.split(' ');
  return { data, hora: h.slice(0, 5) };
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
    body.data?.results, body.data?.items, body.data?.resultados, body.data?.rounds,
    body.data?.events, body.data?.data, body.data?.content
  ];

  for (const c of candidates) {
    if (Array.isArray(c) && c.length > 0) return c;
  }
  return [];
}

function resultSignature(r) {
  return crypto
    .createHash('sha256')
    .update(
      JSON.stringify({
        outcome: r.resultado,
        settledAt: r.fim,
        playerScore: r.pontos_jogador,
        bankerScore: r.pontos_banca
      })
    )
    .digest('hex');
}

// ====================== TELEGRAM ======================
async function telegramSend(texto) {
  if (!BOT_TOKEN || !CHAT_ID) {
    runtime.ultimoErroTelegram = 'BOT_TOKEN ou CHAT_ID não configurado';
    log('TELEGRAM FALHA', runtime.ultimoErroTelegram);
    return false;
  }

  for (let tentativa = 1; tentativa <= 3; tentativa++) {
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
    } catch (error) {
      const data = error.response?.data;
      runtime.ultimoErroTelegram = data ? JSON.stringify(data) : error.message;

      // Erros definitivos: não adianta repetir
      if (error.response && [400, 401, 403, 404].includes(error.response.status)) break;

      // Limite de envio do Telegram: espera o tempo pedido
      const espera = Number(data?.parameters?.retry_after);
      if (Number.isFinite(espera) && espera > 0 && espera <= 20) {
        await sleep(espera * 1000 + 200);
        continue;
      }
    }
    if (tentativa < 3) await sleep(1500 * tentativa);
  }

  log('TELEGRAM FALHA', runtime.ultimoErroTelegram);
  return false;
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
    await pool.query(
      `CREATE INDEX IF NOT EXISTS idx_mesa_settled ON mesa_resultados (settled_at DESC NULLS LAST, observado_em DESC)`
    );

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
    await pool.query(`ALTER TABLE sinais ADD COLUMN IF NOT EXISTS confianca INTEGER`);
    await pool.query(`ALTER TABLE sinais ADD COLUMN IF NOT EXISTS instancia TEXT`);
    await pool.query(`ALTER TABLE sinais ADD COLUMN IF NOT EXISTS ultima_fim TIMESTAMPTZ`);
    await pool.query(`ALTER TABLE sinais ADD COLUMN IF NOT EXISTS passo INTEGER NOT NULL DEFAULT 0`);

    // Garante, ao nível da base de dados, no máximo 1 sinal PENDENTE
    try {
      await pool.query(`
        UPDATE sinais SET estado = 'CANCELADO'
        WHERE estado = 'PENDENTE'
          AND id <> (SELECT MIN(id) FROM sinais WHERE estado = 'PENDENTE')
      `);
      await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS uq_sinal_pendente ON sinais (estado) WHERE estado = 'PENDENTE'`);
    } catch (e) {
      log('DB AVISO', `Índice de pendente único não criado: ${e.message}`);
    }
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_sinais_estado ON sinais (estado)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_sinais_criado ON sinais (criado_em DESC)`);

    // Limpa chaves corrompidas pelo bug antigo (v3.3)
    const del = await pool.query(
      `DELETE FROM mesa_resultados
       WHERE source_key NOT LIKE 'id:%' AND source_key NOT LIKE 'hash:%'
          OR source_key LIKE 'id:(%'
          OR source_key LIKE 'hash: )%'`
    );
    if (del.rowCount > 0) log('DB', `Removidas ${del.rowCount} chaves corrompidas`);

    log('DB', 'Schema pronto');
  })();

  try {
    await schemaPromise;
  } catch (e) {
    schemaPromise = null;
    throw e;
  }
}

// ====================== COLETA ======================
function normalizeRound(item) {
  try {
    const d = item?.data || item?.round || item || {};
    const r =
      d.result && typeof d.result === 'object'
        ? d.result
        : item?.result && typeof item.result === 'object'
        ? item.result
        : {};

    const rawOutcome =
      r.outcome ?? r.resultado ?? r.winner ??
      d.outcome ?? d.resultado ?? d.winner ??
      item?.outcome ?? item?.resultado ?? item?.winner ??
      (typeof d.result === 'string' ? d.result : null) ??
      (typeof item?.result === 'string' ? item.result : null);

    const resultado = normalizeOutcome(rawOutcome);
    if (!resultado) return null;

    const id =
      String(
        d.id ?? d.roundId ?? d.round_id ?? d.eventId ?? d.gameId ??
        item?.id ?? item?.roundId ?? item?.round_id ?? item?.eventId ?? ''
      ).trim() || null;

    const fim =
      validDate(
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

    // CORRIGIDO: interpolação correta
    round.source_key = id ? `id:${id}` : `hash:${resultSignature(round)}`;
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
        'User-Agent': 'CapungaDados/3.7',
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

    const rounds = [...unique.values()].sort((a, b) => new Date(a.fim) - new Date(b.fim));
    const maisRecente = rounds.length ? rounds[rounds.length - 1].fim : null;
    const atrasoMin = maisRecente
      ? Number(((Date.now() - new Date(maisRecente).getTime()) / 60000).toFixed(1))
      : null;

    runtime.ultimoLogAPI = {
      recebidos: items.length,
      normalizados,
      unicos: rounds.length,
      maisRecente,
      atrasoMin
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
    `SELECT source_id AS id, resultado, settled_at AS fim,
            player_score AS pontos_jogador, banker_score AS pontos_banca
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
  for (const r of sample) (r.resultado === 'Jogador' ? j++ : b++);
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

  const best = candidatos[0];
  // Confluência: quantas OUTRAS estratégias apontam para a mesma cor
  best.confluencia = candidatos.filter(c => c !== best && c.cor === best.cor).length;
  return best;
}

// ====================== CONFIANÇA ======================
const ESTRATEGIAS = {
  REPETICAO: stratRepeticao,
  QUEBRA: stratQuebra,
  DOIS_DOIS: stratDoisDois,
  SURF: stratSurf,
  FORCA: stratForca,
  NUMERICA: stratNumerica
};

const CONF_MIN = 80;
const CONF_MAX = 95;
const PESOS = {
  padrao: 0.15,
  tendencia: 0.10,
  momento: 0.10,
  coerencia: 0.05,
  historico: 0.30,
  confluencia: 0.10,
  empates: 0.05,
  fase: 0.15
};

function clamp(x, a = 0, b = 1) {
  return Math.min(b, Math.max(a, x));
}

// Parcela decrescente (mais recente pesa mais) da cor nas últimas N rodadas
function shareDecay(rows, cor, n = 8) {
  let num = 0, den = 0;
  rows.slice(0, n).forEach((r, i) => {
    const w = Math.pow(0.85, i);
    den += w;
    if (r.resultado === cor) num += w;
  });
  return den ? num / den : 0.5;
}

function shareSimple(rows, cor, n = 12) {
  const s = rows.slice(0, n);
  if (!s.length) return 0.5;
  return s.filter(r => r.resultado === cor).length / s.length;
}

// Taxa de trocas de cor (0 = sem trocas, 1 = alternância total) nas últimas N rodadas
function switchRate(rows, n = 10) {
  const s = rows.slice(0, n);
  if (s.length < 3) return 0.5;
  let sw = 0;
  for (let i = 0; i < s.length - 1; i++) if (s[i].resultado !== s[i + 1].resultado) sw++;
  return sw / (s.length - 1);
}

// Backtest: com que frequência esta estratégia acertou (até G2) no histórico guardado
function backtestEstrategia(chave, hist) {
  const fn = ESTRATEGIAS[chave];
  if (!fn || !hist || hist.length < 20) return { n: 0, rate: null };
  let n = 0, hits = 0;
  // hist está em ordem decrescente (0 = mais recente); o "futuro" de k é hist[k-1], hist[k-2], hist[k-3]
  for (let k = 3; k < hist.length - 4; k++) {
    const entry = fn(hist.slice(k, k + 40));
    if (!entry) continue;
    n++;
    const futuro = [hist[k - 1], hist[k - 2], hist[k - 3]];
    if (futuro.some(r => r.resultado === entry.cor)) hits++;
  }
  return { n, rate: n ? hits / n : null };
}

function computeConfidence(entry, rows, hist, empatesRecentes, fase) {
  const cor = entry.cor;
  const chave = entry.estrategia;

  // 1) Força do padrão (prioridade 81..92)
  const fPadrao = clamp((entry.prioridade - 80) / 12);

  // 2) Tendência e 3) Momento
  let fTendencia, fMomento;
  if (chave === 'QUEBRA') {
    const n = streakLen(rows);
    fTendencia = clamp((n - 3) / 4); // exaustão da sequência
    const corSeq = rows[0]?.resultado;
    fMomento = clamp((shareDecay(rows, corSeq, 8) - 0.6) / 0.4); // mesa muito unilateral = esticada
  } else {
    fTendencia = clamp((shareSimple(rows, cor, 12) - 0.4) / 0.3);
    fMomento = clamp((shareDecay(rows, cor, 8) - 0.4) / 0.5);
  }

  // 4) Coerência do padrão com o ritmo da mesa
  const sr = switchRate(rows, 10);
  let fCoerencia;
  if (chave === 'SURF') fCoerencia = sr;
  else if (chave === 'DOIS_DOIS') fCoerencia = 1 - Math.abs(sr - 0.5) * 2;
  else fCoerencia = 1 - sr;

  // 5) Desempenho histórico (backtest, com suavização para amostras pequenas)
  const bt = backtestEstrategia(chave, hist);
  const prior = 0.875, pw = 8;
  const rateSuave = bt.rate === null ? prior : (bt.rate * bt.n + prior * pw) / (bt.n + pw);
  const fHistorico = clamp((rateSuave - 0.8) / 0.15);

  // 6) Confluência com outras estratégias
  const fConfluencia = clamp((entry.confluencia || 0) / 2);

  // 7) Empates recentes (perturbam o padrão)
  const fEmpates = 1 - clamp(empatesRecentes / 4);

  // 8) Fase do bot (últimos sinais fechados)
  const fFase = fase === null ? 0.5 : fase;

  const fatores = {
    padrao: fPadrao, tendencia: fTendencia, momento: fMomento, coerencia: fCoerencia,
    historico: fHistorico, confluencia: fConfluencia, empates: fEmpates, fase: fFase
  };

  let raw = 0;
  for (const k of Object.keys(PESOS)) raw += PESOS[k] * fatores[k];

  const confianca = Math.round(clamp(CONF_MIN + raw * (CONF_MAX - CONF_MIN), CONF_MIN, CONF_MAX));

  return {
    confianca,
    detalhe: {
      raw: Number(raw.toFixed(3)),
      backtest: { amostra: bt.n, acerto: bt.rate === null ? null : Number(bt.rate.toFixed(3)) },
      fatores: Object.fromEntries(Object.entries(fatores).map(([k, v]) => [k, Number(v.toFixed(2))]))
    }
  };
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
    WHERE criado_em >= date_trunc('day', NOW() AT TIME ZONE 'Africa/Luanda') AT TIME ZONE 'Africa/Luanda'
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

function msgNovoSinal(entry) {
  return [
    '🎲🇺🇸 CAPUNGA DADOS 🇺🇸🎲',
    '━━━━━━━━━━━━━━━━━━━━',
    '🚨⚡ ENTRADA CONFIRMADA ⚡🚨',
    '━━━━━━━━━━━━━━━━━━━━',
    `🎯 Cor: ${corDisplay(entry.cor)}`,
    `${entry.emoji} Estratégia: ${entry.nome}`,
    `📈 Confiança: ${entry.confianca}%`,
    '🛡️ Proteção: 🟡 EMPATE',
    `🐓 Gale máximo: ${CONFIG.maxGale}`,
    '💎 Gestão de banca obrigatória',
    `🕒 ${horaAngola()}`,
    '━━━━━━━━━━━━━━━━━━━━'
  ].join('\n');
}

function msgResultadoGreen(signal, state) {
  const titulo =
    state === 'GREEN' ? '✅🏆 GREEENNN 🏆✅\n✨ Sem Gale ✨' :
    state === 'G1'    ? '⚡🏆 GREEN G1 🏆⚡\n🐓 Gale 1' :
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
    `🐓 Gale: ${signal.gale || 0}/${CONFIG.maxGale}`,
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

  // Sinal em andamento: só um de cada vez. Nunca expira, resolve sempre por rodadas.
  const pending = await getPendingSignal();
  if (pending) {
    runtime.motivoSemSinal = `em_andamento_#${pending.id}`;
    return;
  }

  const last = (await pool.query(`SELECT criado_em FROM sinais ORDER BY criado_em DESC LIMIT 1`)).rows[0];
  if (last?.criado_em && Date.now() - new Date(last.criado_em).getTime() < CONFIG.intervaloMinimoMs) {
    runtime.motivoSemSinal = 'intervalo_minimo';
    return;
  }

  const hist = await getRecentOutcomes(300);
  const rows = hist.slice(0, 30);
  const entry = detectBestEntry(rows);

  if (!entry) {
    runtime.motivoSemSinal = 'sem_setup';
    return;
  }

  // Dados extras para o cálculo da confiança
  const empatesRecentes = (await pool.query(
    `SELECT COUNT(*)::int AS c FROM (
       SELECT resultado FROM mesa_resultados ORDER BY settled_at DESC NULLS LAST, observado_em DESC LIMIT 12
     ) t WHERE resultado = 'Empate'`
  )).rows[0].c;

  const ultimos = (await pool.query(
    `SELECT estado FROM sinais WHERE estado IN ('GREEN','G1','G2','LOSS') ORDER BY criado_em DESC LIMIT 5`
  )).rows;
  const fase = ultimos.length >= 3
    ? ultimos.filter(x => x.estado !== 'LOSS').length / ultimos.length
    : null;

  const conf = computeConfidence(entry, rows, hist, empatesRecentes, fase);
  entry.confianca = conf.confianca;

  const refId = round.source_id || round.source_key;
  const jaUsada = await pool.query(`SELECT 1 FROM sinais WHERE resultado_id = $1 LIMIT 1`, [refId]);
  if (jaUsada.rows.length > 0) {
    runtime.motivoSemSinal = 'rodada_ja_usada';
    return;
  }

  let result;
  try {
    result = await pool.query(
      `INSERT INTO sinais (cor, estado, gale, resultado_id, empates, estrategia, nome_estrategia, confianca, instancia, ultima_fim, passo)
       VALUES ($1, 'PENDENTE', 0, $2, 0, $3, $4, $5, $6, $7, 0) RETURNING *`,
      [entry.cor, refId, entry.estrategia, entry.nome, entry.confianca, INSTANCIA, round.fim]
    );
  } catch (e) {
    if (e.code === '23505') {
      runtime.motivoSemSinal = 'ja_existe_pendente';
      return;
    }
    throw e;
  }

  const signal = result.rows[0];
  runtime.ultimoSinal = signal;
  runtime.ultimaEstrategia = entry;
  runtime.motivoSemSinal = null;

  await telegramSend(msgNovoSinal(entry));
  log('SINAL', { id: signal.id, cor: entry.cor, estrategia: entry.estrategia, instancia: INSTANCIA, confianca: entry.confianca, calculo: conf.detalhe });
}

// Atualiza o sinal de forma atômica: só aplica se ninguém mexeu nele desde que o lemos (passo)
async function updateSinal(signal, round, campos) {
  const sets = ['passo = passo + 1', 'ultima_fim = $1'];
  const vals = [round.fim];
  for (const [k, v] of Object.entries(campos)) {
    vals.push(v);
    sets.push(`${k} = $${vals.length}`);
  }
  vals.push(signal.id, Number(signal.passo || 0));
  const r = await pool.query(
    `UPDATE sinais SET ${sets.join(', ')}
     WHERE id = $${vals.length - 1} AND estado = 'PENDENTE' AND passo = $${vals.length}
     RETURNING *`,
    vals
  );
  return r.rows[0] || null;
}

async function resolvePending(round) {
  const signal = await getPendingSignal();
  if (!signal) return;

  // Só aceita rodadas ESTRITAMENTE posteriores à última aplicada (ou à rodada de entrada)
  const base = new Date(signal.ultima_fim || signal.criado_em).getTime();
  if (new Date(round.fim).getTime() <= base) return;

  if (round.resultado === 'Empate') {
    if (!CONFIG.empateProtegido) return;
    const upd = await updateSinal(signal, round, { empates: Number(signal.empates || 0) + 1 });
    if (upd) await telegramSend(msgEmpate(signal));
    return;
  }

  if (round.resultado === signal.cor) {
    const galeNum = Number(signal.gale || 0);
    const state = galeNum === 0 ? 'GREEN' : galeNum === 1 ? 'G1' : 'G2';
    const upd = await updateSinal(signal, round, { estado: state, resultado_final: round.resultado });
    if (upd) {
      const placar = await getPlacarDia();
      await telegramSend(msgResultadoGreen(signal, state) + formatPlacar(placar));
    }
    return;
  }

  const nextGale = Number(signal.gale || 0) + 1;
  if (nextGale <= CONFIG.maxGale) {
    const upd = await updateSinal(signal, round, { gale: nextGale });
    if (upd) await telegramSend(msgGale(signal, nextGale));
    return;
  }

  const upd = await updateSinal(signal, round, { estado: 'LOSS', resultado_final: round.resultado });
  if (upd) {
    const placar = await getPlacarDia();
    await telegramSend(msgResultadoLoss(signal) + formatPlacar(placar));
  }
}

// ====================== LIDERANÇA / OUTRA INSTÂNCIA ======================
let lockClient = null;
let resetInicialFeito = false;
let ultimoAvisoStandby = 0;
let ultimoAvisoEstranha = 0;

async function tryAcquireLeadership() {
  const client = new Client({
    connectionString: DATABASE_URL,
    ssl: isLocalDb ? false : { rejectUnauthorized: false },
    keepAlive: true
  });
  client.on('error', err => {
    log('LOCK ERRO', err.message);
    runtime.lider = false;
    lockClient = null;
  });
  await client.connect();
  const r = await client.query('SELECT pg_try_advisory_lock($1) AS ok', [LOCK_KEY]);
  if (r.rows[0].ok) {
    lockClient = client;
    runtime.lider = true;
    return true;
  }
  await client.end().catch(() => {});
  return false;
}

// Depois do reset inicial, qualquer sinal que não foi criado por esta instância vem de outro processo
async function verificarOutraInstancia() {
  const r = await pool.query(
    `SELECT id, instancia, criado_em FROM sinais WHERE instancia IS DISTINCT FROM $1 ORDER BY id DESC LIMIT 3`,
    [INSTANCIA]
  );
  if (r.rows.length) {
    runtime.alertaOutraInstancia = {
      msg: 'Existem sinais criados por OUTRA instância usando a mesma base de dados',
      exemplos: r.rows.map(x => ({ id: x.id, instancia: x.instancia || 'versao-antiga', criado_em: x.criado_em }))
    };
    if (Date.now() - ultimoAvisoEstranha > 60000) {
      ultimoAvisoEstranha = Date.now();
      log('ALERTA', runtime.alertaOutraInstancia);
    }
  } else {
    runtime.alertaOutraInstancia = null;
  }
}

// ====================== RESET ======================
async function verificarResetDiario() {
  const { data, hora } = agoraAngola();
  if (hora >= '00:00' && hora <= '00:04' && ultimoResetDia !== data) {
    // Nunca apaga sinal em andamento
    await pool.query(`DELETE FROM sinais WHERE estado <> 'PENDENTE'`);
    ultimoResetDia = data;
    log('RESET', `Histórico limpo às 00h Angola (${data})`);
    await telegramSend(`🧹 Histórico de sinais resetado\n🕛 00:00 Angola\n📅 ${data}`);
  }
}

// ====================== SYNC ======================
async function syncOnce() {
  if (!pool) throw new Error('DATABASE_URL não configurada');
  if (runtime.sincronizacaoEmCurso) return { sucesso: false, ignorada: true };

  runtime.sincronizacaoEmCurso = true;

  try {
    await ensureSchema();
    const rounds = await fetchRounds(); // ordem cronológica (antiga -> recente)

    let inserted = 0;
    let duplicates = 0;
    let ultimaNova = null;

    for (const round of rounds) {
      const saved = await insertRound(round);
      if (!saved) {
        duplicates++;
        continue;
      }
      inserted++;
      ultimaNova = round;

      // Só resolve sinais depois do baseline (evita processar histórico antigo)
      if (runtime.baselineFeito) {
        await resolvePending(round);
      }
    }

    // Um único sinal por ciclo, com base na rodada mais recente
    if (runtime.baselineFeito && ultimaNova) {
      await maybeCreateSignal(ultimaNova);
    }

    if (!runtime.baselineFeito && rounds.length > 0) {
      runtime.baselineFeito = true;
      log('BASELINE', { guardadas: inserted, msg: 'Histórico carregado. Sinais ativos a partir de agora.' });
    }

    runtime.ultimaSincronizacao = new Date().toISOString();
    runtime.novosUltimoCiclo = inserted;
    runtime.duplicadosUltimoCiclo = duplicates;
    runtime.erroSincronizacao = null;

    if (inserted > 0) {
      runtime.ultimaRodadaNova = new Date().toISOString();
      runtime.ciclosSemNovos = 0;
      log('NOVAS RODADAS', { quantidade: inserted, ultima: ultimaNova?.resultado, motivo: runtime.motivoSemSinal });
    } else {
      runtime.ciclosSemNovos = (runtime.ciclosSemNovos || 0) + 1;
      // evita spam: loga a cada 10 ciclos sem novidades
      if (runtime.ciclosSemNovos % 10 === 1) {
        log('SYNC', {
          novos: 0,
          duplicados: duplicates,
          ciclosSemNovos: runtime.ciclosSemNovos,
          motivo: runtime.motivoSemSinal,
          api: runtime.ultimoLogAPI
        });
      }
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
    nome: 'Capunga Dados — Unificado v3.7',
    estado: 'online',
    horaAngola: horaAngola(),
    estrategias: ['REPETICAO', 'QUEBRA', 'DOIS_DOIS', 'SURF', 'FORCA', 'NUMERICA']
  });
});

app.get('/health', (req, res) => {
  res.json({
    sucesso: true,
    estado: 'online',
    horaAngola: horaAngola(),
    instancia: runtime.instancia,
    lider: runtime.lider,
    alertaOutraInstancia: runtime.alertaOutraInstancia,
    baselineFeito: runtime.baselineFeito,
    ultimaSincronizacao: runtime.ultimaSincronizacao,
    novosUltimoCiclo: runtime.novosUltimoCiclo,
    ciclosSemNovos: runtime.ciclosSemNovos,
    motivoSemSinal: runtime.motivoSemSinal,
    ultimoLogAPI: runtime.ultimoLogAPI,
    erroSincronizacao: runtime.erroSincronizacao,
    ultimoErroTelegram: runtime.ultimoErroTelegram,
    ultimoSinal: runtime.ultimoSinal ? { id: runtime.ultimoSinal.id, cor: runtime.ultimoSinal.cor } : null
  });
});

app.get('/diagnostico', async (req, res) => {
  let totalResultados = 0, totalSinais = 0, pendentes = 0, recentes = [];
  try {
    await ensureSchema();
    totalResultados = (await pool.query('SELECT COUNT(*)::int AS t FROM mesa_resultados')).rows[0].t;
    totalSinais = (await pool.query('SELECT COUNT(*)::int AS t FROM sinais')).rows[0].t;
    pendentes = (await pool.query(`SELECT COUNT(*)::int AS t FROM sinais WHERE estado = 'PENDENTE'`)).rows[0].t;
    recentes = (
      await pool.query(`
        SELECT resultado, player_score, banker_score, settled_at
        FROM mesa_resultados
        ORDER BY settled_at DESC NULLS LAST LIMIT 15
      `)
    ).rows;
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

app.post('/admin/limpar-sinais', requireAdmin, async (req, res) => {
  try {
    await pool.query(`DELETE FROM sinais`);
    res.json({ sucesso: true });
  } catch (e) {
    res.status(500).json({ sucesso: false, erro: e.message });
  }
});

app.post('/telegram/teste', requireAdmin, async (req, res) => {
  const ok = await telegramSend(`🎲 CAPUNGA DADOS — Teste de conexão OK\n🕒 ${horaAngola()}`);
  res.status(ok ? 200 : 502).json({ sucesso: ok, erro: runtime.ultimoErroTelegram });
});

// ====================== START ======================
let cicloEmCurso = false;

async function cicloPrincipal() {
  if (cicloEmCurso) return;
  cicloEmCurso = true;
  try {
    if (!runtime.lider) {
      let ok = false;
      try {
        ok = await tryAcquireLeadership();
      } catch (e) {
        log('LOCK ERRO', e.message);
      }
      if (!ok) {
        if (Date.now() - ultimoAvisoStandby > 60000) {
          ultimoAvisoStandby = Date.now();
          log('STANDBY', `Instância ${INSTANCIA} em espera: outra instância é a líder (lock ocupado)`);
        }
        return;
      }
      log('LIDER', `Instância ${INSTANCIA} assumiu a liderança`);
      if (!resetInicialFeito) {
        // Deploy = começo limpo: apaga todos os sinais e reinicia a numeração (#1, #2...)
        await pool.query(`TRUNCATE TABLE sinais RESTART IDENTITY`);
        if (RESET_RODADAS) {
          await pool.query(`TRUNCATE TABLE mesa_resultados`);
        }
        resetInicialFeito = true;
        runtime.ultimoSinal = null;
        runtime.ultimaEstrategia = null;
        runtime.motivoSemSinal = null;
        log('RESET', { sinais: 'apagados', numeracao: 'reiniciada', rodadas: RESET_RODADAS ? 'apagadas' : 'mantidas' });
      }
    }

    await syncOnce();
    await verificarOutraInstancia();
    await verificarResetDiario();
  } catch (err) {
    console.error('[CICLO]', err.message);
  } finally {
    cicloEmCurso = false;
  }
}

async function start() {
  if (!DATABASE_URL) throw new Error('DATABASE_URL não configurada');
  if (!BOT_TOKEN || !CHAT_ID) console.warn('⚠️  BOT_TOKEN ou CHAT_ID não configurado');

  await ensureSchema();

  server = app.listen(PORT, '0.0.0.0', () => {
    console.log('==============================================');
    console.log('   CAPUNGA DADOS — UNIFICADO v3.7');
    console.log(`   Instância: ${INSTANCIA}`);
    console.log(`   Porta: ${PORT}`);
    console.log(`   Angola: ${horaAngola()}`);
    console.log(`   Sync a cada: ${CONFIG.intervaloSyncMs / 1000}s`);
    console.log('   Sem limite de reds | Sinais 24h');
    console.log('==============================================');
  });

  await cicloPrincipal();
  syncTimer = setInterval(cicloPrincipal, CONFIG.intervaloSyncMs);

  // Anti-sleep (plano free do Render): auto-ping a cada 10 min
  if (SELF_URL) {
    pingTimer = setInterval(() => {
      axios.get(`${SELF_URL}/health`, { timeout: 10000 }).catch(() => {});
    }, 10 * 60 * 1000);
  }
}

async function shutdown() {
  if (syncTimer) clearInterval(syncTimer);
  if (pingTimer) clearInterval(pingTimer);
  if (server) server.close();
  if (lockClient) await lockClient.end().catch(() => {});
  if (pool) await pool.end();
  process.exit(0);
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

process.on('unhandledRejection', err => console.error('[UNHANDLED]', err?.message || err));

if (require.main === module) {
  start().catch(err => {
    console.error('[FATAL]', err);
    process.exit(1);
  });
} else {
  module.exports = { resolvePending, updateSinal, computeConfidence, telegramSend, runtime };
}
