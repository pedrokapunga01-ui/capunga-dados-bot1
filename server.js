'use strict';

/**
 * CAPUNGA DADOS — UNIFICADO (v3.10)
 *
 * Variáveis obrigatórias: DATABASE_URL, BOT_TOKEN, CHAT_ID, ADMIN_TOKEN
 * Opcional: RENDER_EXTERNAL_URL (Render define sozinho) -> auto-ping anti-sleep
 *
 * v3.10: mensagens simples; confiança = taxa de acerto MEDIDA no histórico (backtest fiel ao bot),
 *        sinais abaixo de MIN_CONFIANCA (80) não são enviados; teto 95.
 * v3.9: fila de mensagens (outbox) em ordem e sem perdas, nº do sinal na entrada,
 *       rodapé com ID da instância (DEBUG_INSTANCIA=false remove), /auditoria.
 * v3.8: núcleo reescrito. Sinais resolvidos a partir da base (não dependem de quem gravou a rodada),
 *       liderança por lease (sem advisory lock), reset só quando o deploy (commit) muda.
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
const SELF_URL = process.env.RENDER_EXTERNAL_URL || '';
const INSTANCIA = crypto.randomBytes(3).toString('hex');
const VERSAO = 'v3.10';
// Rodapé com a instância em cada mensagem (ajuda a ver se há 2 processos a enviar). Desligue com DEBUG_INSTANCIA=false
const DEBUG_INSTANCIA = String(process.env.DEBUG_INSTANCIA || 'true').toLowerCase() !== 'false';
// RESET_RODADAS=true na Render também apaga o histórico de rodadas no deploy (o bot recarrega 50 da API)
const RESET_RODADAS = String(process.env.RESET_RODADAS || '').toLowerCase() === 'true';

const API_BACBO =
  'https://api-cs.casino.org/svc-evolution-game-events/api/bacbo?page=0&size=50&sort=data.settledAt,desc&duration=120&wheelResults=PlayerWon,BankerWon,Tie';

const CONFIG = {
  sinaisAtivos: true,
  streakMin: 3,
  maxGale: 2,
  empateProtegido: true,
  sinalVelhoMs: 10 * 60 * 1000,
  confMin: Number(process.env.MIN_CONFIANCA || 80),
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
  if (cor === 'Banca') return '🔴 VERMELHO';
  if (cor === 'Jogador') return '🔵 AZUL';
  return String(cor || '');
}

function horaCurta() {
  return new Intl.DateTimeFormat('pt-PT', {
    timeZone: 'Africa/Luanda',
    timeStyle: 'medium',
    hour12: false
  }).format(new Date());
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
async function telegramSend(texto, opts = {}) {
  if (!BOT_TOKEN || !CHAT_ID) {
    runtime.ultimoErroTelegram = 'BOT_TOKEN ou CHAT_ID não configurado';
    log('TELEGRAM FALHA', runtime.ultimoErroTelegram);
    return false;
  }

  const maxTent = opts.tentativas || 3;
  for (let tentativa = 1; tentativa <= maxTent; tentativa++) {
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
    if (tentativa < maxTent) await sleep(1500 * tentativa);
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

    await pool.query(`CREATE TABLE IF NOT EXISTS lider (id INTEGER PRIMARY KEY, instancia TEXT, batimento TIMESTAMPTZ)`);
    await pool.query(`CREATE TABLE IF NOT EXISTS meta (chave TEXT PRIMARY KEY, valor TEXT)`);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS outbox (
        id BIGSERIAL PRIMARY KEY,
        criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        sinal_id BIGINT,
        texto TEXT NOT NULL,
        enviado_em TIMESTAMPTZ,
        tentativas INTEGER NOT NULL DEFAULT 0
      )
    `);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_outbox_pend ON outbox (id) WHERE enviado_em IS NULL`);

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
        'User-Agent': 'CapungaDados/3.10',
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
// Confiança = taxa de acerto (fechar GREEN em até G2, empate protegido) que este MESMO tipo de sinal
// realmente teve no histórico guardado. Não é promessa: nenhum padrão garante vitória.
// Maior confiança = esse tipo de sinal acertou mais vezes no histórico (com amostra suficiente).
const PRIOR = 0.875;       // 3 tentativas a ~50% => ~87,5% (ponto de partida quando há poucos dados)
const K_ESTRATEGIA = 20;   // força do prior sobre a taxa da estratégia
const K_BUCKET = 20;       // força da taxa da estratégia sobre a taxa do tipo específico
const CONF_MAX = 95;

// Tipo específico do sinal (ex.: Repetição x4 é diferente de Repetição x3)
function bucketKey(entry) {
  const nome = entry.nome || '';
  switch (entry.estrategia) {
    case 'REPETICAO':
    case 'QUEBRA': {
      const m = nome.match(/x(\d+)/);
      const n = m ? Math.min(Number(m[1]), 6) : 0;
      return `${entry.estrategia}:${n}`;
    }
    case 'DOIS_DOIS':
      return `DOIS_DOIS:${nome.includes('bloco') ? 'bloco' : 'completa'}`;
    default:
      return entry.estrategia;
  }
}

// Repete no passado o que o bot faria: 1 sinal de cada vez, até G2, empate ignorado.
// hist: do mais recente (0) para o mais antigo.
function backtestGeral(hist) {
  const bucket = {};
  const estrategia = {};
  const somar = (obj, key, ganhou) => {
    const o = obj[key] || (obj[key] = { n: 0, h: 0 });
    o.n++;
    if (ganhou) o.h++;
  };

  let k = hist.length - 6;
  while (k >= 3) {
    const entry = detectBestEntry(hist.slice(k, k + 40));
    if (!entry) { k--; continue; }

    let ganhou = false;
    let usadas = 3;
    for (let i = 1; i <= 3; i++) {
      if (hist[k - i].resultado === entry.cor) { ganhou = true; usadas = i; break; }
    }
    somar(bucket, bucketKey(entry), ganhou);
    somar(estrategia, entry.estrategia, ganhou);
    k -= usadas; // o próximo sinal só pode nascer quando este fecha
  }
  return { bucket, estrategia };
}

function computeConfidence(entry, hist) {
  const bt = backtestGeral(hist || []);
  const kb = bucketKey(entry);
  const b = bt.bucket[kb] || { n: 0, h: 0 };
  const e = bt.estrategia[entry.estrategia] || { n: 0, h: 0 };

  // Suavização: poucos dados => aproxima-se da média; muitos dados => vale a taxa medida
  const pEstrategia = (e.h + K_ESTRATEGIA * PRIOR) / (e.n + K_ESTRATEGIA);
  const pTipo = (b.h + K_BUCKET * pEstrategia) / (b.n + K_BUCKET);

  const confianca = Math.min(CONF_MAX, Math.round(pTipo * 100));

  return {
    confianca,
    detalhe: {
      tipo: kb,
      amostraTipo: b.n,
      taxaTipo: b.n ? Number((b.h / b.n).toFixed(3)) : null,
      amostraEstrategia: e.n,
      taxaEstrategia: e.n ? Number((e.h / e.n).toFixed(3)) : null,
      estimativa: Number(pTipo.toFixed(3))
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
  const acertos = (p.green || 0) + (p.g1 || 0) + (p.g2 || 0);
  return `\n📊 Hoje: ✅ ${acertos} · ❌ ${p.loss || 0} · 🟡 ${p.empates || 0} · ${p.percent}%`;
}

async function getPendingSignal() {
  const r = await pool.query(`SELECT * FROM sinais WHERE estado = 'PENDENTE' ORDER BY criado_em ASC LIMIT 1`);
  return r.rows[0] || null;
}

function msgNovoSinal(entry, signal) {
  return [
    `🎲 SINAL #${signal.id}`,
    `Entrar: ${corDisplay(entry.cor)}`,
    `📊 ${entry.nome} · confiança ${entry.confianca}%`,
    `🛡️ Empate protegido · até G${CONFIG.maxGale}`,
    `🕒 ${horaCurta()}`
  ].join('\n');
}

function msgGale(signal, nextGale) {
  return [
    `🐓 SINAL #${signal.id} · GALE ${nextGale}`,
    `Mantém: ${corDisplay(signal.cor)}`
  ].join('\n');
}

function msgEmpate(signal) {
  return [
    `🟡 SINAL #${signal.id} · EMPATE`,
    `Segue pendente: ${corDisplay(signal.cor)}`
  ].join('\n');
}

function msgResultadoGreen(signal, state) {
  const quando = state === 'GREEN' ? 'sem gale' : state === 'G1' ? 'no Gale 1' : 'no Gale 2';
  return [
    `✅ SINAL #${signal.id} · GREEN`,
    `${corDisplay(signal.cor)} · ${quando}`
  ].join('\n');
}

function msgResultadoLoss(signal) {
  return [
    `❌ SINAL #${signal.id} · LOSS`,
    `${corDisplay(signal.cor)} · Gale ${signal.gale || 0}/${CONFIG.maxGale}`
  ].join('\n');
}

// ====================== FILA DE MENSAGENS (OUTBOX) ======================
// Toda mensagem é gravada na base ANTES de ir ao Telegram. Se o Telegram falhar, repete no ciclo seguinte.
// A ordem das mensagens no Telegram é sempre a ordem em que os eventos aconteceram.
let enviandoOutbox = false;

async function enfileirar(texto, sinalId = null) {
  const rodape = DEBUG_INSTANCIA ? `\n⚙️ ${VERSAO} · ${INSTANCIA}` : '';
  await pool.query(`INSERT INTO outbox (sinal_id, texto) VALUES ($1, $2)`, [sinalId, texto + rodape]);
}

async function enviarOutbox() {
  if (enviandoOutbox) return;
  enviandoOutbox = true;
  try {
    const r = await pool.query(`SELECT id, texto, criado_em FROM outbox WHERE enviado_em IS NULL ORDER BY id ASC LIMIT 6`);
    for (const m of r.rows) {
      const claim = await pool.query(`UPDATE outbox SET enviado_em = NOW() WHERE id = $1 AND enviado_em IS NULL RETURNING id`, [m.id]);
      if (!claim.rows.length) continue; // outro processo já pegou esta mensagem

      if (Date.now() - new Date(m.criado_em).getTime() > 20 * 60 * 1000) {
        log('OUTBOX', { id: m.id, msg: 'descartada: com mais de 20 min' });
        continue;
      }

      const ok = await telegramSend(m.texto, { tentativas: 2 });
      if (!ok) {
        await pool.query(`UPDATE outbox SET enviado_em = NULL, tentativas = tentativas + 1 WHERE id = $1`, [m.id]);
        log('OUTBOX', { id: m.id, msg: 'falhou, repete no próximo ciclo', erro: runtime.ultimoErroTelegram });
        return;
      }
      await sleep(1100); // Telegram aceita ~1 mensagem por segundo por chat
    }
  } finally {
    enviandoOutbox = false;
  }
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

  // Um sinal de cada vez. Nunca expira: fecha sempre com GREEN/G1/G2/LOSS.
  const pending = await getPendingSignal();
  if (pending) {
    runtime.motivoSemSinal = `em_andamento_#${pending.id}`;
    return;
  }

  const hist = await getRecentOutcomes(1500);
  const rows = hist.slice(0, 30);
  const entry = detectBestEntry(rows);

  if (!entry) {
    runtime.motivoSemSinal = 'sem_setup';
    return;
  }

  const conf = computeConfidence(entry, hist);
  entry.confianca = conf.confianca;

  // Filtro de qualidade: tipos de sinal que historicamente acertam menos que o mínimo não são enviados
  if (conf.confianca < CONFIG.confMin) {
    runtime.motivoSemSinal = `confianca_baixa_${conf.confianca}`;
    log('FILTRADO', { estrategia: entry.nome, confianca: conf.confianca, minimo: CONFIG.confMin, calculo: conf.detalhe });
    return;
  }

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

  await enfileirar(msgNovoSinal(entry, signal), signal.id);
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

// Aplica UMA rodada ao sinal. Devolve o sinal atualizado (ou null se outro processo já o alterou).
async function aplicarRodada(signal, round) {
  let upd = null;
  let texto = null;
  let fechou = false;

  if (round.resultado === 'Empate' && CONFIG.empateProtegido) {
    upd = await updateSinal(signal, round, { empates: Number(signal.empates || 0) + 1 });
    if (upd) texto = msgEmpate(signal);
  } else if (round.resultado === signal.cor) {
    const galeNum = Number(signal.gale || 0);
    const state = galeNum === 0 ? 'GREEN' : galeNum === 1 ? 'G1' : 'G2';
    upd = await updateSinal(signal, round, { estado: state, resultado_final: round.resultado });
    if (upd) {
      fechou = true;
      texto = msgResultadoGreen(signal, state);
    }
  } else {
    const nextGale = Number(signal.gale || 0) + 1;
    if (nextGale <= CONFIG.maxGale) {
      upd = await updateSinal(signal, round, { gale: nextGale });
      if (upd) texto = msgGale(signal, nextGale);
    } else {
      upd = await updateSinal(signal, round, { estado: 'LOSS', resultado_final: round.resultado });
      if (upd) {
        fechou = true;
        texto = msgResultadoLoss(signal);
      }
    }
  }

  if (!upd) return null;

  // O placar nunca pode impedir o resultado de ser anunciado
  if (fechou) {
    try {
      texto += formatPlacar(await getPlacarDia());
    } catch (e) {
      log('PLACAR ERRO', e.message);
    }
  }

  await enfileirar(texto, signal.id);
  log('APLICADO', {
    sinal: signal.id,
    rodada: round.source_key,
    resultado: round.resultado,
    estado: upd.estado,
    gale: upd.gale,
    empates: upd.empates
  });
  return upd;
}

async function getLatestRound() {
  const r = await pool.query(
    `SELECT source_key, source_id, resultado, settled_at AS fim
     FROM mesa_resultados
     ORDER BY settled_at DESC NULLS LAST, observado_em DESC
     LIMIT 1`
  );
  return r.rows[0] || null;
}

async function getRoundsAfter(fim, limit = 30) {
  const r = await pool.query(
    `SELECT source_key, source_id, resultado, settled_at AS fim
     FROM mesa_resultados
     WHERE settled_at > $1
     ORDER BY settled_at ASC
     LIMIT $2`,
    [fim, limit]
  );
  return r.rows;
}

let limpezaInicialFeita = false;
let ultimaRodadaAvaliada = null;

// Resolve o sinal pendente usando as rodadas que estão na BASE, em ordem.
// Não depende de quem gravou a rodada nem de quando ela chegou.
async function processarPendente() {
  let signal = await getPendingSignal();
  if (!signal) {
    limpezaInicialFeita = true;
    return;
  }

  const base = new Date(signal.ultima_fim || signal.criado_em);

  // Só no primeiro ciclo após um reinício: sinal esquecido de há muito tempo é descartado
  if (!limpezaInicialFeita) {
    limpezaInicialFeita = true;
    if (Date.now() - base.getTime() > CONFIG.sinalVelhoMs) {
      await pool.query(`UPDATE sinais SET estado = 'CANCELADO' WHERE id = $1 AND estado = 'PENDENTE'`, [signal.id]);
      log('SINAL VELHO', { id: signal.id, msg: 'Pendente de antes do reinício, descartado sem contar no placar' });
      return;
    }
  }

  const rounds = await getRoundsAfter(base);
  for (const round of rounds) {
    const upd = await aplicarRodada(signal, round);
    if (!upd) return;                    // outro processo alterou: tenta no próximo ciclo
    if (upd.estado !== 'PENDENTE') return; // fechou
    signal = upd;
  }
}

// Avalia a rodada mais recente UMA vez (e só depois do baseline)
async function avaliarNovoSinal() {
  const latest = await getLatestRound();
  if (!latest) return;
  if (ultimaRodadaAvaliada === latest.source_key) return;
  ultimaRodadaAvaliada = latest.source_key;
  await maybeCreateSignal(latest);
}

// ====================== LIDERANÇA / OUTRA INSTÂNCIA ======================
let lideradoDesde = null;
let resetVerificado = false;
let ultimoAvisoStandby = 0;
let ultimoAvisoEstranha = 0;

// Lease: quem renova o batimento é o líder. Se parar de renovar por 60s, outro assume.
async function renovarLideranca() {
  const r = await pool.query(
    `INSERT INTO lider (id, instancia, batimento) VALUES (1, $1, NOW())
     ON CONFLICT (id) DO UPDATE SET instancia = EXCLUDED.instancia, batimento = NOW()
     WHERE lider.instancia = EXCLUDED.instancia OR lider.batimento < NOW() - INTERVAL '60 seconds'
     RETURNING instancia`,
    [INSTANCIA]
  );
  return r.rows.length > 0;
}

async function largarLideranca() {
  await pool.query(`UPDATE lider SET batimento = to_timestamp(0) WHERE id = 1 AND instancia = $1`, [INSTANCIA]);
}

async function limparSinais() {
  await pool.query(`TRUNCATE TABLE sinais RESTART IDENTITY`);
  await pool.query(`DELETE FROM outbox WHERE enviado_em IS NULL`);
  runtime.ultimoSinal = null;
  runtime.ultimaEstrategia = null;
  runtime.motivoSemSinal = null;
}

// Deploy novo (commit diferente) => começo limpo. Reinício do mesmo deploy => mantém tudo.
async function resetarSeNovoDeploy() {
  const deployAtual = process.env.RENDER_GIT_COMMIT || INSTANCIA;
  const r = await pool.query(`SELECT valor FROM meta WHERE chave = 'deploy'`);
  const anterior = r.rows[0]?.valor || null;

  if (anterior !== deployAtual) {
    await limparSinais();
    if (RESET_RODADAS) await pool.query(`TRUNCATE TABLE mesa_resultados`);
    await pool.query(
      `INSERT INTO meta (chave, valor) VALUES ('deploy', $1)
       ON CONFLICT (chave) DO UPDATE SET valor = EXCLUDED.valor`,
      [deployAtual]
    );
    log('RESET', { motivo: 'novo deploy', sinais: 'apagados', numeracao: 'reiniciada', rodadas: RESET_RODADAS ? 'apagadas' : 'mantidas' });
  } else {
    log('RETOMADA', 'Mesmo deploy: sinais e placar mantidos');
  }
}

// Sinais criados DEPOIS de esta instância assumir, mas por outro processo => há uma instância estranha
async function verificarOutraInstancia() {
  const r = await pool.query(
    `SELECT id, instancia, criado_em FROM sinais
     WHERE criado_em >= $2 AND instancia IS DISTINCT FROM $1
     ORDER BY id DESC LIMIT 3`,
    [INSTANCIA, lideradoDesde || new Date()]
  );
  if (r.rows.length) {
    runtime.alertaOutraInstancia = {
      msg: 'Há sinais criados por OUTRO processo usando a mesma base de dados. Suspenda o outro serviço.',
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
    await pool.query(`DELETE FROM outbox WHERE enviado_em IS NOT NULL AND enviado_em < NOW() - INTERVAL '2 days'`);
    ultimoResetDia = data;
    log('RESET', `Histórico limpo às 00h Angola (${data})`);
    await telegramSend(`🧹 Novo dia · placar zerado (${data})`);
  }
}

// ====================== SYNC ======================
// Só busca na API e grava as rodadas. Sinais são tratados depois, a partir da base.
async function syncOnce() {
  if (!pool) throw new Error('DATABASE_URL não configurada');
  if (runtime.sincronizacaoEmCurso) return { sucesso: false, ignorada: true };

  runtime.sincronizacaoEmCurso = true;

  try {
    await ensureSchema();
    const rounds = await fetchRounds(); // ordem cronológica (antiga -> recente)

    let inserted = 0;
    let duplicates = 0;

    for (const round of rounds) {
      const saved = await insertRound(round);
      if (saved) inserted++;
      else duplicates++;
    }

    runtime.ultimaSincronizacao = new Date().toISOString();
    runtime.novosUltimoCiclo = inserted;
    runtime.duplicadosUltimoCiclo = duplicates;
    runtime.erroSincronizacao = null;

    if (inserted > 0) {
      runtime.ultimaRodadaNova = new Date().toISOString();
      runtime.ciclosSemNovos = 0;
      log('NOVAS RODADAS', { quantidade: inserted, motivo: runtime.motivoSemSinal });
    } else {
      runtime.ciclosSemNovos = (runtime.ciclosSemNovos || 0) + 1;
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
    nome: 'Capunga Dados — Unificado v3.10',
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

app.get('/auditoria', async (req, res) => {
  try {
    await ensureSchema();
    const sinais = (await pool.query(
      `SELECT id, criado_em, cor, estado, gale, empates, estrategia, confianca, instancia, ultima_fim, passo
       FROM sinais ORDER BY id DESC LIMIT 25`
    )).rows;
    const mensagens = (await pool.query(
      `SELECT id, criado_em, sinal_id, enviado_em, tentativas, LEFT(REPLACE(texto, E'\n', ' | '), 110) AS inicio
       FROM outbox ORDER BY id DESC LIMIT 25`
    )).rows;
    const lider = (await pool.query(`SELECT instancia, batimento FROM lider WHERE id = 1`)).rows[0] || null;
    res.json({ sucesso: true, versao: VERSAO, estaInstancia: INSTANCIA, lider, alertaOutraInstancia: runtime.alertaOutraInstancia, sinais, mensagens });
  } catch (e) {
    res.status(500).json({ sucesso: false, erro: e.message });
  }
});

app.post('/admin/sync', requireAdmin, async (req, res) => {
  await cicloPrincipal();
  res.json({ sucesso: true, lider: runtime.lider, instancia: INSTANCIA, motivoSemSinal: runtime.motivoSemSinal });
});

app.post('/admin/reset', requireAdmin, async (req, res) => {
  try {
    await limparSinais();
    log('RESET', { motivo: 'manual (/admin/reset)' });
    res.json({ sucesso: true });
  } catch (e) {
    res.status(500).json({ sucesso: false, erro: e.message });
  }
});

app.post('/telegram/teste', requireAdmin, async (req, res) => {
  const ok = await telegramSend(`✅ Teste de conexão OK · ${horaCurta()}`);
  res.status(ok ? 200 : 502).json({ sucesso: ok, erro: runtime.ultimoErroTelegram });
});

// ====================== START ======================
let cicloEmCurso = false;

async function fazerBaseline() {
  const latest = await getLatestRound();
  if (!latest) return;
  ultimaRodadaAvaliada = latest.source_key;
  runtime.baselineFeito = true;
  log('BASELINE', { msg: 'Histórico carregado. Sinais ativos a partir da próxima rodada.' });
}

async function cicloPrincipal() {
  if (cicloEmCurso) return;
  cicloEmCurso = true;
  try {
    await ensureSchema();

    const lider = await renovarLideranca();
    if (!lider) {
      runtime.lider = false;
      if (Date.now() - ultimoAvisoStandby > 60000) {
        ultimoAvisoStandby = Date.now();
        log('STANDBY', `Instância ${INSTANCIA} em espera: outra instância é a líder`);
      }
      return;
    }

    if (!runtime.lider) {
      runtime.lider = true;
      log('LIDER', `Instância ${INSTANCIA} assumiu a liderança`);
      if (!resetVerificado) {
        await resetarSeNovoDeploy();
        resetVerificado = true;
      }
      lideradoDesde = new Date();
    }

    await syncOnce();
    await processarPendente();

    if (runtime.baselineFeito) await avaliarNovoSinal();
    else await fazerBaseline();

    await enviarOutbox();
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
    console.log('   CAPUNGA DADOS — UNIFICADO v3.10');
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
  try {
    if (pool && runtime.lider) await largarLideranca();
  } catch (_) {}
  if (server) server.close();
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
  module.exports = { aplicarRodada, updateSinal, processarPendente, avaliarNovoSinal, enviarOutbox, enfileirar, computeConfidence, telegramSend, runtime };
}
