'use strict';

/**
 * CAPUNGA — PRESSÃO DE CICLO
 * Robô de sinais Bac Bo com disparo em minutos múltiplos de 4
 *
 * Variáveis obrigatórias:
 *   DATABASE_URL, BOT_TOKEN, CHAT_ID, ADMIN_TOKEN
 *
 * Opcionais:
 *   API_BACBO, INTERVALO_SYNC_MS, JANELA_TOLERANCIA_SEG, MAX_GALE
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
const API_BACBO = process.env.API_BACBO ||
  'https://api-cs.casino.org/svc-evolution-game-events/api/bacbo';

const INTERVALO_SYNC_MS = Math.max(5000, Math.min(Number(process.env.INTERVALO_SYNC_MS) || 7000, 20000));
const JANELA_TOLERANCIA_SEG = Number(process.env.JANELA_TOLERANCIA_SEG) || 90;
const MAX_GALE = Math.min(2, Math.max(0, Number(process.env.MAX_GALE) || 2));

const pool = DATABASE_URL
  ? new Pool({
      connectionString: DATABASE_URL,
      ssl: /localhost|127\.0\.0\.1/i.test(DATABASE_URL) ? false : { rejectUnauthorized: false },
      max: 8,
      idleTimeoutMillis: 20000,
      connectionTimeoutMillis: 12000
    })
  : null;

const runtime = {
  iniciadoEm: new Date().toISOString(),
  sincronizacaoEmCurso: false,
  ultimaSincronizacao: null,
  ultimoIdNovo: null,
  novosUltimoCiclo: 0,
  ultimoSinal: null,
  motivoSemSinal: null,
  ultimoErro: null,
  ultimaDuracaoMs: null,
  ultimaJanela: null
};

let schemaPromise = null;
let server = null;
let timer = null;
let shuttingDown = false;

function log(tag, data) {
  const payload = typeof data === 'string' ? data : JSON.stringify(data);
  console.log(`[\( {new Date().toISOString()}] [ \){tag}] ${payload}`);
}

function horaAngola(date = new Date()) {
  return new Intl.DateTimeFormat('pt-PT', {
    timeZone: 'Africa/Luanda',
    dateStyle: 'short',
    timeStyle: 'medium',
    hour12: false
  }).format(date);
}

function agoraAngola() {
  return new Date(new Date().toLocaleString('en-US', { timeZone: 'Africa/Luanda' }));
}

/* ======================== UTILITÁRIOS DE TEMPO ======================== */

function getMinutoAlvo(date = agoraAngola()) {
  const min = date.getMinutes();
  return Math.floor(min / 4) * 4;
}

function estaNaJanelaDeSinal(date = agoraAngola()) {
  const min = date.getMinutes();
  const seg = date.getSeconds();
  const alvo = getMinutoAlvo(date);

  if (min === alvo) return true;
  if (min === alvo + 1 && seg <= JANELA_TOLERANCIA_SEG) return true;
  return false;
}

function chaveJanela(date = agoraAngola()) {
  const d = date;
  const ano = d.getFullYear();
  const mes = String(d.getMonth() + 1).padStart(2, '0');
  const dia = String(d.getDate()).padStart(2, '0');
  const hora = String(d.getHours()).padStart(2, '0');
  const minuto = String(getMinutoAlvo(d)).padStart(2, '0');
  return `\( {ano}- \){mes}-${dia} \( {hora}: \){minuto}`;
}

/* ======================== NORMALIZAÇÃO ======================== */

function normalizeOutcome(value) {
  if (value && typeof value === 'object') {
    value = value.outcome ?? value.resultado ?? value.winner ?? value.name;
  }
  const s = String(value ?? '')
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .trim().toLowerCase().replace(/[\s_-]+/g, '');

  if (['player', 'jogador', 'playerwin', 'playerwins', 'playerwon', 'blue'].includes(s)) return 'Jogador';
  if (['banker', 'banca', 'bankerwin', 'bankerwins', 'bankerwon', 'red'].includes(s)) return 'Banca';
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
  const r = (d.result && typeof d.result === 'object') ? d.result
    : (item?.result && typeof item.result === 'object' ? item.result : {});

  const status = String(d.status ?? d.estado ?? item?.status ?? item?.estado ?? '').toLowerCase();
  const fim = validDate(d.settledAt ?? d.endTime ?? d.endedAt ?? d.finishedAt ?? d.fim ?? item?.settledAt ?? item?.fim);
  const inicio = validDate(d.startedAt ?? d.startTime ?? d.started_at ?? d.inicio ?? item?.startedAt ?? item?.inicio);

  const rawOutcome = r.outcome ?? r.resultado ?? r.winner ?? d.outcome ?? d.resultado ?? d.winner ??
    item?.outcome ?? item?.resultado ?? item?.winner ??
    (typeof d.result === 'string' ? d.result : null) ??
    (typeof item?.result === 'string' ? item.result : null);

  const resultado = normalizeOutcome(rawOutcome);
  const resolvida = ['resolved', 'settled', 'finished', 'complete', 'completed', 'resolvido', 'concluido', 'finalizado']
    .some(w => status.includes(w)) || Boolean(fim);

  const id = String(d.id ?? d.roundId ?? d.round_id ?? d.eventId ?? item?.id ?? item?.roundId ?? item?.round_id ?? item?.eventId ?? '').trim();
  if (!id || !resultado || !resolvida || !fim) return null;

  const pj = Number(r.playerDice?.score ?? d.playerDice?.score ?? d.playerScore ?? d.player_score ?? d.pontos_jogador);
  const pb = Number(r.bankerDice?.score ?? d.bankerDice?.score ?? d.bankerScore ?? d.banker_score ?? d.pontos_banca);

  return {
    id,
    resultado,
    inicio: inicio || fim,
    fim,
    pontos_jogador: Number.isFinite(pj) ? pj : null,
    pontos_banca: Number.isFinite(pb) ? pb : null
  };
}

/* ======================== BANCO ======================== */

async function ensureSchema() {
  if (!pool) throw new Error('DATABASE_URL não configurada');
  if (schemaPromise) return schemaPromise;

  schemaPromise = (async () => {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS bacbo_history (
        id              TEXT PRIMARY KEY,
        resultado       TEXT NOT NULL,
        inicio          TIMESTAMPTZ,
        fim             TIMESTAMPTZ NOT NULL,
        pontos_jogador  INTEGER,
        pontos_banca    INTEGER,
        guardado_em     TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_history_fim ON bacbo_history (fim DESC)`);

    await pool.query(`
      CREATE TABLE IF NOT EXISTS signal_entries (
        id              BIGSERIAL PRIMARY KEY,
        janela          TEXT NOT NULL,
        criado_em       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        entrada         TEXT NOT NULL,
        estado          TEXT NOT NULL DEFAULT 'ABERTO',
        gale            INTEGER NOT NULL DEFAULT 0,
        estrategia      TEXT,
        confianca       INTEGER,
        rodada_origem   TEXT,
        resultado_final TEXT,
        resolvido_em    TIMESTAMPTZ
      )
    `);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_signal_estado ON signal_entries (estado)`);
    await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS idx_signal_janela ON signal_entries (janela)`);
    await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS idx_signal_um_aberto ON signal_entries ((estado)) WHERE estado = 'ABERTO'`);

    log('DB', { tabelas: 'prontas' });
  })();

  try {
    await schemaPromise;
  } catch (e) {
    schemaPromise = null;
    throw e;
  }
}

/* ======================== TELEGRAM ======================== */

async function telegramSend(texto) {
  if (!BOT_TOKEN || !CHAT_ID) return false;
  try {
    const res = await axios.post(
      `https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`,
      { chat_id: CHAT_ID, text: texto, disable_web_page_preview: true },
      { timeout: 12000 }
    );
    return res.data?.ok === true;
  } catch (e) {
    log('TELEGRAM', e.message);
    return false;
  }
}

function corEmoji(cor) {
  if (cor === 'Banca') return '🔴';
  if (cor === 'Jogador') return '🔵';
  return '🟡';
}

function msgNovoSinal(entry) {
  const emoji = corEmoji(entry.entrada);
  const entrada = String(entry.entrada || '').toUpperCase();
  const confianca = entry.confianca || 0;
  const janela = entry.janela || '—';

  return [
    '🎯  SINAL • PRESSÃO DE CICLO',
    '━━━━━━━━━━━━━━━━',
    `${emoji}  Entrada: ${entrada}`,
    `📊  Confiança: ${confianca}%`,
    `🔁  Gale máximo: ${MAX_GALE}`,
    `🪟  Janela: ${janela}`,
    '━━━━━━━━━━━━━━━━',
    `⏰  ${horaAngola()}`
  ].join('\n');
}

function msgGreen(entry, estado) {
  const emoji = corEmoji(entry.entrada);
  const titulo = estado === 'GREEN' ? '✅ GREEN DIRETO' :
                 estado === 'G1' ? '⚡ GREEN NO GALE 1' : '🔥 GREEN NO GALE 2';

  return [
    titulo,
    '━━━━━━━━━━━━━━━━',
    `${emoji}  ${entry.entrada}`,
    `🪟  ${entry.janela}`,
    `⏰  ${horaAngola()}`
  ].join('\n');
}

function msgGale(entry, gale) {
  const emoji = corEmoji(entry.entrada);

  return [
    `🐓  GALE ${gale}`,
    '━━━━━━━━━━━━━━━━',
    `${emoji}  Manter: ${entry.entrada}`,
    `🪟  ${entry.janela}`,
    `⏰  ${horaAngola()}`
  ].join('\n');
}

function msgRed(entry) {
  const emoji = corEmoji(entry.entrada);

  return [
    '❌  RED',
    '━━━━━━━━━━━━━━━━',
    `${emoji}  ${entry.entrada}`,
    `🪟  ${entry.janela}`,
    `⏰  ${horaAngola()}`
  ].join('\n');
}

/* ======================== ESTRATÉGIA: PRESSÃO DE CICLO ======================== */

function calcularPressao(rows) {
  if (!rows || rows.length < 6) return null;

  const limpos = rows.filter(r => r.resultado === 'Jogador' || r.resultado === 'Banca');
  if (limpos.length < 6) return null;

  let pesoB = 0;
  let pesoJ = 0;
  const pesos = [1.8, 1.6, 1.4, 1.2, 1.0, 0.9, 0.8, 0.7];

  limpos.slice(0, 8).forEach((r, i) => {
    const w = pesos[i] || 0.6;
    if (r.resultado === 'Banca') pesoB += w;
    else pesoJ += w;
  });

  const total = pesoB + pesoJ;
  const pctB = pesoB / total;
  const pctJ = pesoJ / total;

  const seq = limpos.slice(0, 6).map(r => r.resultado[0]);
  const ciclo2 = seq[0] !== seq[1] && seq[0] === seq[2] && seq[1] === seq[3];
  const ciclo3 = seq[0] === seq[3] && seq[1] === seq[4] && seq[2] === seq[5] && seq[0] !== seq[1];

  let somaDiff = 0;
  let contDiff = 0;
  limpos.slice(0, 4).forEach(r => {
    if (r.pontos_jogador != null && r.pontos_banca != null) {
      somaDiff += Math.abs(r.pontos_jogador - r.pontos_banca);
      contDiff++;
    }
  });
  const mediaDiff = contDiff ? somaDiff / contDiff : 0;

  let entrada = null;
  let confianca = 0;
  let motivo = '';

  if (pctB >= 0.62) {
    entrada = 'Banca';
    confianca = Math.round(58 + (pctB - 0.5) * 80);
    motivo = 'Pressão Banca';
    if (ciclo2 || ciclo3) confianca += 8;
  } else if (pctJ >= 0.62) {
    entrada = 'Jogador';
    confianca = Math.round(58 + (pctJ - 0.5) * 80);
    motivo = 'Pressão Jogador';
    if (ciclo2 || ciclo3) confianca += 8;
  }

  if (!entrada && (ciclo2 || ciclo3)) {
    entrada = limpos[0].resultado === limpos[2].resultado ? limpos[0].resultado : limpos[1].resultado;
    confianca = ciclo3 ? 71 : 66;
    motivo = ciclo3 ? 'Ciclo de 3' : 'Ciclo de 2';
  }

  if (entrada && mediaDiff >= 3.5) confianca += 5;

  confianca = Math.min(92, Math.max(55, confianca));

  if (!entrada || confianca < 62) return null;

  return {
    entrada,
    confianca,
    estrategia: 'PRESSAO_CICLO',
    motivo
  };
}

/* ======================== LÓGICA DE SINAIS ======================== */

async function getHistorico(limit = 20) {
  const res = await pool.query(`
    SELECT id, resultado, fim, pontos_jogador, pontos_banca
    FROM bacbo_history
    WHERE resultado IN ('Jogador', 'Banca')
    ORDER BY fim DESC
    LIMIT $1
  `, [limit]);
  return res.rows;
}

async function getAberto() {
  const res = await pool.query(`SELECT * FROM signal_entries WHERE estado = 'ABERTO' LIMIT 1`);
  return res.rows[0] || null;
}

async function jaExisteJanela(janela) {
  const res = await pool.query(`SELECT 1 FROM signal_entries WHERE janela = $1 LIMIT 1`, [janela]);
  return res.rows.length > 0;
}

async function criarSinal(analise, rodadaId, janela) {
  const res = await pool.query(`
    INSERT INTO signal_entries (janela, entrada, estado, gale, estrategia, confianca, rodada_origem)
    VALUES ($1, $2, 'ABERTO', 0, $3, $4, $5)
    RETURNING *
  `, [janela, analise.entrada, analise.estrategia, analise.confianca, rodadaId]);

  const entry = res.rows[0];
  runtime.ultimoSinal = entry;
  runtime.motivoSemSinal = null;

  await telegramSend(msgNovoSinal({
    entrada: entry.entrada,
    confianca: entry.confianca,
    janela: entry.janela
  }));

  log('SINAL CRIADO', { id: entry.id, entrada: entry.entrada, janela, confianca: entry.confianca });
  return entry;
}

async function resolverSinal(rodada) {
  const aberto = await getAberto();
  if (!aberto) return;

  if (String(aberto.rodada_origem) === String(rodada.id)) return;

  if (rodada.resultado === 'Empate') return;

  if (rodada.resultado === aberto.entrada) {
    const estado = aberto.gale === 0 ? 'GREEN' : aberto.gale === 1 ? 'G1' : 'G2';
    await pool.query(`
      UPDATE signal_entries
      SET estado = $1, resultado_final = $2, resolvido_em = NOW()
      WHERE id = $3
    `, [estado, rodada.resultado, aberto.id]);

    await telegramSend(msgGreen(aberto, estado));
    log('GREEN', { id: aberto.id, estado });
    return;
  }

  const proximoGale = aberto.gale + 1;
  if (proximoGale <= MAX_GALE) {
    await pool.query(`UPDATE signal_entries SET gale = $1 WHERE id = $2`, [proximoGale, aberto.id]);
    await telegramSend(msgGale(aberto, proximoGale));
    log('GALE', { id: aberto.id, gale: proximoGale });
  } else {
    await pool.query(`
      UPDATE signal_entries
      SET estado = 'RED', resultado_final = $1, resolvido_em = NOW()
      WHERE id = $2
    `, [rodada.resultado, aberto.id]);
    await telegramSend(msgRed(aberto));
    log('RED', { id: aberto.id });
  }
}

async function tentarGerarSinal(rodada) {
  const agora = agoraAngola();
  if (!estaNaJanelaDeSinal(agora)) {
    runtime.motivoSemSinal = 'fora_da_janela';
    return;
  }

  const janela = chaveJanela(agora);
  runtime.ultimaJanela = janela;

  if (await jaExisteJanela(janela)) {
    runtime.motivoSemSinal = 'janela_ja_usada';
    return;
  }

  const aberto = await getAberto();
  if (aberto) {
    runtime.motivoSemSinal = 'ja_existe_aberto';
    return;
  }

  const historico = await getHistorico(16);
  const analise = calcularPressao(historico);

  if (!analise) {
    runtime.motivoSemSinal = 'pressao_insuficiente';
    return;
  }

  await criarSinal(analise, rodada.id, janela);
}

/* ======================== SYNC ======================== */

async function fetchRounds() {
  const res = await axios.get(API_BACBO, {
    timeout: 16000,
    headers: { Accept: 'application/json', 'User-Agent': 'CapungaPressao/1.0' }
  });

  const items = extractItems(res.data);
  const unique = new Map();
  for (const item of items) {
    const r = normalizeRound(item);
    if (r) unique.set(r.id, r);
  }
  return [...unique.values()].sort((a, b) => new Date(a.fim) - new Date(b.fim));
}

async function syncOnce() {
  if (!pool || runtime.sincronizacaoEmCurso || shuttingDown) return;

  runtime.sincronizacaoEmCurso = true;
  const start = Date.now();

  try {
    await ensureSchema();
    const rounds = await fetchRounds();

    let novos = 0;
    for (const round of rounds) {
      const inserted = await pool.query(`
        INSERT INTO bacbo_history (id, resultado, inicio, fim, pontos_jogador, pontos_banca)
        VALUES ($1,$2,$3,$4,$5,$6)
        ON CONFLICT (id) DO NOTHING
        RETURNING id
      `, [round.id, round.resultado, round.inicio, round.fim, round.pontos_jogador, round.pontos_banca]);

      if (inserted.rows.length === 0) continue;

      novos++;
      runtime.ultimoIdNovo = round.id;

      await resolverSinal(round);
      await tentarGerarSinal(round);
    }

    runtime.novosUltimoCiclo = novos;
    runtime.ultimaSincronizacao = new Date().toISOString();
    runtime.ultimaDuracaoMs = Date.now() - start;
    runtime.ultimoErro = null;

    if (novos > 0) log('SYNC', { novos, janela: runtime.ultimaJanela, motivo: runtime.motivoSemSinal });
  } catch (e) {
    runtime.ultimoErro = e.message;
    log('ERRO', e.message);
  } finally {
    runtime.sincronizacaoEmCurso = false;
  }
}

/* ======================== ROTAS ======================== */

function requireAdmin(req, res, next) {
  if (!ADMIN_TOKEN || req.get('x-admin-token') !== ADMIN_TOKEN) {
    return res.status(401).json({ sucesso: false, erro: 'Não autorizado' });
  }
  next();
}

app.get('/', (_req, res) => {
  res.json({
    nome: 'Capunga — Pressão de Ciclo',
    modo: 'Sinais em minutos 0,4,8,12...',
    estrategia: 'PRESSAO_CICLO',
    maxGale: MAX_GALE,
    horaAngola: horaAngola()
  });
});

app.get('/health', (_req, res) => {
  res.json({
    online: true,
    horaAngola: horaAngola(),
    ultimaSincronizacao: runtime.ultimaSincronizacao,
    ultimoIdNovo: runtime.ultimoIdNovo,
    novosUltimoCiclo: runtime.novosUltimoCiclo,
    ultimaJanela: runtime.ultimaJanela,
    motivoSemSinal: runtime.motivoSemSinal,
    ultimoSinal: runtime.ultimoSinal ? {
      id: runtime.ultimoSinal.id,
      entrada: runtime.ultimoSinal.entrada,
      janela: runtime.ultimoSinal.janela
    } : null,
    erro: runtime.ultimoErro
  });
});

app.get('/sinais', async (req, res) => {
  try {
    await ensureSchema();
    const limit = Math.min(Number(req.query.limit) || 30, 100);
    const r = await pool.query(`SELECT * FROM signal_entries ORDER BY criado_em DESC LIMIT $1`, [limit]);
    res.json({ sucesso: true, total: r.rows.length, sinais: r.rows });
  } catch (e) {
    res.status(500).json({ sucesso: false, erro: e.message });
  }
});

app.get('/historico', async (req, res) => {
  try {
    await ensureSchema();
    const limit = Math.min(Number(req.query.limit) || 50, 200);
    const r = await pool.query(`SELECT * FROM bacbo_history ORDER BY fim DESC LIMIT $1`, [limit]);
    res.json({ sucesso: true, total: r.rows.length, historico: r.rows });
  } catch (e) {
    res.status(500).json({ sucesso: false, erro: e.message });
  }
});

app.post('/telegram/teste', requireAdmin, async (_req, res) => {
  const ok = await telegramSend(`✅ Teste Pressão de Ciclo\n${horaAngola()}`);
  res.json({ sucesso: ok });
});

app.post('/sync', requireAdmin, async (_req, res) => {
  await syncOnce();
  res.json({ sucesso: true, runtime });
});

/* ======================== START ======================== */

async function start() {
  if (!DATABASE_URL) throw new Error('DATABASE_URL obrigatória');

  await ensureSchema();

  server = app.listen(PORT, '0.0.0.0', () => {
    log('START', {
      nome: 'Capunga — Pressão de Ciclo',
      porta: PORT,
      maxGale: MAX_GALE,
      intervaloSync: INTERVALO_SYNC_MS
    });
  });

  await syncOnce();

  timer = setInterval(() => {
    if (!runtime.sincronizacaoEmCurso && !shuttingDown) syncOnce();
  }, INTERVALO_SYNC_MS);
}

async function shutdown() {
  if (shuttingDown) return;
  shuttingDown = true;
  if (timer) clearInterval(timer);
  if (server) await new Promise(r => server.close(r));
  if (pool) await pool.end();
  process.exit(0);
}

process.on('SIGTERM', () => shutdown());
process.on('SIGINT', () => shutdown());

start().catch(e => {
  console.error('[FATAL]', e);
  process.exit(1);
});