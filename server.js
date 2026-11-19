/**
 * CAPUNGA PRIME — BAC BO v22.0
 * Reconstrução do servidor a partir do CAPUNGA DADOS v20.1 enviado pelo utilizador.
 *
 * Principais mudanças:
 * - Empates são eventos neutros: registados e não consomem gale.
 * - GS/G1/G2 representam tentativas de aposta; G1/G2 avançam apenas após derrota.
 * - LOSS só após 3 derrotas efetivas consecutivas (GS, G1 e G2).
 * - Histórico imutável de eventos por sinal, com timestamps UTC e apresentação Africa/Luanda.
 * - Persistência PostgreSQL, recuperação de sinais pendentes, advisory lock em ligação dedicada.
 * - Backoff para HTTP 429/5xx, circuit breaker simples e fallback de fontes.
 * - Suspensão por sequência REAL de perdas consecutivas da estratégia.
 * - Backtest walk-forward com previsões fora da amostra; confiança conservadora com shrinkage.
 * - Sem obrigação de emitir sinal ao fim de 5 minutos: nunca fabricar entradas.
 *
 * ENV obrigatórias: BOT_TOKEN, CHAT_ID, ADMIN_TOKEN, DATABASE_URL
 * ENV opcionais: API_BACBO, API_BACBO_2, PORT, SELF_URL, DEPLOY_ID
 *
 * IMPORTANTE:
 * Nenhum padrão garante resultados futuros. O valor "Força" é uma estimativa
 * conservadora de não atingir LOSS até G2, com empates como devolução.
 */

'use strict';

const crypto = require('crypto');
const express = require('express');
const cors = require('cors');
const axios = require('axios');
const { Pool } = require('pg');

const PORT = Number(process.env.PORT || 10000);
const BOT_TOKEN = process.env.BOT_TOKEN || '';
const CHAT_ID = process.env.CHAT_ID || '';
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || '';
const DATABASE_URL = process.env.DATABASE_URL || '';

const CFG = {
  version: '22.0',
  active: true,
  maxGales: 2,
  maxPending: 1,
  minHistory: 250,
  maxHistory: 5000,
  signalCooldownMs: 4 * 60 * 1000,
  scanIntervalMs: 3000,
  hotScanIntervalMs: 1200,
  minSettledAgeMs: 5000,
  maxStaleMs: 150000,
  recoveryRounds: 4,
  roundGapMs: 7 * 60 * 1000,
  pendingTimeoutMs: 12 * 60 * 1000,
  strategySuspendMs: 4 * 60 * 60 * 1000,
  strategyLossStreak: 3,
  minBacktestN: 80,
  minLiveN: 12,
  minConfidence: 50,
  maxConfidence: 92,
  minScore: 76,
  minStrategyAgreement: 1,
  maxSignalsMemory: 1000,
  apiTimeoutMs: 10000,
  api429BaseMs: 15000,
  api429MaxMs: 180000,
  api5xxBaseMs: 8000,
  alertThrottleMs: 10 * 60 * 1000,
  keepAliveMs: 60000,
  lockKey: 72727222,
  backtestWindow: 20,
  priorWeight: 40,
  // Estimativa neutra para três tentativas em Bac Bo não é uma garantia.
  // Usamos prior conservador e forte shrinkage até existir amostra suficiente.
  priorNonLoss: 0.80
};

const J = 'Jogador';
const B = 'Banca';
const TIE = 'Empate';
const isSide = x => x === J || x === B;
const opposite = x => x === J ? B : x === B ? J : null;

const API_SOURCES = [
  process.env.API_BACBO ||
  'https://api-cs.casino.org/svc-evolution-game-events/api/bacbo?page=0&size=50&sort=data.settledAt,desc&duration=120&wheelResults=PlayerWon,BankerWon,Tie',
  process.env.API_BACBO_2 || ''
].filter(Boolean);

const fmt = new Intl.DateTimeFormat('pt-PT', {
  timeZone: 'Africa/Luanda', hour: '2-digit', minute: '2-digit',
  second: '2-digit', hour12: false
});
const fmtDate = new Intl.DateTimeFormat('pt-PT', {
  timeZone: 'Africa/Luanda', year: 'numeric', month: '2-digit', day: '2-digit',
  hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false
});
const now = () => Date.now();
const time = ms => fmt.format(new Date(ms || now()));
const dateLuanda = ms => new Date((ms || now()) + 3600000).toISOString().slice(0, 10);
const sleep = ms => new Promise(r => setTimeout(r, ms));
const log = (tag, value) => console.log(`[${new Date().toISOString()}] [${tag}]`,
  typeof value === 'string' ? value : JSON.stringify(value));
const clean = v => String(v ?? '').replace(/[&<>"]/g, c =>
  ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
const colorName = c => c === J ? 'AZUL 🔵' : 'VERMELHO 🔴';
const progress = p => {
  const n = Math.max(0, Math.min(10, Math.round(p / 10)));
  return '▰'.repeat(n) + '▱'.repeat(10 - n);
};

const S = {
  hist: [], signals: [], nextId: 1, lastSignalAt: 0, currentStreak: 0,
  recoveryLeft: 0, stats: null, statsAt: -1, suspended: new Map(),
  apiBackoffUntil: 0, apiFailures: 0, lastApiError: null,
  lastApiAlert: 0, lastDbAlert: 0, lastSync: null, sourceIndex: 0,
  lockClient: null, pool: null, busy: false, stopping: false
};
const DAILY = {
  date: dateLuanda(), total: 0, green: 0, g1: 0, g2: 0,
  ties: 0, loss: 0, expired: 0, bestStreak: 0
};
const ALL = {
  total: 0, green: 0, g1: 0, g2: 0, ties: 0, loss: 0, expired: 0
};

const FECHADOS = new Set(['GREEN', 'G1', 'G2', 'LOSS', 'EXPIRADO']);
const pending = () => S.signals.filter(s => s.status === 'PENDENTE');
const eventKey = (signalId, roundId) => `${signalId}:${roundId}`;

function normalizeOutcome(v) {
  if (v && typeof v === 'object') v = v.outcome ?? v.resultado ?? v.winner ?? v.name;
  const s = String(v ?? '').normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .toLowerCase().replace(/[\s_-]+/g, '');
  if (['player','jogador','playerwin','playerwins','playerwon','blue'].includes(s)) return J;
  if (['banker','banca','bankerwin','bankerwins','bankerwon','red','vermelho'].includes(s)) return B;
  if (['tie','draw','empate','equal','push','tied'].includes(s)) return TIE;
  return null;
}
function validTime(v) {
  if (!v) return null;
  const n = new Date(v).getTime();
  return Number.isFinite(n) ? n : null;
}
function numberOrNull(v) {
  if (v === null || v === undefined || v === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}
function extractItems(body) {
  if (Array.isArray(body)) return body;
  if (!body || typeof body !== 'object') return [];
  const candidates = [body.data, body.results, body.items, body.resultados,
    body.rounds, body.events, body.data?.results, body.data?.items,
    body.data?.resultados, body.data?.rounds, body.data?.events, body.data?.data];
  return candidates.find(Array.isArray) || [];
}
function normalizeRound(item) {
  const d = item?.data || item?.round || item || {};
  const result = d.result && typeof d.result === 'object' ? d.result :
    item?.result && typeof item.result === 'object' ? item.result : {};
  const raw = result.outcome ?? result.resultado ?? result.winner ??
    d.outcome ?? d.resultado ?? d.winner ?? item?.outcome ?? item?.resultado ??
    item?.winner ?? (typeof d.result === 'string' ? d.result : null) ??
    (typeof item?.result === 'string' ? item.result : null);
  const id = String(d.id ?? d.roundId ?? d.round_id ?? d.eventId ??
    item?.id ?? item?.roundId ?? item?.round_id ?? item?.eventId ?? '').trim();
  const t = validTime(d.settledAt ?? d.endTime ?? d.endedAt ?? d.finishedAt ??
    d.fim ?? item?.settledAt ?? item?.fim);
  const resultado = normalizeOutcome(raw);
  if (!id || !t || !resultado) return null;
  const pj = numberOrNull(result.playerDice?.score ?? d.playerDice?.score ??
    result.playerScore ?? d.playerScore ?? d.player_score ?? d.pontos_jogador);
  const pb = numberOrNull(result.bankerDice?.score ?? d.bankerDice?.score ??
    result.bankerScore ?? d.bankerScore ?? d.banker_score ?? d.pontos_banca);
  return { id, resultado, t, pj, pb };
}
const hasScore = r => Number.isFinite(r?.pj) && Number.isFinite(r?.pb);
const margin = r => hasScore(r) ? Math.abs(r.pj - r.pb) : null;
const scoreWinner = r => r.resultado === J ? r.pj : r.pb;

async function initDb() {
  S.pool = new Pool({
    connectionString: DATABASE_URL,
    ssl: /localhost|127\.0\.0\.1/i.test(DATABASE_URL) ? false : { rejectUnauthorized: false },
    max: 8
  });
  await S.pool.query('SELECT 1');
  await S.pool.query(`
    CREATE TABLE IF NOT EXISTS capunga_rounds (
      id TEXT PRIMARY KEY, resultado TEXT NOT NULL, t BIGINT NOT NULL,
      pj DOUBLE PRECISION, pb DOUBLE PRECISION
    );
    CREATE INDEX IF NOT EXISTS capunga_rounds_t_idx ON capunga_rounds(t DESC);
    CREATE TABLE IF NOT EXISTS capunga_state_v22 (
      k TEXT PRIMARY KEY, v TEXT NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS capunga_signals_v22 (
      id INTEGER PRIMARY KEY, cor TEXT NOT NULL, strategy_key TEXT NOT NULL,
      strategy_name TEXT NOT NULL, strategy_type TEXT NOT NULL,
      confidence INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'PENDENTE',
      stage TEXT NOT NULL DEFAULT 'GS', tier INTEGER NOT NULL DEFAULT 0,
      created_at TIMESTAMPTZ NOT NULL, closed_at TIMESTAMPTZ,
      final_round_id TEXT, final_result TEXT, final_stage TEXT,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS capunga_signals_v22_status_idx ON capunga_signals_v22(status);
    CREATE INDEX IF NOT EXISTS capunga_signals_v22_created_idx ON capunga_signals_v22(created_at DESC);
    CREATE TABLE IF NOT EXISTS capunga_signal_events_v22 (
      id BIGSERIAL PRIMARY KEY, signal_id INTEGER NOT NULL,
      round_id TEXT, round_time TIMESTAMPTZ, recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      stage TEXT NOT NULL, result TEXT NOT NULL, event_type TEXT NOT NULL,
      UNIQUE(signal_id, round_id, event_type)
    );
    CREATE INDEX IF NOT EXISTS capunga_events_signal_idx ON capunga_signal_events_v22(signal_id, recorded_at);
    CREATE TABLE IF NOT EXISTS capunga_daily_v22 (
      date_key TEXT PRIMARY KEY, data JSONB NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
  log('DB', 'schema v22 pronto');
}
async function stateGet(k) {
  const r = await S.pool.query('SELECT v FROM capunga_state_v22 WHERE k=$1', [k]);
  return r.rows[0]?.v ?? null;
}
async function stateSet(k, v) {
  await S.pool.query(`INSERT INTO capunga_state_v22(k,v) VALUES($1,$2)
    ON CONFLICT(k) DO UPDATE SET v=EXCLUDED.v,updated_at=NOW()`, [k, String(v)]);
}
async function insertRound(r) {
  await S.pool.query(`INSERT INTO capunga_rounds(id,resultado,t,pj,pb) VALUES($1,$2,$3,$4,$5)
    ON CONFLICT(id) DO UPDATE SET resultado=EXCLUDED.resultado,t=EXCLUDED.t,pj=EXCLUDED.pj,pb=EXCLUDED.pb`,
    [r.id, r.resultado, r.t, r.pj, r.pb]);
}
async function saveSignal(s) {
  await S.pool.query(`INSERT INTO capunga_signals_v22
    (id,cor,strategy_key,strategy_name,strategy_type,confidence,status,stage,tier,created_at,closed_at,final_round_id,final_result,final_stage)
    VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
    ON CONFLICT(id) DO UPDATE SET status=EXCLUDED.status,stage=EXCLUDED.stage,
    closed_at=EXCLUDED.closed_at,final_round_id=EXCLUDED.final_round_id,
    final_result=EXCLUDED.final_result,final_stage=EXCLUDED.final_stage,updated_at=NOW()`,
    [s.id,s.cor,s.key,s.nome,s.tipo,s.conf,s.status,s.stage,s.tier,
      new Date(s.t),s.fechadoEm ? new Date(s.fechadoEm) : null,
      s.finalRoundId || null,s.finalResult || null,s.finalStage || null]);
}
async function saveEvent(s, round, stage, eventType) {
  await S.pool.query(`INSERT INTO capunga_signal_events_v22
    (signal_id,round_id,round_time,stage,result,event_type)
    VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(signal_id,round_id,event_type) DO NOTHING`,
    [s.id,round?.id || null,round ? new Date(round.t) : null,stage,
      round?.resultado || 'SEM_RODADA',eventType]);
}
async function acquireLock() {
  // Advisory lock pertence à sessão. Reservamos uma ligação dedicada até ao shutdown.
  S.lockClient = await S.pool.connect();
  const r = await S.lockClient.query('SELECT pg_try_advisory_lock($1) AS ok', [CFG.lockKey]);
  if (!r.rows[0].ok) {
    S.lockClient.release(); S.lockClient = null;
    throw new Error('Outra instância v22 está ativa (advisory lock).');
  }
}
async function releaseLock() {
  if (!S.lockClient) return;
  try { await S.lockClient.query('SELECT pg_advisory_unlock($1)', [CFG.lockKey]); }
  finally { S.lockClient.release(); S.lockClient = null; }
}
async function loadState() {
  const [nextId,lastAt,streak,daily] = await Promise.all([
    stateGet('nextId'),stateGet('lastSignalAt'),stateGet('currentStreak'),stateGet('daily')
  ]);
  if (nextId) S.nextId = Math.max(1, Number(nextId) || 1);
  if (lastAt) S.lastSignalAt = Number(lastAt) || 0;
  if (streak) S.currentStreak = Number(streak) || 0;
  if (daily) {
    try { Object.assign(DAILY, JSON.parse(daily)); } catch {}
  }
  if (DAILY.date !== dateLuanda()) {
    Object.assign(DAILY,{date:dateLuanda(),total:0,green:0,g1:0,g2:0,ties:0,loss:0,expired:0,bestStreak:0});
  }
  const rounds = await S.pool.query(`SELECT id,resultado,t,pj,pb FROM capunga_rounds ORDER BY t DESC LIMIT ${CFG.maxHistory}`);
  S.hist = rounds.rows.map(r => ({id:r.id,resultado:r.resultado,t:Number(r.t),
    pj:r.pj === null ? null : Number(r.pj),pb:r.pb === null ? null : Number(r.pb)})).reverse();
  const sigs = await S.pool.query('SELECT * FROM capunga_signals_v22 ORDER BY created_at DESC LIMIT 1000');
  S.signals = sigs.rows.reverse().map(r => ({
    id:r.id,cor:r.cor,key:r.strategy_key,nome:r.strategy_name,tipo:r.strategy_type,
    conf:r.confidence,status:r.status,stage:r.stage,tier:r.tier,t:new Date(r.created_at).getTime(),
    fechadoEm:r.closed_at ? new Date(r.closed_at).getTime() : null,
    finalRoundId:r.final_round_id,finalResult:r.final_result,finalStage:r.final_stage
  }));
  const counts = await S.pool.query(`SELECT status,COUNT(*)::int n FROM capunga_signals_v22
    WHERE status <> 'PENDENTE' GROUP BY status`);
  for (const k of Object.keys(ALL)) ALL[k] = 0;
  for (const row of counts.rows) {
    ALL.total += row.n;
    if (row.status === 'GREEN') ALL.green = row.n;
    if (row.status === 'G1') ALL.g1 = row.n;
    if (row.status === 'G2') ALL.g2 = row.n;
    if (row.status === 'LOSS') ALL.loss = row.n;
    if (row.status === 'EXPIRADO') ALL.expired = row.n;
  }
  const events = await S.pool.query(`SELECT COUNT(*)::int n FROM capunga_signal_events_v22 WHERE event_type='TIE'`);
  ALL.ties = events.rows[0]?.n || 0;
  log('DB',`carregado: ${S.hist.length} rodadas, ${S.signals.length} sinais`);
}

async function telegram(text) {
  if (!BOT_TOKEN || !CHAT_ID) return {ok:false};
  try {
    const r = await axios.post(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`,
      {chat_id:CHAT_ID,text,parse_mode:'HTML',disable_web_page_preview:true},{timeout:12000});
    return r.data?.ok ? {ok:true,messageId:r.data.result.message_id} : {ok:false,error:JSON.stringify(r.data)};
  } catch(e) { return {ok:false,error:e.response?.data?.description || e.message}; }
}
async function alert(tag, message) {
  const t = now();
  const field = tag === 'API' ? 'lastApiAlert' : 'lastDbAlert';
  if (t - S[field] < CFG.alertThrottleMs) return;
  S[field] = t;
  await telegram(`⚠️ <b>ALERTA ${tag}</b>\n${message}\n⏱ ${time()}`);
}
function signalMessage(s) {
  const strength = s.conf >= 88 ? '💎' : s.conf >= 80 ? '🔥' : '⚡';
  return [
    '━━━━━━━━━━━━━━━━━━',
    `🎲 <b>CAPUNGA PRIME v22 — SINAL #${s.id}</b>`,
    '━━━━━━━━━━━━━━━━━━',
    `🎯 Entrada: <b>${colorName(s.cor)}</b>`,
    `🛡 Gestão: empate não consome gale`,
    `${strength} Força estatística: <b>${s.conf}%</b> ${progress(s.conf)}`,
    `🧠 Estratégia: ${clean(s.nome)}`,
    `🤝 Confirmações: ${s.agree || 1}`,
    `📍 Etapa inicial: GS`,
    `⏰ ${time(s.t)}`,
    '<i>Estimativa estatística; não é garantia de resultado.</i>'
  ].join('\n');
}
function stageMessage(s, stage) {
  return `🔄 <b>SINAL #${s.id} — ${stage}</b>\n🎯 Manter ${colorName(s.cor)}\n🟡 Empates são registados e não avançam gale.\n⏱ ${time()}`;
}
function resultMessage(s) {
  const label = s.status === 'GREEN' ? 'GREEN NO GS' : s.status === 'G1' ? 'GREEN NO G1' :
    s.status === 'G2' ? 'GREEN NO G2' : s.status === 'LOSS' ? 'LOSS' : 'EXPIRADO';
  return `━━━━━━━━━━━━━━━━━━\n${s.status === 'LOSS' ? '❌' : s.status === 'EXPIRADO' ? '⏸' : '✅'} <b>${label} #${s.id}</b>\n`+
    `Etapa final: ${s.finalStage || s.stage}\nResultado: ${s.finalResult || 'indisponível'}\n`+
    `⏱ ${time(s.fechadoEm)}\n📊 ${DAILY.green+DAILY.g1+DAILY.g2} GREEN • ${DAILY.loss} LOSS • ${DAILY.ties} empates`;
}
async function persistDaily() {
  await S.pool.query(`INSERT INTO capunga_daily_v22(date_key,data) VALUES($1,$2::jsonb)
    ON CONFLICT(date_key) DO UPDATE SET data=EXCLUDED.data,updated_at=NOW()`,
    [DAILY.date,JSON.stringify(DAILY)]);
  await stateSet('daily',JSON.stringify(DAILY));
}
async function dailyReset() {
  const key = dateLuanda();
  if (DAILY.date === key) return;
  await telegram(`🕛 <b>FECHO DO DIA ${DAILY.date}</b>\nSinais: ${DAILY.total}\nGREEN: ${DAILY.green+DAILY.g1+DAILY.g2}\nEmpates: ${DAILY.ties}\nLOSS: ${DAILY.loss}\nEXPIRADOS: ${DAILY.expired}`);
  Object.assign(DAILY,{date:key,total:0,green:0,g1:0,g2:0,ties:0,loss:0,expired:0,bestStreak:0});
  await persistDaily();
}

/* ======================== ESTRATÉGIAS ======================== */
function sideRows(rows) { return rows.filter(r => isSide(r.resultado)); }
function streakLength(seq) {
  if (!seq.length) return 0;
  let n=1; while(n<seq.length && seq[n].resultado===seq[0].resultado) n++;
  return n;
}
function alternationLength(seq) {
  if (!seq.length) return 0;
  let n=1; while(n<seq.length && seq[n].resultado!==seq[n-1].resultado) n++;
  return n;
}
function regimeOf(seq) {
  const a=seq.slice(0,15);
  if(a.length<6) return 'NEUTRO';
  let changes=0; for(let i=1;i<a.length;i++) if(a[i].resultado!==a[i-1].resultado) changes++;
  const rate=changes/(a.length-1);
  return rate>.62?'ERRATICO':rate<.38?'TENDENCIA':'NEUTRO';
}
const STRATEGIES = [
  ({seq}) => { const n=streakLength(seq); return n>=3&&n<=6 ?
    {cor:seq[0].resultado,key:`REP_${n}`,nome:`Repetição x${n}`,type:'sequência'}:null; },
  ({seq}) => { const n=streakLength(seq); return n>=4&&n<=7 ?
    {cor:opposite(seq[0].resultado),key:`QUEBRA_${n}`,nome:`Quebra x${n}`,type:'quebra'}:null; },
  ({seq}) => { const n=alternationLength(seq); return n>=4&&n<=8 ?
    {cor:opposite(seq[0].resultado),key:`ALTERNANCIA_${n}`,nome:`Alternância ${n}`,type:'alternância'}:null; },
  ({seq}) => seq.length>=4 && seq[0].resultado===seq[1].resultado &&
    seq[2].resultado===seq[3].resultado && seq[0].resultado!==seq[2].resultado ?
    {cor:seq[0].resultado,key:'BLOCO_2X2',nome:'Bloco 2+2',type:'bloco'}:null,
  ({seq}) => seq.length>=3 && seq[0].resultado===seq[1].resultado &&
    seq[1].resultado===seq[2].resultado && seq.slice(0,3).every(hasScore) &&
    seq.slice(0,3).every(r=>margin(r)>=4) ?
    {cor:seq[0].resultado,key:'MARGEM_FORTE_3',nome:'Três margens fortes',type:'numérica'}:null,
  ({rows,seq}) => rows[0]?.resultado===TIE && seq.length ?
    {cor:seq[0].resultado,key:'POS_EMPATE',nome:'Pós-empate',type:'empate'}:null,
  ({rows}) => rows.length>=3 && rows[0].resultado===TIE &&
    rows[1].resultado===TIE && isSide(rows[2].resultado) ?
    {cor:rows[2].resultado,key:'DUPLO_EMPATE',nome:'Retorno após duplo empate',type:'empate'}:null,
  ({seq}) => seq.length>=2 && seq[0].resultado===seq[1].resultado &&
    seq.slice(0,2).every(hasScore) && margin(seq[0])>=5 && margin(seq[1])>=5 ?
    {cor:seq[0].resultado,key:'DUPLA_MARGEM',nome:'Dupla margem alta',type:'numérica'}:null,
  ({rows}) => {
    const a=rows.slice(0,12).filter(r=>isSide(r.resultado));
    if(a.length<10) return null;
    const b=a.filter(r=>r.resultado===B).length, j=a.length-b;
    return b>=8?{cor:B,key:'VIES_BANCA',nome:'Viés recente da banca',type:'frequência'}:
      j>=8?{cor:J,key:'VIES_JOGADOR',nome:'Viés recente do jogador',type:'frequência'}:null;
  },
  ({seq}) => seq.length>=3 && seq.slice(0,3).every(hasScore) &&
    seq[0].resultado===seq[1].resultado && seq[1].resultado===seq[2].resultado &&
    scoreWinner(seq[0])<scoreWinner(seq[1]) && scoreWinner(seq[1])<scoreWinner(seq[2]) ?
    {cor:seq[0].resultado,key:'ESCALADA_SCORE',nome:'Escalada de pontuação',type:'numérica'}:null
];
function runStrategies(rows) {
  const seq=sideRows(rows);
  const ctx={rows,seq};
  return STRATEGIES.map(fn=>{try{return fn(ctx)}catch{return null}})
    .filter(c=>c&&isSide(c.cor));
}

/* ======================== WALK-FORWARD BACKTEST ======================== */
// Só avalia previsões feitas com informação anterior à rodada prevista.
// Empates não são perda nem green; são neutros e a avaliação continua até G2.
// Cada entrada é avaliada pela primeira ocorrência de GREEN ou por 3 derrotas.
function evaluateAt(history,index,cor) {
  let losses=0, ties=0, lastT=history[index-1]?.t ?? history[index].t;
  for(let k=index;k<history.length;k++) {
    const r=history[k];
    if(r.t-lastT>CFG.roundGapMs) return null;
    lastT=r.t;
    if(r.resultado===TIE) {ties++;continue;}
    if(r.resultado===cor) return {nonLoss:true,green:true,ties,stage:losses};
    losses++;
    if(losses>CFG.maxGales) return {nonLoss:false,green:false,ties,stage:2};
  }
  return null;
}
function buildBacktest() {
  const H=S.hist, stats={};
  if(H.length<CFG.minHistory) return {per:stats,ready:false,observations:0};
  let observations=0;
  // Walk-forward: cada decisão só consulta a janela que termina antes do índice.
  for(let i=CFG.backtestWindow;i<H.length-3;i++) {
    const past=H.slice(Math.max(0,i-CFG.backtestWindow),i).reverse();
    const candidates=runStrategies(past);
    for(const c of candidates) {
      const outcome=evaluateAt(H,i,c.cor);
      if(!outcome) continue;
      const p=stats[c.key]||(stats[c.key]={n:0,nonLoss:0,greens:0,losses:0});
      p.n++; observations++;
      if(outcome.nonLoss)p.nonLoss++;
      if(outcome.green)p.greens++;
      else if(!outcome.nonLoss)p.losses++;
    }
  }
  return {per:stats,ready:true,observations};
}
function getStats(force=false) {
  if(force||!S.stats||S.hist.length-S.statsAt>=25) {
    S.stats=buildBacktest(); S.statsAt=S.hist.length;
  }
  return S.stats;
}
function liveResults(key) {
  return S.signals.filter(s=>s.key===key&&FECHADOS.has(s.status)&&s.status!=='EXPIRADO');
}
function estimateConfidence(candidate,stats) {
  const p=stats.per[candidate.key]||{n:0,nonLoss:0};
  const live=liveResults(candidate.key);
  const liveN=live.length;
  const liveWins=live.filter(s=>s.status!=='LOSS').length;
  // O backtest recebe shrinkage forte. O histórico real só pesa após amostra mínima.
  const prior=CFG.priorNonLoss;
  const bt=(p.nonLoss+prior*CFG.priorWeight)/(p.n+CFG.priorWeight);
  const useLive=liveN>=CFG.minLiveN;
  const raw=useLive ?
    (liveWins+bt*CFG.priorWeight)/(liveN+CFG.priorWeight) : bt;
  // Penalização por amostra pequena; sem bónus por concordância para não inflacionar.
  const samplePenalty=p.n<CFG.minBacktestN?Math.min(10,(CFG.minBacktestN-p.n)/10):0;
  const confidence=Math.round(Math.max(CFG.minConfidence,
    Math.min(CFG.maxConfidence,raw*100-samplePenalty)));
  return {confidence,backtestN:p.n,liveN,backtestRate:bt};
}
function isSuspended(key) {
  const until=S.suspended.get(key)||0;
  if(until>now())return true;
  if(until)S.suspended.delete(key);
  return false;
}
function updateLossStreaks() {
  const byKey=new Map();
  for(const s of S.signals.filter(x=>FECHADOS.has(x.status)&&x.status!=='EXPIRADO').sort((a,b)=>a.t-b.t)) {
    const a=byKey.get(s.key)||[]; a.push(s); byKey.set(s.key,a);
  }
  for(const [key,a] of byKey) {
    const last=a.slice(-CFG.strategyLossStreak);
    if(last.length===CFG.strategyLossStreak&&last.every(x=>x.status==='LOSS'))
      S.suspended.set(key,now()+CFG.strategySuspendMs);
  }
}
function eligibleSignal() {
  if(!CFG.active||pending().length>=CFG.maxPending)return {reason:'sinal pendente'};
  const last=S.hist.at(-1);
  if(!last)return {reason:'sem histórico'};
  if(now()-last.t>CFG.maxStaleMs)return {reason:'API sem dados frescos'};
  if(S.recoveryLeft>0)return {reason:`retoma: faltam ${S.recoveryLeft} rodadas novas`};
  if(now()-last.t<CFG.minSettledAgeMs)return {reason:'rodada ainda recente'};
  if(S.hist.length<CFG.minHistory)return {reason:`histórico insuficiente (${S.hist.length}/${CFG.minHistory})`};
  if(S.lastSignalAt&&now()-S.lastSignalAt<CFG.signalCooldownMs)return {reason:'intervalo mínimo de 4 minutos'};
  return {ok:true};
}
async function generateSignal() {
  const gate=eligibleSignal();
  if(!gate.ok){S.lastReason=gate.reason;return;}
  const stats=getStats();
  if(!stats.ready){S.lastReason='backtest ainda sem amostra';return;}
  const rows=S.hist.slice(-30).reverse();
  const regime=regimeOf(sideRows(rows));
  let candidates=runStrategies(rows).filter(c=>!isSuspended(c.key));
  // Evita usar sinais contraditórios em regime que não corresponde à família.
  candidates=candidates.filter(c=>{
    if(c.type==='quebra'||c.type==='alternância')return regime==='ERRATICO';
    if(c.type==='sequência'||c.type==='frequência')return regime==='TENDENCIA';
    return true;
  });
  const byColor=new Map();
  for(const c of candidates) {
    const a=byColor.get(c.cor)||[];a.push(c);byColor.set(c.cor,a);
  }
  const ranked=[];
  for(const [cor,list] of byColor) {
    const evaluated=list.map(c=>({...c,...estimateConfidence(c,stats)}))
      .sort((a,b)=>b.confidence-a.confidence);
    const best=evaluated[0];
    ranked.push({...best,cor,agree:list.length,agreementKeys:list.map(x=>x.key)});
  }
  ranked.sort((a,b)=>b.confidence-a.confidence);
  const best=ranked[0];
  if(!best){S.lastReason='nenhuma estratégia válida';return;}
  if(best.confidence<CFG.minScore){S.lastReason=`força ${best.confidence}% abaixo do mínimo ${CFG.minScore}%`;return;}
  if(best.agree<CFG.minStrategyAgreement){S.lastReason='consenso insuficiente';return;}
  const t=now();
  const s={id:S.nextId++,cor:best.cor,key:best.key,nome:best.nome,tipo:best.type,
    conf:best.confidence,status:'PENDENTE',stage:'GS',tier:0,t,agree:best.agree,
    backtestN:best.backtestN,liveN:best.liveN,agreementKeys:best.agreementKeys,
    lastRoundTime:S.hist.at(-1).t,losses:0,ties:0,seenRounds:[]};
  const sent=await telegram(signalMessage(s));
  if(!sent.ok){S.nextId--;S.lastReason='Telegram não confirmou envio';return;}
  S.signals.push(s);S.lastSignalAt=t;S.lastReason=null;
  await saveSignal(s);
  await stateSet('nextId',S.nextId);
  await stateSet('lastSignalAt',S.lastSignalAt);
  log('SINAL',{id:s.id,cor:s.cor,confidence:s.conf,key:s.key,agree:s.agree});
}

/* ======================== APURAÇÃO POR EVENTOS ======================== */
async function closeSignal(s,status,round,stage) {
  if(s.status!=='PENDENTE')return;
  s.status=status;s.fechadoEm=now();s.finalRoundId=round?.id||null;
  s.finalResult=round?.resultado||null;s.finalStage=stage;
  const bucket=status==='GREEN'?'green':status==='G1'?'g1':status==='G2'?'g2':
    status==='LOSS'?'loss':'expired';
  DAILY.total++;DAILY[bucket]++;ALL.total++;ALL[bucket]++;
  if(status==='LOSS')S.currentStreak=0;
  else if(status!=='EXPIRADO') {
    S.currentStreak++;
    DAILY.bestStreak=Math.max(DAILY.bestStreak,S.currentStreak);
  }
  await saveSignal(s);
  await stateSet('currentStreak',S.currentStreak);
  await persistDaily();
  await telegram(resultMessage(s));
  updateLossStreaks();
}
async function processRoundForSignal(s,r) {
  if(s.status!=='PENDENTE'||r.t<=s.t||s.seenRounds.includes(r.id))return;
  const prev=s.lastRoundTime||s.t;
  if(r.t-prev>CFG.roundGapMs) {
    await saveEvent(s,r,s.stage,'GAP_EXPIRADO');
    await closeSignal(s,'EXPIRADO',r,s.stage);return;
  }
  s.seenRounds.push(r.id);s.lastRoundTime=r.t;
  if(r.resultado===TIE) {
    s.ties++;
    await saveEvent(s,r,s.stage,'TIE');
    DAILY.ties++;ALL.ties++;
    await persistDaily();
    // Empate é push: continua exatamente na mesma etapa e não consome gale.
    await telegram(`🟡 <b>EMPATE NO ${s.stage} — SINAL #${s.id}</b>\nEtapa mantida: ${s.stage}\nEmpates acumulados: ${s.ties}\n⏱ ${time(r.t)}`);
    return;
  }
  if(r.resultado===s.cor) {
    const status=s.losses===0?'GREEN':s.losses===1?'G1':'G2';
    await saveEvent(s,r,s.stage,'GREEN');
    await closeSignal(s,status,r,s.stage);return;
  }
  // Derrota efetiva: avança etapa; só a terceira derrota fecha LOSS.
  s.losses++;
  await saveEvent(s,r,s.stage,'LOSS');
  if(s.losses>CFG.maxGales) {
    await closeSignal(s,'LOSS',r,s.stage);return;
  }
  s.stage=s.losses===1?'G1':'G2';
  await saveSignal(s);
  await telegram(stageMessage(s,s.stage));
}
async function settleSignals() {
  const rounds=S.hist.slice().sort((a,b)=>a.t-b.t);
  for(const s of pending()) {
    const after=rounds.filter(r=>r.t>s.t);
    for(const r of after) {
      if(s.status!=='PENDENTE')break;
      await processRoundForSignal(s,r);
    }
    if(s.status==='PENDENTE') {
      const last=rounds.at(-1);
      const reference=s.lastRoundTime||s.t;
      if(now()-reference>CFG.pendingTimeoutMs ||
        (last&&now()-last.t>CFG.maxStaleMs)) {
        await saveEvent(s,null,s.stage,'TIMEOUT_EXPIRADO');
        await closeSignal(s,'EXPIRADO',null,s.stage);
      }
    }
  }
}

/* ======================== API E SINCRONIZAÇÃO ======================== */
async function fetchSource(url) {
  const resp=await axios.get(url,{timeout:CFG.apiTimeoutMs,
    headers:{Accept:'application/json','User-Agent':'CapungaPrime/22.0'}});
  const map=new Map();
  for(const item of extractItems(resp.data)) {
    const r=normalizeRound(item);if(r)map.set(r.id,r);
  }
  return [...map.values()].sort((a,b)=>a.t-b.t);
}
async function fetchRounds() {
  if(now()<S.apiBackoffUntil)throw new Error(`API em pausa até ${new Date(S.apiBackoffUntil).toISOString()}`);
  const errors=[];
  for(let i=0;i<API_SOURCES.length;i++) {
    const idx=(S.sourceIndex+i)%API_SOURCES.length;
    try {
      const rounds=await fetchSource(API_SOURCES[idx]);
      S.sourceIndex=idx;S.apiFailures=0;S.apiBackoffUntil=0;S.lastApiError=null;
      return rounds;
    } catch(e) {
      const status=e.response?.status;
      errors.push(`fonte${idx+1}: ${status||e.message}`);
      if(status===429) {
        const retry=Number(e.response?.headers?.['retry-after']);
        const delay=retry>0?retry*1000:Math.min(CFG.api429MaxMs,CFG.api429BaseMs*Math.pow(2,S.apiFailures));
        S.apiBackoffUntil=now()+delay;S.apiFailures++;
      } else if(status>=500||!status) {
        const delay=Math.min(60000,CFG.api5xxBaseMs*Math.pow(2,S.apiFailures));
        S.apiBackoffUntil=now()+delay;S.apiFailures++;
      }
    }
  }
  throw new Error(errors.join(' | ')||'Nenhuma fonte API configurada');
}
async function syncRounds() {
  const rounds=await fetchRounds();
  const map=new Map(S.hist.map(r=>[r.id,r]));
  let newCount=0,updated=0;
  for(const r of rounds) {
    const old=map.get(r.id);
    if(!old){map.set(r.id,r);newCount++;await insertRound(r);}
    else if(old.t!==r.t||old.resultado!==r.resultado||old.pj!==r.pj||old.pb!==r.pb) {
      map.set(r.id,r);updated++;await insertRound(r);
    }
  }
  S.hist=[...map.values()].sort((a,b)=>a.t-b.t||a.id.localeCompare(b.id)).slice(-CFG.maxHistory);
  const newest=S.hist.at(-1);
  if(!newest||now()-newest.t>CFG.maxStaleMs) {
    S.recoveryLeft=Math.max(S.recoveryLeft,CFG.recoveryRounds);
  } else if(S.recoveryLeft>0) {
    S.recoveryLeft=Math.max(0,S.recoveryLeft-newCount);
  }
  S.lastSync=now();
  if(newCount||updated)log('SYNC',{newCount,updated,total:S.hist.length,recoveryLeft:S.recoveryLeft});
}
function syncInterval() {
  return pending().length||S.recoveryLeft>0?CFG.hotScanIntervalMs:CFG.scanIntervalMs;
}
async function tick() {
  if(S.busy||S.stopping)return;
  S.busy=true;
  try {
    await dailyReset();
    if(!S.lastSync||now()-S.lastSync>=syncInterval()) {
      try { await syncRounds(); }
      catch(e) { S.lastApiError=e.message;log('API',e.message);await alert('API',clean(e.message)); }
    }
    await settleSignals();
    await generateSignal();
  } catch(e) {log('TICK_ERRO',e.stack||e.message);}
  finally {S.busy=false;}
}

/* ======================== HTTP ======================== */
const app=express();
app.disable('x-powered-by');
app.use(cors({origin:false}));
app.use(express.json({limit:'200kb'}));
function admin(req,res,next) {
  const token=String(req.get('authorization')||'').replace(/^Bearer\s+/i,'');
  if(!ADMIN_TOKEN)return res.status(503).json({error:'ADMIN_TOKEN não configurado'});
  const a=Buffer.from(token),b=Buffer.from(ADMIN_TOKEN);
  if(a.length!==b.length||!crypto.timingSafeEqual(a,b))return res.status(401).json({error:'não autorizado'});
  next();
}
function statsSummary() {
  const decided=ALL.total-ALL.expired;
  const nonLoss=ALL.green+ALL.g1+ALL.g2;
  return {daily:{...DAILY},all:{...ALL},
    taxaSemLoss:decided>0?(nonLoss+ALL.ties)/decided:null,
    taxaGreen:decided>0?nonLoss/decided:null};
}
app.get('/health',(req,res)=>{
  const last=S.hist.at(-1);
  res.json({ok:!S.lastApiError&&Boolean(S.lastSync)&&Boolean(S.lockClient),
    version:CFG.version,lock:Boolean(S.lockClient),history:S.hist.length,
    signals:S.signals.length,pending:pending().length,recoveryLeft:S.recoveryLeft,
    lastSync:S.lastSync?new Date(S.lastSync).toISOString():null,
    lastRound:last?new Date(last.t).toISOString():null,
    dataStale:!last||now()-last.t>CFG.maxStaleMs,apiError:S.lastApiError,
    daily:DAILY,all:ALL,uptime:Math.round(process.uptime())});
});
app.get('/status',admin,(req,res)=>res.json({
  version:CFG.version,config:CFG,lastReason:S.lastReason||null,
  lastApiError:S.lastApiError,apiBackoffUntil:S.apiBackoffUntil||null,
  recoveryLeft:S.recoveryLeft,suspended:[...S.suspended.entries()],
  lastSignalAt:S.lastSignalAt?new Date(S.lastSignalAt).toISOString():null,
  lastRound:S.hist.at(-1)||null
}));
app.get('/signals',admin,(req,res)=>{
  const n=Math.min(200,Math.max(1,Number(req.query.n)||30));
  res.json(S.signals.slice(-n).reverse());
});
app.get('/events/:id',admin,async(req,res)=>{
  const id=Number(req.params.id);
  const r=await S.pool.query(`SELECT signal_id,round_id,round_time,recorded_at,stage,result,event_type
    FROM capunga_signal_events_v22 WHERE signal_id=$1 ORDER BY recorded_at,id`,[id]);
  res.json(r.rows);
});
app.get('/stats',admin,(req,res)=>res.json(statsSummary()));
app.get('/calibration',admin,(req,res)=>{
  const buckets={};
  for(const s of S.signals) {
    if(!FECHADOS.has(s.status)||s.status==='EXPIRADO')continue;
    const key=s.conf>=90?'90-92':s.conf>=85?'85-89':s.conf>=80?'80-84':'76-79';
    const b=buckets[key]||(buckets[key]={n:0,green:0,loss:0});
    b.n++;if(s.status==='LOSS')b.loss++;else b.green++;
  }
  res.json(buckets);
});
app.get('/backtest',admin,(req,res)=>{
  const stats=getStats(true);
  res.json({ready:stats.ready,observations:stats.observations,strategies:stats.per});
});

/* ======================== ARRANQUE ======================== */
async function start() {
  for(const [name,value] of Object.entries({BOT_TOKEN,CHAT_ID,ADMIN_TOKEN,DATABASE_URL}))
    if(!value)throw new Error(`Variável obrigatória em falta: ${name}`);
  if(!/^postgres(?:ql)?:\/\//i.test(DATABASE_URL))throw new Error('DATABASE_URL inválido');
  await initDb();
  await acquireLock();
  await loadState();
  try {await syncRounds();} catch(e) {S.lastApiError=e.message;log('BOOT_API',e.message);}
  if(!S.hist.length||now()-S.hist.at(-1).t>CFG.maxStaleMs)S.recoveryLeft=CFG.recoveryRounds;
  getStats(true);
  // Reconstruir a etapa dos sinais pendentes com base nos eventos já persistidos.
  const pendingIds=pending().map(s=>s.id);
  if(pendingIds.length) {
    const ev=await S.pool.query(`SELECT signal_id,round_id,stage,result,event_type
      FROM capunga_signal_events_v22 WHERE signal_id=ANY($1::int[]) ORDER BY recorded_at,id`,[pendingIds]);
    for(const s of pending()) {
      const own=ev.rows.filter(e=>e.signal_id===s.id);
      s.seenRounds=own.filter(e=>e.round_id).map(e=>e.round_id);
      s.ties=own.filter(e=>e.event_type==='TIE').length;
      s.losses=own.filter(e=>e.event_type==='LOSS').length;
      s.stage=s.losses===0?'GS':s.losses===1?'G1':'G2';
      const lastRoundId=s.seenRounds.at(-1);
      const last=S.hist.find(r=>r.id===lastRoundId);
      s.lastRoundTime=last?.t||s.t;
    }
  }
  const server=app.listen(PORT,()=>log('HTTP',`porta ${PORT}`));
  const timer=setInterval(()=>tick().catch(e=>log('TICK',e.message)),1000);
  const keep=setInterval(async()=>{
    const url=process.env.SELF_URL||process.env.RENDER_EXTERNAL_URL;
    if(url)try{await axios.get(url.replace(/\/$/,'')+'/health',{timeout:7000});}catch{}
  },CFG.keepAliveMs);
  await tick();
  const shutdown=async()=>{
    if(S.stopping)return;S.stopping=true;
    clearInterval(timer);clearInterval(keep);
    await new Promise(resolve=>server.close(resolve));
    await releaseLock();await S.pool.end();process.exit(0);
  };
  process.on('SIGTERM',shutdown);process.on('SIGINT',shutdown);
}
start().catch(e=>{console.error(e);process.exit(1);});