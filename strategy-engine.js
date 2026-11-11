'use strict';
// Strategy pack restored from the full source; frequency patterns are descriptive,
// and only pass through an out-of-sample rolling backtest + Wilson lower-bound gate.
const J='Jogador', B='Banca', E='Empate';
const isCol=x=>x===J||x===B;
const opp=c=>c===J?B:c===B?J:null;
const hasScore = (r) => r && Number.isFinite(r.pj) && Number.isFinite(r.pb);
const margem = (r) => Math.abs((r.pj || 0) - (r.pb || 0));
const scoreOf = (r) => (r.outcome === J ? r.pj : r.pb);
const scoreRange = (s) => {
  if (!Number.isFinite(s)) return null;
  if (s <= 2) return 'MB';
  if (s <= 4) return 'B';
  if (s <= 6) return 'M';
  if (s <= 8) return 'A';
  return 'MA';
};
function streak(seq) {
  if (!seq.length) return 0;
  let n = 0; const cor = seq[0].resultado;
  for (const r of seq) { if (r.outcome === cor) n++; else break; }
  return n;
}
function alternation(seq) {
  if (seq.length < 2) return 1;
  let L = 1;
  while (L < seq.length && seq[L].resultado !== seq[L - 1].resultado) L++;
  return L;
}
function detectRegime(seq) {
  if (seq.length < 6) return { trend: 0.5, chop: 0.5, label: 'neutro' };
  const n = Math.min(seq.length, 15);
  let changes = 0;
  for (let i = 1; i < n; i++) if (seq[i].resultado !== seq[i - 1].resultado) changes++;
  const chop = changes / (n - 1);
  const trend = 1 - chop;
  const label = chop >= 0.65 ? 'errático' : chop <= 0.35 ? 'tendência' : 'neutro';
  return { trend, chop, label };
}
// Estratégias de "quebra/alternância" só fazem sentido em regime errático;
// estratégias de "sequência" só em regime de tendência. Filtra ruído.
function regimeFit(cand, regime) {
  const k = cand.key;
  if (/SEQ_(QUEBRA|SURF|PINGPONG|2x2|EXTEND|3_1|MOMENTUM|ALT)/.test(k)) return regime.chop >= 0.45;
  if (/SEQ_(REP|STRONG_STREAK)|BANKER_BIAS/.test(k)) return regime.trend >= 0.45;
  return true; // numéricas e de empate são neutras
}

const STRATS = [
  function seqRepeticao({ seq }) {
    const n = streak(seq);
    if (n < 3 || n > 7) return null;
    const strength = [0, 0, 0, 72, 78, 84, 86, 86][n] || 86;
    return { cor: seq[0].resultado, key: `SEQ_REP_${n}`, nome: `Sequência x${n}`, emoji: '🔁', strength, tipo: 'seq' };
  },
  function seqQuebra({ seq }) {
    const n = streak(seq);
    if (n < 4 || n > 8) return null;
    const strength = [0, 0, 0, 0, 74, 80, 84, 85, 85][n] || 85;
    return { cor: opp(seq[0].resultado), key: `SEQ_QUEBRA_${n}`, nome: `Quebra x${n}`, emoji: '💥', strength, tipo: 'seq' };
  },
  function seqSurf({ seq }) {
    const L = alternation(seq);
    if (L < 4) return null;
    const strength = L >= 6 ? 84 : L >= 5 ? 78 : 72;
    return { cor: opp(seq[0].resultado), key: `SEQ_SURF_${L}`, nome: `Surf ${L}`, emoji: '🌊', strength, tipo: 'seq' };
  },
  function seqDoisDois({ seq }) {
    if (seq.length < 4) return null;
    const [a, b, c, d] = seq.map((x) => x.resultado);
    if (a === b && c === d && a !== c) return { cor: c, key: 'SEQ_2x2', nome: 'Bloco 2+2', emoji: '♟️', strength: 76, tipo: 'seq' };
    return null;
  },
  function seqTresUm({ seq }) {
    if (seq.length < 4) return null;
    const [a, b, c, d] = seq.map((x) => x.resultado);
    if (a !== b && b === c && c === d) return { cor: a, key: 'SEQ_3_1', nome: '3→1 confirmado', emoji: '🧩', strength: 78, tipo: 'seq' };
    return null;
  },
  function seqMomentum2({ seq }) { // NOVA v18: [X, Y, Y] — adversário confirmou 2 seguidas
    if (seq.length < 3) return null;
    const [a, b, c] = seq.map((x) => x.resultado);
    if (a !== b && b === c) return { cor: c, key: 'SEQ_MOMENTUM_2', nome: 'Momentum 2', emoji: '⚡', strength: 77, tipo: 'seq' };
    return null;
  },
  function seqBreakWeak4({ seq }) { // NOVA v18: sequência de 4 com margens fracas → tende a quebrar
    const n = streak(seq);
    if (n !== 4) return null;
    const s = seq.slice(0, 4);
    if (!s.every((r) => hasScore(r) && margem(r) <= 2)) return null;
    return { cor: opp(seq[0].resultado), key: 'SEQ_BREAK_WEAK_4', nome: 'Quebra fraca x4', emoji: '🩹', strength: 79, tipo: 'seq' };
  },
  function seqExtend({ seq }) {
    if (seq.length < 5) return null;
    const [a, b, c, d, e] = seq.map((x) => x.resultado);
    if (a === b && b === c && c !== d && d === e) {
      return { cor: a, key: 'SEQ_EXTEND', nome: 'Streak extend', emoji: '⏭️', strength: 80, tipo: 'seq' };
    }
    return null;
  },
  function seqStrongStreak({ seq }) {
    const n = streak(seq);
    if (n < 4 || n > 6) return null;
    const same = seq.slice(0, n);
    if (!same.every((r) => hasScore(r) && margem(r) >= 3)) return null;
    return { cor: seq[0].resultado, key: 'SEQ_STRONG_STREAK', nome: `Dragon ${n}`, emoji: '🐉', strength: 82, tipo: 'seq' };
  },
  function seqPingPong({ seq }) {
    const L = alternation(seq);
    if (L < 4 || L > 10) return null;
    return { cor: opp(seq[0].resultado), key: 'SEQ_PINGPONG', nome: `Ping-pong ${L}`, emoji: '🏓', strength: 79, tipo: 'seq' };
  },
  function seqAlternatingStrong({ seq }) {
    const L = alternation(seq);
    if (L < 3) return null;
    const s = seq.slice(0, L);
    if (!s.every((r) => hasScore(r) && margem(r) >= 3)) return null;
    return { cor: opp(seq[0].resultado), key: 'SEQ_ALT_STRONG', nome: `Alt. forte ${L}`, emoji: '⚔️', strength: 78, tipo: 'seq' };
  },
  function bankerBias20({ seq }) {
    const s = seq.slice(0, 20);
    if (s.length < 15) return null;
    const b = s.filter((r) => r.outcome === B).length;
    const j = s.filter((r) => r.outcome === J).length;
    if (b >= 12) return { cor: B, key: 'BANKER_BIAS_20', nome: 'Viés Banca', emoji: '📉', strength: 80, tipo: 'seq' };
    if (j >= 12) return { cor: J, key: 'BANKER_BIAS_20', nome: 'Viés Jogador', emoji: '📈', strength: 80, tipo: 'seq' };
    return null;
  },
  function numEscada({ seq }) {
    if (seq.length < 3 || !seq.slice(0, 3).every(hasScore)) return null;
    const [a, b, c] = seq;
    if (a.resultado !== b.resultado || b.resultado !== c.resultado) return null;
    const sa = scoreOf(a), sb = scoreOf(b), sc = scoreOf(c);
    if ((sb - sa === 1 && sc - sb === 1) || (sa - sb === 1 && sb - sc === 1)) {
      return { cor: a.resultado, key: 'NUM_ESCADA', nome: `Escada ${sa}→${sb}→${sc}`, emoji: '🪜', strength: 88, tipo: 'num' };
    }
    return null;
  },
  function numRepeteScore({ seq }) {
    if (seq.length < 2 || !hasScore(seq[0]) || !hasScore(seq[1])) return null;
    const [a, b] = seq;
    if (a.resultado !== b.resultado) return null;
    if (scoreOf(a) === scoreOf(b)) return { cor: a.resultado, key: 'NUM_REPETE', nome: `Score repetido ${scoreOf(a)}`, emoji: '🔂', strength: 84, tipo: 'num' };
    return null;
  },
  function numThreeLow({ seq }) { // NOVA v18: 3 vitórias seguidas com scores baixos → virada
    if (seq.length < 3 || !seq.slice(0, 3).every(hasScore)) return null;
    const [a, b, c] = seq;
    if (a.resultado !== b.resultado || b.resultado !== c.resultado) return null;
    if (scoreOf(a) <= 3 && scoreOf(b) <= 3 && scoreOf(c) <= 3) {
      return { cor: opp(a.resultado), key: 'NUM_THREE_LOW', nome: 'Virada de baixos x3', emoji: '📉', strength: 81, tipo: 'num' };
    }
    return null;
  },
  function numGatilho_0({ seq }) {
    if (!seq.length || !hasScore(seq[0])) return null;
    if (scoreOf(seq[0]) === 0) return { cor: opp(seq[0].resultado), key: 'NUM_GATILHO_0', nome: 'Gatilho 0', emoji: '🎯', strength: 86, tipo: 'num' };
    return null;
  },
  function numGatilho_1({ seq }) {
    if (!seq.length || !hasScore(seq[0])) return null;
    if (scoreOf(seq[0]) === 1) return { cor: opp(seq[0].resultado), key: 'NUM_GATILHO_1', nome: 'Gatilho 1', emoji: '🎯', strength: 84, tipo: 'num' };
    return null;
  },
  function numGatilho_9({ seq }) {
    if (!seq.length || !hasScore(seq[0])) return null;
    if (scoreOf(seq[0]) === 9) return { cor: opp(seq[0].resultado), key: 'NUM_GATILHO_9', nome: 'Gatilho 9', emoji: '🎯', strength: 88, tipo: 'num' };
    return null;
  },
  function numGatilho_8({ seq }) {
    if (!seq.length || !hasScore(seq[0])) return null;
    if (scoreOf(seq[0]) === 8) return { cor: opp(seq[0].resultado), key: 'NUM_GATILHO_8', nome: 'Gatilho 8', emoji: '🎯', strength: 78, tipo: 'num' };
    return null;
  },
  function numMargemAlta({ rows }) {
    const r = rows[0];
    if (!r || !isCol(r.outcome) || !hasScore(r) || margem(r) < 5) return null;
    return { cor: r.outcome, key: 'NUM_MARGEM5', nome: `Margem alta ${r.pj}×${r.pb}`, emoji: '📊', strength: 82, tipo: 'num' };
  },
  function numDiff4x3({ seq }) {
    if (seq.length < 3) return null;
    const t = seq.slice(0, 3);
    if (!t.every((r) => hasScore(r) && margem(r) >= 4 && r.outcome === t[0].resultado)) return null;
    return { cor: t[0].resultado, key: 'NUM_DIFF4x3', nome: 'Diff ≥4 ×3', emoji: '🔢', strength: 85, tipo: 'num' };
  },
  function numViradaBaixa({ seq }) {
    if (seq.length < 2) return null;
    const [a, b] = seq;
    if (!hasScore(a) || !hasScore(b) || a.resultado !== b.resultado) return null;
    if (scoreOf(a) > 4 || scoreOf(b) > 4) return null;
    return { cor: opp(a.resultado), key: 'NUM_VIRADA_BAIXA', nome: 'Virada de baixos', emoji: '🔄', strength: 80, tipo: 'num' };
  },
  function numEspelho({ rows }) {
    const r = rows[0];
    if (!r || !hasScore(r) || r.pj !== r.pb) return null;
    return { cor: r.outcome, key: 'NUM_ESPELHO', nome: `Espelho ${r.pj}×${r.pb}`, emoji: '🪞', strength: 83, tipo: 'num' };
  },
  function numTendencia({ rows }) {
    const s = rows.slice(0, 8).filter(hasScore);
    if (s.length < 6) return null;
    const mj = s.reduce((a, r) => a + r.pj, 0) / s.length;
    const mb = s.reduce((a, r) => a + r.pb, 0) / s.length;
    const diff = mj - mb;
    if (Math.abs(diff) < 1.5) return null;
    return {
      cor: diff > 0 ? J : B, key: 'NUM_TENDENCIA',
      nome: `Tendência ${mj.toFixed(1)}×${mb.toFixed(1)}`, emoji: '📈',
      strength: Math.abs(diff) >= 2.2 ? 84 : 76, tipo: 'num'
    };
  },
  function numRangeSalto({ seq }) {
    if (seq.length < 2 || !hasScore(seq[0]) || !hasScore(seq[1])) return null;
    const [a, b] = seq;
    const ra = scoreRange(scoreOf(a)), rb = scoreRange(scoreOf(b));
    if (!ra || !rb) return null;
    if ((ra === 'MB' && rb === 'MA') || (ra === 'MA' && rb === 'MB')) {
      return { cor: opp(a.resultado), key: 'NUM_SALTO_RANGE', nome: `Salto ${scoreOf(b)}→${scoreOf(a)}`, emoji: '📐', strength: 81, tipo: 'num' };
    }
    return null;
  },
  function numScoreTrend3({ seq }) {
    if (seq.length < 3 || !seq.slice(0, 3).every(hasScore)) return null;
    const [a, b, c] = seq;
    if (a.resultado !== b.resultado || b.resultado !== c.resultado) return null;
    const sa = scoreOf(a), sb = scoreOf(b), sc = scoreOf(c);
    if (sa < sb && sb < sc) {
      return { cor: a.resultado, key: 'NUM_SCORE_TREND_3', nome: `Escala ascendente ${sa}→${sc}`, emoji: '📊', strength: 82, tipo: 'num' };
    }
    return null;
  },
  function numDoubleHigh({ seq }) {
    if (seq.length < 2 || !seq.slice(0, 2).every(hasScore)) return null;
    const [a, b] = seq;
    if (a.resultado !== b.resultado) return null;
    if (margem(a) >= 5 && margem(b) >= 5) {
      return { cor: a.resultado, key: 'NUM_DOUBLE_HIGH', nome: 'Dupla margem alta', emoji: '💪', strength: 84, tipo: 'num' };
    }
    return null;
  },
  function numNatural({ rows }) {
    const r = rows[0];
    if (!r || !hasScore(r)) return null;
    if (r.pj >= 8 && r.pb >= 8) {
      return { cor: r.outcome, key: 'NUM_NATURAL', nome: `Natural ${r.pj}×${r.pb}`, emoji: '🎴', strength: 86, tipo: 'num' };
    }
    return null;
  },
  function numFibonacci({ seq }) {
    if (!seq.length || !hasScore(seq[0])) return null;
    const s = scoreOf(seq[0]);
    if (s === 5 || s === 8) {
      return { cor: seq[0].resultado, key: 'NUM_FIBONACCI', nome: `Fibonacci ${s}`, emoji: '🌀', strength: 80, tipo: 'num' };
    }
    return null;
  },
  function numMomentumShift({ seq }) {
    if (seq.length < 4 || !seq.slice(0, 4).every(hasScore)) return null;
    const [a, b, c, d] = seq;
    const sa = scoreOf(a), sb = scoreOf(b), sc = scoreOf(c), sd = scoreOf(d);
    const m1 = sa - sb, m2 = sb - sc, m3 = sc - sd;
    if (m1 < 0 && m2 < 0 && m3 < 0) {
      return { cor: opp(a.resultado), key: 'MOMENTUM_SHIFT', nome: 'Mudança de momentum', emoji: '🔀', strength: 78, tipo: 'num' };
    }
    return null;
  },
  function numPosEmpate({ rows, seq }) {
    if (!rows[0] || rows[0].resultado !== E || !seq.length) return null;
    return { cor: seq[0].resultado, key: 'NUM_POS_EMPATE', nome: 'Pós-empate', emoji: '🎯', strength: 74, tipo: 'num' };
  },
  function tieCascade({ rows }) {
    const ties = rows.slice(0, 3).filter((r) => r.outcome === E).length;
    if (ties < 2) return null;
    const pre = rows.find((r) => isCol(r.outcome));
    if (!pre) return null;
    return { cor: pre.resultado, key: 'TIE_CASCADE', nome: 'Cascata de empates', emoji: '🟡', strength: 76, tipo: 'num' };
  },
  function tieSingle({ rows, seq }) {
    if (!rows[0] || rows[0].resultado !== E) return null;
    if (!rows[1] || rows[1].resultado === E) return null;
    if (!seq.length) return null;
    return { cor: opp(seq[0].resultado), key: 'TIE_SINGLE', nome: 'Empate isolado', emoji: '🔀', strength: 77, tipo: 'num' };
  },
  function tieDoubleBack({ rows }) { // NOVA v18: dois empates seguidos → volta a cor anterior
    if (rows.length < 3) return null;
    if (rows[0].resultado !== E || rows[1].resultado !== E) return null;
    if (!isCol(rows[2].resultado)) return null;
    return { cor: rows[2].resultado, key: 'TIE_DOUBLE_BACK', nome: 'Retorno pós-duplo empate', emoji: '🔙', strength: 78, tipo: 'num' };
  }
];

function runStrategies(rows) {
  const seq = rows.filter((r) => isCol(r.outcome));
  const ctx = { rows, seq };
  const out = [];
  for (const fn of STRATS) {
    try { const r = fn(ctx); if (r && isCol(r.cor)) out.push(r); } catch (_) {}
  }
  return out;
}


function wilsonLower(w,n,z=1.96){ if(!n)return 0; const p=w/n,z2=z*z; return (p+z2/(2*n)-z*Math.sqrt((p*(1-p)+z2/(4*n))/n))/(1+z2/n); }
function backtest(history, _maxGale=2, maxGapMs=7*60*1000){
  const per={};
  // The first 60% defines the training history; the final 40% is held out.
  // Score only the next decisive color: Gale wins are not evidence of prediction skill.
  const holdoutStart=Math.max(30,Math.floor(history.length*0.6));
  for(let i=holdoutStart;i<history.length;i++){
    const rows=history.slice(Math.max(0,i-30),i).reverse().map(r=>({...r,resultado:r.outcome,pj:r.player,pb:r.banker}));
    for(const c of runStrategies(rows)){
      const observed=history[i], previous=history[i-1];
      if(!observed || !previous || observed.at-previous.at>maxGapMs || observed.outcome===E) continue;
      const p=per[c.key]||(per[c.key]={n:0,w:0}); p.n++; if(observed.outcome===c.cor)p.w++;
    }
  }
  for(const p of Object.values(per)){p.rate=p.n?p.w/p.n:0;p.lower=wilsonLower(p.w,p.n,3.0);}
  return per;
}
let cachedSignature='', cachedPer={};
function analyze(history,{minSamples=60,minLowerBound=0.55,maxGale=2,maxGapMs=7*60*1000}={}){
  if(history.length<60)return {candidate:null,per:{}};
  const tail=history.at(-1); const signature=`${history.length}:${tail?.id}:${tail?.at}`;
  if(signature!==cachedSignature){cachedPer=backtest(history,maxGale,maxGapMs);cachedSignature=signature;}
  const recent=history.slice(-30).reverse().map(r=>({...r,resultado:r.outcome,pj:r.player,pb:r.banker}));
  const seq=recent.filter(r=>isCol(r.outcome));
  const regime=detectRegime(seq);
  const per=cachedPer;
  const eligible=runStrategies(recent).filter(c=>regimeFit(c,regime)).map(c=>({...c,evidence:per[c.key]||{n:0,w:0,rate:0,lower:0}})).filter(c=>c.evidence.n>=minSamples&&c.evidence.lower>=minLowerBound);
  if(!eligible.length)return {candidate:null,per,regime};
  const groups=new Map();
  for(const c of eligible){if(!groups.has(c.cor))groups.set(c.cor,[]);groups.get(c.cor).push(c);}
  const ranked=[...groups.entries()].map(([color,list])=>{const champion=[...list].sort((a,b)=>b.evidence.lower-a.evidence.lower)[0];return {color,list,champion,lower:champion.evidence.lower,rate:champion.evidence.rate,n:champion.evidence.n};}).sort((a,b)=>b.lower-a.lower||b.list.length-a.list.length);
  const best=ranked[0];
  return {candidate:{color:best.color,strategy:best.champion.nome,key:best.champion.key,agree:best.list.length,rate:best.rate,lower:best.lower,n:best.n},per,regime};
}
module.exports={analyze,backtest,wilsonLower,STRATS};
