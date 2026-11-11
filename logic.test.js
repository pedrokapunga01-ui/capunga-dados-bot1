'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { normalizeOutcome, toMs, firstArray, normalizeRound, transitionPattern, wilsonLower, settleStatus, resolveWindow, assessFeed, luandaDate } = require('./server');
const strategyEngine = require('./strategy-engine');

test('normaliza resultados em idiomas e formatos comuns', () => {
  assert.equal(normalizeOutcome('PlayerWon'), 'Jogador');
  assert.equal(normalizeOutcome('banker_won'), 'Banca');
  assert.equal(normalizeOutcome('Empate'), 'Empate');
  assert.equal(normalizeOutcome('unknown'), null);
});
test('normaliza timestamps em segundos, milissegundos e ISO', () => {
  assert.equal(toMs(1700000000), 1700000000000);
  assert.equal(toMs('1700000000000'), 1700000000000);
  assert.equal(toMs('not a date'), null);
});
test('extrai arrays encapsulados e normaliza a rodada', () => {
  assert.deepEqual(firstArray({ data: { results: [{ id: 1 }] } }), [{ id: 1 }]);
  const round = normalizeRound({ id: 'r-1', settledAt: '2026-11-11T00:00:00Z', outcome: 'BankerWon', playerScore: 4, bankerScore: 8 });
  assert.equal(round.id, 'r-1');
  assert.equal(round.outcome, 'Banca');
  assert.equal(round.banker, 8);
});
test('descreve apenas sequência recente e exige contexto mínimo', () => {
  const make = (outcome) => ({ outcome });
  const rows = ['Banca','Banca','Banca','Banca','Jogador','Banca','Jogador','Jogador'].map(make);
  assert.deepEqual(transitionPattern(rows), { color: 'Banca', strategy: 'sequência observada 4' });
  assert.equal(transitionPattern(rows.slice(0, 5)), null);
});
test('restaura as 34 regras e rejeita amostra curta para análise', () => {
  assert.equal(strategyEngine.STRATS.length, 34);
  const shortHistory = Array.from({ length: 50 }, (_, i) => ({ id: String(i), at: i * 25000, outcome: i % 2 ? 'Banca' : 'Jogador' }));
  assert.equal(strategyEngine.analyze(shortHistory).candidate, null);
});
test('backtest aleatório não aprova candidatos só porque houve Gale', () => {
  let x = 7;
  const history = Array.from({ length: 1000 }, (_, i) => {
    x = (x * 48271) % 2147483647;
    return { id: String(i), at: i * 25000, outcome: x % 10 < 4 ? 'Jogador' : x % 10 < 8 ? 'Banca' : 'Empate', player: x % 10, banker: (x >> 3) % 10 };
  });
  assert.equal(strategyEngine.analyze(history, { minSamples: 100, minLowerBound: 0.55 }).candidate, null);
});
test('limite Wilson não supera taxa observada e fica abaixo de 50% para amostra equilibrada', () => {
  assert.ok(wilsonLower(50, 100) < 0.5);
  assert.ok(wilsonLower(80, 100) < 0.8);
  assert.equal(wilsonLower(0, 0), 0);
});
test('classifica empate como push e cores como green/loss por tentativa', () => {
  assert.equal(settleStatus({ outcome: 'Empate' }, 'Jogador', 0), 'EMPATE');
  assert.equal(settleStatus({ outcome: 'Jogador' }, 'Jogador', 1), 'G1');
  assert.equal(settleStatus({ outcome: 'Banca' }, 'Jogador', 2), null);
});
test('não transforma uma lacuna temporal em LOSS', () => {
  const base = { at: 0, outcome: 'Banca' };
  const result = resolveWindow(base, [{ at: 8 * 60_000, outcome: 'Jogador' }], 'Banca');
  assert.equal(result.final, null);
  assert.equal(result.uncertain, true);
});
test('mantém GREEN confirmado antes de uma lacuna posterior', () => {
  const base = { at: 0, outcome: 'Banca' };
  const result = resolveWindow(base, [{ at: 30_000, outcome: 'Jogador' }, { at: 8 * 60_000, outcome: 'Banca' }], 'Jogador');
  assert.equal(result.final.status, 'GREEN');
});
test('só fecha LOSS após três tentativas consecutivas e válidas', () => {
  const base = { at: 0, outcome: 'Jogador' };
  const after = [1,2,3].map(i => ({ at: i * 30_000, outcome: 'Banca' }));
  const result = resolveWindow(base, after, 'Jogador');
  assert.equal(result.final.status, 'LOSS');
  assert.equal(result.final.attempt, 2);
});
test('mantém a apuração pendente quando ainda faltam tentativas', () => {
  const base = { at: 0, outcome: 'Jogador' };
  const result = resolveWindow(base, [{ at: 30_000, outcome: 'Banca' }, { at: 60_000, outcome: 'Banca' }], 'Jogador');
  assert.equal(result.final, null);
  assert.equal(result.uncertain, false);
});
test('só considera íntegro o feed fresco que reencontra um ID conhecido', () => {
  const now = Date.now();
  const previous = [{ id: 'old', at: now - 30_000, outcome: 'Banca' }];
  const current = [...previous, { id: 'new', at: now - 1_000, outcome: 'Jogador' }];
  assert.equal(assessFeed(current, { metOverlap: true, includesTrusted: true }, previous, now).healthy, true);
  assert.equal(assessFeed(current, { metOverlap: true, includesTrusted: false }, previous, now).healthy, false);
  assert.equal(assessFeed(current, { metOverlap: false }, previous, now).healthy, false);
});
test('feed obsoleto ou chegada retroativa fica marcado como não confiável', () => {
  const now = Date.now();
  const previous = [{ id: 'old', at: now - 10_000, outcome: 'Banca' }];
  const late = [{ id: 'late', at: now - 200_000, outcome: 'Jogador' }];
  assert.equal(assessFeed(late, { metOverlap: true }, previous, now).healthy, false);
  const reordered = [previous[0], { id: 'older-new', at: now - 20_000, outcome: 'Jogador' }];
  assert.equal(assessFeed(reordered, { metOverlap: true }, previous, now).healthy, false);
});
test('a data usada no histórico é formatada no fuso Angola', () => {
  assert.match(luandaDate(new Date('2026-11-10T23:30:00Z')), /^2026-11-11$/);
});
