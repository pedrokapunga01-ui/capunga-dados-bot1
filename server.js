JavaScript
'use strict';

/**
 * CAPUNGA DADOS — BAC BO · GALE 2 · SLOTS 3MIN
 * Estratégia multi-camada, PostgreSQL, Telegram e Express.
 * Nenhum método garante resultados no Bac Bo.
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

function boolEnv(v, fb = false) {
  if (v === undefined || v === '') return fb;
  return ['true', '1', 'yes', 'sim', 'on'].includes(
    String(v).trim().toLowerCase()
  );
}

function intEnv(name, fb, min, max) {
  const n = Number.parseInt(process.env[name], 10);
  if (!Number.isFinite(n)) return fb;
  return Math.max(min, Math.min(max, n));
}

const CONFIG = {
  sinaisAtivos: boolEnv(process.env.SINAIS_ATIVOS, true),
  maxGale: 2,
  empateProtegido: boolEnv(process.env.EMPATE_PROTEGIDO, true),
  sinalExpiraMin: intEnv('SINAL_EXPIRA_MIN', 6, 3, 20),
  intervaloSyncMs: intEnv('INTERVALO_SYNC_MS', 6000, 4000, 30000),
  keepAliveMs: intEnv('KEEP_ALIVE_MS', 90000, 30000, 300000),
  consensoMin: intEnv('CONSENSO_MIN', 1, 1, 5),
  minPrioridade: intEnv('MIN_PRIORIDADE', 72, 0, 100),
  slotJanelaMs: intEnv('SLOT_JANELA_MS', 110000, 45000, 170000),
  keepAliveUrl: process.env.KEEP_ALIVE_URL ||
    process.env.RENDER_EXTERNAL_URL || ''
};

/* ================= TEMPO DE ANGOLA ================= */

const TZ = 'Africa/Luanda';

function agoraLuanda() {
  return new Date(
    new Date().toLocaleString('en-US', { timeZone: TZ })
  );
}

function horaAngola() {
  return new Intl.DateTimeFormat('pt-PT', {
    timeZone: TZ,
    dateStyle: 'short',
    timeStyle: 'medium',
    hour12: false
  }).format(new Date());
}

function obterSlotAtual() {
  const now = agoraLuanda();
  const min = now.getMinutes();
  const slotMin = Math.floor(min / 3) * 3;
  const slotStart = new Date(now);
  slotStart.setMinutes(slotMin, 0, 0);

  const elapsed = now.getTime() - slotStart.getTime();
  if (elapsed < 0 || elapsed > CONFIG.slotJanelaMs) return null;

  const y = slotStart.getFullYear();
  const m = String(slotStart.getMonth() + 1).padStart(2, '0');
  const d = String(slotStart.getDate()).padStart(2, '0');
  const h = String(slotStart.getHours()).padStart(2, '0');
  const mi = String(slotMin).padStart(2, '0');

  return `${y}-${m}-${d} ${h}:${mi}`;
}

/* ================= ANÁLISE DA MESA ================= */

const OPOSTO = { Banca: 'Jogador', Jogador: 'Banca' };

function decisivos(hist) {
  return hist.filter(r => r === 'Banca' || r === 'Jogador');
}

function sequencia(dec) {
  if (!dec.length) return { cor: null, tamanho: 0 };

  let n = 1;
  while (n < dec.length && dec[n] === dec[0]) n++;

  return { cor: dec[0], tamanho: n };
}

function contar(dec, n) {
  const s = dec.slice(0, n);
  const banca = s.filter(x => x === 'Banca').length;

  return {
    n: s.length,
    banca,
    jogador: s.length - banca,
    pctBanca: s.length ? banca / s.length : 0.5
  };
}

function zProp(k, n, p0 = 0.5) {
  return n
    ? (k / n - p0) / Math.sqrt((p0 * (1 - p0)) / n)
    : 0;
}

function taxaAlt(dec, n) {
  const s = dec.slice(0, n);
  if (s.length < 2) return 0.5;

  let t = 0;
  for (let i = 1; i < s.length; i++) {
    if (s[i] !== s[i - 1]) t++;
  }

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
    if (Math.abs(z) >= 1.45) {
      viesLongo = z > 0 ? 'Banca' : 'Jogador';
    }
  }

  if (!viesLongo && c100.n >= 70) {
    const z = zProp(c100.banca, c100.n);
    if (Math.abs(z) >= 1.35) {
      viesLongo = z > 0 ? 'Banca' : 'Jogador';
    }
  }

  const modo = alt20 >= 0.58
    ? 'alternante'
    : alt20 <= 0.36
      ? 'streak'
      : 'misto';

  return {
    dec, seq, c8, c12, c20, c50, c100,
    alt12, alt20, viesMedio, viesLongo,
    modo, amostra: dec.length
  };
}

function bonusMesa(cor, mesa) {
  let b = 0;

  if (mesa.viesMedio === cor) b += 11;
  if (mesa.viesLongo === cor) b += 13;
  if (mesa.viesMedio && mesa.viesMedio !== cor) b -= 9;
  if (mesa.viesLongo && mesa.viesLongo !== cor) b -= 6;

  if (mesa.modo === 'streak' &&
      mesa.seq.cor === cor && mesa.seq.tamanho >= 3) b += 9;

  if (mesa.modo === 'alternante' &&
      mesa.seq.cor && OPOSTO[mesa.seq.cor] === cor) b += 9;

  if (mesa.c12.n >= 10) {
    const dom = mesa.c12.banca > mesa.c12.jogador
      ? 'Banca' : 'Jogador';

    if (dom === cor &&
        Math.abs(mesa.c12.banca - mesa.c12.jogador) >= 4) b += 8;
  }

  if (mesa.alt12 <= 0.28 && mesa.seq.cor === cor) b += 6;

  return b;
}