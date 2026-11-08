'use strict';

/**
 * CAPUNGA DADOS — Coletor robusto + Gerador de sinais
 *
 * - Coleta com deteção de lacunas (diff de snapshots)
 * - Sinais só são gerados sobre histórico CONTÍGUO (sem rodadas em falta)
 * - Lacuna => cancela sinal pendente + pausa N rodadas
 * - Confiança do sinal (0-99): concordância entre estratégias + saúde da janela
 * - RESET_ON_DEPLOY=true => limpa histórico e sinais ao arrancar
 *
 * Env: DATABASE_URL BOT_TOKEN CHAT_ID ADMIN_TOKEN
 *   SINAIS_ATIVOS STREAK_MIN MAX_GALE EMPATE_PROTEGIDO MAX_REDS_DIA
 *   SINAL_EXPIRA_MIN CONFIANCA_MIN GAP_PAUSA_RODADAS RESET_ON_DEPLOY
 *   BACBO_SOURCE_MODE BACBO_API_URL POLL_INTERVAL_MS ...
 */

try { require('dotenv').config(); } catch (e) {
  if (e.code !== 'MODULE_NOT_FOUND' || !/dotenv/.test(e.message)) throw e;
}

const express = require('express');
const cors = require('cors');
const axios = require('axios');
const crypto = require('node:crypto');
const { Pool } = require('pg');
const { CasinoScoresSource, DEFAULT_URL } = require('./browser-source');
const { CasinoScoresApiSource, DEFAULT_API_URL } = require('./casino-api-source');

const app = express();
app.disable('x-powered-by');
app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Referrer-Policy', 'no-referrer');
  res.setHeader('X-Frame-Options', 'DENY');