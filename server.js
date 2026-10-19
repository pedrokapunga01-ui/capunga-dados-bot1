'use strict';

/**
 * CAPUNGA DADOS — MODO PAUSA
 *
 * Substitui temporariamente o server.js no Render quando quiseres desligar os sinais.
 * Ao arrancar:
 *   1) Envia UMA mensagem: "Robô desligado. Volte mais tarde."
 *   2) Envia UM sticker
 *   3) Não envia mais nada (sem sync Bac Bo, sem sinais)
 *
 * Para reactivar: volta a colocar o server.js original e faz deploy.
 *
 * Env necessárias: BOT_TOKEN, CHAT_ID
 * Opcional: PORT, STICKER_ID (file_id de um sticker do Telegram)
 */

const express = require('express');
const axios = require('axios');

const app = express();
app.disable('x-powered-by');

const PORT = Number(process.env.PORT || 10000);
const BOT_TOKEN = process.env.BOT_TOKEN || '';
const CHAT_ID = process.env.CHAT_ID || '';
// Sticker padrão (podes trocar pelo file_id do teu sticker)
const STICKER_ID = (process.env.STICKER_ID || '').trim();

let avisoEnviado = false;

function horaAngola() {
  return new Intl.DateTimeFormat('pt-PT', {
    timeZone: 'Africa/Luanda',
    dateStyle: 'short',
    timeStyle: 'medium',
    hour12: false
  }).format(new Date());
}

async function sendMessage(text) {
  if (!BOT_TOKEN || !CHAT_ID) {
    console.error('[PAUSA] BOT_TOKEN ou CHAT_ID em falta');
    return false;
  }
  try {
    const r = await axios.post(
      `https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`,
      {
        chat_id: CHAT_ID,
        text,
        disable_web_page_preview: true
      },
      { timeout: 15000 }
    );
    return r.data?.ok === true;
  } catch (e) {
    console.error('[PAUSA] Erro mensagem:', e.response?.data || e.message);
    return false;
  }
}

async function sendSticker(fileId) {
  if (!BOT_TOKEN || !CHAT_ID || !fileId) return false;
  // Ignora placeholder inválido
  if (!fileId || fileId.length < 10) {
    console.log('[PAUSA] STICKER_ID não configurado — só mensagem de texto');
    return false;
  }
  try {
    const r = await axios.post(
      `https://api.telegram.org/bot${BOT_TOKEN}/sendSticker`,
      { chat_id: CHAT_ID, sticker: fileId },
      { timeout: 15000 }
    );
    return r.data?.ok === true;
  } catch (e) {
    console.error('[PAUSA] Erro sticker:', e.response?.data || e.message);
    return false;
  }
}

async function avisarUmaVez() {
  if (avisoEnviado) return;
  avisoEnviado = true;

  const texto = [
    '╔════════════════════════╗',
    '║  ⏸️  ROBÔ DESLIGADO  ⏸️  ║',
    '╚════════════════════════╝',
    '',
    '🛑 Robô desligado.',
    '⏳ Volte mais tarde.',
    '',
    `🕒 ${horaAngola()}`
  ].join('\n');

  const okMsg = await sendMessage(texto);
  console.log('[PAUSA] Mensagem enviada:', okMsg);

  const okSticker = await sendSticker(STICKER_ID);
  console.log('[PAUSA] Sticker enviado:', okSticker);

  console.log('[PAUSA] Modo pausa activo — sem mais mensagens.');
}

app.get('/', (req, res) => {
  res.json({
    nome: 'Capunga Dados',
    modo: 'PAUSA',
    estado: 'desligado',
    mensagem: 'Robô desligado. Volte mais tarde.',
    horaAngola: horaAngola()
  });
});

app.get('/health', (req, res) => {
  res.json({
    sucesso: true,
    modo: 'PAUSA',
    estado: 'desligado',
    horaAngola: horaAngola()
  });
});

app.get('/diagnostico', (req, res) => {
  res.json({
    sucesso: true,
    modo: 'PAUSA',
    sinais: false,
    avisoEnviado,
    horaAngola: horaAngola()
  });
});

// Qualquer outra rota — silêncio (só JSON)
app.use((req, res) => {
  res.status(503).json({
    sucesso: false,
    modo: 'PAUSA',
    erro: 'Robô desligado. Volte mais tarde.'
  });
});

app.listen(PORT, '0.0.0.0', () => {
  console.log('======================================');
  console.log('CAPUNGA DADOS — MODO PAUSA');
  console.log(`Porta ${PORT} | ${horaAngola()}`);
  console.log('Sem sinais · Sem sync Bac Bo');
  console.log('======================================');
  // Envia o aviso poucos segundos após subir
  setTimeout(() => {
    avisarUmaVez().catch((e) => console.error('[PAUSA]', e.message));
  }, 2000);
});
