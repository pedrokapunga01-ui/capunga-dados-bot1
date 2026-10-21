
'use strict';

/**
 * CAPUNGA DADOS — BAC BO
 * Servidor Telegram com controlo de sinais e Gale 2.
 *
 * Requer Node.js 18+
 *
 * Variáveis obrigatórias:
 * BOT_TOKEN
 * CHAT_ID
 * INGEST_TOKEN
 *
 * Opcional:
 * PORT=3000
 * MAX_SINAIS_HORA=25
 * MIN_AMOSTRA=60
 * MIN_VANTAGEM=0.05
 * INTERVALO_MINIMO_MS=60000
 *
 * POST /ingest
 * GET  /health
 * GET  /status
 * POST /result
 *
 * Não garante ganhos nem prevê resultados com certeza.
 */

const http = require('node:http');
const crypto = require('node:crypto');

const CONFIG = {
  port: Number(process.env.PORT || 3000),
  botToken: process.env.BOT_TOKEN || '',
  chatId: process.env.CHAT_ID || '',
  ingestToken: process.env.INGEST_TOKEN || '',

  maxSinaisHora: Math.max(
    1,
    Math.min(25, Number(process.env.MAX_SINAIS_HORA || 25))
  ),

  minAmostra: Math.max(
    30,
    Number(process.env.MIN_AMOSTRA || 60)
  ),

  minVantagem: Math.max(
    0,
    Math.min(0.5, Number(process.env.MIN_VANTAGEM || 0.05))
  ),

  intervaloMinimoMs: Math.max(
    0,
    Number(process.env.INTERVALO_MINIMO_MS || 60000)
  ),

  historicoMaximo: 2000,
  galeMaximo: 2,
  timeoutMs: 10000
};

if (!CONFIG.botToken || !CONFIG.chatId || !CONFIG.ingestToken) {
  console.error(
    '[CONFIG] Define BOT_TOKEN, CHAT_ID e INGEST_TOKEN.'
  );
  process.exit(1);
}

const estado = {
  iniciadoEm: new Date().toISOString(),
  rodadas: [],
  sinais: [],
  ultimoSinalEm: 0,
  ultimaRodadaId: null,
  ultimoErro: null,
  recebidos: 0,
  duplicados: 0,
  publicados: 0,
  resultados: {
    GREEN: 0,
    G1: 0,
    G2: 0,
    LOSS: 0,
    PENDENTE: 0,
    CANCELADO: 0
  }
};

function agora() {
  return new Date().toISOString();
}

function idSeguro(valor) {
  return crypto
    .createHash('sha256')
    .update(String(valor))
    .digest('hex');
}

function resposta(res, status, dados) {
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store'
  });

  res.end(JSON.stringify(dados, null, 2));
}

function lerCor(valor) {
  if (typeof valor !== 'string') return null;

  const cor = valor.trim().toUpperCase();

  if (['RED', 'VERMELHO', 'R'].includes(cor)) return 'RED';
  if (['BLUE', 'AZUL', 'B'].includes(cor)) return 'BLUE';
  if (['TIE', 'EMPATE', 'DRAW', 'T'].includes(cor)) return 'TIE';

  return null;
}

function lerJSON(req) {
  return new Promise((resolve, reject) => {
    let corpo = '';
    let tamanho = 0;
    const limite = 64 * 1024;

    req.on('data', parte => {
      tamanho += parte.length;

      if (tamanho > limite) {
        reject(new Error('Corpo demasiado grande.'));
        req.destroy();
        return;
      }

      corpo += parte;
    });

    req.on('end', () => {
      try {
        resolve(JSON.parse(corpo || '{}'));
      } catch {
        reject(new Error('JSON inválido.'));
      }
    });

    req.on('error', reject);
  });
}

function autorizado(req) {
  const fornecido = String(req.headers['x-ingest-token'] || '');
  const esperado = CONFIG.ingestToken;

  if (!fornecido || fornecido.length > 500) return false;

  const a = Buffer.from(fornecido);
  const b = Buffer.from(esperado);

  return a.length === b.length &&
    crypto.timingSafeEqual(a, b);
}

async function telegram(texto) {
  const url =
    `https://api.telegram.org/bot${CONFIG.botToken}/sendMessage`;

  const controller = new AbortController();
  const timeout = setTimeout(
    () => controller.abort(),
    CONFIG.timeoutMs
  );

  try {
    const r = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        chat_id: CONFIG.chatId,
        text: texto,
        disable_web_page_preview: true
      }),
      signal: controller.signal
    });

    const dados = await r.json();

    if (!r.ok || !dados.ok) {
      throw new Error(
        dados.description || `Telegram HTTP ${r.status}`
      );
    }

    return dados.result;
  } finally {
    clearTimeout(timeout);
  }
}

function estatisticas() {
  const validas = estado.rodadas.filter(
    r => r.cor === 'RED' || r.cor === 'BLUE'
  );

  const reds = validas.filter(r => r.cor === 'RED').length;
  const blues = validas.length - reds;

  const total = validas.length;

  return {
    total,
    reds,
    blues,
    empate: estado.rodadas.filter(r => r.cor === 'TIE').length,
    taxaRed: total ? reds / total : 0,
    taxaBlue: total ? blues / total : 0
  };
}

/**
 * Escolhe uma direção apenas quando a amostra permite
 * comparar frequências observadas.
 *
 * Isto NÃO é uma previsão garantida. Em jogos independentes,
 * a frequência passada pode não ter valor preditivo futuro.
 */
function calcularCandidato() {
  const s = estatisticas();

  if (s.total < CONFIG.minAmostra) {
    return {
      aprovado: false,
      motivo: 'Amostra insuficiente',
      amostra: s.total
    };
  }

  const vantagem = Math.abs(s.taxaRed - s.taxaBlue);
  const direcao = s.taxaRed > s.taxaBlue ? 'RED' : 'BLUE';

  if (vantagem < CONFIG.minVantagem) {
    return {
      aprovado: false,
      motivo: 'Diferença estatística abaixo do mínimo',
      amostra: s.total,
      vantagem
    };
  }

  return {
    aprovado: true,
    direcao,
    amostra: s.total,
    vantagem,
    frequenciaRed: s.taxaRed,
    frequenciaBlue: s.taxaBlue
  };
}

function sinaisNaUltimaHora() {
  const limite = Date.now() - 60 * 60 * 1000;

  return estado.sinais.filter(
    s => new Date(s.criadoEm).getTime() >= limite
  ).length;
}

function criarTextoSinal(sinal) {
  return [
    '🎲 CAPUNGA DADOS — BAC BO',
    '',
    `📌 Sinal: ${sinal.direcao}`,
    '🛡 Gale máximo: 2',
    '🤝 Empate: aguardar e registar separadamente',
    `📊 Amostra histórica: ${sinal.amostra}`,
    `📈 Diferença observada: ${(sinal.vantagem * 100).toFixed(1)}%`,
    '',
    '⚠️ Estatística histórica não garante a próxima rodada.',
    '⚠️ Não aumentar apostas para recuperar perdas.',
    '',
    `🆔 ID: ${sinal.id}`,
    `🕒 ${sinal.criadoEm}`
  ].join('\n');
}

async function publicarSinal(candidato) {
  const quantidade = sinaisNaUltimaHora();

  if (quantidade >= CONFIG.maxSinaisHora) {
    return { publicado: false, motivo: 'Limite horário atingido' };
  }

  const agoraMs = Date.now();

  if (
    agoraMs - estado.ultimoSinalEm <
    CONFIG.intervaloMinimoMs
  ) {
    return { publicado: false, motivo: 'Intervalo mínimo não cumprido' };
  }

  const sinal = {
    id: crypto.randomUUID(),
    direcao: candidato.direcao,
    amostra: candidato.amostra,
    vantagem: candidato.vantagem,
    criadoEm: agora(),
    estado: 'PENDENTE',
    gale: 0,
    rodadaReferencia: estado.ultimaRodadaId
  };

  // Não publicar antes de o Telegram confirmar o envio.
  await telegram(criarTextoSinal(sinal));

  estado.sinais.push(sinal);
  estado.ultimoSinalEm = Date.now();
  estado.publicados++;

  console.log('[SINAL PUBLICADO]', sinal.id, sinal.direcao);

  return { publicado: true, sinal };
}

function atualizarResultado(sinal, cor) {
  if (sinal.estado !== 'PENDENTE') {
    return { atualizado: false, motivo: 'Sinal já encerrado' };
  }

  if (cor === 'TIE') {
    return {
      atualizado: false,
      motivo: 'Empate registado; não conta como vitória ou derrota'
    };
  }

  if (cor === sinal.direcao) {
    sinal.estado = sinal.gale === 0 ? 'GREEN' : `G${sinal.gale}`;
    estado.resultados[sinal.estado]++;
    return { atualizado: true, estado: sinal.estado };
  }

  if (sinal.gale < CONFIG.galeMaximo) {
    sinal.gale++;
    return {
      atualizado: true,
      estado: 'PENDENTE',
      gale: sinal.gale
    };
  }

  sinal.estado = 'LOSS';
  estado.resultados.LOSS++;

  return { atualizado: true, estado: 'LOSS' };
}

async function processarRodada(dados) {
  const cor = lerCor(dados.cor || dados.resultado);

  if (!cor) {
    throw new Error('Cor inválida: usa RED, BLUE ou TIE.');
  }

  const rodadaId = String(
    dados.rodadaId || dados.id || ''
  ).trim();

  if (!rodadaId) {
    throw new Error('Falta rodadaId único.');
  }

  if (estado.rodadas.some(r => r.id === rodadaId)) {
    estado.duplicados++;
    return { duplicado: true, rodadaId };
  }

  const rodada = {
    id: rodadaId,
    cor,
    timestamp: dados.timestamp || agora(),
    mesa: dados.mesa ? String(dados.mesa).slice(0, 100) : null,
    apostadores: Number.isFinite(Number(dados.apostadores))
      ? Number(dados.apostadores)
      : null
  };

  if (
    rodada.apostadores !== null &&
    (!Number.isFinite(rodada.apostadores) ||
     rodada.apostadores < 0)
  ) {
    rodada.apostadores = null;
  }

  estado.rodadas.push(rodada);
  estado.recebidos++;
  estado.ultimaRodadaId = rodadaId;

  if (estado.rodadas.length > CONFIG.historicoMaximo) {
    estado.rodadas.shift();
  }

  // Atualiza o sinal pendente mais recente.
  const pendente = [...estado.sinais]
    .reverse()
    .find(s => s.estado === 'PENDENTE');

  if (pendente && rodadaId !== pendente.rodadaReferencia) {
    const resultado = atualizarResultado(pendente, cor);

    if (resultado.atualizado) {
      console.log('[RESULTADO]', pendente.id, resultado);
    }
  }

  // O empate não gera um novo sinal.
  if (cor === 'TIE') {
    return { rodada, sinal: null, motivo: 'Empate' };
  }

  const candidato = calcularCandidato();

  if (!candidato.aprovado) {
    return { rodada, sinal: null, motivo: candidato.motivo };
  }

  // Evita vários sinais simultaneamente sem resolver o anterior.
  const aindaPendente = estado.sinais.some(
    s => s.estado === 'PENDENTE'
  );

  if (aindaPendente) {
    return {
      rodada,
      sinal: null,
      motivo: 'Existe um sinal anterior pendente'
    };
  }

  const publicado = await publicarSinal(candidato);

  return { rodada, sinal: publicado };
}

function statusPublico() {
  return {
    nome: 'CAPUNGA DADOS — BAC BO',
    iniciadoEm: estado.iniciadoEm,
    agora: agora(),
    configuracao: {
      maxSinaisHora: CONFIG.maxSinaisHora,
      minAmostra: CONFIG.minAmostra,
      minVantagem: CONFIG.minVantagem,
      galeMaximo: CONFIG.galeMaximo
    },
    estatisticas: estatisticas(),
    sinaisUltimaHora: sinaisNaUltimaHora(),
    recebidos: estado.recebidos,
    duplicados: estado.duplicados,
    publicados: estado.publicados,
    resultados: estado.resultados,
    ultimaRodada: estado.ultimaRodadaId,
    ultimoErro: estado.ultimoErro
  };
}

const servidor = http.createServer(async (req, res) => {
  const url = new URL(
    req.url,
    `http://${req.headers.host || 'localhost'}`
  );

  try {
    if (req.method === 'GET' && url.pathname === '/health') {
      return resposta(res, 200, {
        ok: true,
        servico: 'CAPUNGA DADOS',
        hora: agora()
      });
    }

    if (req.method === 'GET' && url.pathname === '/status') {
      return resposta(res, 200, statusPublico());
    }

    if (
      req.method === 'POST' &&
      ['/ingest', '/result'].includes(url.pathname)
    ) {
      if (!autorizado(req)) {
        return resposta(res, 401, {
          erro: 'Token de ingestão inválido.'
        });
      }

      const dados = await lerJSON(req);

      if (url.pathname === '/ingest') {
        const resultado = await processarRodada(dados);
        return resposta(res, 200, { ok: true, resultado });
      }

      const cor = lerCor(dados.cor || dados.resultado);
      const id = String(dados.sinalId || '');

      if (!cor || !id) {
        return resposta(res, 400, {
          erro: 'Indica sinalId e cor válida.'
        });
      }

      const sinal = estado.sinais.find(s => s.id === id);

      if (!sinal) {
        return resposta(res, 404, {
          erro: 'Sinal não encontrado.'
        });
      }

      const resultado = atualizarResultado(sinal, cor);

      return resposta(res, 200, {
        ok: true,
        resultado,
        status: statusPublico()
      });
    }

    return resposta(res, 404, { erro: 'Rota não encontrada.' });
  } catch (erro) {
    estado.ultimoErro = {
      hora: agora(),
      mensagem: erro.message
    };

    console.error('[ERRO]', erro);

    return resposta(res, 500, {
      erro: 'Falha ao processar pedido.',
      detalhe: erro.message
    });
  }
});

servidor.listen(CONFIG.port, () => {
  console.log('====================================');
  console.log(' CAPUNGA DADOS — BAC BO');
  console.log(` Porta: ${CONFIG.port}`);
  console.log(` Limite: ${CONFIG.maxSinaisHora}/hora`);
  console.log(` Gale máximo: ${CONFIG.galeMaximo}`);
  console.log('====================================');
});

function encerrar() {
  console.log('[SHUTDOWN] A encerrar servidor...');

  servidor.close(() => process.exit(0));

  setTimeout(() => process.exit(1), 10000).unref();
}

process.on('SIGTERM', encerrar);
process.on('SIGINT', encerrar);

process.on('unhandledRejection', erro => {
  console.error('[PROMISE NÃO TRATADA]', erro);
});

process.on('uncaughtException', erro => {
  console.error('[ERRO FATAL]', erro);
  process.exit(1);
});
