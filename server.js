
========================================================================================
FICHEIRO: README-telegram-bacbo-v2.md
========================================================================================

# Bac Bo Telegram — atualização do servidor

Esta atualização integra-se no motor existente em `src/server/capunga/` do aplicativo Bac Bo. Não é um servidor Express independente nem deve substituir o motor do projeto: preserva as regras de decisão e o registo já existentes.

## Alterações incluídas

- Conserva a lógica já existente de sinal seletivo, controlo de cadência, histórico integral e Gale 2.
- Durante a espera por uma entrada, envia uma única mensagem «A analisar o mercado»; apaga-a quando o sinal é anunciado, sem criar mensagens repetidas a cada ciclo.
- Ao fechar uma entrada vencedora, consulta a rodada persistida que a resolveu e indica a cor vencedora e a pontuação real disponível. Em caso de empate, identifica-o como tal.
- Distingue GREEN, G1 e G2 nas mensagens, apresenta os totais/folha do dia e não envia mensagem individual de LOSS. Os losses continuam no registo e nos totais posteriores.
- Inclui um teste automatizado da notificação com resultado e pontuação.

## Aplicação

Copie os ficheiros do ZIP para os mesmos caminhos relativos no projeto, mantendo os ficheiros existentes e substituindo apenas os cinco caminhos incluídos. Não copie quaisquer credenciais ou ficheiros de base de dados. As credenciais Telegram e a configuração de base de dados continuam a ser as já usadas pelo projeto (`BOT_TOKEN`, `CHAT_ID` e configuração de persistência existente).

Teste o motor com:

```sh
node --experimental-strip-types --test src/server/capunga/pure.test.ts
```

O teste cobre, entre outros casos, Gale 2, empate e mensagem com pontuação. Para funcionamento contínuo 24/7, o processo e a base de dados têm de correr num alojamento que não suspenda o serviço e a API externa/Telegram têm de estar acessíveis. Nenhum código pode prometer disponibilidade sem falhas.

## Limite de previsão

Bac Bo é um jogo de azar. Padrões do histórico não permitem garantir 99% ou 100% de acerto; a taxa depende dos resultados futuros e pode variar. Não use a mensagem de «análise» como promessa de previsão nem aumente apostas para recuperar perdas.


========================================================================================
FICHEIRO: src/server/capunga/tick.ts
========================================================================================

import type { CicloPainel, Painel } from "./contrato.ts";
import { classificarSaude, decidirPagina, descreverLacuna, ORCAMENTO_PAGINAS, proximoAtraso, TAMANHO_PAGINA } from "./leitura.ts";
import { analisarMesa } from "./mesa.ts";
import { apiLatestUrl, apiPageUrl, extractItems, parseRounds, separarRodadas } from "./normalize.ts";
import { avancar } from "./avancar.ts";
import { estudar } from "./estudar.ts";
import { resumirFolha, textoPct } from "./folha.ts";
import { ESPERA_MAX_MS, resolverEntrada } from "./espera.ts";
import * as store from "./store.ts";
import { apagar, enviar, estadoAnalise, telegramConfigurado } from "./telegram.ts";
import {
  boolEnv,
  EXPIRA_MS,
  HISTORICO_MS,
  horaLuanda,
  LIVE_EDGE_MS,
  ms,
} from "./tempo.ts";
import {
  horaCurta,
  motivoHumano,
  textoEntrada,
  textoExpirado,
  textoFechoCompleto,
  textoFolha,
  textoGale,
  textoLacuna,
} from "./textos.ts";
import {
  isSide,
  type FolhaNum,
} from "./types.ts";

/**
 * Processamento por cursor.
 * Cada rodada é julgada na hora em que a mesa a fechou (`fim`).
 * Se o processo dormiu e a cauda da API ainda traz as rodadas do meio,
 * o fio aberto atravessa-as por ordem — gale 1, gale 2, fecho —
 * e o Telegram só recebe o estado em que o fio ficou, não uma entrada nova
 * carimbada no acordar.
 * Se faltar uma fatia (pausa maior que a mesa aguenta), o sinal fica sem fio.
 * Não é marcado green nem stop com a primeira rodada que aparece depois.
 */

const THROTTLE_MS = 2_500;
const FILA_POR_CICLO = 400;

interface Runtime {
  ticking: Promise<void> | null;
  lastTickAt: number;
  lastError: string | null;
  lastOkAt: string | null;
  motivo: string;
  http: number | null;
  apiError: string | null;
  retomado: boolean;
  fioNota: string | null;
  ultimoKeep: number;
  relogio: boolean;
  falhasSeguidas: number;
  ultimoPainel: Painel | null;
  aviso: string | null;
  dbOk: boolean;
  ultimaRodadaEm: string | null;
  ultimaGravacaoEm: string | null;
  ciclo: CicloPainel;
  arrancouEm: string;
  arranques: number;
  venenos: Record<string, number>;
  diario: string[];
  retomarPagina: number;
  hidratado: boolean;
}

function cicloVazio(): CicloPainel {
  return {
    inicio: null,
    fim: null,
    recebidas: 0,
    processadas: 0,
    duplicadas: 0,
    rejeitadas: 0,
    paginas: 0,
    duracaoMs: 0,
    tentativas: 0,
    motivoPagina: null,
  };
}

const g = globalThis as typeof globalThis & { __capungaRuntime?: Runtime };

function runtime(): Runtime {
  g.__capungaRuntime ??= {
    ticking: null,
    lastTickAt: 0,
    lastError: null,
    lastOkAt: null,
    motivo: "aguardando",
    http: null,
    apiError: null,
    retomado: false,
    fioNota: null,
    ultimoKeep: 0,
    relogio: false,
    falhasSeguidas: 0,
    ultimoPainel: null,
    aviso: null,
    dbOk: true,
    ultimaRodadaEm: null,
    ultimaGravacaoEm: null,
    ciclo: cicloVazio(),
    arrancouEm: new Date().toISOString(),
    arranques: 1,
    venenos: {},
    diario: [],
    retomarPagina: 0,
    hidratado: false,
  };
  return g.__capungaRuntime;
}

function linha(evento: string, extra: Record<string, unknown> = {}): void {
  const rt = runtime();
  const texto = JSON.stringify({ pulso: evento, em: new Date().toISOString(), ...extra });
  rt.diario.push(texto);
  if (rt.diario.length > 40) rt.diario.splice(0, rt.diario.length - 40);
  console.info(texto);
}

function dormir(msEspera: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, msEspera));
}

async function lerPagina(url: string): Promise<{ itens: unknown[]; http: number; total: number | null; tentativas: number; cru: unknown }> {
  let ultimo: unknown;
  for (let tentativa = 0; tentativa < 3; tentativa += 1) {
    try {
      const response = await fetch(url, {
        headers: { accept: "application/json", "user-agent": "CapungaMesa/2.0" },
        signal: AbortSignal.timeout(8_000),
        cache: "no-store",
      });
      if (!response.ok) throw new Error(`api_${response.status}`);
      const cru: unknown = await response.json();
      const totalRaw = response.headers.get("x-total-count");
      const totalNum = totalRaw === null ? Number.NaN : Number(totalRaw);
      const itens = extractItems(cru);
      return {
        itens: itens.length ? itens : cru && typeof cru === "object" && !Array.isArray(cru) ? [cru] : [],
        http: response.status,
        total: Number.isFinite(totalNum) ? totalNum : null,
        tentativas: tentativa + 1,
        cru,
      };
    } catch (error) {
      ultimo = error;
      if (tentativa < 2) await dormir(400 * (tentativa + 1));
    }
  }
  if (ultimo instanceof Error && (ultimo.name === "TimeoutError" || ultimo.name === "AbortError")) {
    throw new Error("api_timeout");
  }
  throw ultimo instanceof Error ? ultimo : new Error("api");
}

/**
 * Lê a cauda e, se a ponta guardada não estiver nela, continua página a página.
 * O orçamento é só deste ciclo: a página seguinte fica marcada e o pulso retoma.
 * A quantidade já gravada no registo não entra nesta decisão.
 */
async function fetchRounds(pontaConhecida: string | null): Promise<{
  rounds: ReturnType<typeof parseRounds>;
  http: number;
  paginas: number;
  tentativas: number;
  motivoPagina: string;
  rejeitadas: number;
  duplicadas: number;
  recebidas: number;
}> {
  const rt = runtime();
  const itens: unknown[] = [];
  let http = 0;
  let paginas = 0;
  let tentativas = 0;
  let motivoPagina = "vazia";
  let total: number | null = null;
  const vistas = new Set<number>();

  const consumir = async (page: number): Promise<boolean> => {
    if (vistas.has(page)) return true;
    if (page > 0) await dormir(120);
    let pagina: Awaited<ReturnType<typeof lerPagina>>;
    try {
      pagina = await lerPagina(apiPageUrl(page, TAMANHO_PAGINA));
    } catch (error) {
      if (page === 0) throw error;
      motivoPagina = "pagina_incompleta";
      rt.retomarPagina = page;
      rt.aviso =
        "Uma página a meio falhou. As mãos já recebidas neste ciclo ficam gravadas e a leitura retoma no próximo pulso.";
      return false;
    }
    vistas.add(page);
    http = pagina.http;
    tentativas += pagina.tentativas;
    paginas += 1;
    if (pagina.total !== null) total = pagina.total;
    if (
      page === 0 &&
      pagina.itens.length > 0 &&
      separarRodadas(pagina.itens).rounds.length === 0 &&
      !Array.isArray(pagina.cru)
    ) {
      throw new Error("api_corpo");
    }
    itens.push(...pagina.itens);
    const ids = pagina.itens.map((item, index) => {
      if (!item || typeof item !== "object") return `item-${index}`;
      const root = item as { id?: unknown; data?: { id?: unknown } };
      return String(root.data?.id ?? root.id ?? `item-${index}`);
    });
    const decisao = decidirPagina({
      pagina: page,
      recebidos: pagina.itens.length,
      tamanho: TAMANHO_PAGINA,
      total,
      ids,
      pontaConhecida,
    });
    motivoPagina = decisao.motivo;
    return decisao.continuar;
  };

  let seguir = await consumir(0);
  if (seguir) {
    const inicio = rt.retomarPagina > 1 ? rt.retomarPagina : 1;
    let proxima = inicio;
    for (let page = inicio; seguir && paginas < ORCAMENTO_PAGINAS; page += 1) {
      seguir = await consumir(page);
      proxima = page + 1;
      if (motivoPagina === "pagina_incompleta") break;
    }
    if (seguir && motivoPagina !== "pagina_incompleta") {
      motivoPagina = "orcamento";
      rt.retomarPagina = proxima;
    } else if (motivoPagina !== "pagina_incompleta") {
      rt.retomarPagina = 0;
    }
  } else if (motivoPagina !== "pagina_incompleta") {
    rt.retomarPagina = 0;
  }

  try {
    const ultimoJogo = await lerPagina(apiLatestUrl());
    tentativas += ultimoJogo.tentativas;
    itens.push(...ultimoJogo.itens);
  } catch {
    /* a lista já traz a ponta; o /latest é só reforço */
  }
  const separado = separarRodadas(itens);
  return {
    rounds: separado.rounds,
    http,
    paginas,
    tentativas,
    motivoPagina,
    rejeitadas: separado.rejeitadas.length,
    duplicadas: separado.duplicadas,
    recebidas: itens.length,
  };
}

async function keepAlive(): Promise<void> {
  const rt = runtime();
  if (Date.now() - rt.ultimoKeep < 4 * 60_000) return;
  const url = process.env.KEEP_ALIVE_URL || process.env.RENDER_EXTERNAL_URL || "";
  if (!url) return;
  rt.ultimoKeep = Date.now();
  const base = url.replace(/\/$/, "");
  const target = base.includes("/api/health") || base.endsWith("/health") ? base : `${base}/api/health`;
  try {
    await fetch(target, { signal: AbortSignal.timeout(8_000) });
  } catch {
    /* o ping é só para o hospedeiro não dormir */
  }
}

function linhasDe(rows: store.SinalRow[]) {
  return rows.map((r) => ({
    estado: r.estado,
    cor: r.cor,
    resultadoFinal: r.resultadoFinal,
    resolvidoEm: r.resolvidoEm,
    criadoEm: r.criadoEm,
  }));
}

async function esperaDoPainel(): Promise<Painel["espera"]> {
  const fecho = await store.ultimoFechoEm();
  const silencioMs = fecho ? Math.max(0, Date.now() - ms(fecho)) : ESPERA_MAX_MS;
  return {
    tetoMs: ESPERA_MAX_MS,
    silencioMs,
    restanteMs: Math.max(0, ESPERA_MAX_MS - silencioMs),
  };
}

async function snapshot(): Promise<Painel> {
  const rt = runtime();
  const totalRodadas = await store.countRounds();
  const recent = await store.recentRounds(store.PAGINA_REGISTO);
  const newest = recent.map((r) => r.resultado);
  const mesa = recent.length ? analisarMesa(newest) : null;
  const aberto = await store.getAberto();
  const doDia = await store.sinaisDoDia();
  const recentes = await store.sinaisRecentes(14);
  const folha = resumirFolha(linhasDe(doDia));
  const evento = await store.ultimoEvento();
  const tip = recent[0] ?? null;
  const ritmo = tip
    ? await store.ritmoDesde(tip.fim)
    : {
        decisivasDesdeFecho: 0,
        sinaisNaJanela: 0,
        lossStreak: 0,
        ultimoStopCor: null,
        houveSinal: false,
      };
  const estudo = estudar(
    [...recent].reverse().map((r) => ({ id: r.id, fim: r.fim, resultado: r.resultado })),
  );
  const atraso = tip ? Date.now() - ms(tip.fim) : null;
  return {
    nome: "Capunga Mesa",
    versao: "pulso-24h",
    hora: horaLuanda(),
    fonte: store.fonteDados(),
    telegram: telegramConfigurado(),
    aoVivo: atraso !== null && atraso < 90_000,
    retomado: rt.retomado,
    atrasoMs: atraso,
    motivo: rt.motivo,
    motivoHumano: motivoHumano(rt.motivo),
    sincronizadoEm: rt.lastOkAt,
    erro: rt.falhasSeguidas >= 2 ? rt.lastError : null,
    aviso: rt.aviso,
    rodadas: recent.map((r) => ({
      id: r.id,
      seq: r.seq,
      resultado: r.resultado,
      fim: r.fim,
      pontosJogador: r.pontosJogador,
      pontosBanca: r.pontosBanca,
      dadosJogador: r.dadosJogador,
      dadosBanca: r.dadosBanca,
    })),
    haMais: totalRodadas > recent.length,
    mesa,
    aberto: aberto
      ? {
          id: aberto.id,
          cor: aberto.cor,
          gale: aberto.gale,
          criadoEm: aberto.criadoEm,
          horaMesa: horaCurta(aberto.criadoEm),
          confianca: aberto.confianca,
          estrategia: aberto.estrategia,
          flexivel: aberto.flexivel,
          anunciado: aberto.anunciado,
        }
      : null,
    folha,
    folhaTexto: textoFolha(folha),
    sinais: recentes.map((s) => ({
      id: s.id,
      cor: s.cor,
      estado: s.estado,
      gale: s.gale,
      criadoEm: s.criadoEm,
      horaMesa: horaCurta(s.criadoEm),
      resolvidoEm: s.resolvidoEm,
      horaFecho: s.resolvidoEm ? horaCurta(s.resolvidoEm) : null,
      resultadoFinal: s.resultadoFinal,
      estrategia: s.estrategia,
      confianca: s.confianca,
      flexivel: s.flexivel,
    })),
    ritmo: {
      decisivasDesdeFecho: ritmo.decisivasDesdeFecho,
      sinaisNaJanela: ritmo.sinaisNaJanela,
      lossStreak: ritmo.lossStreak,
    },
    estudo: {
      sinais: estudo.sinais,
      gestao: estudo.folha.gestao,
      pureza: estudo.folha.pureza,
      suficiente: estudo.suficiente,
      amostra: estudo.amostra,
    },
    fio: evento
      ? {
          tipo: evento.tipo,
          detalhe: evento.detalhe,
          em: evento.ocorridoEm,
          horaMesa: horaCurta(evento.ocorridoEm),
        }
      : null,
    espera: await esperaDoPainel(),
    totais: { rodadas: totalRodadas, sinaisDia: doDia.length },
    saude: classificarSaude({
      dbOk: rt.dbOk,
      apiErro: rt.falhasSeguidas >= 2 ? rt.lastError : rt.apiError && rt.falhasSeguidas > 0 ? rt.apiError : null,
      sincronizadoEm: rt.lastOkAt,
      ultimaRodadaEm: tip?.fim ?? rt.ultimaRodadaEm,
      agora: Date.now(),
    }),
    ciclo: rt.ciclo,
    lacunas: await store.lacunasRecentes(5).catch(() => []),
  };
}

async function flush(retomado: boolean): Promise<void> {
  const fechos = await store.fechosPorAvisar();
  for (const sinal of fechos) {
    if (sinal.anunciado) {
      if (sinal.estado === "LACUNA") await enviar(textoLacuna());
      else if (sinal.estado === "CANCELADO") await enviar(textoExpirado(sinal.id));
      else if (
        sinal.estado === "GREEN" ||
        sinal.estado === "G1" ||
        sinal.estado === "G2"
      ) {
        const linhas = linhasDe(await store.sinaisDoDia());
        const rodada = sinal.rodadaId ? await store.roundById(sinal.rodadaId) : null;
        await enviar(
          textoFechoCompleto(sinal.estado, sinal.resultadoFinal === "Empate", linhas, rodada ?? undefined),
        );
      }
      await apagar(sinal.gale1MsgId);
      await apagar(sinal.gale2MsgId);
    }
    await store.marcarFechoAvisado(sinal.id);
  }

  const aberto = await store.getAberto();
  if (!aberto) {
    await estadoAnalise(true);
    return;
  }

  await estadoAnalise(false);
  if (!aberto.anunciado) {
    const sent = await enviar(textoEntrada(aberto.cor));
    if (!sent.ok) return;
    await store.marcarAnuncio(aberto.id);
  }

  if (aberto.gale > aberto.galeAvisado && aberto.gale >= 1) {
    const nivel: 1 | 2 = aberto.gale >= 2 ? 2 : 1;
    if (nivel === 2 && aberto.gale1MsgId) await apagar(aberto.gale1MsgId);
    const saltou = retomado || aberto.gale - aberto.galeAvisado > 1;
    const sent = await enviar(textoGale(nivel, saltou));
    await store.marcarGaleAvisado(aberto.id, nivel, sent.id, nivel);
  }
}

async function aplicarFila(queue: store.RodadaRow[], now: number): Promise<boolean> {
  const rt = runtime();
  let retomado = false;
  const tip = queue[queue.length - 1];
  for (const round of queue) {
    try {
    const historico = round.fim ? now - ms(round.fim) > HISTORICO_MS : false;
    if (historico) retomado = true;
    let aberto = await store.getAberto();
    if (aberto && round.id !== aberto.ultimaRodadaId && round.id !== aberto.resultadoId && round.fim > aberto.criadoEm) {
      const gap = ms(round.fim) - ms(aberto.ultimaAplicadaEm);
      const passo = avancar(aberto.gale, aberto.cor, round.resultado, gap);
      if (passo.tipo === "lacuna") {
        await store.fecharSinal({
          id: aberto.id,
          estado: "LACUNA",
          resultadoFinal: null,
          resolvidoEm: round.fim,
          rodadaId: round.id,
          empateExtra: false,
        });
        await store.registarEvento({
          sinalId: aberto.id,
          tipo: "LACUNA",
          detalhe: "buraco entre rodadas",
          rodadaId: round.id,
          ocorridoEm: round.fim,
        });
        rt.motivo = "lacuna";
        rt.fioNota = "Sinal sem fio. Não foi julgado no acordar.";
        aberto = null;
      } else if (passo.tipo === "fechar" && passo.estado) {
        await store.fecharSinal({
          id: aberto.id,
          estado: passo.estado,
          resultadoFinal: passo.porEmpate ? "Empate" : round.resultado,
          resolvidoEm: round.fim,
          rodadaId: round.id,
          empateExtra: Boolean(passo.porEmpate),
        });
        await store.registarEvento({
          sinalId: aberto.id,
          tipo: "FECHO",
          detalhe: passo.porEmpate ? `${passo.estado} empate` : passo.estado,
          rodadaId: round.id,
          ocorridoEm: round.fim,
        });
        rt.motivo = "fechado";
        aberto = null;
      } else if (passo.tipo === "stop") {
        await store.fecharSinal({
          id: aberto.id,
          estado: "LOSS",
          resultadoFinal: round.resultado,
          resolvidoEm: round.fim,
          rodadaId: round.id,
          empateExtra: false,
        });
        await store.registarEvento({
          sinalId: aberto.id,
          tipo: "STOP",
          detalhe: "LOSS",
          rodadaId: round.id,
          ocorridoEm: round.fim,
        });
        rt.motivo = "stop";
        aberto = null;
      } else if (passo.tipo === "gale" && passo.gale) {
        await store.marcarGale(aberto.id, passo.gale, round.id, round.fim);
        await store.registarEvento({
          sinalId: aberto.id,
          tipo: passo.gale === 1 ? "GALE1" : "GALE2",
          detalhe: `gale ${passo.gale}`,
          rodadaId: round.id,
          ocorridoEm: round.fim,
        });
        rt.motivo = passo.gale === 1 ? "gale1" : "gale2";
      }
    }

    const isTip = tip?.id === round.id;
    const live = now - ms(round.fim) <= LIVE_EDGE_MS;
    aberto = await store.getAberto();
    if (aberto) {
      rt.motivo = rt.motivo.startsWith("gale") ? rt.motivo : "sinal_aberto";
    } else if (!boolEnv("SINAIS_ATIVOS", true)) {
      rt.motivo = "sinais_pausados";
    } else if (!isTip) {
      rt.motivo = "historico";
    } else if (!live) {
      rt.motivo = "ponta_fria";
    } else if (!isSide(round.resultado)) {
      rt.motivo = "rodada_empate";
    } else if (await store.catalisadorUsado(round.id)) {
      rt.motivo = "aguardando";
    } else {
      const recent = await store.recentRounds(180);
      const ateAqui = recent.filter((r) => r.fim < round.fim || (r.fim === round.fim && r.seq <= round.seq));
      const newest = ateAqui.map((r) => r.resultado);
      const ctx = await store.ritmoDesde(round.fim);
      const fecho = await store.ultimoFechoEm();
      const silencio = fecho ? Math.max(0, ms(round.fim) - ms(fecho)) : ESPERA_MAX_MS;
      const escolha = resolverEntrada(newest, ctx, silencio);
      if (!escolha.ok) {
        rt.motivo = escolha.codigo || "aguardando";
      } else {
        const entrada = escolha.entrada;
        const criado = await store.abrirSinal({
          cor: entrada.cor,
          criadoEm: round.fim,
          resultadoId: round.id,
          estrategia: entrada.estrategia,
          familias: entrada.familias.join("+"),
          confianca: entrada.confianca,
          votos: entrada.votos,
          flexivel: entrada.flexivel,
          motivo: entrada.ids.join("+"),
        });
        if (criado) {
          await store.registarEvento({
            sinalId: criado.id,
            tipo: "ABRIU",
            detalhe: `${entrada.cor} ${entrada.estrategia}`,
            rodadaId: round.id,
            ocorridoEm: round.fim,
          });
          rt.motivo = entrada.codigo;
        }
      }
    }
    } catch {
      rt.lastError = "Uma rodada falhou e foi ultrapassada só depois de repetir. A mesa segue.";
      rt.falhasSeguidas += 1;
      const n = (rt.venenos[round.id] ?? 0) + 1;
      rt.venenos[round.id] = n;
      if (n < 3) break;
      linha("rodada_rejeitada", { id: round.id, tentativas: n });
    }
    try {
      await store.setCursor(round.fim, round.seq, round.id);
      delete rt.venenos[round.id];
    } catch {
      rt.lastError = "Não consegui gravar o lugar da mesa. Tento já.";
      rt.dbOk = false;
      break;
    }
  }
  rt.retomado = retomado;
  await flush(retomado);
  return retomado;
}

async function expirarSeMorto(now: number): Promise<void> {
  const aberto = await store.getAberto();
  if (!aberto) return;
  const tip = await store.ponta();
  if (!tip) return;
  if (tip.fim > aberto.criadoEm && tip.id !== aberto.resultadoId) return;
  if (now - ms(tip.fim) < EXPIRA_MS) return;
  await store.fecharSinal({
    id: aberto.id,
    estado: "CANCELADO",
    resultadoFinal: null,
    resolvidoEm: tip.fim,
    rodadaId: tip.id,
    empateExtra: false,
  });
  await store.registarEvento({
    sinalId: aberto.id,
    tipo: "EXPIRADO",
    detalhe: "sem rodada nova",
    rodadaId: tip.id,
    ocorridoEm: tip.fim,
  });
  runtime().motivo = "expirado";
  await flush(false);
}

async function tickCorpo(): Promise<void> {
  const rt = runtime();
  await hidratar(rt);
  const inicio = new Date().toISOString();
  const t0 = Date.now();
  linha("ciclo_inicio", { arranque: rt.arrancouEm });
  try {
    const antes = await store.ponta();
    rt.dbOk = true;
    rt.ultimaRodadaEm = antes?.fim ?? null;
    const leitura = await fetchRounds(antes?.id ?? null);
    rt.http = leitura.http;
    rt.apiError = null;
    const gravacao = await store.upsertRounds(leitura.rounds);
    rt.ultimaGravacaoEm = new Date().toISOString();
    const lacuna = descreverLacuna(antes, leitura.rounds, 90_000);
    if (lacuna) {
      rt.aviso = lacuna;
      await store.registarLacuna(antes?.fim ?? null, leitura.rounds[0]?.fim ?? null, lacuna).catch(() => undefined);
    } else if (leitura.motivoPagina === "orcamento") {
      rt.aviso =
        "A fonte ainda tem páginas por trás. Este ciclo parou no orçamento e o próximo retoma nessa página. Nada foi inventado e o robô não parou.";
      await store.registarLacuna(antes?.fim ?? null, null, rt.aviso).catch(() => undefined);
    } else if (!leitura.rounds.length) {
      rt.aviso = "A fonte devolveu uma janela vazia. O histórico guardado mantém-se.";
    } else if (leitura.motivoPagina !== "pagina_incompleta") {
      rt.aviso = null;
    }
    if (leitura.rejeitadas) {
      linha("rodadas_rejeitadas", { n: leitura.rejeitadas });
    }
    rt.ciclo = {
      inicio,
      fim: new Date().toISOString(),
      recebidas: leitura.recebidas,
      processadas: gravacao.novas,
      duplicadas: gravacao.duplicadas + leitura.duplicadas,
      rejeitadas: leitura.rejeitadas + gravacao.falhas,
      paginas: leitura.paginas,
      duracaoMs: Date.now() - t0,
      tentativas: leitura.tentativas,
      motivoPagina: leitura.motivoPagina,
    };
    let cursor = await store.getCursor();
    if (!cursor.ultimoFim) {
      const tip = await store.ponta();
      if (!tip) {
        rt.motivo = "semeando";
        rt.lastOkAt = new Date().toISOString();
        rt.lastError = null;
        rt.falhasSeguidas = 0;
        rt.retomado = false;
        await persistirPulso(rt, "semeando");
        linha("ciclo_fim", { motivo: "semeando", ...rt.ciclo });
        return;
      }
      const idade = Date.now() - ms(tip.fim);
      if (idade <= LIVE_EDGE_MS) {
        await store.setCursor(new Date(ms(tip.fim) - 1).toISOString(), 0, "");
        cursor = await store.getCursor();
      } else {
        await store.setCursor(tip.fim, tip.seq, tip.id);
        rt.motivo = "semeando";
        rt.lastOkAt = new Date().toISOString();
        rt.lastError = null;
        rt.falhasSeguidas = 0;
        rt.retomado = false;
        rt.ultimaRodadaEm = tip.fim;
        await persistirPulso(rt, "semeando");
        linha("ciclo_fim", { motivo: "semeando", rodadas: 1 });
        return;
      }
    }
    const queue = await store.roundsAfter(cursor, FILA_POR_CICLO);
    if (!queue.length) {
      await expirarSeMorto(Date.now());
      if (runtime().motivo !== "expirado") {
        const aberto = await store.getAberto();
        rt.motivo = aberto ? "sinal_aberto" : leitura.rounds.length ? "aguardando" : "janela_vazia";
        await flush(false);
      }
    } else {
      await aplicarFila(queue, Date.now());
      if (queue.length >= FILA_POR_CICLO) {
        rt.aviso = rt.aviso ?? "Há um bloco grande por julgar. Continua no próximo pulso, sem apagar o que já ficou gravado.";
      }
    }
    const ponta = await store.ponta();
    rt.ultimaRodadaEm = ponta?.fim ?? rt.ultimaRodadaEm;
    rt.lastOkAt = new Date().toISOString();
    rt.lastError = null;
    rt.falhasSeguidas = 0;
    rt.dbOk = true;
    rt.ciclo = { ...rt.ciclo, fim: new Date().toISOString(), duracaoMs: Date.now() - t0 };
    await persistirPulso(rt, "ok");
    linha("ciclo_fim", {
      motivo: rt.motivo,
      paginas: rt.ciclo.paginas,
      recebidas: rt.ciclo.recebidas,
      novas: rt.ciclo.processadas,
      fila: queue.length,
    });
    await keepAlive();
  } catch (error) {
    rt.falhasSeguidas += 1;
    const msg = error instanceof Error ? error.message : "falha";
    const db = /relation|postgres|pglite|ECONNREFUSED|connect/i.test(msg);
    if (db) {
      rt.dbOk = false;
      rt.lastError = "A base de dados falhou este ciclo. O pulso mantém-se e tenta de novo.";
    } else {
      rt.apiError = msg;
      rt.lastError = "A mesa não respondeu. Continuo a tentar.";
    }
    rt.ciclo = {
      ...rt.ciclo,
      inicio,
      fim: new Date().toISOString(),
      duracaoMs: Date.now() - t0,
    };
    linha("ciclo_falha", { erro: msg, falhas: rt.falhasSeguidas, db });
    try {
      await persistirPulso(rt, db ? "base" : "api");
    } catch {
      /* o registo do pulso não pode derrubar o ciclo */
    }
    try {
      await expirarSeMorto(Date.now());
    } catch {
      /* uma falha de fecho não pode parar o pulso */
    }
  }
}

async function persistirPulso(rt: Runtime, estado: string): Promise<void> {
  await store.gravarPulso({
    ultimoOk: rt.lastOkAt,
    ultimaRodada: rt.ultimaRodadaEm,
    ultimaGravacao: rt.ultimaGravacaoEm,
    ultimoErro: rt.lastError,
    ultimoAviso: rt.aviso,
    recebidas: rt.ciclo.recebidas,
    gravadas: rt.ciclo.processadas,
    duplicadas: rt.ciclo.duplicadas,
    rejeitadas: rt.ciclo.rejeitadas,
    paginas: rt.ciclo.paginas,
    duracaoMs: rt.ciclo.duracaoMs,
    tentativas: rt.ciclo.tentativas,
    arranques: rt.arranques,
    estado,
  });
}

function garantirRelogio(): void {
  const rt = runtime();
  if (rt.relogio) return;
  rt.relogio = true;
  linha("relogio", { arranque: rt.arrancouEm });
  const bater = () => {
    const atraso = proximoAtraso(rt.falhasSeguidas);
    const timer = setTimeout(() => {
      const trabalho = tickAndSnapshot().catch((error) => {
        linha("relogio_erro", { erro: error instanceof Error ? error.message : "falha" });
      });
      const teto = new Promise<void>((resolve) => setTimeout(resolve, 45_000));
      void Promise.race([trabalho.then(() => undefined), teto]).finally(() => {
        if (runtime().relogio) bater();
      });
    }, atraso);
    if (typeof timer === "object" && timer && "unref" in timer && process.env.VERCEL) timer.unref();
  };
  bater();
}

export function iniciarPulso(): void {
  garantirRelogio();
}

async function hidratar(rt: Runtime): Promise<void> {
  if (rt.hidratado) return;
  rt.hidratado = true;
  try {
    const pulso = await store.lerPulso();
    rt.arranques = (pulso?.arranques ?? 0) + 1;
    if (pulso?.ultimaRodada) rt.ultimaRodadaEm = pulso.ultimaRodada;
    if (pulso?.ultimaGravacao) rt.ultimaGravacaoEm = pulso.ultimaGravacao;
    linha("arranque", { arranques: rt.arranques, ultimaRodada: rt.ultimaRodadaEm });
  } catch (error) {
    rt.dbOk = false;
    linha("arranque_sem_base", { erro: error instanceof Error ? error.message : "falha" });
  }
}

export async function tickAndSnapshot(): Promise<Painel> {
  garantirRelogio();
  const rt = runtime();
  const now = Date.now();
  if (rt.ticking) {
    await rt.ticking;
  } else if (now - rt.lastTickAt >= THROTTLE_MS) {
    rt.lastTickAt = now;
    const job = tickCorpo().finally(() => {
      if (runtime().ticking === job) runtime().ticking = null;
    });
    rt.ticking = job;
    await job;
  }
  try {
    const painel = await snapshot();
    rt.ultimoPainel = painel;
    return painel;
  } catch {
    const erro = "A folha falhou um instante. A mesa segue.";
    rt.lastError = erro;
    if (rt.ultimoPainel) {
      return {
        ...rt.ultimoPainel,
        erro,
        saude: {
          ...rt.ultimoPainel.saude,
          sucesso: false,
          estado: "degradado",
          http: 503,
          motivoFalha: erro,
        },
      };
    }
    return painelVazio(erro);
  }
}

function painelVazio(erro: string): Painel {
  const folha = resumirFolha([]);
  return {
    nome: "Capunga Mesa",
    versao: "pulso-24h",
    hora: horaLuanda(),
    fonte: store.fonteDados(),
    telegram: telegramConfigurado(),
    aoVivo: false,
    retomado: false,
    atrasoMs: null,
    motivo: "aguardando",
    motivoHumano: motivoHumano("aguardando"),
    sincronizadoEm: null,
    erro,
    aviso: null,
    rodadas: [],
    haMais: false,
    mesa: null,
    aberto: null,
    folha,
    folhaTexto: textoFolha(folha),
    sinais: [],
    ritmo: { decisivasDesdeFecho: 0, sinaisNaJanela: 0, lossStreak: 0 },
    estudo: { sinais: 0, gestao: null, pureza: null, suficiente: false, amostra: 0 },
    fio: null,
    espera: { tetoMs: ESPERA_MAX_MS, silencioMs: 0, restanteMs: ESPERA_MAX_MS },
    totais: { rodadas: 0, sinaisDia: 0 },
    saude: classificarSaude({
      dbOk: false,
      apiErro: erro,
      sincronizadoEm: null,
      ultimaRodadaEm: null,
      agora: Date.now(),
    }),
    ciclo: cicloVazio(),
    lacunas: [],
  };
}

export function gestaoLegenda(folha: FolhaNum): string {
  return `Gestão ${textoPct(folha.gestao)}`;
}


========================================================================================
FICHEIRO: src/server/capunga/store.ts
========================================================================================

import { getSql, dbSource, type DbSource } from "@/lib/db";
import { inicioDoDiaLuanda } from "./tempo.ts";
import type { EstadoSinal, Outcome, Round, Side, SinalAberto } from "./types.ts";

export function fonteDados(): DbSource {
  return dbSource;
}

function iso(value: unknown): string {
  if (value instanceof Date) return value.toISOString();
  if (typeof value === "string" && value) {
    const t = new Date(value).getTime();
    if (Number.isFinite(t)) return new Date(t).toISOString();
  }
  return new Date(0).toISOString();
}

function isoOrNull(value: unknown): string | null {
  if (value === null || value === undefined || value === "") return null;
  return iso(value);
}

function num(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function flag(value: unknown): boolean {
  return value === true || value === 1 || value === "t" || value === "true" || value === "1";
}

function parDados(a: unknown, b: unknown): [number, number] | null {
  const x = num(a);
  const y = num(b);
  if (x === null || y === null) return null;
  if (x < 1 || x > 6 || y < 1 || y > 6) return null;
  return [x, y];
}

function int(value: unknown, fb = 0): number {
  const n = Number(value);
  return Number.isFinite(n) ? Math.trunc(n) : fb;
}

export interface RodadaRow extends Round {
  seq: number;
}

export interface CursorRow {
  ultimoFim: string | null;
  ultimoSeq: number | null;
  ultimoId: string | null;
}

export interface SinalRow {
  id: number;
  criadoEm: string;
  cor: Side;
  estado: EstadoSinal;
  gale: number;
  resultadoId: string | null;
  resultadoFinal: string | null;
  empates: number;
  estrategia: string | null;
  familias: string | null;
  confianca: number | null;
  votos: number;
  flexivel: boolean;
  anunciado: boolean;
  resolvidoEm: string | null;
}

export interface EventoRow {
  id: number;
  sinalId: number | null;
  tipo: string;
  detalhe: string | null;
  rodadaId: string | null;
  ocorridoEm: string;
}

function mapRound(row: Record<string, unknown>): RodadaRow {
  return {
    id: String(row.id),
    resultado: row.resultado as Outcome,
    inicio: isoOrNull(row.inicio),
    fim: iso(row.fim),
    pontosJogador: num(row.pontos_jogador),
    pontosBanca: num(row.pontos_banca),
    dadosJogador: parDados(row.dado_j1, row.dado_j2),
    dadosBanca: parDados(row.dado_b1, row.dado_b2),
    multiplicador: row.multiplicador === null || row.multiplicador === undefined ? null : String(row.multiplicador),
    seq: int(row.seq),
  };
}

function mapAberto(row: Record<string, unknown>): SinalAberto {
  const criado = iso(row.criado_em);
  return {
    id: int(row.id),
    cor: row.cor as Side,
    gale: int(row.gale),
    criadoEm: criado,
    resultadoId: String(row.resultado_id ?? ""),
    ultimaAplicadaEm: isoOrNull(row.ultima_aplicada_em) ?? criado,
    ultimaRodadaId: String(row.ultima_rodada_id ?? row.resultado_id ?? ""),
    estrategia: String(row.estrategia ?? ""),
    familias: String(row.familias ?? ""),
    confianca: int(row.confianca),
    anunciado: flag(row.anunciado),
    fechoAvisado: flag(row.fecho_avisado),
    galeAvisado: int(row.gale_avisado),
    gale1MsgId: num(row.gale1_msg_id),
    gale2MsgId: num(row.gale2_msg_id),
    empates: int(row.empates),
    flexivel: flag(row.flexivel),
  };
}

export async function upsertRound(round: Round): Promise<boolean> {
  const sql = await getSql();
  const rows = await sql.query<{ id: string }>(
    `insert into bacbo_rodadas
       (id, resultado, estado, inicio, fim, pontos_jogador, pontos_banca, multiplicador,
        dado_j1, dado_j2, dado_b1, dado_b2)
     values ($1, $2, 'Resolved', $3, $4, $5, $6, $7, $8, $9, $10, $11)
     on conflict (id) do update set
       dado_j1 = coalesce(bacbo_rodadas.dado_j1, excluded.dado_j1),
       dado_j2 = coalesce(bacbo_rodadas.dado_j2, excluded.dado_j2),
       dado_b1 = coalesce(bacbo_rodadas.dado_b1, excluded.dado_b1),
       dado_b2 = coalesce(bacbo_rodadas.dado_b2, excluded.dado_b2),
       pontos_jogador = coalesce(bacbo_rodadas.pontos_jogador, excluded.pontos_jogador),
       pontos_banca = coalesce(bacbo_rodadas.pontos_banca, excluded.pontos_banca),
       multiplicador = coalesce(bacbo_rodadas.multiplicador, excluded.multiplicador)
     returning id`,
    [
      round.id,
      round.resultado,
      round.inicio,
      round.fim,
      round.pontosJogador,
      round.pontosBanca,
      round.multiplicador,
      round.dadosJogador?.[0] ?? null,
      round.dadosJogador?.[1] ?? null,
      round.dadosBanca?.[0] ?? null,
      round.dadosBanca?.[1] ?? null,
    ],
  );
  return rows.length > 0;
}

export interface GravacaoRodadas {
  gravadas: number;
  novas: number;
  duplicadas: number;
  falhas: number;
}

/** Grava todas as mãos. Uma falha no lote não pode deitar fora as outras. */
export async function upsertRounds(rounds: Round[]): Promise<GravacaoRodadas> {
  const vazio = { gravadas: 0, novas: 0, duplicadas: 0, falhas: 0 };
  if (!rounds.length) return vazio;
  const sql = await getSql();
  const ids = [...new Set(rounds.map((r) => r.id))];
  const ja = new Set<string>();
  for (let i = 0; i < ids.length; i += 80) {
    const fatia = ids.slice(i, i + 80);
    const marks = fatia.map((_, n) => `$${n + 1}`).join(", ");
    try {
      const rows = await sql.query<{ id: string }>(
        `select id from bacbo_rodadas where id in (${marks})`,
        fatia,
      );
      for (const row of rows) ja.add(String(row.id));
    } catch {
      /* a contagem de duplicados é diagnóstico; a gravação segue */
    }
  }
  let gravadas = 0;
  let falhas = 0;
  const tamanho = 40;
  for (let i = 0; i < rounds.length; i += tamanho) {
    const chunk = rounds.slice(i, i + tamanho);
    const values: unknown[] = [];
    const linhas = chunk.map((round, index) => {
      const base = index * 11;
      values.push(
        round.id,
        round.resultado,
        round.inicio,
        round.fim,
        round.pontosJogador,
        round.pontosBanca,
        round.multiplicador,
        round.dadosJogador?.[0] ?? null,
        round.dadosJogador?.[1] ?? null,
        round.dadosBanca?.[0] ?? null,
        round.dadosBanca?.[1] ?? null,
      );
      const p = (n: number) => `$${base + n}`;
      return `(${p(1)}, ${p(2)}, 'Resolved', ${p(3)}, ${p(4)}, ${p(5)}, ${p(6)}, ${p(7)}, ${p(8)}, ${p(9)}, ${p(10)}, ${p(11)})`;
    });
    try {
      const rows = await sql.query<{ id: string }>(
        `insert into bacbo_rodadas
           (id, resultado, estado, inicio, fim, pontos_jogador, pontos_banca, multiplicador,
            dado_j1, dado_j2, dado_b1, dado_b2)
         values ${linhas.join(", ")}
         on conflict (id) do update set
           dado_j1 = coalesce(bacbo_rodadas.dado_j1, excluded.dado_j1),
           dado_j2 = coalesce(bacbo_rodadas.dado_j2, excluded.dado_j2),
           dado_b1 = coalesce(bacbo_rodadas.dado_b1, excluded.dado_b1),
           dado_b2 = coalesce(bacbo_rodadas.dado_b2, excluded.dado_b2),
           pontos_jogador = coalesce(bacbo_rodadas.pontos_jogador, excluded.pontos_jogador),
           pontos_banca = coalesce(bacbo_rodadas.pontos_banca, excluded.pontos_banca),
           multiplicador = coalesce(bacbo_rodadas.multiplicador, excluded.multiplicador)
         returning id`,
        values,
      );
      gravadas += rows.length;
    } catch {
      for (const round of chunk) {
        try {
          if (await upsertRound(round)) gravadas += 1;
          else falhas += 1;
        } catch {
          falhas += 1;
        }
      }
    }
  }
  const duplicadas = rounds.filter((r) => ja.has(r.id)).length;
  return { gravadas, novas: Math.max(0, gravadas - duplicadas), duplicadas, falhas };
}

/** Página do registo, da mais recente para a mais antiga. Sem teto: a página anda. */
export const PAGINA_REGISTO = 120;

export async function roundsBefore(fim: string, seq: number, limit: number): Promise<RodadaRow[]> {
  const sql = await getSql();
  const rows = await sql.query<Record<string, unknown>>(
    `select id, resultado, inicio, fim, pontos_jogador, pontos_banca, multiplicador, dado_j1, dado_j2, dado_b1, dado_b2, seq
       from bacbo_rodadas
      where fim < $1
         or (fim = $1 and seq < $2)
      order by fim desc, seq desc
      limit $3`,
    [fim, seq, limit],
  );
  return rows.map(mapRound);
}

export async function getCursor(): Promise<CursorRow> {
  const sql = await getSql();
  const rows = await sql.query<Record<string, unknown>>(
    `select ultimo_fim, ultimo_seq, ultimo_id from bacbo_cursor where id = 1`,
  );
  const row = rows[0];
  if (!row) return { ultimoFim: null, ultimoSeq: null, ultimoId: null };
  return {
    ultimoFim: isoOrNull(row.ultimo_fim),
    ultimoSeq: row.ultimo_seq === null || row.ultimo_seq === undefined ? null : int(row.ultimo_seq),
    ultimoId: row.ultimo_id ? String(row.ultimo_id) : null,
  };
}

export async function setCursor(fim: string, seq: number, id: string): Promise<void> {
  const sql = await getSql();
  await sql.query(
    `insert into bacbo_cursor (id, ultimo_fim, ultimo_seq, ultimo_id)
     values (1, $1, $2, $3)
     on conflict (id) do update set
       ultimo_fim = excluded.ultimo_fim,
       ultimo_seq = excluded.ultimo_seq,
       ultimo_id = excluded.ultimo_id,
       atualizado_em = now()
     where bacbo_cursor.ultimo_fim is null
        or excluded.ultimo_fim > bacbo_cursor.ultimo_fim
        or (excluded.ultimo_fim = bacbo_cursor.ultimo_fim and excluded.ultimo_seq >= bacbo_cursor.ultimo_seq)`,
    [fim, seq, id],
  );
}

export async function roundsAfter(cursor: CursorRow, limit = 400): Promise<RodadaRow[]> {
  const sql = await getSql();
  const teto = Math.max(1, Math.min(2000, Math.trunc(limit)));
  if (!cursor.ultimoFim || cursor.ultimoSeq === null) {
    const rows = await sql.query<Record<string, unknown>>(
      `select id, resultado, inicio, fim, pontos_jogador, pontos_banca, multiplicador, dado_j1, dado_j2, dado_b1, dado_b2, seq
         from bacbo_rodadas
        order by fim asc, seq asc
        limit $1`,
      [teto],
    );
    return rows.map(mapRound);
  }
  const rows = await sql.query<Record<string, unknown>>(
    `select id, resultado, inicio, fim, pontos_jogador, pontos_banca, multiplicador, dado_j1, dado_j2, dado_b1, dado_b2, seq
       from bacbo_rodadas
      where fim > $1
         or (fim = $1 and seq > $2)
      order by fim asc, seq asc
      limit $3`,
    [cursor.ultimoFim, cursor.ultimoSeq, teto],
  );
  return rows.map(mapRound);
}

export async function recentRounds(limit: number): Promise<RodadaRow[]> {
  const sql = await getSql();
  const rows = await sql.query<Record<string, unknown>>(
    `select id, resultado, inicio, fim, pontos_jogador, pontos_banca, multiplicador, dado_j1, dado_j2, dado_b1, dado_b2, seq
       from bacbo_rodadas
      order by fim desc, seq desc
      limit $1`,
    [limit],
  );
  return rows.map(mapRound);
}
export async function roundById(id: string): Promise<RodadaRow | null> {
  const sql = await getSql();
  const rows = await sql.query<Record<string, unknown>>(
    `select id, resultado, inicio, fim, pontos_jogador, pontos_banca, multiplicador,
            dado_j1, dado_j2, dado_b1, dado_b2, seq
       from bacbo_rodadas where id = $1 limit 1`,
    [id],
  );
  return rows[0] ? mapRound(rows[0]) : null;
}

export async function countRounds(): Promise<number> {
  const sql = await getSql();
  const rows = await sql.query<{ n: number }>(`select count(*)::int as n from bacbo_rodadas`);
  return int(rows[0]?.n);
}

export async function getAberto(): Promise<SinalAberto | null> {
  const sql = await getSql();
  const rows = await sql.query<Record<string, unknown>>(
    `select * from bacbo_sinais where estado = 'PENDENTE' order by id asc limit 1`,
  );
  const row = rows[0];
  return row ? mapAberto(row) : null;
}

export async function catalisadorUsado(id: string): Promise<boolean> {
  const sql = await getSql();
  const rows = await sql.query<{ n: number }>(
    `select count(*)::int as n from bacbo_sinais where resultado_id = $1`,
    [id],
  );
  return int(rows[0]?.n) > 0;
}

export interface NovoSinal {
  cor: Side;
  criadoEm: string;
  resultadoId: string;
  estrategia: string;
  familias: string;
  confianca: number;
  votos: number;
  flexivel: boolean;
  motivo: string;
}

export async function abrirSinal(novo: NovoSinal): Promise<SinalAberto | null> {
  const sql = await getSql();
  try {
    const rows = await sql.query<Record<string, unknown>>(
      `insert into bacbo_sinais
         (criado_em, cor, estado, gale, resultado_id, ultima_rodada_id, ultima_aplicada_em,
          empates, estrategia, familias, confianca, votos, motivo, flexivel)
       values ($1, $2, 'PENDENTE', 0, $3, $3, $1, 0, $4, $5, $6, $7, $8, $9)
       returning *`,
      [
        novo.criadoEm,
        novo.cor,
        novo.resultadoId,
        novo.estrategia,
        novo.familias,
        novo.confianca,
        novo.votos,
        novo.motivo,
        novo.flexivel,
      ],
    );
    const row = rows[0];
    return row ? mapAberto(row) : null;
  } catch (error) {
    const msg = error instanceof Error ? error.message : String(error);
    if (msg.toLowerCase().includes("unique") || msg.toLowerCase().includes("duplicate")) return null;
    throw error;
  }
}

export async function marcarGale(id: number, gale: number, rodadaId: string, quando: string): Promise<void> {
  const sql = await getSql();
  await sql.query(
    `update bacbo_sinais
        set gale = $2, ultima_rodada_id = $3, ultima_aplicada_em = $4
      where id = $1 and estado = 'PENDENTE'`,
    [id, gale, rodadaId, quando],
  );
}

export async function fecharSinal(args: {
  id: number;
  estado: EstadoSinal;
  resultadoFinal: string | null;
  resolvidoEm: string;
  rodadaId: string;
  empateExtra: boolean;
}): Promise<void> {
  const sql = await getSql();
  await sql.query(
    `update bacbo_sinais
        set estado = $2,
            resultado_final = $3,
            resolvido_em = $4,
            ultima_rodada_id = $5,
            ultima_aplicada_em = $4,
            empates = empates + $6
      where id = $1 and estado = 'PENDENTE'`,
    [
      args.id,
      args.estado,
      args.resultadoFinal,
      args.resolvidoEm,
      args.rodadaId,
      args.empateExtra ? 1 : 0,
    ],
  );
}

export async function marcarAnuncio(id: number): Promise<void> {
  const sql = await getSql();
  await sql.query(`update bacbo_sinais set anunciado = true where id = $1`, [id]);
}

export async function marcarGaleAvisado(id: number, gale: number, messageId: number | null, qual: 1 | 2): Promise<void> {
  const sql = await getSql();
  if (qual === 1) {
    await sql.query(
      `update bacbo_sinais set gale_avisado = $2, gale1_msg_id = coalesce($3, gale1_msg_id) where id = $1`,
      [id, gale, messageId],
    );
  } else {
    await sql.query(
      `update bacbo_sinais set gale_avisado = $2, gale2_msg_id = coalesce($3, gale2_msg_id) where id = $1`,
      [id, gale, messageId],
    );
  }
}

export async function marcarFechoAvisado(id: number): Promise<void> {
  const sql = await getSql();
  await sql.query(`update bacbo_sinais set fecho_avisado = true where id = $1`, [id]);
}

export async function registarEvento(args: {
  sinalId: number | null;
  tipo: string;
  detalhe: string | null;
  rodadaId: string | null;
  ocorridoEm: string;
}): Promise<void> {
  const sql = await getSql();
  await sql.query(
    `insert into bacbo_eventos (sinal_id, tipo, detalhe, rodada_id, ocorrido_em)
     values ($1, $2, $3, $4, $5)`,
    [args.sinalId, args.tipo, args.detalhe, args.rodadaId, args.ocorridoEm],
  );
}

export async function sinaisDoDia(): Promise<SinalRow[]> {
  const sql = await getSql();
  const rows = await sql.query<Record<string, unknown>>(
    `select id, criado_em, cor, estado, gale, resultado_id, resultado_final, empates,
            estrategia, familias, confianca, votos, flexivel, anunciado, resolvido_em
       from bacbo_sinais
      where criado_em >= $1
      order by criado_em asc, id asc`,
    [inicioDoDiaLuanda()],
  );
  return rows.map(mapSinal);
}

export async function sinaisRecentes(limit: number): Promise<SinalRow[]> {
  const sql = await getSql();
  const rows = await sql.query<Record<string, unknown>>(
    `select id, criado_em, cor, estado, gale, resultado_id, resultado_final, empates,
            estrategia, familias, confianca, votos, flexivel, anunciado, resolvido_em
       from bacbo_sinais
      order by criado_em desc, id desc
      limit $1`,
    [limit],
  );
  return rows.map(mapSinal);
}

function mapSinal(row: Record<string, unknown>): SinalRow {
  return {
    id: int(row.id),
    criadoEm: iso(row.criado_em),
    cor: row.cor as Side,
    estado: row.estado as EstadoSinal,
    gale: int(row.gale),
    resultadoId: row.resultado_id ? String(row.resultado_id) : null,
    resultadoFinal: row.resultado_final ? String(row.resultado_final) : null,
    empates: int(row.empates),
    estrategia: row.estrategia ? String(row.estrategia) : null,
    familias: row.familias ? String(row.familias) : null,
    confianca: num(row.confianca),
    votos: int(row.votos),
    flexivel: flag(row.flexivel),
    anunciado: flag(row.anunciado),
    resolvidoEm: isoOrNull(row.resolvido_em),
  };
}

export interface AvisoFecho {
  id: number;
  estado: EstadoSinal;
  cor: Side;
  resultadoFinal: string | null;
  rodadaId: string | null;
  anunciado: boolean;
  gale1MsgId: number | null;
  gale2MsgId: number | null;
}

export async function fechosPorAvisar(): Promise<AvisoFecho[]> {
  const sql = await getSql();
  const rows = await sql.query<Record<string, unknown>>(
    `select id, estado, cor, resultado_final, ultima_rodada_id, anunciado, gale1_msg_id, gale2_msg_id
       from bacbo_sinais
      where estado <> 'PENDENTE' and fecho_avisado = false
      order by resolvido_em asc nulls last, id asc
      limit 6`,
  );
  return rows.map((row) => ({
    id: int(row.id),
    estado: row.estado as EstadoSinal,
    cor: row.cor as Side,
    resultadoFinal: row.resultado_final ? String(row.resultado_final) : null,
    rodadaId: row.ultima_rodada_id ? String(row.ultima_rodada_id) : null,
    anunciado: flag(row.anunciado),
    gale1MsgId: num(row.gale1_msg_id),
    gale2MsgId: num(row.gale2_msg_id),
  }));
}
export async function ultimoEvento(): Promise<EventoRow | null> {
  const sql = await getSql();
  const rows = await sql.query<Record<string, unknown>>(
    `select id, sinal_id, tipo, detalhe, rodada_id, ocorrido_em
       from bacbo_eventos
      order by ocorrido_em desc, id desc
      limit 1`,
  );
  const row = rows[0];
  if (!row) return null;
  return {
    id: int(row.id),
    sinalId: num(row.sinal_id),
    tipo: String(row.tipo),
    detalhe: row.detalhe ? String(row.detalhe) : null,
    rodadaId: row.rodada_id ? String(row.rodada_id) : null,
    ocorridoEm: iso(row.ocorrido_em),
  };
}

export async function ritmoDesde(roundFim: string): Promise<{
  decisivasDesdeFecho: number;
  sinaisNaJanela: number;
  lossStreak: number;
  ultimoStopCor: Side | null;
  houveSinal: boolean;
}> {
  const sql = await getSql();
  const fechados = await sql.query<Record<string, unknown>>(
    `select estado, cor, resolvido_em
       from bacbo_sinais
      where estado in ('GREEN', 'G1', 'G2', 'LOSS')
      order by resolvido_em desc nulls last, id desc
      limit 8`,
  );
  let lossStreak = 0;
  let ultimoStopCor: Side | null = null;
  let lastResolvido: string | null = null;
  if (fechados[0]) {
    lastResolvido = isoOrNull(fechados[0].resolvido_em);
    if (fechados[0].estado === "LOSS") ultimoStopCor = fechados[0].cor as Side;
    for (const row of fechados) {
      if (row.estado !== "LOSS") break;
      lossStreak += 1;
    }
  }
  const houveRows = await sql.query<{ n: number }>(`select count(*)::int as n from bacbo_sinais`);
  const houveSinal = int(houveRows[0]?.n) > 0;

  let decisivasDesdeFecho = 999;
  if (lastResolvido) {
    const c = await sql.query<{ n: number }>(
      `select count(*)::int as n from bacbo_rodadas
        where resultado in ('Banca', 'Jogador') and fim > $1 and fim <= $2`,
      [lastResolvido, roundFim],
    );
    decisivasDesdeFecho = int(c[0]?.n);
  }

  const boundary = await sql.query<{ fim: unknown }>(
    `select fim from bacbo_rodadas
      where resultado in ('Banca', 'Jogador') and fim <= $1
      order by fim desc
      offset 39 limit 1`,
    [roundFim],
  );
  const desde = boundary[0] ? iso(boundary[0].fim) : null;
  const janela = await sql.query<{ n: number }>(
    desde
      ? `select count(*)::int as n from bacbo_sinais where criado_em >= $1`
      : `select count(*)::int as n from bacbo_sinais`,
    desde ? [desde] : [],
  );

  return {
    decisivasDesdeFecho,
    sinaisNaJanela: int(janela[0]?.n),
    lossStreak,
    ultimoStopCor,
    houveSinal,
  };
}

export async function ultimoFechoEm(): Promise<string | null> {
  const sql = await getSql();
  const rows = await sql.query<Record<string, unknown>>(
    `select resolvido_em from bacbo_sinais
      where resolvido_em is not null
      order by resolvido_em desc
      limit 1`,
  );
  const row = rows[0];
  return row ? isoOrNull(row.resolvido_em) : null;
}

export async function ponta(): Promise<RodadaRow | null> {
  const sql = await getSql();
  const rows = await sql.query<Record<string, unknown>>(
    `select id, resultado, inicio, fim, pontos_jogador, pontos_banca, multiplicador, dado_j1, dado_j2, dado_b1, dado_b2, seq
       from bacbo_rodadas
      order by fim desc, seq desc
      limit 1`,
  );
  const row = rows[0];
  return row ? mapRound(row) : null;
}

export interface PulsoRow {
  ultimoOk: string | null;
  ultimaRodada: string | null;
  ultimaGravacao: string | null;
  ultimoErro: string | null;
  ultimoAviso: string | null;
  recebidas: number;
  gravadas: number;
  duplicadas: number;
  rejeitadas: number;
  paginas: number;
  duracaoMs: number;
  tentativas: number;
  arranques: number;
  estado: string;
}

export async function gravarPulso(pulso: PulsoRow): Promise<void> {
  const sql = await getSql();
  await sql.query(
    `insert into bacbo_pulso (
       id, ultimo_ok, ultima_rodada, ultima_gravacao, ultimo_erro, ultimo_aviso,
       recebidas, gravadas, duplicadas, rejeitadas, paginas, duracao_ms, tentativas, arranques, estado
     ) values (1, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
     on conflict (id) do update set
       ultimo_ok = excluded.ultimo_ok,
       ultima_rodada = excluded.ultima_rodada,
       ultima_gravacao = excluded.ultima_gravacao,
       ultimo_erro = excluded.ultimo_erro,
       ultimo_aviso = excluded.ultimo_aviso,
       recebidas = excluded.recebidas,
       gravadas = excluded.gravadas,
       duplicadas = excluded.duplicadas,
       rejeitadas = excluded.rejeitadas,
       paginas = excluded.paginas,
       duracao_ms = excluded.duracao_ms,
       tentativas = excluded.tentativas,
       arranques = greatest(bacbo_pulso.arranques, excluded.arranques),
       estado = excluded.estado,
       atualizado_em = now()`,
    [
      pulso.ultimoOk,
      pulso.ultimaRodada,
      pulso.ultimaGravacao,
      pulso.ultimoErro,
      pulso.ultimoAviso,
      pulso.recebidas,
      pulso.gravadas,
      pulso.duplicadas,
      pulso.rejeitadas,
      pulso.paginas,
      pulso.duracaoMs,
      pulso.tentativas,
      pulso.arranques,
      pulso.estado,
    ],
  );
}

export async function lerPulso(): Promise<PulsoRow | null> {
  const sql = await getSql();
  const rows = await sql.query<Record<string, unknown>>(
    `select ultimo_ok, ultima_rodada, ultima_gravacao, ultimo_erro, ultimo_aviso,
            recebidas, gravadas, duplicadas, rejeitadas, paginas, duracao_ms, tentativas, arranques, estado
       from bacbo_pulso where id = 1`,
  );
  const row = rows[0];
  if (!row) return null;
  return {
    ultimoOk: isoOrNull(row.ultimo_ok),
    ultimaRodada: isoOrNull(row.ultima_rodada),
    ultimaGravacao: isoOrNull(row.ultima_gravacao),
    ultimoErro: row.ultimo_erro ? String(row.ultimo_erro) : null,
    ultimoAviso: row.ultimo_aviso ? String(row.ultimo_aviso) : null,
    recebidas: int(row.recebidas),
    gravadas: int(row.gravadas),
    duplicadas: int(row.duplicadas),
    rejeitadas: int(row.rejeitadas),
    paginas: int(row.paginas),
    duracaoMs: int(row.duracao_ms),
    tentativas: int(row.tentativas),
    arranques: int(row.arranques),
    estado: String(row.estado ?? "aguardando"),
  };
}

export async function registarLacuna(de: string | null, ate: string | null, motivo: string): Promise<void> {
  const sql = await getSql();
  const recentes = await sql.query<{ n: number }>(
    `select count(*)::int as n from bacbo_lacunas
      where motivo = $1 and detetado_em > now() - interval '10 minutes'`,
    [motivo],
  );
  if (int(recentes[0]?.n) > 0) return;
  await sql.query(
    `insert into bacbo_lacunas (de, ate, motivo) values ($1, $2, $3)`,
    [de, ate, motivo],
  );
}

export async function lacunasRecentes(limit = 5): Promise<{ em: string; motivo: string }[]> {
  const sql = await getSql();
  const rows = await sql.query<Record<string, unknown>>(
    `select motivo, detetado_em from bacbo_lacunas order by detetado_em desc limit $1`,
    [limit],
  );
  return rows.map((row) => ({
    em: iso(row.detetado_em),
    motivo: String(row.motivo ?? ""),
  }));
}


========================================================================================
FICHEIRO: src/server/capunga/telegram.ts
========================================================================================

/** Telegram. O token nunca sai em log nem no painel. */

interface SendResult {
  ok: boolean;
  id: number | null;
  erro: string | null;
}

let analysisMessageId: number | null = null;

function credenciais(): { token: string; chat: string } | null {
  const token = process.env.BOT_TOKEN?.trim() ?? "";
  const chat = process.env.CHAT_ID?.trim() ?? "";
  if (!token || !chat) return null;
  return { token, chat };
}

export function telegramConfigurado(): boolean {
  return credenciais() !== null;
}

async function chamar(method: string, body: Record<string, unknown>): Promise<SendResult> {
  const cred = credenciais();
  if (!cred) return { ok: false, id: null, erro: "sem_telegram" };
  try {
    const response = await fetch(`https://api.telegram.org/bot${cred.token}/${method}`, {
      method: "POST",
      headers: { "content-type": "application/json", accept: "application/json" },
      body: JSON.stringify({ chat_id: cred.chat, disable_web_page_preview: true, ...body }),
      signal: AbortSignal.timeout(12_000),
    });
    const data = (await response.json()) as {
      ok?: boolean;
      description?: string;
      result?: { message_id?: number };
    };
    if (data.ok) return { ok: true, id: data.result?.message_id ?? null, erro: null };
    return { ok: false, id: null, erro: data.description ?? `http_${response.status}` };
  } catch (error) {
    return { ok: false, id: null, erro: error instanceof Error ? error.message : "telegram" };
  }
}

export async function enviar(texto: string): Promise<SendResult> {
  return chamar("sendMessage", { text: texto });
}

export async function apagar(messageId: number | null): Promise<void> {
  if (!messageId) return;
  await chamar("deleteMessage", { message_id: messageId });
}

/** Mantém uma única mensagem informativa; nunca envia uma nova a cada ciclo. */
export async function estadoAnalise(ativo: boolean): Promise<void> {
  if (!ativo) {
    if (analysisMessageId !== null) await apagar(analysisMessageId);
    analysisMessageId = null;
    return;
  }

  if (analysisMessageId !== null) return;
  const text = [
    "🔎 A analisar o mercado Bac Bo…",
    "A aguardar uma configuração válida. Os resultados não são garantidos.",
  ].join("\n");

  if (analysisMessageId === null) {
    const sent = await enviar(text);
    if (sent.ok && sent.id !== null) analysisMessageId = sent.id;
  }
}


========================================================================================
FICHEIRO: src/server/capunga/textos.ts
========================================================================================

import { textoPct, type LinhaFolha } from "./folha.ts";
import { resumirFolha } from "./folha.ts";
import { horaLuanda } from "./tempo.ts";
import { titulosDe } from "./logics.ts";
import type { FolhaNum, Outcome, Side } from "./types.ts";

export function textoEntrada(cor: Side): string {
  const linha = cor === "Jogador" ? "🔵 Entrada confirmada" : "🔴 Entrada confirmada";
  return [linha, "🟡 Proteção no empate"].join("\n");
}

export function textoGale(nivel: 1 | 2, retomado: boolean): string {
  const base = `🐔 Gale ${nivel}`;
  if (!retomado) return base;
  return [base, "Fio retomado na hora do sinal, não no acordar."].join("\n");
}

export function textoResultado(
  estado: "GREEN" | "G1" | "G2" | "LOSS",
  porEmpate: boolean,
): string {
  if (porEmpate) return "🟡 GREEN · Empate";
  if (estado === "GREEN") return "🟩 GREEN";
  if (estado === "G1") return "🟨 GREEN · G1";
  if (estado === "G2") return "🟧 GREEN · G2";
  return "🟥 STOP";
}

export function textoLacuna(): string {
  return [
    "〰️ Sem fio",
    "A mesa andou e as rodadas do meio não voltaram.",
    "Não fechei este sinal na hora em que acordei.",
  ].join("\n");
}

export function textoExpirado(id: number): string {
  return `⌛ Sinal #${id} expirou sem rodada nova`;
}

export function textoFolha(f: FolhaNum): string {
  const seq = f.sequencia > 0 ? `+${f.sequencia}` : String(f.sequencia);
  return [
    "FOLHA DO DIA",
    `Limpos ${f.limpos} · G1 ${f.gale1} · G2 ${f.gale2}`,
    `Empates ${f.empates} · Stops ${f.stops}`,
    f.semFio ? `Sem fio ${f.semFio}` : "",
    `Gestão ${textoPct(f.gestao)} · Pureza ${textoPct(f.pureza)}`,
    `Sequência ${seq}`,
  ]
    .filter(Boolean)
    .join("\n");
}

export function textoFechoCompleto(
  estado: "GREEN" | "G1" | "G2" | "LOSS",
  porEmpate: boolean,
  linhas: LinhaFolha[],
  rodada?: { resultado: Outcome; pontosJogador: number | null; pontosBanca: number | null },
): string {
  const detalhes: string[] = [];
  if (rodada?.pontosJogador !== null && rodada?.pontosJogador !== undefined && rodada.pontosBanca !== null) {
    detalhes.push(`🎲 Resultado: 🔵 Jogador ${rodada.pontosJogador} · 🔴 Banca ${rodada.pontosBanca}`);
    if (rodada.resultado === "Empate") {
      detalhes.push("🟡 Cor vencedora: Empate");
    } else {
      const lado = rodada.resultado === "Banca" ? "🔴 Banca" : "🔵 Jogador";
      const pontos = rodada.resultado === "Banca" ? rodada.pontosBanca : rodada.pontosJogador;
      detalhes.push(`🏆 Cor vencedora: ${lado} · ${pontos} pontos`);
    }
  } else if (rodada) {
    detalhes.push(`🏆 Resultado da rodada: ${rodada.resultado}`);
  }
  return [textoResultado(estado, porEmpate), ...detalhes, "", textoFolha(resumirFolha(linhas))]
    .filter((linha, index, all) => linha !== "" || all[index - 1] !== "")
    .join("\n");
}

export const MOTIVOS: Record<string, string> = {
  amostra_curta: "Histórico ainda curto para um sinal puro.",
  sem_familia: "Nenhuma lógica limpa reconheceu a mesa.",
  sem_consenso: "Só uma família viu a entrada. Falta consenso.",
  isolado_sujo: "Padrão isolado, e a mesa não confirmou.",
  conflito: "As famílias discordam. Entrada recusada.",
  abaixo_da_barra: "A pureza ficou abaixo da barra desta mesa.",
  mesa_mista: "Regime misturado. Sem entrada.",
  ritmo_rodadas: "Ainda faltam rodadas decisivas desde o último fecho.",
  ritmo_cheio: "A janela já tem sinais demais. Só pureza elite.",
  ritmo_teto: "Teto da janela. A mesa precisa de ar.",
  anti_revanche: "Troca de cor logo após um stop. Recusado.",
  sinal_aberto: "Há um fio aberto.",
  ponta_fria: "A ponta já arrefeceu. Não abri sinal no acordar.",
  rodada_empate: "Empate não abre fio.",
  historico: "Rodada antiga — só ficou no histórico.",
  semeando: "A ler o histórico inicial. Sem sinais retroativos.",
  entrada: "Entrada aberta na ponta da mesa.",
  entrada_tardia: "Entrada tardia, ainda dentro do filtro.",
  entrada_espera: "Cinco minutos sem sinal. Entrada aberta na direção da mesa.",
  sinais_pausados: "Sinais pausados.",
  lacuna: "Houve um buraco na mesa. O fio antigo não foi julgado no acordar.",
  expirado: "O sinal expirou sem rodada nova.",
  aguardando: "De olho na mesa.",
  janela_vazia: "A fonte não trouxe mãos neste instante. O histórico fica e o pulso continua.",
  fechado: "O fio fechou.",
  gale1: "Gale 1.",
  gale2: "Gale 2.",
  stop: "Stop.",
};

export function motivoHumano(codigo: string | null | undefined): string {
  if (!codigo) return "De olho na mesa.";
  return MOTIVOS[codigo] ?? "De olho na mesa.";
}

export function etiquetaEstrategia(ids: string[]): string {
  return titulosDe(ids).join(" · ");
}

export function horaCurta(iso: string): string {
  return horaLuanda(new Date(iso).getTime());
}


========================================================================================
FICHEIRO: src/server/capunga/pure.test.ts
========================================================================================

import assert from "node:assert/strict";
import test from "node:test";
import { avancar } from "./avancar.ts";
import { entradaPorEspera, ESPERA_MAX_MS, resolverEntrada } from "./espera.ts";
import { decidir } from "./decide.ts";
import { estudar } from "./estudar.ts";
import { resumirFolha } from "./folha.ts";
import { fromTape } from "./mesa.ts";
import { ritmoPermite } from "./ritmo.ts";
import { parseRounds } from "./normalize.ts";
import { textoFechoCompleto } from "./textos.ts";
import { inicioDoDiaLuanda, LACUNA_MS, LIVE_EDGE_MS } from "./tempo.ts";
import { RITMO_LIVRE, type RitmoCtx } from "./types.ts";

const livre: RitmoCtx = { ...RITMO_LIVRE };

test("mensagem de fecho identifica empate e a pontuação real da rodada", () => {
  const texto = textoFechoCompleto(
    "G2",
    false,
    [],
    { resultado: "Banca", pontosJogador: 7, pontosBanca: 9 },
  );
  assert.match(texto, /GREEN · G2/);
  assert.match(texto, /Jogador 7 · 🔴 Banca 9/);
  assert.match(texto, /Cor vencedora: 🔴 Banca · 9 pontos/);
});

test("gale 2: duas falhas recuperam, a terceira é stop", () => {
  assert.equal(avancar(0, "Banca", "Jogador", 30_000).tipo, "gale");
  assert.equal(avancar(1, "Banca", "Jogador", 30_000).tipo, "gale");
  assert.equal(avancar(1, "Banca", "Jogador", 30_000).gale, 2);
  assert.equal(avancar(2, "Banca", "Banca", 30_000).tipo, "fechar");
  assert.equal(avancar(2, "Banca", "Banca", 30_000).estado, "G2");
  assert.equal(avancar(2, "Banca", "Jogador", 30_000).tipo, "stop");
});

test("empate fecha green e não avança gale", () => {
  const passo = avancar(1, "Jogador", "Empate", 32_000);
  assert.equal(passo.tipo, "fechar");
  assert.equal(passo.estado, "G1");
  assert.equal(passo.porEmpate, true);
});

test("buraco na mesa não julga o sinal no acordar", () => {
  const passo = avancar(0, "Banca", "Banca", LACUNA_MS + 5_000);
  assert.equal(passo.tipo, "lacuna");
  const natural = avancar(0, "Banca", "Jogador", 120_000);
  assert.equal(natural.tipo, "gale");
});

test("antes dos 5 min o teto não abre mesa curta", () => {
  const fita = fromTape("BBJ");
  assert.equal(entradaPorEspera(fita, ESPERA_MAX_MS - 1), null);
  assert.equal(resolverEntrada(fita, livre, 60_000).ok, false);
});

test("aos 5 min a próxima mão entra mesmo sem padrão puro", () => {
  const fita = fromTape("BBJ");
  const tarde = resolverEntrada(fita, livre, ESPERA_MAX_MS);
  assert.equal(tarde.ok, true);
  if (tarde.ok) {
    assert.equal(tarde.entrada.porTeto, true);
    assert.equal(tarde.entrada.codigo, "entrada_espera");
    assert.equal(tarde.entrada.cor, "Banca");
  }
});

test("teto de 5 min fura o ritmo quando a mesa já está limpa", () => {
  const fita = fromTape("B".repeat(40));
  const ctx = { ...livre, sinaisNaJanela: 12, houveSinal: true, decisivasDesdeFecho: 20 };
  assert.equal(resolverEntrada(fita, ctx, 60_000).ok, false);
  const tarde = resolverEntrada(fita, ctx, ESPERA_MAX_MS);
  assert.equal(tarde.ok, true);
  if (tarde.ok) {
    assert.equal(tarde.entrada.cor, "Banca");
    assert.equal(tarde.entrada.porTeto, true);
  }
});

test("corrida limpa de banca pede banca, não o contrário", () => {
  const fita = "B".repeat(40);
  const j = decidir(fromTape(fita), livre);
  assert.ok(j.sinal, j.recusa);
  assert.equal(j.sinal?.cor, "Banca");
  assert.ok((j.sinal?.familias.length ?? 0) >= 2);
  assert.ok((j.sinal?.pureza ?? 0) >= 82);
});

test("alternância estrita pede a cor oposta à última", () => {
  const fita = "BJ".repeat(20);
  const j = decidir(fromTape(fita), livre);
  assert.ok(j.sinal, j.recusa);
  assert.equal(j.sinal?.cor, "Banca");
});

test("amostra curta não abre", () => {
  const j = decidir(fromTape("BJBJBBJJBJB"), livre);
  assert.equal(j.sinal, null);
  assert.equal(j.recusa, "amostra_curta");
});

test("ritmo bloqueia metralhadora e revanche imediata", () => {
  const fita = "B".repeat(40);
  const j = decidir(fromTape(fita), livre);
  assert.ok(j.sinal);
  const cheio = ritmoPermite(j.sinal, { ...livre, sinaisNaJanela: 12, houveSinal: true });
  assert.equal(cheio.ok, false);
  if (!cheio.ok) assert.equal(cheio.codigo, "ritmo_teto");
  const revanche = ritmoPermite(j.sinal, {
    ...livre,
    houveSinal: true,
    decisivasDesdeFecho: 2,
    ultimoStopCor: "Jogador",
    sinaisNaJanela: 1,
  });
  assert.equal(revanche.ok, false);
});

test("folha vazia não finge 100 e o empate não conta como pureza de cor", () => {
  const vazia = resumirFolha([]);
  assert.equal(vazia.gestao, null);
  assert.equal(vazia.pureza, null);
  const folha = resumirFolha([
    { estado: "GREEN", cor: "Banca", resultadoFinal: "Banca", resolvidoEm: "2026-10-20T10:00:00.000Z", criadoEm: "2026-10-20T09:59:00.000Z" },
    { estado: "GREEN", cor: "Banca", resultadoFinal: "Empate", resolvidoEm: "2026-10-20T10:02:00.000Z", criadoEm: "2026-10-20T10:01:00.000Z" },
    { estado: "G2", cor: "Jogador", resultadoFinal: "Jogador", resolvidoEm: "2026-10-20T10:06:00.000Z", criadoEm: "2026-10-20T10:03:00.000Z" },
    { estado: "LOSS", cor: "Banca", resultadoFinal: "Jogador", resolvidoEm: "2026-10-20T10:10:00.000Z", criadoEm: "2026-10-20T10:07:00.000Z" },
    { estado: "LACUNA", cor: "Banca", resultadoFinal: null, resolvidoEm: "2026-10-20T10:12:00.000Z", criadoEm: "2026-10-20T10:11:00.000Z" },
  ]);
  assert.equal(folha.limpos, 1);
  assert.equal(folha.empates, 1);
  assert.equal(folha.gale2, 1);
  assert.equal(folha.stops, 1);
  assert.equal(folha.semFio, 1);
  assert.equal(folha.fechados, 4);
  assert.equal(folha.gestao, 75);
  assert.equal(folha.pureza, 33.3);
  assert.equal(folha.sequencia, -1);
});

test("estudo carimba o sinal na hora da rodada, não num relógio posterior", () => {
  const base = Date.parse("2026-10-20T18:00:00.000Z");
  const rounds = Array.from({ length: 48 }, (_, i) => ({
    id: `r${i}`,
    fim: new Date(base + i * 32_000).toISOString(),
    resultado: "Banca" as const,
  }));
  const estudo = estudar(rounds);
  assert.ok(estudo.sinais >= 1);
  assert.ok((estudo.folha.limpos ?? 0) >= 1);
  assert.equal(estudo.folha.stops, 0);
  const dia = inicioDoDiaLuanda(Date.parse("2026-10-20T22:30:00.000Z"));
  assert.equal(dia, "2026-10-19T23:00:00.000Z");
});

test("o registo não corta a cauda: cada mão resolvida entra", () => {
  const body = Array.from({ length: 80 }, (_, i) => ({
    id: `raiz${i}`,
    data: {
      id: `mao${i}`,
      status: "Resolved",
      settledAt: new Date(Date.parse("2026-10-20T12:00:00.000Z") + i * 30_000).toISOString(),
      result: {
        outcome: i % 2 ? "PlayerWon" : "BankerWon",
        playerDice: { score: 9, first: 5, second: 4 },
        bankerDice: { score: 6, first: 3, second: 3 },
      },
    },
  }));
  const rounds = parseRounds(body);
  assert.equal(rounds.length, 80);
  assert.equal(rounds[0]?.id, "mao0");
  assert.equal(rounds[79]?.id, "mao79");
});

test("a cauda curta da api não pode parecer mesa vazia nem calar o pulso", () => {
  assert.throws(() => parseRounds([]), /api_vazia/);
  assert.throws(() => parseRounds({ error: "blocked" }), /api_corpo|api_vazia/);
  assert.ok(LIVE_EDGE_MS >= 120_000);
});

test("estudo não atravessa lacuna como se a rodada tardia fosse a seguinte", () => {
  const a = Date.parse("2026-10-20T18:00:00.000Z");
  const rounds: Array<{ id: string; fim: string; resultado: "Banca" | "Jogador" }> = [
    ...Array.from({ length: 30 }, (_, i) => ({
      id: `a${i}`,
      fim: new Date(a + i * 32_000).toISOString(),
      resultado: "Banca" as const,
    })),
  ];
  const corte = rounds.length;
  rounds.push({
    id: "tarde",
    fim: new Date(a + corte * 32_000 + 30 * 60_000).toISOString(),
    resultado: "Jogador",
  });
  const estudo = estudar(rounds);
  assert.ok(estudo.folha.semFio >= 1 || estudo.folha.stops === 0);
});