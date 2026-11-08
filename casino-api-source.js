'use strict';

const DEFAULT_API_URL =
  'https://api-cs.casino.org/svc-evolution-game-events/api/bacbo?page=0&size=50&sort=data.settledAt,desc&duration=120&wheelResults=PlayerWon,BankerWon,Tie';

class CasinoScoresApiSource {
  constructor({
    url = DEFAULT_API_URL,
    runtime
  } = {}) {
    this.url = url;
    this.runtime = runtime;
  }

  async snapshot() {
    const started = Date.now();

    const response = await fetch(this.url, {
      headers: {
        Accept: 'application/json'
      },
      signal: AbortSignal.timeout(15000)
    });

    if (this.runtime) {
      this.runtime.lastHttpStatus = response.status;
      this.runtime.lastLatencyMs = Date.now() - started;
    }

    if (!response.ok) {
      throw new Error(
        `Fonte CasinoScores respondeu HTTP ${response.status}`
      );
    }

    const payload = await response.json();

    if (!Array.isArray(payload)) {
      throw new Error(
        'Fonte CasinoScores não retornou uma lista de rodadas'
      );
    }

    if (this.runtime) {
      this.runtime.browserConnected = false;
      this.runtime.pageUrl = this.url;
    }

    return payload;
  }

  async close() {}
}

module.exports = {
  CasinoScoresApiSource,
  DEFAULT_API_URL
};
