'use strict';

const puppeteer = require('puppeteer');

const DEFAULT_URL =
  'https://www.casino.org/casinoscores/bac-bo/';

class CasinoScoresSource {
  constructor({
    url = DEFAULT_URL,
    navigationTimeoutMs = 30000,
    waitMs = 4000,
    runtime
  } = {}) {
    this.url = url;
    this.navigationTimeoutMs = navigationTimeoutMs;
    this.waitMs = waitMs;
    this.runtime = runtime;
    this.browser = null;
    this.page = null;
  }

  async connect() {
    if (this.page && !this.page.isClosed()) {
      return;
    }

    this.browser = await puppeteer.launch({
      headless: true,
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-gpu'
      ]
    });

    this.page = await this.browser.newPage();

    await this.page.setViewport({
      width: 1440,
      height: 1200,
      deviceScaleFactor: 1
    });

    await this.page.setUserAgent(
      'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/131 Safari/537.36'
    );

    await this.page.goto(this.url, {
      waitUntil: 'domcontentloaded',
      timeout: this.navigationTimeoutMs
    });

    await this.page.waitForSelector(
      '#SpinHistoryTableBacBo tbody tr',
      {
        timeout: this.navigationTimeoutMs
      }
    );

    if (this.runtime) {
      this.runtime.browserConnected = true;
    }
  }

  async snapshot() {
    try {
      await this.connect();

      const rows = await this.page.$$eval(
        '#SpinHistoryTableBacBo tbody tr',
        elements =>
          elements.map((row, index) => {
            const cells = [
              ...row.querySelectorAll('td')
            ];

            const finished =
              cells[0]?.innerText
                ?.replace(/\s+/g, ' ')
                .trim() || '';

            const outcomeText =
              cells[1]?.innerText
                ?.replace(/\s+/g, ' ')
                .trim() || '';

            const payout =
              cells[2]?.innerText
                ?.replace(/\s+/g, ' ')
                .trim() || null;

            const winners =
              cells[3]?.innerText
                ?.replace(/\s+/g, ' ')
                .trim() || null;

            const playerMatch = outcomeText.match(
              /P:\s*(?:[^0-9]*?)Σ\s*(\d+)/i
            );

            const bankerMatch = outcomeText.match(
              /B:\s*(?:[^0-9]*?)Σ\s*(\d+)/i
            );

            return {
              sourceId:
                row.getAttribute('data-id') ||
                row.dataset?.id ||
                null,

              finished,
              outcomeText,

              playerScore: playerMatch
                ? Number(playerMatch[1])
                : null,

              bankerScore: bankerMatch
                ? Number(bankerMatch[1])
                : null,

              payout,
              winners,
              sourceIndex: index
            };
          })
      );

      if (!rows.length) {
        throw new Error(
          'Tabela SpinHistoryTableBacBo sem rodadas'
        );
      }

      if (this.runtime) {
        this.runtime.browserConnected = true;
        this.runtime.pageUrl = this.page.url();
      }

      return rows;
    } catch (error) {
      if (this.runtime) {
        this.runtime.browserConnected = false;
      }

      await this.reconnect();
      throw error;
    }
  }

  async reconnect() {
    await this.close();
  }

  async close() {
    this.page = null;

    if (this.browser) {
      try {
        await this.browser.close();
      } catch (_) {}
    }

    this.browser = null;

    if (this.runtime) {
      this.runtime.browserConnected = false;
    }
  }
}

module.exports = {
  CasinoScoresSource,
  DEFAULT_URL
};
