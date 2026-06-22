import json
import logging

logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(message)s")

import main
from playwright.sync_api import sync_playwright

title = main.GRAFANA_PANEL_TITLE_GAMES_GENERAL
sess = main.grafana_login_session()
caps = main._grafana_resolve_alert_solo_captures(
    sess, [title], relative_from="now-1h", relative_to="now"
)
url, solo_title, panel_id = caps[0]
cookies = main._playwright_cookie_list(sess)
timeout_ms = 60000

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
    ctx = browser.new_context(viewport={"width": 1400, "height": 800})
    ctx.add_cookies(cookies)
    page = ctx.new_page()
    page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
    try:
        page.wait_for_load_state("networkidle", timeout=20000)
    except Exception:
        pass
    page.wait_for_timeout(4000)

    info = page.evaluate(
        r"""() => {
          const out = {};
          // Find any element whose exact text is 1492288
          const all = Array.from(document.querySelectorAll('*'));
          const hit = all.filter(e => (e.children.length === 0) && (e.textContent || '').trim() === '1492288');
          out.exactTextNodes = hit.slice(0, 5).map(e => {
            const path = [];
            let n = e;
            for (let i = 0; i < 6 && n; i++) {
              let d = n.tagName ? n.tagName.toLowerCase() : '';
              if (n.getAttribute) {
                const tid = n.getAttribute('data-testid');
                const cls = n.getAttribute('class');
                if (tid) d += '[testid=' + tid + ']';
                if (cls) d += '.' + String(cls).split(/\s+/).slice(0,3).join('.');
              }
              path.push(d);
              n = n.parentElement;
            }
            return { tag: e.tagName, testid: e.getAttribute('data-testid'), cls: e.getAttribute('class'), path };
          });
          // Count common legend selectors
          const sels = [
            '[data-testid="viz-legend"]',
            '.viz-legend',
            'section[aria-label="Legend"]',
            '[data-testid*="VizLegend series"]',
            '[data-testid*="VizLegend"]',
            'table.viz-legend tbody tr',
            'button[type="button"]',
            '[class*="LegendItem"]',
            '[class*="Legend"]',
            '[aria-label*="legend" i]',
          ];
          out.counts = {};
          for (const s of sels) {
            try { out.counts[s] = document.querySelectorAll(s).length; } catch(e) { out.counts[s] = 'ERR'; }
          }
          // Sample data-testids present
          const tids = new Set();
          all.forEach(e => { const t = e.getAttribute && e.getAttribute('data-testid'); if (t) tids.add(t); });
          out.sampleTestids = Array.from(tids).filter(t => /legend|series|viz/i.test(t)).slice(0, 30);
          return out;
        }"""
    )
    print(json.dumps(info, indent=2, ensure_ascii=False))
    browser.close()
