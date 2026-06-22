import json, logging
logging.basicConfig(level=logging.WARNING)
import main
from playwright.sync_api import sync_playwright

want = "1492288"
title = main.GRAFANA_PANEL_TITLE_GAMES_GENERAL
sess = main.grafana_login_session()
url, solo_title, panel_id = main._grafana_resolve_alert_solo_captures(sess, [title], relative_from="now-1h", relative_to="now")[0]
cookies = main._playwright_cookie_list(sess)

def snapshot(page):
    return page.evaluate(
        r"""(want) => {
          const rows = Array.from(document.querySelectorAll('tr[class*="LegendRow"]'));
          // chart series count: uPlot draws series as paths; count legend rows whose label not dimmed
          const sample = rows.slice(0,3).map(r => {
            const b = r.querySelector('button');
            return { rowCls: r.className, rowStyle: r.getAttribute('style'),
                     btnCls: b && b.className, btnStyle: b && b.getAttribute('style'),
                     opacity: getComputedStyle(r).opacity,
                     txt: (b && b.textContent || '').trim() };
          });
          // find target row
          let target = null;
          for (const r of rows) {
            const b = r.querySelector('button');
            if (b && (b.textContent||'').trim() === want) {
              target = { rowCls: r.className, rowStyle: r.getAttribute('style'),
                         btnStyle: b.getAttribute('style'), opacity: getComputedStyle(r).opacity,
                         bg: getComputedStyle(r).backgroundColor };
              break;
            }
          }
          // count uPlot canvas series via legend label color swatches that are visible
          return { rowsTotal: rows.length, sample, target };
        }""",
        want,
    )

def click_target(page):
    page.evaluate(
        r"""(want) => {
          const btns = Array.from(document.querySelectorAll('button'));
          const el = btns.find(b => (b.textContent||'').trim()===want && /LegendLabel/.test(b.className));
          if (el) { el.scrollIntoView({block:'center'}); }
          return !!el;
        }""", want)
    c = page.evaluate(
        r"""(want) => {
          const btns = Array.from(document.querySelectorAll('button'));
          const el = btns.find(b => (b.textContent||'').trim()===want && /LegendLabel/.test(b.className));
          const r = el.getBoundingClientRect();
          return { x: r.left+r.width/2, y: r.top+r.height/2 };
        }""", want)
    page.mouse.click(c["x"], c["y"])
    page.wait_for_timeout(1500)

with sync_playwright() as p:
    b = p.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage"])
    ctx = b.new_context(viewport={"width":1400,"height":800}); ctx.add_cookies(cookies)
    page = ctx.new_page(); page.goto(url, wait_until="domcontentloaded", timeout=60000)
    try: page.wait_for_load_state("networkidle", timeout=20000)
    except Exception: pass
    page.wait_for_timeout(4000)
    print("BEFORE:", json.dumps(snapshot(page), ensure_ascii=False), flush=True)
    click_target(page)
    print("AFTER :", json.dumps(snapshot(page), ensure_ascii=False), flush=True)
    b.close()
