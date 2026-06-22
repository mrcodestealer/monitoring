import logging
logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(message)s")

import main
from playwright.sync_api import sync_playwright

want = "1492288"
title = main.GRAFANA_PANEL_TITLE_GAMES_GENERAL
sess = main.grafana_login_session()
caps = main._grafana_resolve_alert_solo_captures(sess, [title], relative_from="now-1h", relative_to="now")
url, solo_title, panel_id = caps[0]
cookies = main._playwright_cookie_list(sess)

def vis(page):
    return page.evaluate(
        r"""() => {
          const rows = Array.from(document.querySelectorAll('tr[class*="LegendRow"]'));
          let visible = 0;
          rows.forEach(r => { if (parseFloat(getComputedStyle(r).opacity) >= 0.5) visible++; });
          return { total: rows.length, visible };
        }"""
    )

def find_coords(page, label):
    return page.evaluate(
        r"""(label) => {
          const btns = Array.from(document.querySelectorAll('button'));
          const hit = btns.filter(b => (b.textContent || '').trim() === label && /LegendLabel/.test(b.className));
          const any = btns.filter(b => (b.textContent || '').trim() === label);
          const info = { labelBtns: hit.length, anyBtns: any.length };
          const el = hit[0] || any[0];
          if (el) {
            el.scrollIntoView({block:'center'});
            const r = el.getBoundingClientRect();
            info.cls = el.className;
            info.rect = { x: r.left + r.width/2, y: r.top + r.height/2, w: r.width, h: r.height };
          }
          return info;
        }""",
        label,
    )

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
    ctx = browser.new_context(viewport={"width": 1400, "height": 800})
    ctx.add_cookies(cookies)
    page = ctx.new_page()
    page.goto(url, wait_until="domcontentloaded", timeout=60000)
    try:
        page.wait_for_load_state("networkidle", timeout=20000)
    except Exception:
        pass
    page.wait_for_timeout(4000)

    print("before:", vis(page), flush=True)
    c = find_coords(page, want)
    print("coords:", c, flush=True)
    if not c.get("rect"):
        print("NO BUTTON FOUND", flush=True); browser.close(); raise SystemExit(1)

    x = c["rect"]["x"]; y = c["rect"]["y"]
    page.mouse.click(x, y)
    page.wait_for_timeout(1500)
    print("after single-click coords:", vis(page), flush=True)
    page.screenshot(path="_iso_probe_single.png")

    # toggle back
    c2 = find_coords(page, want)
    page.mouse.click(c2["rect"]["x"], c2["rect"]["y"])
    page.wait_for_timeout(1500)
    print("after second-click:", vis(page), flush=True)

    browser.close()
