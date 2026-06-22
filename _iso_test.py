import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

import main

want = "1492288"
title = main.GRAFANA_PANEL_TITLE_GAMES_GENERAL

sess = main.grafana_login_session()
caps = main._grafana_resolve_alert_solo_captures(
    sess, [title], relative_from="now-1h", relative_to="now"
)
print("caps_count:", len(caps), "panel_ids:", [pid for (_u, _t, pid) in caps], flush=True)
if not caps:
    print("NO CAPS - panel not found", flush=True)
    sys.exit(1)

url, solo_title, panel_id = caps[0]
print("panel_id=", panel_id, flush=True)
print("url=", url.encode("ascii", "replace").decode(), flush=True)

png = main._grafana_headless_screenshot_png_at_url(
    sess,
    url,
    highlight_panel_titles=[solo_title],
    solo_primary_title=solo_title,
    isolate_series_label=want,
    isolate_series_candidates=[want],
    dense_legend_isolate=True,
)
out = "_iso_test_out.png"
with open(out, "wb") as f:
    f.write(png)
print("WROTE", out, len(png), "bytes", flush=True)
