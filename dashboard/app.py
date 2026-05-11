"""
Komponen 4 — Dashboard: Serving Layer
GitTrend | ETS Big Data | Kelompok 8
Dikerjakan: AZARIA RAISSA MAULIDINNISA (5027241043)
"""
import json
import os
from flask import Flask, render_template, jsonify

app = Flask(__name__)
# Di Docker, data di-mount ke /app/data
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


def load_json(filename, default=None):
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path):
        return default if default is not None else {}
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return default if default is not None else {}


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/data")
def api_data():
    spark    = load_json("spark_results.json", {})
    live_api = load_json("live_api.json", [])
    live_rss = load_json("live_rss.json", [])

    # Dedup top10_repos berdasarkan full_name
    seen, deduped = set(), []
    for r in spark.get("top10_repos", []):
        if r.get("full_name") not in seen:
            seen.add(r.get("full_name"))
            deduped.append(r)
    spark["top10_repos"] = deduped

    live_api = sorted(live_api, key=lambda x: x.get("timestamp", ""), reverse=True)[:20]
    live_rss = sorted(live_rss, key=lambda x: x.get("timestamp", ""), reverse=True)[:15]

    return jsonify({"spark": spark, "live_api": live_api, "live_rss": live_rss})


@app.route("/api/health")
def health():
    files = {f: os.path.exists(os.path.join(DATA_DIR, f))
             for f in ["spark_results.json", "live_api.json", "live_rss.json"]}
    return jsonify({"status": "ok", "files": files})


if __name__ == "__main__":
    os.makedirs(DATA_DIR, exist_ok=True)
    print("GitTrend Dashboard → http://localhost:5000")
    app.run(host="0.0.0.0", port=5000, debug=False)
