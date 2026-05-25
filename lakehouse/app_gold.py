"""
Lakehouse Dashboard — Gold Layer
GitTrend | Tugas Week 12 | Kelompok 8
Dikerjakan: Revalina Erica Permatasari (5027241007)

Flask server terpisah di port 5001.
Membaca langsung dari tabel Gold Delta Lake (hasil pipeline lakehouse).
"""
import glob
import os
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

GOLD_DIR = "/app/lakehouse/lakehouse_data"


def baca_parquet(path):
    try:
        files = glob.glob(f"{path}/*.parquet")
        if not files:
            return []
        
        # Urutkan file berdasarkan waktu modifikasi terbaru dahulu
        files.sort(key=os.path.getmtime, reverse=True)
        
        import pyarrow.parquet as pq
        import pyarrow as pa
        
        tables = []
        for f in files:
            try:
                tables.append(pq.read_table(f))
            except Exception as e:
                print(f"[dashboard-error] Gagal membaca file {f}: {e}")
                pass
                
        if not tables:
            return []
            
        # Filter hanya tabel yang memiliki skema kompatibel dengan file terbaru
        compatible_tables = []
        base_schema = tables[0].schema
        for t in tables:
            if t.schema == base_schema:
                compatible_tables.append(t)
            else:
                print(f"[dashboard-warning] Skema parquet lama dilewati karena tidak cocok dengan skema terbaru.")
                
        if not compatible_tables:
            return []
            
        combined = pa.concat_tables(compatible_tables)
        d = combined.to_pydict()
        keys = list(d.keys())
        if not keys:
            return []
            
        result = []
        seen = set()
        for i in range(len(d[keys[0]])):
            row = {}
            for k in keys:
                val = d[k][i]
                if hasattr(val, "item"):
                    val = val.item()
                if hasattr(val, "isoformat"):
                    val = str(val)
                row[k] = val
                
            # Kunci deduplikasi pintar
            uniq_key = row.get("full_name") or row.get("repo") or row.get("word") or row.get("language") or str(row)
            if uniq_key not in seen:
                seen.add(uniq_key)
                result.append(row)
        return result
    except Exception as e:
        print(f"[dashboard-error] Error di baca_parquet: {e}")
        return []


HTML = """<!DOCTYPE html>
<html lang="id">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>GitTrend Lakehouse — Gold Layer</title>
  <link href="https://fonts.googleapis.com/css2?family=Syne:wght@400;700;800&family=DM+Mono:wght@400;500&family=DM+Sans:wght@400;500&display=swap" rel="stylesheet">
  <script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
  <style>
    :root {
      --bg: #0a0a0f;
      --surface: #111118;
      --card: #16161f;
      --border: rgba(255,255,255,0.07);
      --border2: rgba(255,255,255,0.14);
      --text: #e8e8f0;
      --muted: #6b6b80;
      --accent: #7c6af7;
      --green: #4fd1a5;
      --orange: #f4a261;
      --red: #f07070;
      --gold: #fbbf24;
      --mono: 'DM Mono', monospace;
      --sans: 'DM Sans', sans-serif;
      --display: 'Syne', sans-serif;
    }
    *,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
    body{background:var(--bg);color:var(--text);font-family:var(--sans);font-size:14px;line-height:1.6;min-height:100vh;overflow-x:hidden}

    header{position:sticky;top:0;z-index:100;background:rgba(10,10,15,0.94);backdrop-filter:blur(14px);border-bottom:1px solid var(--border);padding:0 28px;height:58px;display:flex;align-items:center;justify-content:space-between}
    .logo{display:flex;align-items:center;gap:10px}
    .logo-mark{width:30px;height:30px;background:var(--gold);border-radius:8px;display:flex;align-items:center;justify-content:center;font-family:var(--display);font-weight:800;font-size:12px;color:#000;letter-spacing:-0.03em}
    .logo-text{font-family:var(--display);font-weight:700;font-size:1rem;letter-spacing:-0.02em}
    .logo-sub{color:var(--muted);font-size:0.68rem;font-family:var(--mono);margin-top:1px}
    .hdr-right{display:flex;align-items:center;gap:14px}
    .gold-badge{display:flex;align-items:center;gap:6px;font-family:var(--mono);font-size:0.67rem;color:var(--gold);background:rgba(251,191,36,0.08);border:1px solid rgba(251,191,36,0.2);padding:3px 10px;border-radius:20px}
    #last-update{font-family:var(--mono);font-size:0.67rem;color:var(--muted)}

    main{max-width:1300px;margin:0 auto;padding:28px 40px 60px;display:grid;gap:20px}
    .sec-label{font-family:var(--mono);font-size:0.64rem;letter-spacing:0.1em;text-transform:uppercase;color:var(--muted);margin-bottom:14px;display:flex;align-items:center;gap:8px}
    .sec-label::after{content:'';flex:1;height:1px;background:var(--border)}

    .card{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:22px 28px}
    .card-title{font-family:var(--display);font-size:0.92rem;font-weight:700;margin-bottom:16px;color:var(--text);display:flex;align-items:center;gap:8px}
    .ctag{font-family:var(--mono);font-size:0.6rem;padding:2px 7px;border-radius:4px;background:rgba(251,191,36,0.1);color:var(--gold);border:1px solid rgba(251,191,36,0.2);font-weight:400}
    .ctag-purple{font-family:var(--mono);font-size:0.6rem;padding:2px 7px;border-radius:4px;background:rgba(124,106,247,0.12);color:var(--accent);border:1px solid rgba(124,106,247,0.2);font-weight:400}

    .stats-row{display:grid;grid-template-columns:repeat(auto-fit,minmax(155px,1fr));gap:13px}
    .stat{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:15px 17px}
    .stat-label{font-family:var(--mono);font-size:0.63rem;text-transform:uppercase;letter-spacing:0.08em;color:var(--muted);margin-bottom:7px}
    .stat-val{font-family:var(--display);font-size:2rem;font-weight:800;line-height:1;color:var(--gold)}
    .stat-val.g{color:var(--green)}
    .stat-val.sm{font-size:.85rem;color:var(--green);margin-top:4px}

    .two-col{display:grid;grid-template-columns:1fr 1fr;gap:20px}
    @media(max-width:860px){.two-col{grid-template-columns:1fr}}

    .lang-rows{display:flex;flex-direction:column;gap:8px}
    .lang-row{display:flex;align-items:center;gap:12px}
    .lang-name{font-family:var(--mono);font-size:0.71rem;width:90px;flex-shrink:0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;color:var(--text)}
    .lang-bar-bg{flex:1;height:10px;background:rgba(255,255,255,0.04);border-radius:5px;overflow:hidden;min-width:60px}
    .lang-bar{height:100%;border-radius:5px;background:linear-gradient(90deg,var(--gold),var(--orange));transition:width .8s cubic-bezier(.22,.68,0,1.2)}
    .lang-cnt{font-family:var(--mono);font-size:0.67rem;color:var(--muted);width:34px;text-align:right;flex-shrink:0}
    .lang-pct{font-family:var(--mono);font-size:0.67rem;color:var(--gold);width:44px;text-align:right;flex-shrink:0}

    .chart-wrap{position:relative;width:100%;max-width:220px;margin:0 auto}
    .chart-center{position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);text-align:center;pointer-events:none}
    .chart-center-val{font-family:var(--display);font-size:1.6rem;font-weight:800;color:var(--gold);line-height:1}
    .chart-center-label{font-family:var(--mono);font-size:0.6rem;color:var(--muted);margin-top:2px}

    .repo-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px}
    @media(max-width:860px){.repo-grid{grid-template-columns:1fr}}
    .repo-item{display:flex;align-items:flex-start;gap:11px;padding:11px 13px;background:var(--surface);border:1px solid var(--border);border-radius:10px;transition:border-color .2s}
    .repo-item:hover{border-color:var(--border2)}
    .repo-rank{font-family:var(--mono);font-size:0.65rem;color:var(--muted);width:18px;flex-shrink:0;padding-top:2px;text-align:center}
    .repo-rank.top{color:var(--gold);font-weight:500}
    .repo-info{flex:1;min-width:0}
    .repo-name{font-family:var(--mono);font-size:0.78rem;font-weight:500;color:var(--accent);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;text-decoration:none;display:block}
    .repo-name:hover{text-decoration:underline}
    .repo-desc{font-size:0.71rem;color:var(--muted);margin-top:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
    .repo-meta{display:flex;gap:7px;margin-top:5px;align-items:center;flex-wrap:wrap}
    .repo-lang{font-family:var(--mono);font-size:0.61rem;padding:1px 7px;border-radius:4px;background:rgba(124,106,247,0.1);color:var(--accent);border:1px solid rgba(124,106,247,0.18)}
    .repo-stars{font-family:var(--mono);font-size:0.71rem;color:var(--orange);white-space:nowrap}

    .word-cloud{display:flex;flex-wrap:wrap;gap:6px}
    .wchip{font-family:var(--mono);padding:4px 11px;border-radius:20px;background:var(--surface);border:1px solid var(--border);color:var(--text);transition:all .15s;cursor:default;line-height:1.4}
    .wchip:hover{border-color:var(--gold);color:var(--gold)}
    .wchip .cnt{color:var(--muted);margin-left:4px;font-size:.85em}
    .wchip.xl{font-size:.88rem;padding:5px 14px}
    .wchip.lg{font-size:.8rem;padding:4px 12px}
    .wchip.md{font-size:.74rem}
    .wchip.sm{font-size:.68rem;padding:3px 9px;color:var(--muted)}

    .feed-scroll{display:flex;flex-direction:column;gap:7px;max-height:360px;overflow-y:auto;padding-right:3px}
    .feed-scroll::-webkit-scrollbar{width:3px}
    .feed-scroll::-webkit-scrollbar-thumb{background:var(--border2);border-radius:3px}
    .feed-item{padding:10px 12px;background:var(--surface);border:1px solid var(--border);border-left:2px solid var(--gold);border-radius:0 8px 8px 0;font-size:0.74rem}
    .feed-title{font-weight:500;color:var(--text);margin-bottom:3px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
    .feed-meta{font-family:var(--mono);font-size:0.63rem;color:var(--muted)}

    .delta-badge{display:inline-block;font-family:var(--mono);font-size:0.6rem;padding:1px 7px;border-radius:3px;background:rgba(251,191,36,0.1);color:var(--gold);border:1px solid rgba(251,191,36,0.2);margin-right:4px}
    .empty{font-family:var(--mono);font-size:0.74rem;color:var(--muted);text-align:center;padding:30px 0}
    .status-ok{color:var(--green)}
    .status-err{color:var(--red)}

    footer{text-align:center;padding:20px;font-family:var(--mono);font-size:0.63rem;color:var(--muted);border-top:1px solid var(--border);margin-top:10px}
  </style>
</head>
<body>
  <header>
    <div class="logo">
      <div class="logo-mark">GL</div>
      <div>
        <div class="logo-text">GitTrend Lakehouse</div>
        <div class="logo-sub">Gold Layer · Tugas Week 12 · Kelompok 8</div>
      </div>
    </div>
    <div class="hdr-right">
      <div class="gold-badge">🥇 DELTA LAKE</div>
      <div id="last-update">—</div>
    </div>
  </header>

  <main>

    <!-- STATS -->
    <div>
      <div class="sec-label">Ringkasan Pipeline Lakehouse</div>
      <div class="stats-row">
        <div class="stat">
          <div class="stat-label">Total Repo</div>
          <div class="stat-val" id="s-silver">—</div>
        </div>
        <div class="stat">
          <div class="stat-label">Bahasa Unik</div>
          <div class="stat-val" id="s-langs">—</div>
        </div>
        <div class="stat">
          <div class="stat-label">Total Bintang</div>
          <div class="stat-val sm" id="s-dedup">—</div>
        </div>
        <div class="stat">
          <div class="stat-label">Emerging Topics</div>
          <div class="stat-val g" id="s-topics">—</div>
        </div>
      </div>
    </div>

    <!-- LANGUAGE DIST + DONUT -->
    <div class="card">
      <div class="card-title">
        &#9646; Distribusi Bahasa Pemrograman
        <span class="ctag">Gold · language_dist</span>
        <span class="ctag-purple">Repro ETS Analisis 1</span>
      </div>
      <div style="display:grid;grid-template-columns:1fr auto;gap:32px;align-items:start">
        <div style="min-width:0">
          <div style="display:flex;gap:12px;margin-bottom:10px;font-family:var(--mono);font-size:0.62rem;color:var(--muted)">
            <span style="width:90px;flex-shrink:0">Bahasa</span>
            <span style="flex:1;min-width:60px">Bar</span>
            <span style="width:34px;text-align:right;flex-shrink:0">Repo</span>
            <span style="width:44px;text-align:right;flex-shrink:0">%</span>
          </div>
          <div class="lang-rows" id="lang-rows"><div class="empty">Memuat data Gold…</div></div>
        </div>
        <div style="width:200px;flex-shrink:0">
          <div class="chart-wrap">
            <canvas id="donut-chart" width="200" height="200"></canvas>
            <div class="chart-center">
              <div class="chart-center-val" id="donut-total">—</div>
              <div class="chart-center-label">bahasa</div>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- TOP REPOS -->
    <div class="card">
      <div class="card-title">
        &#9733; Top 10 Repositori Terpopuler
        <span class="ctag">Gold · top_repos</span>
        <span class="ctag-purple">Repro ETS Analisis 2</span>
      </div>
      <div class="repo-grid" id="repo-list"><div class="empty">Memuat data Gold…</div></div>
    </div>

    <!-- STAR VELOCITY + EMERGING TOPICS -->
    <div class="two-col">
      <div class="card">
        <div class="card-title">
          🚀 Star Velocity
          <span class="ctag">Gold · star_velocity</span>
        </div>
        <div class="feed-scroll" id="velocity-list"><div class="empty">Memuat data Gold…</div></div>
      </div>
      <div class="card">
        <div class="card-title">
          🔥 Emerging Topics
          <span class="ctag">Gold · emerging_topics</span>
        </div>
        <div class="word-cloud" id="emerging-cloud"><div class="empty">Memuat data Gold…</div></div>
      </div>
    </div>

    <!-- API RSS JOIN -->
    <div class="card">
      <div class="card-title">
        🔗 Repo × Berita — Cross-source Join
        <span class="ctag">Gold · api_rss_join</span>
      </div>
      <div id="join-list"><div class="empty">Memuat data Gold…</div></div>
    </div>

    <!-- HEALTH -->
    <div class="card">
      <div class="card-title">&#9679; Status Pipeline</div>
      <div id="health-status"><div class="empty">Memuat…</div></div>
    </div>

  </main>

  <footer>
    GitTrend Lakehouse · Gold Delta Lake · Auto-refresh setiap 60 detik &nbsp;·&nbsp; Kelompok 8
  </footer>

  <script>
    function fmt(n) {
      if (n == null) return "—";
      if (n >= 1000000) return (n/1000000).toFixed(1)+"M";
      if (n >= 1000) return (n/1000).toFixed(1)+"k";
      return String(n);
    }
    function esc(s) {
      return String(s||"").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;");
    }

    let donutChart = null;
    const COLORS = ["#fbbf24","#f4a261","#4fd1a5","#7c6af7","#60a5fa","#f07070","#a78bfa","#34d399","#fb923c","#38bdf8","#c084fc","#2dd4bf","#e879f9","#818cf8","#fde68a"];

    function renderDonut(langs) {
      const ctx = document.getElementById("donut-chart").getContext("2d");
      const top = langs.slice(0,10);
      document.getElementById("donut-total").textContent = langs.length;
      if (donutChart) {
        donutChart.data.labels = top.map(l=>l.language);
        donutChart.data.datasets[0].data = top.map(l=>l.jumlah_repo);
        donutChart.data.datasets[0].backgroundColor = COLORS.slice(0,top.length);
        donutChart.update(); return;
      }
      donutChart = new Chart(ctx, {
        type:"doughnut",
        data:{labels:top.map(l=>l.language),datasets:[{data:top.map(l=>l.jumlah_repo),backgroundColor:COLORS.slice(0,top.length),borderColor:"#16161f",borderWidth:3,hoverOffset:6}]},
        options:{cutout:"68%",plugins:{legend:{display:false},tooltip:{backgroundColor:"#111118",borderColor:"rgba(255,255,255,0.1)",borderWidth:1,titleColor:"#e8e8f0",bodyColor:"#6b6b80",titleFont:{family:"'DM Mono',monospace",size:11},bodyFont:{family:"'DM Mono',monospace",size:11},padding:10,callbacks:{label:c=>` ${c.label}: ${c.parsed} repo`}}},animation:{duration:800,easing:"easeInOutQuart"}}
      });
    }

    function renderLangs(langs) {
      const el = document.getElementById("lang-rows");
      if (!langs||!langs.length){el.innerHTML='<div class="empty">Belum ada data</div>';return;}
      const max = Math.max(...langs.map(l=>l.jumlah_repo||0),1);
      const total = langs.reduce((s,l)=>s+(l.jumlah_repo||0),0);
      el.innerHTML = langs.slice(0,12).map(l=>{
        const pct = total?((l.jumlah_repo/total)*100).toFixed(1):0;
        const w = Math.round((l.jumlah_repo/max)*100);
        return `<div class="lang-row">
          <div class="lang-name">${esc(l.language)}</div>
          <div class="lang-bar-bg"><div class="lang-bar" style="width:${w}%"></div></div>
          <div class="lang-cnt">${l.jumlah_repo}</div>
          <div class="lang-pct">${pct}%</div>
        </div>`;
      }).join("");
      renderDonut(langs);
    }

    function renderRepos(repos) {
      const el = document.getElementById("repo-list");
      if (!repos||!repos.length){el.innerHTML='<div class="empty">Belum ada data</div>';return;}
      const medals=["①","②","③"];
      const left=repos.slice(0,5), right=repos.slice(5,10);
      const ordered=[];
      for(let i=0;i<5;i++){if(left[i])ordered.push({...left[i],_i:i});if(right[i])ordered.push({...right[i],_i:i+5});}
      el.innerHTML = ordered.map(r=>{
        const rank=medals[r._i]||(r._i+1);
        const url=r.html_url||`https://github.com/${r.full_name}`;
        return `<div class="repo-item">
          <div class="repo-rank ${r._i<3?'top':''}">${rank}</div>
          <div class="repo-info">
            <a class="repo-name" href="${esc(url)}" target="_blank">${esc(r.full_name)}</a>
            <div class="repo-desc">${esc(r.description_preview||"No description")}</div>
            <div class="repo-meta">
              ${r.language?`<span class="repo-lang">${esc(r.language)}</span>`:""}
              <span class="repo-stars">★ ${fmt(r.stargazers_count)}</span>
            </div>
          </div>
        </div>`;
      }).join("");
    }

    function renderVelocity(items) {
      const el = document.getElementById("velocity-list");
      if (!items||!items.length){el.innerHTML='<div class="empty">Butuh multi-sesi untuk kalkulasi lag</div>';return;}
      el.innerHTML = items.slice(0,10).map(r=>`
        <div class="feed-item">
          <div class="feed-title">${esc(r.full_name)}</div>
          <div class="feed-meta"><span class="delta-badge">DELTA</span>+${fmt(r.total_star_gain)} bintang · ${esc(r.language||"-")} · ${fmt(r.current_stars)} total</div>
        </div>`).join("");
    }

    function renderEmerging(words) {
      const el = document.getElementById("emerging-cloud");
      if (!words||!words.length){el.innerHTML='<div class="empty">Belum ada data</div>';return;}
      const max=Math.max(...words.map(w=>w.count_recent||0),1);
      el.innerHTML = words.slice(0,40).map(w=>{
        const r=(w.count_recent||0)/max;
        const cls=r>0.7?"wchip xl":r>0.45?"wchip lg":r>0.25?"wchip md":"wchip sm";
        return `<span class="${cls}">${esc(w.word)}<span class="cnt">${w.count_recent}</span></span>`;
      }).join("");
    }

    function renderJoin(items) {
      const el = document.getElementById("join-list");
      if (!items||!items.length){el.innerHTML='<div class="empty">Belum ada kecocokan repo-berita (data RSS masih sedikit)</div>';return;}
      el.innerHTML = `<div style="display:flex;flex-direction:column;gap:7px">`+items.slice(0,10).map(r=>`
        <div class="feed-item">
          <div class="feed-title"><a href="${esc(r.berita_url||"#")}" target="_blank">${esc(r.berita_judul)}</a></div>
          <div class="feed-meta"><span class="delta-badge">REPO</span>${esc(r.repo)} · ${esc(r.language||"-")} · ★${fmt(r.stargazers_count)} · ${esc(r.berita_sumber||"")}</div>
        </div>`).join("")+`</div>`;
    }

    function renderHealth(data) {
      const el = document.getElementById("health-status");
      let html = '<div style="display:grid;grid-template-columns:1fr 1fr;gap:20px">';
      html += '<div><div style="font-family:var(--mono);font-size:0.7rem;color:var(--muted);margin-bottom:8px">ETS Files</div>';
      Object.entries(data.files||{}).forEach(([k,v])=>{
        html+=`<div style="font-family:var(--mono);font-size:0.72rem;margin-bottom:4px"><span class="${v?'status-ok':'status-err'}">${v?"✅":"❌"}</span> ${k}</div>`;
      });
      html += '</div><div><div style="font-family:var(--mono);font-size:0.7rem;color:var(--muted);margin-bottom:8px">Gold Tables</div>';
      Object.entries(data.gold_tables||{}).forEach(([k,v])=>{
        html+=`<div style="font-family:var(--mono);font-size:0.72rem;margin-bottom:4px"><span class="${v?'status-ok':'status-err'}">${v?"✅":"❌"}</span> ${k}</div>`;
      });
      html += '</div></div>';
      el.innerHTML = html;
    }

    async function loadAll() {
      try {
        const [gRes, hRes] = await Promise.all([fetch("/api/gold"), fetch("/api/health")]);
        const g = await gRes.json();
        const h = await hRes.json();

        const langs = g.language_dist||[];
        const repos = g.top_repos||[];
        const totalRepo = langs.reduce((s,l)=>s+(l.jumlah_repo||0),0);
        const totalBintang = langs.reduce((s,l)=>s+(l.total_bintang||0),0);
        document.getElementById("s-silver").textContent = fmt(totalRepo)||"—";
        document.getElementById("s-langs").textContent = langs.length||"—";
        document.getElementById("s-dedup").textContent = fmt(totalBintang)||"—";
        document.getElementById("s-topics").textContent = fmt((g.emerging_topics||[]).length)||"—";

        renderLangs(langs);
        renderRepos(repos);
        renderVelocity(g.star_velocity||[]);
        renderEmerging(g.emerging_topics||[]);
        renderJoin(g.api_rss_join||[]);
        renderHealth(h);

        document.getElementById("last-update").textContent =
          "Update: " + new Date().toLocaleTimeString("id-ID");
      } catch(e) {
        document.getElementById("last-update").textContent = "Error: "+e.message;
      }
    }

    loadAll();
    setInterval(loadAll, 60000);
  </script>
</body>
</html>"""


@app.route("/")
def index():
    return render_template_string(HTML)


@app.route("/api/gold")
def api_gold():
    return jsonify({
        "language_dist":   baca_parquet(f"{GOLD_DIR}/gold/language_dist"),
        "top_repos":       baca_parquet(f"{GOLD_DIR}/gold/top_repos"),
        "star_velocity":   baca_parquet(f"{GOLD_DIR}/gold/star_velocity"),
        "emerging_topics": baca_parquet(f"{GOLD_DIR}/gold/emerging_topics"),
        "api_rss_join":    baca_parquet(f"{GOLD_DIR}/gold/api_rss_join"),
        "source":          "gold_delta_lake",
    })


@app.route("/api/health")
def health():
    ets_data = "/app/data"
    files = {f: os.path.exists(os.path.join(ets_data, f))
             for f in ["spark_results.json", "live_api.json", "live_rss.json"]}
    gold_tables = {t: os.path.exists(f"{GOLD_DIR}/gold/{t}")
                   for t in ["language_dist","top_repos","star_velocity","emerging_topics","api_rss_join"]}
    return jsonify({"status": "ok", "files": files, "gold_tables": gold_tables})


if __name__ == "__main__":
    print("[lakehouse-dashboard] http://localhost:5001")
    app.run(host="0.0.0.0", port=5001, debug=False)