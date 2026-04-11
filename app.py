"""
Monitor de Mercado v2
- Coleta automática via Yahoo Finance a cada 5 minutos
- Histórico persistente em SQLite
- API REST para o dashboard
"""

import sqlite3
import json
import time
import threading
import logging
from datetime import datetime, date
from pathlib import Path
from flask import Flask, jsonify, render_template, request

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

app = Flask(__name__, template_folder='templates', static_folder='static')
DB_PATH = Path(__file__).parent / 'mercado.db'

# ── Mapeamento Yahoo Finance ──────────────────────────────────────────────────
# ticker_yahoo: código exibido no dashboard

ATIVOS_RISCO = [
    {"yahoo": "GC=F",    "cod": "GC",      "nome": "Ouro Futuros",          "inv": True},
    {"yahoo": "HG=F",    "cod": "HG",      "nome": "Cobre Futuros",         "inv": False},
    {"yahoo": "CL=F",    "cod": "CL",      "nome": "Petróleo WTI",          "inv": False},
    {"yahoo": "ZS=F",    "cod": "ZS",      "nome": "Soja Chicago",          "inv": False},
    {"yahoo": "YM=F",    "cod": "YM",      "nome": "Dow Jones Futuros",     "inv": False},
    {"yahoo": "^OSEAX",  "cod": ".OSEAX",  "nome": "Oslo All Share",        "inv": False},
    {"yahoo": "^GDOW",   "cod": ".GDOW",   "nome": "Global Dow USD",        "inv": False},
    {"yahoo": "VALE",    "cod": "VALE.K",  "nome": "Vale SA ADR",           "inv": False},
    {"yahoo": "PBR",     "cod": "PBR",     "nome": "Petrobras ADR",         "inv": False},
    {"yahoo": "EWZ",     "cod": "EWZ",     "nome": "iShares Brazil ETF",    "inv": False},
    {"yahoo": "XLF",     "cod": "XLF",     "nome": "Financial Select SPDR", "inv": False},
    {"yahoo": "XLP",     "cod": "XLP",     "nome": "Consumer Staples SPDR", "inv": True},
    {"yahoo": "XLE",     "cod": "XLE",     "nome": "Energy Select SPDR",    "inv": False},
    {"yahoo": "XME",     "cod": "XME",     "nome": "Metals & Mining SPDR",  "inv": False},
    {"yahoo": "EEM",     "cod": "EEM",     "nome": "Emerging Markets ETF",  "inv": False},
    {"yahoo": "SOXX",    "cod": "SOXX",    "nome": "Semiconductor ETF",     "inv": False},
    {"yahoo": "^BSESN",  "cod": ".BSESN",  "nome": "BSE Sensex 30",         "inv": False},
    {"yahoo": "XC=F",    "cod": "CHINA50", "nome": "China A50 Futuros",     "inv": False},
    {"yahoo": "^HSI",    "cod": "HSI",     "nome": "Hang Seng",             "inv": False},
    {"yahoo": "^DJSH",   "cod": ".DJSH",   "nome": "Dow Jones Shanghai",    "inv": False},
    {"yahoo": "399001.SZ","cod": ".SZI",   "nome": "SZSE Component",        "inv": False},
    {"yahoo": "000001.SS","cod": ".SSEC",  "nome": "Shanghai Composite",    "inv": False},
]

ATIVOS_DOLAR = [
    {"yahoo": "UUP",     "cod": "DX",      "nome": "Índice Dólar (UUP)",    "inv": False},
    {"yahoo": "^VIX",    "cod": "VX",      "nome": "S&P 500 VIX",           "inv": False},
    {"yahoo": "MXN=X",   "cod": "USD/MXN", "nome": "USD/MXN",               "inv": False},
    {"yahoo": "NOK=X",   "cod": "USD/NOK", "nome": "USD/NOK",               "inv": False},
    {"yahoo": "NZDUSD=X","cod": "USD/NZD", "nome": "USD/NZD",               "inv": False},
    {"yahoo": "AUDUSD=X","cod": "USD/AUD", "nome": "USD/AUD",               "inv": False},
    {"yahoo": "KRW=X",   "cod": "USD/KRW", "nome": "USD/KRW",               "inv": False},
    {"yahoo": "CNY=X",   "cod": "USD/CNY", "nome": "USD/CNY",               "inv": False},
    {"yahoo": "EURBRL=X","cod": "EUR/BRL", "nome": "EUR/BRL",               "inv": True},
]

CHINA_CODS = {"CHINA50", "HSI", ".DJSH", ".SZI", ".SSEC"}
THR = 0.003  # 0.3%

TODOS = ATIVOS_RISCO + ATIVOS_DOLAR
YAHOO_MAP = {a["yahoo"]: a for a in TODOS}  # yahoo_ticker -> info

# ── Banco de dados ─────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS cotacoes (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                ts        TEXT NOT NULL,
                data      TEXT NOT NULL,
                hora      TEXT NOT NULL,
                cod       TEXT NOT NULL,
                nome      TEXT NOT NULL,
                preco     REAL,
                variacao  REAL,
                sinal     TEXT,
                grupo     TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_cotacoes_data ON cotacoes(data);
            CREATE INDEX IF NOT EXISTS idx_cotacoes_cod  ON cotacoes(cod, data);

            CREATE TABLE IF NOT EXISTS snapshots (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                ts         TEXT NOT NULL,
                data       TEXT NOT NULL,
                hora       TEXT NOT NULL,
                ra INTEGER, rq INTEGER, rn INTEGER,
                da INTEGER, dq INTEGER, dn INTEGER,
                acum_risco INTEGER,
                acum_dolar INTEGER,
                china_media REAL
            );
            CREATE INDEX IF NOT EXISTS idx_snap_data ON snapshots(data);
        """)
    log.info("Banco inicializado: %s", DB_PATH)

# ── Lógica de sinal ────────────────────────────────────────────────────────────

def calc_sinal(variacao, inv=False):
    if variacao is None:
        return "Neutro"
    v = -variacao if inv else variacao
    if v < -THR:  return "Alta"
    if v >  THR:  return "Queda"
    return "Neutro"

# ── Coleta Yahoo Finance ───────────────────────────────────────────────────────

def coletar_yahoo():
    """Busca cotações do Yahoo Finance e salva no banco."""
    try:
        import yfinance as yf
    except ImportError:
        log.error("yfinance não instalado. Rode: pip install yfinance")
        return False

    tickers = list(YAHOO_MAP.keys())
    log.info("Coletando %d tickers do Yahoo Finance...", len(tickers))

    try:
        data = yf.download(
            tickers,
            period="2d",
            interval="1d",
            group_by="ticker",
            auto_adjust=True,
            progress=False,
            threads=True,
        )
    except Exception as e:
        log.error("Erro ao baixar do Yahoo Finance: %s", e)
        return False

    agora    = datetime.now()
    ts       = agora.isoformat()
    data_str = agora.strftime("%Y-%m-%d")
    hora_str = agora.strftime("%H:%M:%S")

    rows = []
    for yahoo_tick, info in YAHOO_MAP.items():
        try:
            if len(tickers) == 1:
                df = data
            else:
                df = data[yahoo_tick] if yahoo_tick in data.columns.get_level_values(0) else None

            if df is None or df.empty:
                continue

            df = df.dropna()
            if len(df) < 1:
                continue

            preco_atual = float(df["Close"].iloc[-1])
            if len(df) >= 2:
                preco_ant = float(df["Close"].iloc[-2])
                variacao  = (preco_atual - preco_ant) / preco_ant if preco_ant else 0.0
            else:
                variacao = 0.0

            sinal = calc_sinal(variacao, info["inv"])
            grupo = "risco" if info in ATIVOS_RISCO else "dolar"

            rows.append((ts, data_str, hora_str, info["cod"], info["nome"],
                         preco_atual, variacao, sinal, grupo))
        except Exception as e:
            log.warning("Falha em %s: %s", yahoo_tick, e)

    if not rows:
        log.warning("Nenhum dado coletado.")
        return False

    with get_db() as conn:
        conn.executemany(
            "INSERT INTO cotacoes(ts,data,hora,cod,nome,preco,variacao,sinal,grupo) VALUES(?,?,?,?,?,?,?,?,?)",
            rows
        )

    log.info("Salvas %d cotações.", len(rows))
    _salvar_snapshot(data_str, hora_str, ts)
    return True

def _salvar_snapshot(data_str, hora_str, ts):
    """Calcula resumo do momento e salva snapshot."""
    with get_db() as conn:
        # Pega última cotação de cada ativo no dia
        rows = conn.execute("""
            SELECT cod, sinal, variacao, grupo
            FROM cotacoes
            WHERE data = ?
              AND id IN (SELECT MAX(id) FROM cotacoes WHERE data = ? GROUP BY cod)
        """, (data_str, data_str)).fetchall()

    ra=rq=rn=da=dq=dn=0
    china_vars = []

    for r in rows:
        s = r["sinal"]
        if r["grupo"] == "risco":
            if s=="Alta": ra+=1
            elif s=="Queda": rq+=1
            else: rn+=1
        else:
            if s=="Alta": da+=1
            elif s=="Queda": dq+=1
            else: dn+=1
        if r["cod"] in CHINA_CODS and r["variacao"] is not None:
            china_vars.append(r["variacao"])

    china_media = sum(china_vars)/len(china_vars) if china_vars else 0.0

    with get_db() as conn:
        conn.execute("""
            INSERT INTO snapshots(ts,data,hora,ra,rq,rn,da,dq,dn,acum_risco,acum_dolar,china_media)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
        """, (ts, data_str, hora_str, ra, rq, rn, da, dq, dn, ra-rq, da-dq, china_media))

# ── Scheduler simples com threading ───────────────────────────────────────────

def _scheduler_loop():
    INTERVALO = 5 * 60  # 5 minutos
    while True:
        try:
            coletar_yahoo()
        except Exception as e:
            log.error("Erro no scheduler: %s", e)
        time.sleep(INTERVALO)

def start_scheduler():
    t = threading.Thread(target=_scheduler_loop, daemon=True)
    t.start()
    log.info("Scheduler iniciado — coleta a cada 5 minutos.")

# ── API REST ───────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/agora")
def api_agora():
    """Última cotação de cada ativo."""
    hoje = date.today().isoformat()
    with get_db() as conn:
        rows = conn.execute("""
            SELECT cod, nome, preco, variacao, sinal, grupo, hora
            FROM cotacoes
            WHERE data = ?
              AND id IN (SELECT MAX(id) FROM cotacoes WHERE data=? GROUP BY cod)
            ORDER BY grupo, cod
        """, (hoje, hoje)).fetchall()

    ativos_risco, ativos_dolar = [], []
    for r in rows:
        item = dict(r)
        if r["grupo"] == "risco":
            ativos_risco.append(item)
        else:
            ativos_dolar.append(item)

    # garante que todos os ativos aparecem mesmo sem dado
    cods_com_dado = {r["cod"] for r in rows}
    for a in ATIVOS_RISCO:
        if a["cod"] not in cods_com_dado:
            ativos_risco.append({"cod":a["cod"],"nome":a["nome"],"preco":None,
                                  "variacao":None,"sinal":"Neutro","grupo":"risco","hora":"--"})
    for a in ATIVOS_DOLAR:
        if a["cod"] not in cods_com_dado:
            ativos_dolar.append({"cod":a["cod"],"nome":a["nome"],"preco":None,
                                  "variacao":None,"sinal":"Neutro","grupo":"dolar","hora":"--"})

    # resumo
    ra=rq=rn=da=dq=dn=0
    china_vars=[]
    for item in ativos_risco:
        s=item["sinal"]
        if s=="Alta": ra+=1
        elif s=="Queda": rq+=1
        else: rn+=1
        if item["cod"] in CHINA_CODS and item["variacao"]:
            china_vars.append(item["variacao"])
    for item in ativos_dolar:
        s=item["sinal"]
        if s=="Alta": da+=1
        elif s=="Queda": dq+=1
        else: dn+=1

    china_media = sum(china_vars)/len(china_vars) if china_vars else 0

    # snapshots de hoje para gráfico
    with get_db() as conn:
        snaps = conn.execute(
            "SELECT hora,ra,rq,rn,da,dq,dn,acum_risco,acum_dolar,china_media FROM snapshots WHERE data=? ORDER BY ts",
            (hoje,)
        ).fetchall()

    return jsonify({
        "ativos_risco": ativos_risco,
        "ativos_dolar": ativos_dolar,
        "resumo": {"ra":ra,"rq":rq,"rn":rn,"da":da,"dq":dq,"dn":dn,
                   "acum_risco":ra-rq,"acum_dolar":da-dq,"china_media":china_media},
        "snapshots": [dict(s) for s in snaps],
        "ultima_coleta": datetime.now().strftime("%H:%M:%S"),
        "hoje": hoje,
    })

@app.route("/api/historico/datas")
def api_datas():
    """Lista de datas disponíveis no histórico."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT DISTINCT data FROM snapshots ORDER BY data DESC LIMIT 90"
        ).fetchall()
    return jsonify([r["data"] for r in rows])

@app.route("/api/historico/<data_str>")
def api_historico_data(data_str):
    """Snapshots e cotações de um dia específico."""
    with get_db() as conn:
        snaps = conn.execute(
            "SELECT hora,ra,rq,rn,da,dq,dn,acum_risco,acum_dolar,china_media FROM snapshots WHERE data=? ORDER BY ts",
            (data_str,)
        ).fetchall()
        # última cotação do dia por ativo
        cotacoes = conn.execute("""
            SELECT cod,nome,preco,variacao,sinal,grupo,hora
            FROM cotacoes
            WHERE data=?
              AND id IN (SELECT MAX(id) FROM cotacoes WHERE data=? GROUP BY cod)
            ORDER BY grupo, cod
        """, (data_str, data_str)).fetchall()

    return jsonify({
        "data": data_str,
        "snapshots": [dict(s) for s in snaps],
        "cotacoes": [dict(c) for c in cotacoes],
    })

@app.route("/api/coletar", methods=["POST"])
def api_coletar():
    """Força coleta manual imediata."""
    ok = coletar_yahoo()
    return jsonify({"status": "ok" if ok else "erro"})

@app.route("/api/status")
def api_status():
    with get_db() as conn:
        total = conn.execute("SELECT COUNT(*) as n FROM cotacoes").fetchone()["n"]
        ultima = conn.execute("SELECT MAX(ts) as t FROM cotacoes").fetchone()["t"]
        dias   = conn.execute("SELECT COUNT(DISTINCT data) as n FROM snapshots").fetchone()["n"]
    return jsonify({"total_registros": total, "ultima_coleta": ultima, "dias_historico": dias})

# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    init_db()
    # Primeira coleta ao iniciar
    threading.Thread(target=coletar_yahoo, daemon=True).start()
    start_scheduler()
    log.info("Servidor iniciado em http://0.0.0.0:5000")
    app.run(host="0.0.0.0", port=5000, debug=False)
