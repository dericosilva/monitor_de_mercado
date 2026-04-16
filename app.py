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
import os
from datetime import datetime, date, timezone, timedelta
from pathlib import Path
from flask import Flask, jsonify, render_template, request

# Fuso horário de Brasília (UTC-3)
BRT = timezone(timedelta(hours=-3))
def agora_brt():
    return datetime.now(BRT)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

# Suprime aviso cosmético do yfinance sobre TzCache
logging.getLogger('yfinance').setLevel(logging.ERROR)
import warnings
warnings.filterwarnings('ignore', message='.*TzCache.*')

app = Flask(__name__, template_folder='templates', static_folder='static')
DB_PATH = Path(os.environ.get('DB_PATH', str(Path(__file__).parent / 'mercado.db')))
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# ── Mapeamento Yahoo Finance ──────────────────────────────────────────────────
# ticker_yahoo: código exibido no dashboard

ATIVOS_RISCO = [
    {"yahoo": "GC=F",    "cod": "GC",      "nome": "Ouro Futuros",          "inv": True},
    {"yahoo": "HG=F",    "cod": "HG",      "nome": "Cobre Futuros",         "inv": True},
    {"yahoo": "CL=F",    "cod": "CL",      "nome": "Petróleo WTI",          "inv": True},
    {"yahoo": "ZS=F",    "cod": "ZS",      "nome": "Soja Chicago",          "inv": True},
    {"yahoo": "YM=F",    "cod": "YM",      "nome": "Dow Jones Futuros",     "inv": True},
    {"yahoo": "^OSEAX",  "cod": ".OSEAX",  "nome": "Oslo All Share",        "inv": True},
    {"yahoo": "^GDOW",   "cod": ".GDOW",   "nome": "Global Dow USD",        "inv": True},
    {"yahoo": "VALE",    "cod": "VALE.K",  "nome": "Vale SA ADR",           "inv": True},
    {"yahoo": "PBR",     "cod": "PBR",     "nome": "Petrobras ADR",         "inv": True},
    {"yahoo": "EWZ",     "cod": "EWZ",     "nome": "iShares Brazil ETF",    "inv": True},
    {"yahoo": "XLF",     "cod": "XLF",     "nome": "Financial Select SPDR", "inv": True},
    {"yahoo": "XLP",     "cod": "XLP",     "nome": "Consumer Staples SPDR", "inv": True},
    {"yahoo": "XLE",     "cod": "XLE",     "nome": "Energy Select SPDR",    "inv": True},
    {"yahoo": "XME",     "cod": "XME",     "nome": "Metals & Mining SPDR",  "inv": True},
    {"yahoo": "EEM",     "cod": "EEM",     "nome": "Emerging Markets ETF",  "inv": True},
    {"yahoo": "SOXX",    "cod": "SOXX",    "nome": "Semiconductor ETF",     "inv": True},
    {"yahoo": "^BSESN",  "cod": ".BSESN",  "nome": "BSE Sensex 30",         "inv": True},
    {"yahoo": "XC=F",    "cod": "CHINA50", "nome": "China A50 Futuros",     "inv": True},
    {"yahoo": "^HSI",    "cod": "HSI",     "nome": "Hang Seng",             "inv": True},
    {"yahoo": "^DJSH",   "cod": ".DJSH",   "nome": "Dow Jones Shanghai",    "inv": True},
    {"yahoo": "399001.SZ","cod": ".SZI",   "nome": "SZSE Component",        "inv": True},
    {"yahoo": "000001.SS","cod": ".SSEC",  "nome": "Shanghai Composite",    "inv": True},
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
    {"yahoo": "EURBRL=X","cod": "EUR/BRL", "nome": "EUR/BRL",               "inv": False},
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
                china_media REAL,
                ab_inst INTEGER,
                aceleracao INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_snap_data ON snapshots(data);
        """)
        # Adiciona colunas novas em banco existente (ignora erro se já existir)
        for col in ["ab_inst INTEGER", "aceleracao INTEGER"]:
            try:
                conn.execute(f"ALTER TABLE snapshots ADD COLUMN {col}")
            except Exception:
                pass
    log.info("Banco inicializado: %s", DB_PATH)

# ── Lógica de sinal ────────────────────────────────────────────────────────────

def calc_sinal(variacao, inv=False):
    if variacao is None:
        return "Neutro"
    v = -variacao if inv else variacao
    if v > THR:   return "Alta"
    if v < -THR:  return "Queda"
    return "Neutro"

# ── Coleta Yahoo Finance ───────────────────────────────────────────────────────

def coletar_yahoo():
    """Busca cotações do Yahoo Finance e salva no banco."""
    try:
        import yfinance as yf
        import tempfile, os
        yf.set_tz_cache_location(os.path.join(tempfile.gettempdir(), 'yf_tz_cache'))
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

    agora    = agora_brt()
    ts       = agora.isoformat()
    data_str = agora.strftime("%Y-%m-%d")
    hora_str = agora.strftime("%H:%M")  # sem segundos, igual à planilha

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
        rows = conn.execute("""
            SELECT cod, sinal, variacao, grupo
            FROM cotacoes
            WHERE data = ?
              AND id IN (SELECT MAX(id) FROM cotacoes WHERE data = ? GROUP BY cod)
        """, (data_str, data_str)).fetchall()

    ra=rq=rn=da=dq=dn=0
    china_vars = []

    for r in rows:
        s   = r["sinal"]
        v   = r["variacao"] or 0
        grp = r["grupo"]

        # ── contagem de sinais (threshold 0,30%) ──
        if grp == "risco":
            if s=="Alta":  ra+=1
            elif s=="Queda": rq+=1
            else: rn+=1
        else:
            if s=="Alta":  da+=1
            elif s=="Queda": dq+=1
            else: dn+=1

        if r["cod"] in CHINA_CODS and r["variacao"] is not None:
            china_vars.append(r["variacao"])

    china_media = sum(china_vars)/len(china_vars) if china_vars else 0.0

    # A-B instantâneo dos sinais (threshold 0,30%)
    total_alta  = ra + da
    total_queda = rq + dq
    ab_inst = total_alta - total_queda

    # Aceleração = acumula ab_inst ao longo do DIA (reseta todo dia)
    # Busca apenas snapshots do mesmo dia para evitar herdar valores corrompidos
    with get_db() as conn:
        snaps_hoje = conn.execute(
            "SELECT COUNT(*) as n, MAX(aceleracao) as ult FROM snapshots WHERE data=?",
            (data_str,)
        ).fetchone()
    
    if snaps_hoje and snaps_hoje["n"] > 0 and snaps_hoje["ult"] is not None:
        # Já tem snapshots hoje — pega o último valor acumulado do dia
        with get_db() as conn:
            ultimo = conn.execute(
                "SELECT aceleracao FROM snapshots WHERE data=? ORDER BY id DESC LIMIT 1",
                (data_str,)
            ).fetchone()
        aceleracao_anterior = ultimo["aceleracao"] if ultimo["aceleracao"] is not None else 0
    else:
        # Primeiro snapshot do dia — começa do zero
        aceleracao_anterior = 0
    
    aceleracao = aceleracao_anterior + ab_inst

    with get_db() as conn:
        conn.execute("""
            INSERT INTO snapshots(ts,data,hora,ra,rq,rn,da,dq,dn,acum_risco,acum_dolar,china_media,ab_inst,aceleracao)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (ts, data_str, hora_str, ra, rq, rn, da, dq, dn, ra-rq, da-dq, china_media, ab_inst, aceleracao))

# ── Scheduler simples com threading ───────────────────────────────────────────

_coleta_lock = threading.Lock()

def _scheduler_loop():
    INTERVALO = 5 * 60  # 5 minutos
    while True:
        try:
            if _coleta_lock.acquire(blocking=False):
                try:
                    coletar_yahoo()
                finally:
                    _coleta_lock.release()
            else:
                log.warning("Coleta já em andamento, pulando ciclo.")
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
            "SELECT hora,ra,rq,rn,da,dq,dn,acum_risco,acum_dolar,china_media,ab_inst,aceleracao FROM snapshots WHERE data=? ORDER BY ts",
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
            "SELECT hora,ra,rq,rn,da,dq,dn,acum_risco,acum_dolar,china_media,ab_inst,aceleracao FROM snapshots WHERE data=? ORDER BY ts",
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
    if _coleta_lock.acquire(blocking=False):
        try:
            ok = coletar_yahoo()
        finally:
            _coleta_lock.release()
        return jsonify({"status": "ok" if ok else "erro"})
    return jsonify({"status": "ocupado", "msg": "Coleta já em andamento"})

@app.route("/api/resetar_aceleracao", methods=["POST"])
def api_resetar_aceleracao():
    """Recalcula aceleração corretamente para todos os dias no banco."""
    data_param = request.json.get("data") if request.json else None
    hoje_brt = agora_brt().strftime("%Y-%m-%d")
    
    with get_db() as conn:
        if data_param:
            datas = [data_param]
        else:
            # Recalcula todos os dias
            rows_d = conn.execute("SELECT DISTINCT data FROM snapshots ORDER BY data").fetchall()
            datas = [r["data"] for r in rows_d]
        
        total = 0
        for data_str in datas:
            rows = conn.execute(
                "SELECT id, ra, rq, da, dq FROM snapshots WHERE data=? ORDER BY id",
                (data_str,)
            ).fetchall()
            acum = 0
            for r in rows:
                ab = ((r["ra"] or 0) + (r["da"] or 0)) - ((r["rq"] or 0) + (r["dq"] or 0))
                acum += ab
                conn.execute(
                    "UPDATE snapshots SET ab_inst=?, aceleracao=? WHERE id=?",
                    (ab, acum, r["id"])
                )
            total += len(rows)
    
    return jsonify({"status": "ok", "snapshots_corrigidos": total, "datas": datas})

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
    def primeira_coleta():
        if _coleta_lock.acquire(blocking=False):
            try:
                coletar_yahoo()
            finally:
                _coleta_lock.release()
    threading.Thread(target=primeira_coleta, daemon=True).start()
    start_scheduler()
    log.info("Servidor iniciado em http://0.0.0.0:5000")
    app.run(host="0.0.0.0", port=5000, debug=False)
