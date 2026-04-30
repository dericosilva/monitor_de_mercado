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
try:
    from flask_cors import CORS
except ImportError:
    CORS = None

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
if CORS:
    CORS(app, resources={r"/api/*": {"origins": "*"}})
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

def mercado_aberto():
    """Verifica se algum mercado relevante está aberto (BRT)."""
    agora = agora_brt()
    dia = agora.weekday()  # 0=seg, 6=dom
    hora = agora.hour
    minuto = agora.minute
    hm = hora * 60 + minuto

    # Fim de semana — nenhum mercado aberto
    if dia >= 5:
        return False

    # Mercados cobertos e seus horários em BRT:
    # Ásia (Hang Seng, Shanghai, SZSE):   20:00-03:00 BRT
    # Europa (Oslo):                       05:00-13:30 BRT
    # EUA (NYSE/NASDAQ, futuros):          10:00-17:00 BRT (regular) / 07:00-17:30 BRT (futuros)
    # Futuros EUA pré-market:              07:00-10:00 BRT
    # Câmbio:                              24h em dias úteis

    # Janela útil: 05:00 - 18:30 BRT (cobre Europa + EUA)
    # Adicionalmente: 20:00 - 03:00 BRT (Ásia)
    abertura_europa = 5 * 60      # 05:00
    fechamento_eua  = 18 * 60 + 30  # 18:30
    abertura_asia   = 20 * 60     # 20:00
    # Dia seguinte Ásia até 03:00 = só verificamos se hm < 180 (3h) OU hm > abertura_asia

    if abertura_europa <= hm <= fechamento_eua:
        return True
    if hm >= abertura_asia:
        return True
    if hm < (3 * 60):  # antes das 3h = final do pregão asiático
        return True

    return False

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

def coletar_yahoo(salvar_snapshot=True):
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

    agora    = agora_brt()
    ts       = agora.isoformat()
    data_str = agora.strftime("%Y-%m-%d")
    hora_str = agora.strftime("%H:%M")

    def extrair_valor(df, coluna, idx):
        """Extrai float de DataFrame com possível MultiIndex de colunas."""
        try:
            val = df[coluna].iloc[idx]
            # Se for Series (MultiIndex), pega o primeiro valor
            if hasattr(val, 'iloc'):
                val = val.iloc[0]
            return float(val)
        except Exception:
            return None

    rows = []
    for yahoo_tick, info in YAHOO_MAP.items():
        try:
            # Tenta dados intraday de 5 min primeiro
            df = yf.download(
                yahoo_tick,
                period="1d",
                interval="5m",
                auto_adjust=True,
                progress=False,
                threads=False,
            )

            usar_intraday = df is not None and not df.empty and len(df.dropna()) >= 2

            if usar_intraday:
                df = df.dropna()
                preco_atual = extrair_valor(df, "Close", -1)
                preco_ref   = extrair_valor(df, "Open",   0)
            else:
                # Fallback: fechamento diário vs dia anterior
                df = yf.download(
                    yahoo_tick,
                    period="5d",
                    interval="1d",
                    auto_adjust=True,
                    progress=False,
                    threads=False,
                )
                if df is None or df.empty:
                    continue
                df = df.dropna()
                if len(df) < 2:
                    continue
                preco_atual = extrair_valor(df, "Close", -1)
                preco_ref   = extrair_valor(df, "Close", -2)

            if preco_atual is None or preco_ref is None or preco_ref == 0:
                continue

            variacao = (preco_atual - preco_ref) / preco_ref
            sinal    = calc_sinal(variacao, info["inv"])
            grupo    = "risco" if info in ATIVOS_RISCO else "dolar"

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

    if salvar_snapshot and mercado_aberto():
        _salvar_snapshot(data_str, hora_str, ts)
        log.info("Snapshot salvo — mercado aberto.")
    elif not salvar_snapshot:
        log.info("Snapshot ignorado — extensão Investing ativa como fonte principal.")
    else:
        log.info("Mercado fechado — snapshot ignorado.")

    return True

THR_ACEL = 0.001  # 0,10% — threshold da aceleração

def _salvar_snapshot(data_str, hora_str, ts):
    """Calcula resumo do momento e salva snapshot."""

    # ── Cotações atuais ──
    with get_db() as conn:
        rows_atuais = conn.execute("""
            SELECT cod, sinal, variacao, preco, grupo
            FROM cotacoes
            WHERE data = ?
              AND id IN (SELECT MAX(id) FROM cotacoes WHERE data = ? GROUP BY cod)
        """, (data_str, data_str)).fetchall()

    # ── Cotações do snapshot anterior (para calcular aceleração) ──
    # Pega o penúltimo conjunto de cotações salvas (snapshot anterior)
    with get_db() as conn:
        ultima_hora = conn.execute("""
            SELECT hora FROM cotacoes
            WHERE data = ?
            GROUP BY hora
            ORDER BY MAX(id) DESC
            LIMIT 1 OFFSET 1
        """, (data_str,)).fetchone()

    # Pega variação% do snapshot anterior para comparar aceleração
    variacoes_ant = {}
    if ultima_hora:
        with get_db() as conn:
            rows_ant = conn.execute("""
                SELECT cod, variacao FROM cotacoes
                WHERE data = ? AND hora = ?
                  AND id IN (SELECT MAX(id) FROM cotacoes WHERE data=? AND hora=? GROUP BY cod)
            """, (data_str, ultima_hora["hora"], data_str, ultima_hora["hora"])).fetchall()
        variacoes_ant = {r["cod"]: r["variacao"] for r in rows_ant}

    ra=rq=rn=da=dq=dn=0
    china_vars = []
    acel_inst = 0

    for r in rows_atuais:
        s   = r["sinal"]
        grp = r["grupo"]
        cod = r["cod"]

        # ── Contagem de sinais (threshold 0,30%) — direcional IBov ──
        if grp == "risco":
            if s=="Alta":  ra+=1
            elif s=="Queda": rq+=1
            else: rn+=1
        else:
            if s=="Alta":  da+=1
            elif s=="Queda": dq+=1
            else: dn+=1

        # ── Aceleração (threshold 0,10%) ──
        # Compara VARIAÇÃO% atual vs VARIAÇÃO% do snapshot anterior
        # delta positivo = ativo subiu mais que antes
        # delta negativo = ativo caiu mais (ou subiu menos) que antes
        # Isso funciona mesmo com preços estáticos entre coletas
        var_atual = r["variacao"] or 0
        var_ant   = variacoes_ant.get(cod)

        if var_ant is not None:
            delta = var_atual - var_ant

            if grp == "risco":
                if   delta >  THR_ACEL: acel_inst -= 1  # risco acelerou pra cima → dólar cai
                elif delta < -THR_ACEL: acel_inst += 1  # risco acelerou pra baixo → dólar sobe
            else:
                if   delta >  THR_ACEL: acel_inst += 1  # dólar acelerou pra cima
                elif delta < -THR_ACEL: acel_inst -= 1  # dólar acelerou pra baixo

        if r["cod"] in CHINA_CODS and r["variacao"] is not None:
            china_vars.append(r["variacao"])

    china_media = sum(china_vars)/len(china_vars) if china_vars else 0.0
    ab_inst = (ra + da) - (rq + dq)

    # ── Aceleração acumulada do dia ──
    with get_db() as conn:
        ultimo = conn.execute(
            "SELECT aceleracao FROM snapshots WHERE data=? ORDER BY id DESC LIMIT 1",
            (data_str,)
        ).fetchone()
    aceleracao_anterior = (ultimo["aceleracao"] if ultimo and ultimo["aceleracao"] is not None else 0)
    aceleracao = aceleracao_anterior + acel_inst

    with get_db() as conn:
        conn.execute("""
            INSERT INTO snapshots(ts,data,hora,ra,rq,rn,da,dq,dn,acum_risco,acum_dolar,china_media,ab_inst,aceleracao)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (ts, data_str, hora_str, ra, rq, rn, da, dq, dn, ra-rq, da-dq, china_media, ab_inst, aceleracao))
    
    log.info("Snapshot: Alta=%d Queda=%d AcelInst=%d AcelAcum=%d", ra+da, rq+dq, acel_inst, aceleracao)

# ── Scheduler simples com threading ───────────────────────────────────────────

_coleta_lock = threading.Lock()

def _scheduler_loop():
    """Coleta Yahoo Finance como backup — só salva snapshot se extensão não coletou nos últimos 10 min."""
    INTERVALO = 5 * 60  # 5 minutos
    while True:
        try:
            # Verifica se a extensão coletou recentemente (últimos 10 min)
            hoje = agora_brt().strftime("%Y-%m-%d")
            with get_db() as conn:
                ultimo = conn.execute(
                    "SELECT MAX(ts) as t FROM cotacoes WHERE data=? AND hora >= ?",
                    (hoje, (agora_brt() - __import__('datetime').timedelta(minutes=10)).strftime("%H:%M"))
                ).fetchone()
            
            extensao_ativa = ultimo and ultimo["t"]
            
            if extensao_ativa:
                log.info("Extensão ativa — Yahoo Finance em modo backup (sem snapshot).")
                # Coleta mas NÃO salva snapshot (extensão já faz isso)
                if _coleta_lock.acquire(blocking=False):
                    try:
                        coletar_yahoo(salvar_snapshot=False)
                    finally:
                        _coleta_lock.release()
            else:
                log.info("Extensão inativa — Yahoo Finance como fonte principal.")
                if _coleta_lock.acquire(blocking=False):
                    try:
                        coletar_yahoo(salvar_snapshot=True)
                    finally:
                        _coleta_lock.release()
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
        "ultima_coleta": agora_brt().strftime("%H:%M:%S"),
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

@app.route("/api/investing_push", methods=["POST"])
def api_investing_push():
    """Recebe dados da extensão Chrome que lê o Investing.com."""
    try:
        data = request.get_json()
        if not data or 'ativos' not in data:
            return jsonify({"status": "erro", "msg": "payload inválido"}), 400

        ativos = data['ativos']
        agora_b = agora_brt()
        ts       = agora_b.isoformat()
        data_str = agora_b.strftime("%Y-%m-%d")
        hora_str = agora_b.strftime("%H:%M")

        rows = []
        for a in ativos:
            cod      = a.get('cod')
            grupo    = a.get('grupo')
            nome     = a.get('nome', cod)
            preco    = a.get('preco')
            variacao = a.get('variacao')

            if not cod or variacao is None:
                continue

            # Descobre se ativo é invertido
            todos = ATIVOS_RISCO + ATIVOS_DOLAR
            info  = next((x for x in todos if x['cod'] == cod), None)
            inv   = info.get('inv', False) if info else False

            sinal = calc_sinal(variacao, inv)
            rows.append((ts, data_str, hora_str, cod, nome, preco, variacao, sinal, grupo))

        if not rows:
            return jsonify({"status": "erro", "msg": "nenhum ativo válido"}), 400

        with get_db() as conn:
            conn.executemany(
                "INSERT INTO cotacoes(ts,data,hora,cod,nome,preco,variacao,sinal,grupo) VALUES(?,?,?,?,?,?,?,?,?)",
                rows
            )

        log.info("Investing Push: %d ativos recebidos da extensão", len(rows))
        _salvar_snapshot(data_str, hora_str, ts)
        return jsonify({"status": "ok", "recebidos": len(rows)})

    except Exception as e:
        log.error("Erro no investing_push: %s", e)
        return jsonify({"status": "erro", "msg": str(e)}), 500

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

@app.route("/api/limpar_duplicados", methods=["POST"])
def api_limpar_duplicados():
    """Remove snapshots duplicados mantendo apenas 1 por hora por dia."""
    data_param = request.json.get("data") if request.json else None
    
    with get_db() as conn:
        if data_param:
            datas = [data_param]
        else:
            rows = conn.execute("SELECT DISTINCT data FROM snapshots ORDER BY data").fetchall()
            datas = [r["data"] for r in rows]
        
        total_removidos = 0
        for data_str in datas:
            # Pega todos os snapshots do dia ordenados
            snaps = conn.execute(
                "SELECT id, hora FROM snapshots WHERE data=? ORDER BY id",
                (data_str,)
            ).fetchall()
            
            # Mantém apenas o ÚLTIMO snapshot de cada intervalo de 5 minutos
            vistos = {}
            ids_manter = set()
            for s in snaps:
                # Agrupa por hora:minuto arredondado para 5 min
                h, m = s["hora"].split(":")[:2]
                m_round = str((int(m) // 5) * 5).zfill(2)
                chave = f"{h}:{m_round}"
                vistos[chave] = s["id"]  # sobrescreve, fica com o último
            
            ids_manter = set(vistos.values())
            todos_ids = {s["id"] for s in snaps}
            ids_remover = todos_ids - ids_manter
            
            if ids_remover:
                conn.execute(
                    f"DELETE FROM snapshots WHERE id IN ({','.join(str(i) for i in ids_remover)})"
                )
                total_removidos += len(ids_remover)
                log.info("Limpeza %s: removidos %d duplicados, mantidos %d", 
                         data_str, len(ids_remover), len(ids_manter))
    
    return jsonify({"status": "ok", "removidos": total_removidos, "datas": datas})

@app.route("/api/limpar_fora_pregao", methods=["POST"])
def api_limpar_fora_pregao():
    """Remove snapshots salvos fora do horário de pregão (antes 05:00 ou depois 19:30)."""
    data_param = request.json.get("data") if request.json else None
    hoje = agora_brt().strftime("%Y-%m-%d")
    data_str = data_param or hoje
    
    with get_db() as conn:
        removidos = conn.execute(
            "DELETE FROM snapshots WHERE data=? AND (hora < '05:00' OR hora > '19:30')",
            (data_str,)
        ).rowcount
        # Recalcula aceleração do dia
        rows = conn.execute(
            "SELECT id, ra, rq, da, dq FROM snapshots WHERE data=? ORDER BY id",
            (data_str,)
        ).fetchall()
        acum = 0
        for r in rows:
            ab = ((r["ra"]or 0)+(r["da"]or 0))-((r["rq"]or 0)+(r["dq"]or 0))
            acum += ab
            conn.execute("UPDATE snapshots SET ab_inst=?, aceleracao=? WHERE id=?", (ab, acum, r["id"]))
    
    return jsonify({"status": "ok", "removidos": removidos, "snapshots_restantes": len(rows)})

@app.route("/api/status")
def api_status():
    with get_db() as conn:
        total  = conn.execute("SELECT COUNT(*) as n FROM cotacoes").fetchone()["n"]
        ultima = conn.execute("SELECT MAX(ts) as t FROM cotacoes").fetchone()["t"]
        dias   = conn.execute("SELECT COUNT(DISTINCT data) as n FROM snapshots").fetchone()["n"]
        # Mostra últimos 3 snapshots para diagnóstico
        ultimos = conn.execute(
            "SELECT data, hora, ra, rq, da, dq, ab_inst FROM snapshots ORDER BY id DESC LIMIT 3"
        ).fetchall()
    return jsonify({
        "total_registros": total,
        "ultima_coleta": ultima,
        "dias_historico": dias,
        "ultimos_snapshots": [dict(r) for r in ultimos],
        "hora_servidor_brt": agora_brt().strftime("%Y-%m-%d %H:%M:%S")
    })

@app.route("/api/limpar_snapshots_utc", methods=["POST"])
def api_limpar_snapshots_utc():
    """Remove snapshots com hora em UTC (>= 20:00) que deveriam estar em BRT."""
    hoje = agora_brt().strftime("%Y-%m-%d")
    with get_db() as conn:
        # Snapshots salvos com hora UTC ficam entre 20:00-23:59 quando BRT é 17:00-20:59
        removidos = conn.execute(
            "DELETE FROM snapshots WHERE data=? AND hora >= '20:00'", (hoje,)
        ).rowcount
    return jsonify({"status": "ok", "removidos": removidos})

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
