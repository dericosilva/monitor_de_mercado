# ⚡ Monitor de Mercado v2

Dashboard com coleta automática via Yahoo Finance a cada 5 minutos,
histórico completo em SQLite e busca por data.

---

## 🚀 Deploy na Nuvem (Railway — recomendado, grátis)

### 1. Crie uma conta gratuita
→ https://railway.app (login com GitHub)

### 2. Suba o projeto
```bash
# Instale o CLI do Railway
npm install -g @railway/cli

# Dentro da pasta market_monitor_v2:
railway login
railway init
railway up
```

### 3. Abra o link gerado
O Railway vai te dar um link tipo:
`https://monitor-mercado-production.up.railway.app`

O programa já começa a coletar dados automaticamente! ✅

---

## 🖥️ Alternativa: Render.com (também gratuito)

1. Crie conta em https://render.com
2. Clique em **New → Web Service**
3. Conecte seu GitHub com a pasta do projeto
4. Configure:
   - **Build Command:** `pip install -r requirements.txt`
   - **Start Command:** `python app.py`
5. Clique em **Deploy**

---

## 💻 Rodar local (para testar)

```bash
pip install -r requirements.txt
python app.py
# Acesse: http://localhost:5000
```

---

## ⏱ Como funciona

| Evento | Frequência |
|--------|-----------|
| Coleta Yahoo Finance | A cada **5 minutos** automático |
| Snapshot salvo no banco | A cada **5 minutos** |
| Tela atualiza | A cada **30 segundos** |
| Histórico guardado | **Para sempre** (SQLite em disco) |

---

## 📅 Aba Histórico

- Clique em **📅 Histórico** no menu
- Escolha qualquer data disponível
- Vê o gráfico de evolução daquele dia
- Vê a tabela de todos os ativos com preço e sinal do fechamento

---

## 📁 Estrutura

```
market_monitor_v2/
├── app.py              ← servidor + coleta + API
├── requirements.txt
├── Dockerfile
├── README.md
├── mercado.db          ← criado automaticamente (histórico)
└── templates/
    └── index.html      ← dashboard completo
```

---

## 🔌 API

| Endpoint | Descrição |
|----------|-----------|
| `GET /api/agora` | Cotações ao vivo + resumo do dia |
| `GET /api/historico/datas` | Lista de datas disponíveis |
| `GET /api/historico/2025-04-07` | Dados de um dia específico |
| `POST /api/coletar` | Força coleta imediata |
| `GET /api/status` | Info do banco de dados |

---

## 📊 Ativos monitorados

### Risco (22 ativos)
Ouro, Cobre, Petróleo WTI, Soja, Dow Jones, Oslo All Share,
Global Dow, Vale, Petrobras, EWZ, XLF, XLP, XLE, XME, EEM, SOXX,
BSE Sensex, China A50, Hang Seng, DJ Shanghai, SZSE, Shanghai

### Dólar (9 ativos)
Índice Dólar, VIX, USD/MXN, USD/NOK, USD/NZD, USD/AUD,
USD/KRW, USD/CNY, EUR/BRL
