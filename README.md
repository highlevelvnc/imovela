# ◆ Imovela
### Lead intelligence imobiliária

Plataforma de descoberta automatizada e priorização de oportunidades
imobiliárias em Portugal. Multi-fonte, multi-canal, alimentada por IA e
desenhada para profissionais que precisam de **chegar primeiro**.

> **"Encontrar o negócio antes da concorrência — com dados, não com sorte."**

---

## Porquê Imovela?

A captação de leads imobiliários por meios manuais escala mal: pesquisar
3+ portais por zona, todos os dias, e ainda distinguir oportunidade real
de ruído leva horas — tempo que a concorrência usa para fechar negócios.

A Imovela automatiza o ciclo completo:

| Sem Imovela                                    | Com Imovela                                              |
|-----------------------------------------------|---------------------------------------------------------|
| Pesquisas manuais em 5+ portais diariamente   | Recolha contínua, agendada às 08:00                    |
| Misturar agências e proprietários directos    | Classificador ML separa FSBO de agência (≈87% acerto)  |
| Perder quedas de preço silenciosas            | Detecção automática de price-drops com alerta urgent   |
| Não saber se o vendedor é repeat ou novato    | Sweep de perfis OLX flag super-sellers (≥5 anúncios)   |
| Negócios perdidos por demora no contacto      | Alertas HOT em tempo real por e-mail e Telegram        |
| CRM em folha de Excel                         | Pipeline visual no dashboard, com notas e histórico    |

---

## Quick Start

```bash
# 1. Aceder à pasta e criar ambiente virtual
cd imovela
python -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate

# 2. Instalar dependências
pip install -r requirements.txt
playwright install chromium       # Para scraping do Idealista

# 3. Configurar
cp .env.example .env              # Editar credenciais e zonas-alvo

# 4. Inicializar (cria DB + dados demo opcionais)
python main.py init
python main.py seed-demo          # opcional

# 5. Abrir o dashboard
python main.py dashboard          # → http://localhost:8501
```

---

## Comandos CLI

```bash
python main.py init                  # Criar tabelas na base de dados
python main.py seed-demo             # Carregar 35 leads de demonstração
python main.py seed-demo --clear     # Limpar e recarregar dados demo
python main.py reset-db              # Apagar e recriar base de dados (com confirmação)

python main.py run                   # Pipeline completo (scraping + processamento + scoring + alertas)
python main.py scrape                # Apenas scraping
python main.py scrape --sources olx,imovirtual --zones Lisboa,Cascais
python main.py process               # Processar listagens raw
python main.py score                 # Calcular scores de todas as oportunidades
python main.py alerts                # Verificar e enviar alertas HOT
python main.py report                # Enviar relatório diário

python main.py status                # Ver estatísticas do sistema
python main.py export --format csv --score-min 50
python main.py scheduler             # Arrancar agendamento diário (08:00)
python main.py scheduler --run-now   # Arrancar + executar imediatamente
python main.py dashboard             # Abrir Streamlit em localhost:8501
```

---

## Fluxo de Demo — Apresentação ao Cliente

### Passo 1 — Preparação (uma vez)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
python main.py init
```

### Passo 2 — Carregar dados realistas

```bash
python main.py seed-demo
# ✓ 35 oportunidades carregadas
# ✓ Lisboa (10), Cascais (8), Sintra (5), Almada (4), Seixal (4), Sesimbra (4)
# ✓ 6 HOT · 16 WARM · 13 COLD
# ✓ CRM com notas, fases e histórico de preços
```

### Passo 3 — Abrir dashboard

```bash
python main.py dashboard
# → Abrir browser em http://localhost:8501
```

### Passo 4 — Reiniciar se necessário

```bash
python main.py reset-db && python main.py seed-demo
```

---

## Roteiro de Demonstração — 5 Minutos

### ⏱ 0:00 — Abertura (30s)
> *"Este sistema monitoriza automaticamente o OLX, Imovirtual e Idealista todos os dias às 8h. Quando encontra uma oportunidade com sinais de urgência ou preço abaixo do mercado, classifica-a como HOT e avisa-te imediatamente."*

**Mostrar:** Dashboard → KPIs no topo (HOT count, Score médio, Novos hoje)

---

### ⏱ 0:30 — Os HOT Leads (1:30)
> *"Estes são os leads mais quentes de hoje — proprietários directos, preço abaixo do mercado, com urgência detectada."*

**Mostrar:**
- Cards HOT com borda vermelha e score em destaque
- Apontar para um card: score 88, "proprietário a emigrar", 285.000€ vs. benchmark 4.500€/m²
- Clicar em "Ver anúncio →" para abrir o portal
- Mostrar o gráfico de distribuição de scores e o gráfico por zona

**Frase chave:** *"Em vez de gastar 2h a pesquisar portais, vês aqui os 5 melhores negócios do dia em segundos."*

---

### ⏱ 2:00 — Como funciona o Score (1:00)
**Mostrar:** Página Oportunidades → Seleccionar lead HOT → Painel de detalhe

- Mostrar a decomposição do score (barras por dimensão)
- Apontar: "30 pts de oportunidade de preço = 18% abaixo do mercado da zona"
- Apontar: "25 pts urgência = palavras-chave 'emigrar', 'herança', 'divórcio' no anúncio"
- Apontar: "20 pts proprietário directo = sem agência no meio"

**Frase chave:** *"O sistema lê o anúncio, detecta os sinais e dá-te uma pontuação transparente — não é uma caixa negra."*

---

### ⏱ 3:00 — CRM integrado (1:00)
**Mostrar:** Página CRM → Kanban com contagens por fase

- Abrir um lead em "negociação"
- Mostrar as notas de chamadas registadas
- Demonstrar: escrever uma nota nova → "Visita confirmada para sexta" → Guardar
- Mover um lead de "contactado" para "negociação"

**Frase chave:** *"Toda a informação de cada negócio num único sítio — sem Excel, sem post-its."*

---

### ⏱ 4:00 — Automatização e alertas (45s)
**Mostrar:** Página Motor → botões de controlo

- Mostrar botão "Executar pipeline" → *"Isto acontece automaticamente todos os dias às 8h"*
- Mostrar configuração de alertas no `.env.example` (Telegram/email)
- **Mostrar:** Exportar CSV

**Frase chave:** *"Quando um lead HOT aparece, recebes uma mensagem no Telegram com todos os detalhes — mesmo quando não tens o computador aberto."*

---

### ⏱ 4:45 — Encerramento (15s)
> *"Na Fase 2, isto passa para a nuvem — sem precisares de ter o computador ligado. Mas já hoje, com este MVP, és capaz de identificar os melhores negócios de Lisboa e Cascais antes de qualquer concorrente."*

---

## Sistema de Scoring

| Dimensão | Pontos | Critério |
|----------|--------|----------|
| Oportunidade de Preço | 30 | % abaixo do benchmark €/m² da zona |
| Sinais de Urgência | 25 | "urgente", "herança", "divórcio", "emigração", etc. |
| Proprietário Directo | 20 | Sem agência intermediária |
| Dias no Mercado | 15 | >30, >60, >90 dias — vendedor motivado |
| Qualidade de Dados | 5 | Completude: telefone, área, zona |
| Prioridade de Zona | 5 | Lisboa/Cascais vs. outras zonas |

| Classificação | Score | Acção |
|---------------|-------|-------|
| 🔴 **HOT** | ≥ 75 | Alerta imediato — contactar hoje |
| 🟡 **WARM** | 50–74 | Contactar esta semana |
| 🔵 **COLD** | < 50 | Monitorizar — aguardar alterações |

### Benchmarks €/m² configurados

| Zona | Tipologia | Benchmark |
|------|-----------|-----------|
| Lisboa | T2 | 4.500 €/m² |
| Cascais | T2 | 4.700 €/m² |
| Sintra | T2 | 2.700 €/m² |
| Almada | T2 | 2.500 €/m² |
| Seixal | T2 | 2.000 €/m² |
| Sesimbra | T2 | 2.400 €/m² |

---

## Checklist de Validação Visual

Após abrir o dashboard (`http://localhost:8501`):

**📊 Dashboard**
- [ ] KPIs no topo: HOT Leads = 6, Score Médio ≈ 52
- [ ] Cards HOT com borda vermelha lateral e score em destaque
- [ ] Gráfico de scores com linha HOT (vermelho) e WARM (amarelo)
- [ ] Gráfico de barras por zona (Lisboa em destaque)
- [ ] Funil CRM com 5 fases

**🎯 Oportunidades**
- [ ] 35 linhas na tabela
- [ ] Coluna Score com barra de progresso
- [ ] Filtro por zona/tipologia/classificação funciona
- [ ] Painel de detalhe mostra decomposição do score em barras

**📋 CRM**
- [ ] Kanban header com contagem por fase (cores diferentes)
- [ ] Leads em "negociação" têm notas registadas
- [ ] Botão "Mover →" funciona e recarrega a página

**⚙️ Motor**
- [ ] Estado do sistema mostra contagens correctas
- [ ] Tabela de fontes activas visível

**📤 Exportar**
- [ ] Geração de CSV com botão de download
- [ ] Geração de JSON com botão de download

---

## Configuração de Alertas

### Email (Gmail)

```env
ALERT_EMAIL_ENABLED=true
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=seuemail@gmail.com
SMTP_PASSWORD=sua_app_password    # Google Account → App Passwords
ALERT_EMAIL_TO=nuno.reis@email.com
```

### Telegram (recomendado — mais rápido)

```env
ALERT_TELEGRAM_ENABLED=true
TELEGRAM_BOT_TOKEN=123456:ABC...  # Obter via @BotFather no Telegram
TELEGRAM_CHAT_ID=123456789        # Obter via @userinfobot
```

---

## Erros Comuns

| Erro | Causa | Solução |
|------|-------|---------|
| `ModuleNotFoundError: streamlit` | Dependências em falta | `pip install -r requirements.txt` |
| `No module named 'playwright'` | Playwright não instalado | `pip install playwright && playwright install chromium` |
| `no such table: leads` | BD não inicializada | `python main.py init` |
| `Database is locked` | Outro processo activo | Fechar outros terminais com Python |
| `Port 8501 already in use` | Streamlit já a correr | Fechar instância anterior ou usar `--server.port 8502` |
| Dashboard vazio / sem leads | BD vazia | `python main.py seed-demo` |
| `pydantic_settings not found` | Versão antiga | `pip install pydantic-settings` |
| Dashboard lento ao arrancar | Cache a construir | Aguardar 5–10s na primeira abertura |

---

## Estrutura do Projecto

```
nunoreis-leadengine/
├── main.py                    # CLI: init, run, scrape, score, dashboard, seed-demo, reset-db
├── config/
│   └── settings.py            # Configuração central (pydantic-settings)
├── scrapers/
│   ├── base.py                # Classe base: HTTP + retry + anti-block
│   ├── olx.py                 # OLX Portugal
│   ├── imovirtual.py          # Imovirtual (maior portal imobiliário PT)
│   ├── idealista.py           # Idealista PT (Playwright — JS rendering)
│   └── anti_block/
│       ├── rate_limiter.py    # Token bucket + backoff adaptativo
│       └── proxy_manager.py   # Rotação de proxies e 20 user-agents
├── pipeline/
│   ├── normalizer.py          # Raw → modelo canónico por fonte
│   ├── deduplicator.py        # Fingerprint SHA-256 + merge de fontes
│   ├── enricher.py            # Benchmark €/m², keywords urgência, geocoding
│   └── runner.py              # Orquestrador ETL
├── scoring/
│   └── scorer.py              # 6 dimensões · 0–100 pts · HOT/WARM/COLD
├── storage/
│   ├── models.py              # SQLAlchemy ORM (SQLite → PostgreSQL pronto)
│   ├── database.py            # Engine, sessão, init_db
│   └── repository.py          # CRUD pattern
├── crm/
│   └── manager.py             # Estágios, notas, pipeline Kanban
├── alerts/
│   └── notifier.py            # Email (SMTP) + Telegram Bot
├── reports/
│   └── generator.py           # CSV/JSON export, stats
├── scheduler/
│   └── jobs.py                # APScheduler — execução diária às 08:00
├── dashboard/
│   └── app.py                 # Streamlit — UI premium dark theme
├── data/
│   └── seed_demo.py           # 35 leads demo para apresentação
└── tests/
    ├── test_scoring.py
    ├── test_pipeline.py
    └── test_scrapers.py
```

---

## Fontes Configuradas

| Fonte | Método | JS? | Anti-block |
|-------|--------|-----|------------|
| OLX Portugal | httpx + BeautifulSoup | Não | Rate limit + UA rotation |
| Imovirtual | httpx + BeautifulSoup | Não | Rate limit + UA rotation |
| Idealista PT | Playwright | Sim | Headless + fingerprint masking |
| ERA / Remax | *(Fase 2)* | Sim | A configurar |

---

## Adicionar uma Nova Fonte

O sistema foi desenhado para ser extensível em 5 passos:

1. **Criar scraper** a partir do template:
   ```bash
   cp scrapers/imovirtual.py scrapers/era.py
   ```

2. **Implementar** `SOURCE = "era"` e os métodos `scrape_zone()` e `_parse_card()`

3. **Registar** em `scrapers/__init__.py`:
   ```python
   from .era import ERAScraper
   ```

4. **Registar** em `pipeline/runner.py`:
   ```python
   "era": ERAScraper,
   ```

5. **Adicionar normalizador** em `pipeline/normalizer.py`:
   ```python
   elif source == "era":
       return self._normalize_era(raw)
   ```

Não são necessárias alterações na BD, no scorer, no CRM nem no dashboard.

---

## Migração para Fase 2

Para passar de SQLite local para PostgreSQL em nuvem:

```env
# .env — apenas esta linha muda:
DATABASE_URL=postgresql+psycopg2://user:pass@localhost:5432/imovela
```

Nenhum código precisa de ser alterado — SQLAlchemy abstrai o motor de BD.

**Fase 2 roadmap:** Docker + PostgreSQL + Redis + Celery + FastAPI + React frontend

---

*Imovela · Lead intelligence imobiliária · Portugal*
