# Personal Finance Aggregator

Python toolkit for **personal finance analytics**: it reads bank operations from Excel, aggregates month-to-date spending per card, builds a JSON “dashboard” payload (greeting, top transactions, FX, stocks), writes category reports to JSON files, and can filter likely person-to-person transfers. Exchange rates come from the **Central Bank of Russia** (XML); stock quotes use **Financial Modeling Prep** (list API + `APISP500` key).

![Python](https://img.shields.io/badge/python-3.13+-blue.svg)
![Pandas](https://img.shields.io/badge/pandas-analytics-green.svg)
![Poetry](https://img.shields.io/badge/poetry-managed-41454A.svg)
![Pytest](https://img.shields.io/badge/tests-pytest-orange.svg)
[![Tests](https://github.com/AJLbN0H/personal-finance-aggregator/actions/workflows/tests.yml/badge.svg)](https://github.com/AJLbN0H/personal-finance-aggregator/actions/workflows/tests.yml)

## Features

- **Dashboard JSON** — `main_page()` returns a JSON string with time-based greeting, per-card spend and cashback (masked last digits), top 5 transactions by payment amount, user-selected currency rates vs RUB, and prices for configured tickers.
- **Excel pipeline** — loads `data/operations.xlsx` with the column layout expected by the code (Russian bank export fields such as `Дата платежа`, `Сумма платежа`, `Категория`, etc.).
- **CBR FX** — `get_currency_rates_by_cbr` pulls daily rates from `cbr.ru` XML; amounts in foreign currency are converted via `exchange` helpers.
- **Stock list** — `get_user_stocks` calls Financial Modeling Prep’s stock list endpoint and matches symbols from `user_settings.json`.
- **Category reports** — `spending_by_category` (with `@write_report`) writes JSON under `data/` (default name `data/spending_by_category_<YYYY-MM-DD>.json`).
- **Transfer filter** — `search_individual_transfers` returns JSON for “Переводы” rows that look like transfers to individuals (regex on description).
- **Logging** — each main module logs to `logs/*.log`.

## Stack

| Layer        | Technology                          |
| ------------ | ----------------------------------- |
| Data         | Pandas                              |
| HTTP         | Requests                            |
| Secrets      | python-dotenv (`.env`)              |
| Excel        | pandas (`read_excel`; openpyxl stack) |
| Packaging    | Poetry (`pyproject.toml` / lockfile) |
| Tests        | Pytest (+ pytest-cov in dev group)  |
| Lint / types | Black, Flake8, isort, Mypy (optional groups) |

## Project layout

- `src/views.py` — `main_page`, greeting, cards summary, top transactions, currencies, stocks.
- `src/utils.py` — Excel read, `user_settings.json`, CBR rates, currency conversion decorators/helpers.
- `src/services.py` — `search_individual_transfers`.
- `src/reports.py` — `write_report` decorator, `spending_by_category`.
- `tests/` — unit tests for the above.
- `user_settings.json` — `user_currencies`, `user_stocks` (tickers for FMP).
- `data/` — place `operations.xlsx` here; generated reports land here too.

## Quick start

1. Clone the repo:

   ```bash
   git clone https://github.com/AJLbN0H/personal-finance-aggregator.git
   cd personal-finance-aggregator
   ```

2. Copy the environment template and set your FMP API key:

   ```bash
   cp .env.example .env
   ```

   Set `APISP500` to a valid [Financial Modeling Prep](https://financialmodelingprep.com/) API key. Without it, stock prices in `main_page` will be empty or fail gracefully depending on the response.

3. Install dependencies (the **dev** group includes Pytest):

   ```bash
   poetry install --no-interaction --with dev
   ```

   For formatters / linters / Mypy as well:

   ```bash
   poetry install --no-interaction --with dev,lint
   ```

4. Prepare inputs:

   - Put your export at **`data/operations.xlsx`**.
   - Adjust **`user_settings.json`** (`user_currencies`, `user_stocks`) as needed.

5. Run the main aggregator from Python (library-style; there is no `main.py`):

   ```bash
   poetry run python -c "from src.views import main_page; print(main_page())"
   ```

   Optional fixed datetime (only the **date** part is applied to transaction windows; greeting still uses the real clock unless you change the code):

   ```bash
   poetry run python -c "from src.views import main_page; print(main_page('2026-03-21 12:00:00'))"
   ```

> **Secrets:** do not commit real API keys. Keep them in `.env` (gitignored) or your CI/host secret store. The template is `.env.example`.

## Tests

```bash
poetry run pytest
```

Optional coverage:

```bash
poetry run pytest --cov=src --cov-report=term-missing
```

## Local run without Poetry

If you prefer a venv and pip, install the runtime packages from `pyproject.toml` (e.g. `pandas`, `python-dotenv`, `requests`, `black` as declared) plus **openpyxl** for Excel, and dev tools (`pytest`, `pytest-cov`) for tests. **Poetry + lockfile** is the supported path and matches CI.

```bash
python -m venv .venv
.venv\Scripts\activate
# source .venv/bin/activate   # Linux/macOS

pip install pandas python-dotenv requests black openpyxl pytest pytest-cov
cp .env.example .env
# edit .env — set APISP500

pytest
```

## CI

GitHub Actions runs on pushes/PRs to **`main`** / **`develop`** (PRs target **`main`**): **Python 3.13**, `poetry install --with dev`, then `poetry run pytest`.

Workflow: [`.github/workflows/tests.yml`](.github/workflows/tests.yml).

## Roadmap

- Optional **CLI entrypoint** (e.g. `python -m` or Typer) for reports and `main_page` output to stdout/file.
- **Exported `requirements.txt`** (or documented `pip-tools` flow) for environments without Poetry.
- **HTTP mocking** in tests for CBR/FMP to avoid network flakiness.
- Stricter handling of **missing Excel / empty frames** with structured errors in the JSON API surface.
