# Personal Finance Aggregator

![Python](https://img.shields.io/badge/python-3.13%2B-blue.svg)
![Pandas](https://img.shields.io/badge/pandas-data--processing-green.svg)
![Pytest](https://img.shields.io/badge/tests-pytest-orange.svg)

**Personal Finance Aggregator** is a Python project for transaction analytics and report generation.
It processes bank operations from Excel files, builds aggregated JSON output for a dashboard-like view,
fetches currency rates from the Central Bank of Russia, and retrieves stock prices from Financial Modeling Prep.

## Features

- Analyze transactions with `pandas`.
- Build a consolidated JSON response with:
  - greeting by current time,
  - card spending summary,
  - top transactions,
  - preferred currency rates,
  - selected stock prices.
- Generate category spending reports as JSON files.
- Find person-to-person transfer transactions.
- Log module execution and error details to `logs/`.

## Tech Stack

- Python 3.13+
- Poetry
- Pandas
- Requests
- Pytest

## Project Structure

- `src/views.py` - orchestration logic for the main JSON response (`main_page`).
- `src/reports.py` - report decorators and category spending report.
- `src/services.py` - transfer filtering service.
- `src/utils.py` - shared utilities (Excel loading, date parsing, currency conversion, settings loading).
- `tests/` - automated unit tests.

## Installation

1. Clone the repository:

   `git clone https://github.com/AJLbN0H/personal-finance-aggregator.git`

2. Install dependencies:

   `poetry install`

3. Set up environment variables:

   - Copy `.env.example` to `.env`
   - Set `APISP500` with your Financial Modeling Prep API key

4. Prepare input files:

   - Put operations data into `data/operations.xlsx`
   - Configure `user_settings.json` (currencies and stocks)

## Usage

This project is library-style (no `main.py` entrypoint).  
Call `main_page` from Python:

`poetry run python -c "from src.views import main_page; print(main_page())"`

Or with a specific datetime:

`poetry run python -c "from src.views import main_page; print(main_page('2026-03-21 12:00:00'))"`

## Testing

Run the test suite:

`poetry run pytest`

Current coverage in this repository includes modules in `src/` with 40+ tests.

## Notes

- Logs are written to `logs/`.
- Currency rates are fetched from CBR XML API.
- Stock prices are fetched from Financial Modeling Prep.
