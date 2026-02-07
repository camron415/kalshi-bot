# Kalshi Predictive Trading Bot

A high-performance, AI-driven trading bot for Kalshi prediction markets. This bot uses external data and Grok-4 (xAI) to forecast event probabilities and execute trades with edge, risk management, and dry-run support.

## Features
- Kalshi-only: No Polymarket code or dependencies
- Predictive trading using Grok-4 (xAI API)
- NewsAPI and external data integration
- Kelly criterion risk management
- Dry-run and live trading modes
- Logging, error handling, and graceful shutdown

## Setup
1. Copy your Kalshi API key, xAI API key, and NewsAPI key into a `.env` file:
   - `KALSHI_API_KEY=...`
   - `XAI_API_KEY=...`
   - `NEWSAPI_KEY=...`
2. Build and run with Cargo:
   ```sh
   cargo build --release
   cargo run --release
   ```

## Documentation
See `CONVERSION_PLAN.md` for migration details.
