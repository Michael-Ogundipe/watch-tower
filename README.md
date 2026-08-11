# Watchtower

> An AI-powered market analysis and decision-support system that combines multi-timeframe technical analysis, market-structure detection, signal confluence, and LLM-based reasoning.

## Why I Built It

Analyzing multiple timeframes manually and repeatedly feeding screenshots into an LLM is slow, repetitive, and difficult to scale when market conditions are changing quickly.

I wanted to replace that workflow with a system that continuously consumes structured market data, performs multi-timeframe analysis, identifies confluences, and uses AI to reason over the resulting market state.

## Overview

Watchtower is an end-to-end market intelligence platform designed to continuously analyze market data, identify technical structures, build confluence-based setups, and use an LLM reasoning layer to validate and explain potential setups.

The system combines deterministic technical analysis with AI-assisted reasoning rather than relying solely on an LLM to interpret raw market data.

### Watchtower currently analyzes:

- Market trends
- Break of Structure (BOS)
- Change of Character (CHoCH)
- Liquidity sweeps
- Order Blocks
- Fair Value Gaps (FVGs)
- Multi-timeframe confluence
- Directional market bias
- Potential trade setups

Detected signals are transformed into structured market state and passed through a confluence engine before being evaluated by the AI reasoning layer.

The resulting analysis is presented through a real-time dashboard and distributed through multiple notification channels.

---

## Architecture


```text
                    ┌─────────────────────────┐
                    │      Market Data        │
                    └────────────┬────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │  Market Data Pipeline   │
                    │                         │
                    │  Candle Processing      │
                    │  Timeframe Aggregation  │
                    └────────────┬────────────┘
                                 │
                                 ▼
              ┌────────────────────────────────────┐
              │       Technical Analysis Engine    │
              │                                    │
              │  • Trend Detection                 │
              │  • BOS / CHoCH                     │
              │  • Liquidity Sweeps                │
              │  • Order Blocks                    │
              │  • Fair Value Gaps                 │
              └────────────────┬───────────────────┘
                               │
                               ▼
              ┌────────────────────────────────────┐
              │          Confluence Engine         │
              │                                    │
              │  Combines technical signals        │
              │  into structured market state      │
              └────────────────┬───────────────────┘
                               │
                               ▼
              ┌────────────────────────────────────┐
              │         AI Reasoning Layer         │
              │                                    │
              │         OpenAI + LangGraph         │
              │                                    │
              │    Setup validation + explanation  │
              └────────────────┬───────────────────┘
                               │
                ┌──────────────┼──────────────┐
                ▼              ▼              ▼
          ┌──────────┐   ┌───────────┐   ┌──────────┐
          │ Flutter  │   │ Telegram  │   │ Discord  │
          │ Dashboard│   │ Alerts    │   │ Alerts   │
          └──────────┘   └───────────┘   └──────────┘


```

## Tech Stack
**AI / LLM**
- OpenAI
- LangGraph
- LLM-based reasoning
- Structured AI context
**Backend**
- Python
- FastAPI
- PostgreSQL
- SQLAlchemy
- Redis
- Celery
**Frontend**
- Flutter
- Riverpod
- Syncfusion Charts

**Infrastructure**
- Docker
- GitHub Actions
- CI/CD

**Integrations**
- Telegram
- Discord
- Push Notifications

## Status

🚧 Active Development

Watchtower is currently being developed toward a production-ready market monitoring and decision-support system.



