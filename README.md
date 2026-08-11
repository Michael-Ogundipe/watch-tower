# Watchtower

> An AI-powered market analysis and trading intelligence system that combines multi-timeframe technical analysis, market-structure detection, signal confluence, and LLM-based reasoning.

AI Watchtower is an end-to-end trading intelligence platform designed to continuously analyze market data, identify technical structures, build confluence-based trade setups, and use an LLM reasoning layer to validate and explain potential setups.

The system combines deterministic technical analysis with AI-assisted reasoning rather than relying solely on an LLM to interpret raw market data.

---

## Overview

AI Watchtower analyzes market data across multiple timeframes, from higher-timeframe structure down to lower-timeframe execution signals.

The system detects:

- Market trends
- Break of Structure (BOS)
- Change of Character (CHoCH)
- Liquidity sweeps
- Order blocks
- Fair Value Gaps (FVGs)
- Multi-timeframe confluence
- Directional market bias
- Potential trade setups

Detected signals are transformed into structured market state and passed through a confluence engine before being evaluated by an LLM reasoning layer.

The result is presented through a real-time dashboard and distributed through multiple notification channels.

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
              │       AI Reasoning Layer           │
              │                                    │
              │       OpenAI + LangGraph            │
              │                                    │
              │  Setup validation + explanation    │
              └────────────────┬───────────────────┘
                               │
                ┌──────────────┼──────────────┐
                ▼              ▼              ▼
          ┌──────────┐   ┌───────────┐   ┌──────────┐
          │ Flutter  │   │ Telegram  │   │ Discord  │
          │Dashboard │   │ Alerts    │   │ Alerts   │
          └──────────┘   └───────────┘   └──────────┘
