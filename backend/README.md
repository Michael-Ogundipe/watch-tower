# watchtower — data pipeline + LLM reasoner

This is Milestone 0's first slice: fetch multi-timeframe candles, feed
structured JSON to Claude, get back a top-down bias verdict. Replaces the
screenshot-to-ChatGPT workflow with something you can run in one command.

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env
# fill in TWELVEDATA_API_KEY and ANTHROPIC_API_KEY in .env
```

## Run

```bash
# with real data (needs both API keys)
python main.py --symbol EUR/USD

# with fake data, to try the pipeline before you have keys
python main.py --symbol EUR/USD --mock
```

## What's here

| File | Role |
|---|---|
| `models.py` | `Candle`, `TimeframeSnapshot`, `MarketSnapshot` — typed domain objects (this is Milestone 1 pulled forward, since the LLM layer needs a real schema to serialize) |
| `data_provider.py` | `DataProvider` abstract interface + `TwelveDataProvider` (real) + `MockDataProvider` (fake, for testing without keys or quota) |
| `reasoner.py` | Prompt + strict JSON schema (`BiasVerdict`) for the LLM call |
| `main.py` | CLI wiring it all together |

## Known gaps (by design — this is Milestone 0, not the final system)

- No webhook trigger yet — this runs on demand, not on a price event.
- No structure/confluence engine (BOS, CHoCH, order blocks, etc.) — the LLM
  is reasoning over raw candle summaries only, not your specific ICT-style
  concepts. That's Milestone 3.
- No persistence — nothing is logged or stored between runs. That's
  Milestone 6/7, and you'll want it before you can evaluate whether this is
  actually any good.
- `TwelveDataProvider` is one implementation of `DataProvider`. If you'd
  rather pull from OANDA, MT5, or your broker, only `data_provider.py`
  needs to change — nothing else depends on Twelve Data specifically.
