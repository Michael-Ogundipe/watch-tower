"""
Usage:
    python main.py --symbol EUR/USD
    python main.py --symbol EUR/USD --mock      # no API keys needed, fake data

This replaces the screenshot-to-ChatGPT workflow: fetch real multi-timeframe
data, feed it to the LLM as structured JSON, get a structured bias verdict
back. No chart images, no manual copy-paste.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys

from data_provider import ALL_TIMEFRAMES, DataProvider, MockDataProvider, TwelveDataProvider
from deriv_provider import DerivDataProvider
from reasoner import get_bias


def build_provider(source: str) -> DataProvider:
    if source == "mock":
        return MockDataProvider()
    if source == "twelvedata":
        return TwelveDataProvider()
    if source == "deriv":
        return DerivDataProvider()
    raise ValueError(f"Unknown data source: {source}")


async def run(symbol: str, source: str, count: int) -> None:
    provider = build_provider(source)

    print(f"Fetching {symbol} across {', '.join(ALL_TIMEFRAMES)} via {source}...", file=sys.stderr)
    snapshot = await provider.fetch_snapshot(symbol, ALL_TIMEFRAMES, count=count)

    if hasattr(provider, "close"):
        await provider.close()

    print("Asking the model for a top-down bias read...", file=sys.stderr)
    verdict = get_bias(snapshot)

    print(json.dumps(verdict.model_dump(), indent=2))
    print(f"\n{verdict.symbol}: {verdict.bias.upper()} (confidence {verdict.confidence}/5)", file=sys.stderr)
    for tf_read in verdict.timeframe_reads:
        print(f"  {tf_read.timeframe}: {tf_read.read}", file=sys.stderr)
    print(f"  Invalidation: {verdict.invalidation_note}", file=sys.stderr)


def main() -> None:
    parser = argparse.ArgumentParser(description="WatchTower top-down bias check")
    parser.add_argument("--symbol", required=True, help="e.g. 'EUR/USD' or 'Jump 75'")
    parser.add_argument("--count", type=int, default=50, help="candles per timeframe")
    parser.add_argument(
        "--source",
        choices=["mock", "twelvedata", "deriv"],
        default="mock",
        help="data source (default: mock, no keys required)",
    )
    args = parser.parse_args()

    asyncio.run(run(args.symbol, args.source, args.count))


if __name__ == "__main__":
    main()
