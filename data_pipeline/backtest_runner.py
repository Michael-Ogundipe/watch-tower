import argparse
import asyncio
import contextlib
import csv
import io
import json
import os
from datetime import datetime
from pathlib import Path

from models import Candle, Timeframe
from market_data_service import MarketDataService
from replay_engine import ReplayEngine
from deriv_client import DerivClient


# ============================================================
# V1 BASELINE
# ============================================================

SYMBOLS = [
    "R_75",
    "JD75",
    "JD25"
]

M1_COUNT = 130_000
HIGHER_TIMEFRAME_COUNT = 26_000

# H4 is not consumed by ReplayEngine / the V1 strategy.
# Skipping this unused download does not alter strategy logic.
BACKTEST_TIMEFRAMES = (
    Timeframe.M1,
    Timeframe.M5,
    Timeframe.M15,
    Timeframe.H1,
)

BASELINE_EXPECTED = {
    "filled": 574,
    "wins": 212,
    "losses": 362,
    "net_r": 62.0,
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="WatchTower V1 backtest runner (strategy unchanged)."
    )

    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Ignore cached candles and download fresh historical data.",
    )

    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress per-candle/per-trade replay output.",
    )

    parser.add_argument(
        "--verify",
        action="store_true",
        help="Check the aggregate result against the original +62R baseline.",
    )

    parser.add_argument(
        "--cache-dir",
        default="backtest_cache",
        help="Directory used for historical candle cache.",
    )

    parser.add_argument(
        "--m1-count",
        type=int,
        default=M1_COUNT,
        help=f"M1 candle count (default: {M1_COUNT:,}).",
    )

    parser.add_argument(
        "--higher-count",
        type=int,
        default=HIGHER_TIMEFRAME_COUNT,
        help=(
            "Maximum candle count for each higher timeframe; actual requests are "
            "capped to the M1 replay-window duration "
            f"(default max: {HIGHER_TIMEFRAME_COUNT:,})."
        ),
    )

    return parser.parse_args()


def candle_to_dict(candle: Candle):
    return {
        "symbol": candle.symbol,
        "timeframe": candle.timeframe.name,
        "open": candle.open,
        "high": candle.high,
        "low": candle.low,
        "close": candle.close,
        "timestamp": candle.timestamp.isoformat(),
    }


def candle_from_dict(data):
    return Candle(
        symbol=data["symbol"],
        timeframe=Timeframe[data["timeframe"]],
        open=data["open"],
        high=data["high"],
        low=data["low"],
        close=data["close"],
        timestamp=datetime.fromisoformat(data["timestamp"]),
    )


def cache_path(cache_dir, symbol, timeframe, count):
    return (
        Path(cache_dir)
        / f"{symbol}_{timeframe.name}_{count}.json"
    )


def load_cache(path):
    with path.open("r", encoding="utf-8") as file:
        payload = json.load(file)

    return [candle_from_dict(item) for item in payload["candles"]]


def save_cache(path, candles):
    path.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "version": 1,
        "candles": [candle_to_dict(candle) for candle in candles],
    }

    temp_path = path.with_suffix(".tmp")

    with temp_path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, separators=(",", ":"))

    temp_path.replace(path)


async def fetch_one_timeframe(
    symbol,
    timeframe,
    count,
    cache_dir,
    refresh,
):
    path = cache_path(
        cache_dir,
        symbol,
        timeframe,
        count,
    )

    if not refresh:
        cache_source = path if path.exists() else None
        cached_candles = None
        if cache_source is None:
            # A larger cache contains the same most-recent window. Reuse it
            # when the requested timeframe count has been reduced, instead
            # of triggering a network download just because the filename's
            # count changed.
            candidates = []
            for candidate in path.parent.glob(f"{symbol}_{timeframe.name}_*.json"):
                try:
                    cached_count = int(candidate.stem.rsplit("_", 1)[1])
                except (IndexError, ValueError):
                    continue
                if cached_count > count:
                    candidates.append((cached_count, candidate))
            for _cached_count, candidate in sorted(candidates):
                candidate_candles = load_cache(candidate)
                if len(candidate_candles) >= count:
                    cache_source = candidate
                    cached_candles = candidate_candles
                    break

        if cache_source is not None:
            candles = cached_candles if cached_candles is not None else load_cache(cache_source)
            if len(candles) > count:
                candles = candles[-count:]
                save_cache(path, candles)

        print(
            f"[{symbol}] CACHE "
            f"{timeframe.name}: {len(candles):,} candles"
        )

        return timeframe, candles

    client = DerivClient()

    try:
        await client.connect()

        print(
            f"[{symbol}] DOWNLOAD "
            f"{timeframe.name}: {count:,} candles"
        )

        candles = await client.get_candles(
            symbol=symbol,
            timeframe=timeframe,
            count=count,
        )

        if not candles:
            raise RuntimeError(
                f"No candles returned for {symbol} {timeframe.name}"
            )

        save_cache(path, candles)

        print(
            f"[{symbol}] SAVED "
            f"{timeframe.name}: {len(candles):,} candles"
        )

        return timeframe, candles

    finally:
        await client.close()


async def load_symbol_data(
    symbol,
    m1_count,
    higher_count,
    cache_dir,
    refresh,
):
    requests = []

    for timeframe in BACKTEST_TIMEFRAMES:
        if timeframe == Timeframe.M1:
            count = m1_count
        else:
            # The replay window is defined by M1 history. Request only enough
            # bars on each strategy timeframe to cover that same duration;
            # `higher_count` remains the caller's upper bound. This avoids
            # downloading and parsing unused months/years of M15/H1 candles.
            window_seconds = m1_count * Timeframe.M1.value
            covering_count = (window_seconds + timeframe.value - 1) // timeframe.value
            count = min(higher_count, covering_count)

        requests.append(
            fetch_one_timeframe(
                symbol,
                timeframe,
                count,
                cache_dir,
                refresh,
            )
        )

    # Each timeframe gets its own websocket because the Deriv websocket
    # client does not allow concurrent recv() calls on one connection.
    results = await asyncio.gather(*requests)

    return {
        timeframe: candles
        for timeframe, candles in results
    }


async def run_backtest(
    symbol,
    m1_count,
    higher_count,
    cache_dir,
    refresh,
    quiet,
):
    print("\n")
    print("=" * 70)
    print(f"BACKTESTING {symbol}")
    print("=" * 70)

    candles_by_timeframe = await load_symbol_data(
        symbol,
        m1_count,
        higher_count,
        cache_dir,
        refresh,
    )

    m1_candles = candles_by_timeframe[Timeframe.M1]

    if not m1_candles:
        print(f"[{symbol}] No M1 candles returned.")
        return None

    replay_start = m1_candles[0].timestamp
    replay_end = m1_candles[-1].timestamp

    print(
        f"\n[{symbol}] BACKTEST WINDOW\n"
        f"{replay_start} → {replay_end}"
    )

    # Match the original behavior: higher-timeframe candles are restricted
    # to the M1 replay end, while ReplayEngine handles completion-time
    # ordering and warmup.
    for timeframe, candles in candles_by_timeframe.items():
        candles_by_timeframe[timeframe] = [
            candle
            for candle in candles
            if candle.timestamp <= replay_end
        ]

    service = MarketDataService(symbol)

    replay_engine = ReplayEngine(
        service,
        min_entry_rr=float(os.environ.get("WATCHTOWER_MIN_ENTRY_RR", "1.0")),
    )

    if quiet:
        with contextlib.redirect_stdout(io.StringIO()):
            simulator = replay_engine.replay(
                candles_by_timeframe
            )
    else:
        simulator = replay_engine.replay(
            candles_by_timeframe
        )

    # Always print the compact result even when replay output is suppressed.
    filled = simulator.total_filled
    win_rate = (
        simulator.total_wins / filled * 100
        if filled
        else 0.0
    )

    print(
        f"[{symbol}] RESULT "
        f"filled={filled} "
        f"wins={simulator.total_wins} "
        f"losses={simulator.total_losses} "
        f"timeouts={simulator.total_timeouts} "
        f"win_rate={win_rate:.2f}% "
        f"net_r={simulator.total_r:.2f}R "
        f"PF={simulator.profit_factor:.2f}"
    )

    return simulator


def save_backtest_results(results, total_filled, total_wins, total_losses, total_r, verify=False, expected=None):
    """Persist backtest outputs without changing strategy calculations."""
    out = Path("backtest_results")
    out.mkdir(parents=True, exist_ok=True)

    summary = {
        "total_filled": total_filled,
        "total_wins": total_wins,
        "total_losses": total_losses,
        "total_r": total_r,
        "verification_requested": verify,
    }
    if expected is not None:
        checks = {
            "filled": total_filled == expected["filled"],
            "wins": total_wins == expected["wins"],
            "losses": total_losses == expected["losses"],
            "net_r": abs(total_r - expected["net_r"]) < 1e-9,
        }
        summary["baseline_expected"] = expected
        summary["baseline_checks"] = checks
        summary["baseline_pass"] = all(checks.values())

    pair_rows = []
    for simulator in results:
        filled = simulator.total_filled
        quality_stats = {}
        for category in ("A+", "A", "B", "C", None):
            trades = [trade for trade in simulator.completed_trades if trade.quality_category == category]
            if trades:
                quality_stats[category or "unrated"] = {
                    "trades": len(trades),
                    "wins": sum(trade.status.value == "win" for trade in trades),
                    "losses": sum(trade.status.value == "loss" for trade in trades),
                    "win_rate": sum(trade.status.value == "win" for trade in trades) / len(trades) * 100,
                    "average_rr": sum(trade.potential_rr or 0 for trade in trades) / len(trades),
                    "net_r": sum(trade.result_r or 0 for trade in trades),
                }
        pair_rows.append({
            "symbol": simulator.symbol,
            "signals": simulator.total_signals,
            "filled": simulator.total_filled,
            "wins": simulator.total_wins,
            "losses": simulator.total_losses,
            "timeouts": simulator.total_timeouts,
            "rejected_min_rr": simulator.total_rejected_min_rr,
            "invalidated_pending": simulator.total_invalidated,
            "invalidation_reasons": simulator.invalidation_reasons,
            "win_rate": (simulator.total_wins / filled * 100) if filled else 0.0,
            "net_r": simulator.total_r,
            "profit_factor": simulator.profit_factor,
            "quality_performance": quality_stats,
        })

    summary["pairs"] = pair_rows
    (out / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    with (out / "pair_results.csv").open("w", newline="", encoding="utf-8") as f:
        fields = ["symbol", "signals", "filled", "wins", "losses", "timeouts", "win_rate", "net_r", "profit_factor"]
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(pair_rows)

    print(f"Backtest results saved to: {out.resolve()}")


async def main():
    args = parse_args()

    if args.m1_count <= 0 or args.higher_count <= 0:
        raise SystemExit("Candle counts must be positive.")

    results = []

    for symbol in SYMBOLS:
        simulator = await run_backtest(
            symbol=symbol,
            m1_count=args.m1_count,
            higher_count=args.higher_count,
            cache_dir=args.cache_dir,
            refresh=args.refresh,
            quiet=args.quiet,
        )

        if simulator is not None:
            results.append(simulator)

    print("\n")
    print("=" * 90)
    print("PAIR COMPARISON")
    print("=" * 90)

    print(
        f"{'PAIR':<10}"
        f"{'SIGNALS':>10}"
        f"{'FILLED':>10}"
        f"{'WINS':>8}"
        f"{'LOSSES':>10}"
        f"{'TIMEOUTS':>12}"
        f"{'WIN %':>10}"
        f"{'NET R':>10}"
        f"{'PF':>10}"
    )

    print("-" * 90)

    for simulator in results:
        filled = simulator.total_filled

        win_rate = (
            simulator.total_wins / filled * 100
            if filled > 0
            else 0.0
        )

        print(
            f"{simulator.symbol:<10}"
            f"{simulator.total_signals:>10}"
            f"{simulator.total_filled:>10}"
            f"{simulator.total_wins:>8}"
            f"{simulator.total_losses:>10}"
            f"{simulator.total_timeouts:>12}"
            f"{win_rate:>9.2f}%"
            f"{simulator.total_r:>9.2f}R"
            f"{simulator.profit_factor:>10.2f}"
        )

    print("=" * 90)

    total_filled = sum(s.total_filled for s in results)
    total_wins = sum(s.total_wins for s in results)
    total_losses = sum(s.total_losses for s in results)
    total_r = sum(s.total_r for s in results)

    print(
        f"TOTAL: filled={total_filled} "
        f"wins={total_wins} "
        f"losses={total_losses} "
        f"net_r={total_r:.2f}R"
    )

    if args.verify:
        expected = BASELINE_EXPECTED

        checks = {
            "filled": total_filled == expected["filled"],
            "wins": total_wins == expected["wins"],
            "losses": total_losses == expected["losses"],
            "net_r": abs(total_r - expected["net_r"]) < 1e-9,
        }

        print("\nBASELINE VERIFICATION")
        print("-" * 40)

        for name, passed in checks.items():
            print(
                f"{name:<10} "
                f"{'PASS' if passed else 'FAIL'} "
                f"(actual={locals()['total_' + name] if name != 'net_r' else total_r}, "
                f"expected={expected[name]})"
            )

        if all(checks.values()):
            print("BASELINE: PASS — original +62R result reproduced.")
        else:
            print(
                "BASELINE: FAIL — the supplied historical dataset did not "
                "reproduce the original +62R result."
            )

    save_backtest_results(
        results, total_filled, total_wins, total_losses, total_r,
        verify=args.verify, expected=BASELINE_EXPECTED if args.verify else None,
    )

    print(
        "\nV1 strategy logic remains unchanged. "
        "The changes are backtest/data-path performance optimizations."
    )


if __name__ == "__main__":
    asyncio.run(main())
