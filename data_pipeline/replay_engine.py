import heapq
from datetime import timedelta

from models import Candle, Timeframe
from market_structure import MarketStructure
from trade_simulator import TradeSimulator


class ReplayEngine:

    PRIORITY = {
        Timeframe.H1: 0,
        Timeframe.M15: 1,
        Timeframe.M5: 2,
        Timeframe.M1: 3,
    }

    def __init__(self, service, min_entry_rr: float = 0.0):
        self.service = service
        self.min_entry_rr = min_entry_rr

    @staticmethod
    def completion_time(candle: Candle):
        return (
            candle.timestamp
            + timedelta(seconds=candle.timeframe.value)
        )

    def _replay_ranges(
        self,
        candles_by_timeframe,
        replay_start,
        replay_end,
    ):
        """
        Return (start_index, end_index) for each replay timeframe.

        Candle lists are already sorted by timestamp, so slicing by timestamp
        lets the replay avoid building and sorting one large combined list.
        """
        ranges = {}

        for timeframe, candles in candles_by_timeframe.items():
            if timeframe == Timeframe.H4:
                continue

            # A candle participates when its completion time is strictly
            # after replay_start and <= replay_end.
            lower_timestamp = (
                replay_start
                - timedelta(seconds=timeframe.value)
            )
            upper_timestamp = (
                replay_end
                - timedelta(seconds=timeframe.value)
            )

            start = 0
            while (
                start < len(candles)
                and candles[start].timestamp <= lower_timestamp
            ):
                start += 1

            end = start
            while (
                end < len(candles)
                and candles[end].timestamp <= upper_timestamp
            ):
                end += 1

            ranges[timeframe] = (start, end)

        return ranges

    def _iter_replay_candles(
        self,
        candles_by_timeframe,
        ranges,
    ):
        """
        K-way merge of already sorted timeframe streams.

        This produces the same ordering as the original global sort:
        completion_time first, then H1 -> M15 -> M5 -> M1.
        """
        heap = []

        for timeframe, (start, end) in ranges.items():
            if start >= end:
                continue

            candle = candles_by_timeframe[timeframe][start]
            heapq.heappush(
                heap,
                (
                    self.completion_time(candle),
                    self.PRIORITY.get(timeframe, 99),
                    timeframe.value,
                    start,
                    timeframe,
                    end,
                ),
            )

        while heap:
            (
                _completed_at,
                _priority,
                _timeframe_value,
                index,
                timeframe,
                end,
            ) = heapq.heappop(heap)

            candles = candles_by_timeframe[timeframe]
            candle = candles[index]

            yield candle

            next_index = index + 1
            if next_index < end:
                next_candle = candles[next_index]

                heapq.heappush(
                    heap,
                    (
                        self.completion_time(next_candle),
                        self.PRIORITY.get(timeframe, 99),
                        timeframe.value,
                        next_index,
                        timeframe,
                        end,
                    ),
                )

    def replay(
        self,
        candles_by_timeframe: dict[Timeframe, list[Candle]],
    ):
        m1_candles = candles_by_timeframe[Timeframe.M1]

        replay_start = m1_candles[0].timestamp
        replay_end = m1_candles[-1].timestamp

        strategy_timeframes = {
            Timeframe.M5,
            Timeframe.M15,
            Timeframe.H1,
        }

        # ====================================================
        # WARMUP
        #
        # Only use candles that had COMPLETED before the
        # replay began. This preserves the original look-ahead
        # protection.
        # ====================================================

        for timeframe in strategy_timeframes:

            candles = candles_by_timeframe.get(
                timeframe,
                [],
            )

            warmup_candles = [
                candle
                for candle in candles
                if self.completion_time(candle) <= replay_start
            ]

            self.service.structures[timeframe] = MarketStructure(
                warmup_candles.copy()
            )

        ranges = self._replay_ranges(
            candles_by_timeframe,
            replay_start,
            replay_end,
        )

        total_candles = sum(
            end - start
            for start, end in ranges.values()
        )

        print(
            f"\nREPLAY START "
            f"{replay_start} → {replay_end}"
        )

        print(
            f"Replay candles: {total_candles:,}"
        )

        print(
            "Strategy timeframes: "
            "H1 → M15 → M5"
        )

        print(
            "M1 used for trade execution only.\n"
        )

        trade_simulator = TradeSimulator(
            self.service.symbol, min_entry_rr=self.min_entry_rr
        )

        processed = 0
        last_progress = 0

        for candle in self._iter_replay_candles(
            candles_by_timeframe,
            ranges,
        ):

            processed += 1

            if candle.timeframe == Timeframe.M1:

                trade_simulator.process_candle(candle)

            elif candle.timeframe in strategy_timeframes:

                signal = (
                    self.service.process_completed_candle(
                        candle
                    )
                )
                if candle.timeframe == Timeframe.M5:
                    trade_simulator.invalidate_pending_setup(candle, self.service)

                if signal:

                    trade_simulator.total_signals += 1

                    trade = (
                        trade_simulator.create_trade(
                            signal,
                            self.completion_time(candle),
                        )
                    )

                    if trade:

                        print(
                            f"REPLAY SIGNAL FOUND "
                            f"{signal['direction'].value.upper()} "
                            f"SCORE {signal['score']} "
                            f"QUALITY {signal.get('quality', {}).get('category', 'NOT_AVAILABLE')} "
                            f"{signal.get('quality', {}).get('score', 'NOT_AVAILABLE')} "
                            f"TIME "
                            f"{self.completion_time(candle)}"
                        )

            if (
                processed - last_progress >= 5_000
                or processed == total_candles
            ):

                percent = (
                    processed
                    / total_candles
                    * 100
                    if total_candles
                    else 100
                )

                print(
                    f"[{self.service.symbol}] "
                    f"REPLAY PROGRESS "
                    f"{processed:,}/{total_candles:,} "
                    f"({percent:.1f}%)"
                )

                last_progress = processed

        print(
            f"\n[{self.service.symbol}] "
            f"REPLAY COMPLETE"
        )

        trade_simulator.print_summary()

        return trade_simulator
