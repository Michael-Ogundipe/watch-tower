from deriv_client import DerivClient
from candle_engine import CandleEngine
from models import Candle, Timeframe
from market_structure import MarketStructure
from bos_detector import BOSDetector
from liquidity_sweep_detector import LiquiditySweepDetector
from liquidity_detector import LiquidityDetector
from fvg_detector import FVGDetector
from order_block_detector import OrderBlockDetector
from confluence_engine import ConfluenceEngine



TIMEFRAMES = [
    Timeframe.M1,
    Timeframe.M5,
    Timeframe.M15,
    Timeframe.H1,
    Timeframe.H4,
]


class MarketDataService:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.client = DerivClient()

        self.engines = {
            timeframe: CandleEngine(timeframe)
            for timeframe in TIMEFRAMES
        }

        self.structures = {}
        self.bos_detectors = {
            timeframe: BOSDetector(timeframe)
            for timeframe in TIMEFRAMES
        }

        self.order_block_detectors = {
            timeframe: OrderBlockDetector(timeframe)
            for timeframe in Timeframe
        }

        self.liquidity_sweep_detectors = {
            timeframe: LiquiditySweepDetector()
            for timeframe in TIMEFRAMES
        }

        self.liquidity_detectors = {
            timeframe: LiquidityDetector(timeframe)
            for timeframe in TIMEFRAMES
        }

        self.fvg_detectors = {
            timeframe: FVGDetector(timeframe)
            for timeframe in TIMEFRAMES
        }

        self.confluence_engine = ConfluenceEngine(
            self.structures,
            self.fvg_detectors,
            self.order_block_detectors,
            self.liquidity_sweep_detectors,
            self.bos_detectors,
        )

        self.last_confluence_signal = None

    async def initialize(self):
        await self.client.connect()

        for timeframe, engine in self.engines.items():
            candles = await self.client.get_candles(
                symbol=self.symbol,
                timeframe=timeframe,
                count=100,
            )

            engine.initialize(candles)

            self.structures[timeframe] = MarketStructure(candles)

            print(
                f"Loaded {len(candles)} "
                f"{timeframe.name} candles"
            )

    def get_candles(self, timeframe: Timeframe) -> list[Candle]:
        engine = self.engines[timeframe]

        return engine.candles


    def get_structure_state(self, timeframe: Timeframe):
        structure = self.structures.get(timeframe)

        if structure is None:
            return None

        return structure.get_state()


    def process_completed_candle(self, completed_candle):
        timeframe = completed_candle.timeframe

        structure = self.structures[timeframe]

        structure.process_candle(completed_candle)

        # FVG
        fvg_detector = self.fvg_detectors[timeframe]

        fvg = fvg_detector.check(
            structure.candles,
        )

        if fvg:
            print(
                f"FVG {fvg.fvg_type.value.upper()} "
                f"on {timeframe.name} "
                f"at {completed_candle.timestamp} "
                f"zone: {fvg.lower} - {fvg.upper} "
                f"status: {fvg.status.value.upper()}"
            )

        # Liquidity
        liquidity_detector = self.liquidity_detectors[timeframe]

        pools = liquidity_detector.detect(
            structure.swing_highs,
            structure.swing_lows,
        )

        sweep_detector = self.liquidity_sweep_detectors[timeframe]

        sweep = sweep_detector.check(
            completed_candle,
            pools,
        )

        if sweep:
            print(
                f"LIQUIDITY SWEEP "
                f"{sweep.liquidity_type.value.upper()} "
                f"on {timeframe.name} "
                f"at {sweep.candle.timestamp}"
            )

        # Order Block lifecycle
        order_block_detector = self.order_block_detectors[timeframe]

        updated_order_blocks = (
            order_block_detector.update_status(
                completed_candle
            )
        )

        for order_block in updated_order_blocks:
            print(
                f"ORDER BLOCK "
                f"{order_block.block_type.value.upper()} "
                f"{order_block.status.value.upper()} "
                f"on {timeframe.name} "
                f"zone: {order_block.lower} - "
                f"{order_block.upper}"
            )

        # BOS / CHOCH
        bos_detector = self.bos_detectors[timeframe]

        state = structure.get_state()

        bos = bos_detector.check(
            completed_candle,
            structure.swing_highs,
            structure.swing_lows,
            state.bias,
        )

        if bos:
            print(
                f"{bos.break_type.value.upper()} "
                f"{bos.direction.value.upper()} "
                f"on {timeframe.name} "
                f"at {bos.candle.timestamp}"
            )

            order_block = order_block_detector.check(
                bos,
                structure.candles,
            )

            if order_block:
                print(
                    f"ORDER BLOCK "
                    f"{order_block.block_type.value.upper()} "
                    f"CREATED "
                    f"on {timeframe.name} "
                    f"at {order_block.candle.timestamp} "
                    f"zone: {order_block.lower} - "
                    f"{order_block.upper}"
                )

        # Confluence
        analysis = self.confluence_engine.analyze()

        if analysis:
            signal_key = (
                analysis["direction"],
                analysis["score"],
                tuple(analysis["reasons"]),
            )

            if signal_key != self.last_confluence_signal:
                print(
                    f"CONFLUENCE "
                    f"{analysis['direction'].value.upper()} "
                    f"SCORE {analysis['score']}"
                )

                for reason in analysis["reasons"]:
                    print(f"  ✓ {reason}")

                self.last_confluence_signal = signal_key

                return analysis

        return None
    

    

    async def run(self):
        async for tick in self.client.subscribe_ticks(self.symbol):

            for engine in self.engines.values():
                completed_candle = engine.process_tick(tick)

                if completed_candle:
                    self.process_completed_candle(
                        completed_candle
                    )

                