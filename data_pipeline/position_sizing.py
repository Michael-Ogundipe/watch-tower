"""
Converts a technical stop-loss (a real price, from the order block) into a
Deriv Multiplier position, holding your dollar risk fixed instead of your
stake.

Deriv Multipliers take stop_loss/take_profit as MONETARY amounts, not
prices. If you fix the stake and multiplier, a wide order block and a tight
one end up risking wildly different dollar amounts for the "same" setup.
Fixing the risk instead means the stop_loss/take_profit dollar amounts are
CONSTANT across every trade (risk_amount and 2x risk_amount, since your
confluence engine always targets 1:2 R) -- only the stake varies, scaled to
whatever stop distance this particular order block happens to have.
"""

from dataclasses import dataclass


@dataclass
class SizedPosition:
    stake: float
    multiplier: float
    stop_loss_amount: float
    take_profit_amount: float
    stop_distance_pct: float


def compute_stake(
    entry_price: float,
    stop_loss_price: float,
    multiplier: float,
    risk_amount: float,
    reward_multiple: float = 2.0,
    min_stake: float = 1.0,
    max_stake: float = 25.0,
) -> SizedPosition | None:
    """
    entry_price / stop_loss_price: the actual technical levels from your
        order block -- unchanged from what ConfluenceEngine/TradeSimulator
        already compute.
    multiplier: the multiplier you're trading with (fixed; discover valid
        values for your symbol via DerivTradingClient.get_contracts_for()
        rather than assuming).
    risk_amount: how much you're willing to lose if the stop is hit, e.g. 5.00.
    reward_multiple: matches ConfluenceEngine's fixed 1:2 R (take_profit
        amount = risk_amount * reward_multiple).
    min_stake / max_stake: hard bounds. A trade is rejected if the requested
        dollar risk cannot be achieved inside these bounds. This prevents
        Watch Tower from silently taking a much smaller risk on tight stops
        or a larger risk on wide stops.

    Returns None if the stop distance is degenerate (zero or entry==stop),
    same convention TradeSimulator.create_trade already uses.
    """
    stop_distance_pct = abs(entry_price - stop_loss_price) / entry_price

    if stop_distance_pct <= 0:
        return None

    raw_stake = risk_amount / (multiplier * stop_distance_pct)

    # Do not silently reduce the requested risk when the stop is too
    # tight, and do not increase risk just because the minimum stake is
    # larger than the calculated stake. A trade is valid only when the
    # requested risk can actually be achieved within the stake bounds.
    if raw_stake < min_stake or raw_stake > max_stake:
        return None

    stake = round(raw_stake, 2)
    actual_risk = stake * multiplier * stop_distance_pct

    return SizedPosition(
        stake=round(stake, 2),
        multiplier=multiplier,
        stop_loss_amount=round(actual_risk, 2),
        take_profit_amount=round(actual_risk * reward_multiple, 2),
        stop_distance_pct=stop_distance_pct,
    )
