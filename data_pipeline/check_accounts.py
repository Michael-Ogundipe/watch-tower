"""
Run this by itself first -- before touching live_trader.py.

It only calls the read-only accounts endpoint, so there's no risk of it
doing anything to your balance. The goal is just to see the real response
shape and find your demo account's id.
"""

from deriv_trading_client import DerivTradingClient
import config

client = DerivTradingClient(config.APP_ID, config.API_TOKEN)
client.list_accounts()
