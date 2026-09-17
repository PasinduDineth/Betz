"""Safely validate Binance Prediction Trading Get Quote access.

This script never calls Place Order and cannot buy or sell anything.
"""

from __future__ import annotations

import argparse
from decimal import Decimal

from scanner import BinanceClient, BinanceMarketClient, Config, market_text, outcome_public_price, outcome_token


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a no-order Binance Prediction Get Quote preflight")
    parser.add_argument("--limit-price", help="Test a LIMIT quote at this price instead of a MARKET quote")
    args = parser.parse_args()
    cfg = Config()
    if not all((cfg.binance_key, cfg.binance_secret, cfg.wallet_address)):
        raise SystemExit("Missing BINANCE_API_KEY, BINANCE_API_SECRET, or BINANCE_WALLET_ADDRESS")
    if not (cfg.wallet_address.startswith("0x") and len(cfg.wallet_address) == 42):
        raise SystemExit("BINANCE_WALLET_ADDRESS must be the chain-56 BSC 0x address")

    markets = BinanceMarketClient(cfg).markets()
    candidates = []
    for market in markets:
        market_id = str(market.get("marketId") or market.get("id") or "")
        if not market_id or not any(keyword in market_text(market) for keyword in cfg.football_keywords):
            continue
        outcome = outcome_token(market)
        if outcome:
            candidates.append((market_id, market, outcome))
    if not candidates:
        raise SystemExit("No football market with a tradable outcome token was found")

    market_id, market, (token_id, outcome) = max(
        candidates,
        key=lambda item: int(item[0]) if item[0].isdigit() else -1,
    )
    client = BinanceClient(cfg)
    client.sync_time()
    if args.limit_price:
        limit_price = Decimal(args.limit_price)
        if not (Decimal("0") < limit_price < Decimal("1")):
            raise SystemExit("--limit-price must be between 0 and 1")
        quote = client.get_quote(token_id, "BUY", cfg.buy_usdt, limit_price, cfg.entry_slippage)
        quote_mode = f"LIMIT (price cap {limit_price})"
    else:
        quote = client.get_market_quote(token_id, cfg.buy_usdt)
        quote_mode = "MARKET"

    # Deliberately omit quoteId: this command is a configuration check, not an
    # order workflow. A quote alone cannot move funds or place a trade.
    print("GET QUOTE PREFLIGHT SUCCESS")
    print(f"market_id={market_id}")
    print(f"title={market.get('title') or market.get('question') or market_id}")
    print(f"outcome={outcome}")
    print(f"quote_mode={quote_mode}")
    print(f"public_price={outcome_public_price(market, token_id)}")
    print(f"average_price={quote.get('averagePrice')}")
    print(f"last_price={quote.get('lastPrice')}")
    print(f"chance={quote.get('chance')}")
    print(f"amount_in_usdt={cfg.buy_usdt}")


if __name__ == "__main__":
    main()
