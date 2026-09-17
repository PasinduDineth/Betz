"""Safely validate Binance Prediction Trading Get Quote access.

This script never calls Place Order and cannot buy or sell anything.
"""

from __future__ import annotations

from scanner import BinanceClient, BinanceMarketClient, Config, market_text, outcome_token


def main() -> None:
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
    quote = client.get_market_quote(token_id, cfg.buy_usdt)

    # Deliberately omit quoteId: this command is a configuration check, not an
    # order workflow. A quote alone cannot move funds or place a trade.
    print("GET QUOTE PREFLIGHT SUCCESS")
    print(f"market_id={market_id}")
    print(f"title={market.get('title') or market.get('question') or market_id}")
    print(f"outcome={outcome}")
    print(f"average_price={quote.get('averagePrice')}")
    print(f"last_price={quote.get('lastPrice')}")
    print(f"chance={quote.get('chance')}")
    print(f"amount_in_usdt={cfg.buy_usdt}")


if __name__ == "__main__":
    main()
