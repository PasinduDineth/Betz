# Prediction market live scanner/trader

This project watches Binance's Prediction market event list and Binance prediction-market order books. When a qualifying outcome's best ask is at or below `ENTRY_MAX_PRICE`, it requests a Binance LIMIT quote and, only when `LIVE_TRADING=true`, submits a GTC BUY order up to `BUY_USDT`.

When a tracked buy is confirmed filled and the best bid reaches `EXIT_MIN_PRICE`, it requests a LIMIT SELL quote and submits a GTC SELL order for the tracked filled share quantity.

## Important

This is real-money software. The default is `LIVE_TRADING=false`. Set it to `true` only after checking every setting and understanding the risks. The bot does not guarantee fills, queue priority, liquidity, or profit.

## Setup

```powershell
Copy-Item .env.example .env
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python scanner.py
```

The bot needs a Binance API key/secret and Binance prediction wallet address/ID. Market discovery uses Binance's web market-event endpoint and does not require a Predict.fun API key. Use an API key restricted to the minimum permissions needed. Do not paste secrets into chat or commit `.env`.

## Strategy rules

- `MARKET_SCOPE=football` considers markets whose title/question matches `FOOTBALL_KEYWORDS`. `MARKET_SCOPE=all` considers every discovered market. All-category live scanning is deliberately restricted to one open position and one order per market.
- At most one entry order per market and at most `MAX_OPEN_POSITIONS` tracked positions.
- Entry is triggered by the best ask, not merely the last traded price.
- Entry price is capped by `ENTRY_MAX_PRICE`; the amount is capped by `BUY_USDT`.
- In REST mode, a fresh signed Binance order-book snapshot triggers an entry only when its best ask is at or below `ENTRY_MAX_PRICE`. It triggers an exit only when bid depth at or above `EXIT_MIN_PRICE` covers the filled shares. A price can still move between snapshot and execution, so the order uses a LIMIT cap/floor rather than a market order.
- Orders use Binance's documented quote-then-place flow. LIMIT orders use GTC.
- State is persisted in `data/state.json`; logs are written to `logs/trader.log`.
- If credentials or required market/token fields are missing, the bot logs and skips rather than guessing.

The Binance web market endpoint is not a stable public developer API and its request schema can change. The parser accepts common field names and logs an actionable error when a market cannot be mapped to an outcome token. The request body can be overridden with `BINANCE_MARKETS_BODY_JSON` after inspecting the browser request.

## Free mobile alerts

The scanner supports [ntfy](https://ntfy.sh), an open-source push notification system. Install the ntfy app on your phone, subscribe to a long random topic name, and set the same topic in `.env`:

```env
NTFY_SERVER=https://ntfy.sh
NTFY_TOPIC=use-a-long-random-private-topic-name
```

The scanner sends alerts for new in-scope markets, entry signals, exit signals, and live order submissions. Public ntfy topics are effectively bearer names, so do not use a short or guessable topic.

## Run continuously on Ubuntu

After cloning the project to the Droplet:

```bash
chmod +x install_service.sh
./install_service.sh
```

If the script creates `.env`, edit it first, then run the installer again. The installer creates a systemd service that starts at boot, restarts after crashes, and continues after the SSH window closes.

View logs:

```bash
sudo journalctl -u binance-prediction-scanner -f
```

Stop or restart:

```bash
sudo systemctl stop binance-prediction-scanner
sudo systemctl restart binance-prediction-scanner
```
