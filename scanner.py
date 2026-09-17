from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import queue
import signal
import threading
import time
import uuid
import urllib.parse
from dataclasses import dataclass, asdict
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import requests
import websocket
from dotenv import load_dotenv


load_dotenv()

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
LOG_DIR = ROOT / "logs"
DATA_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler(LOG_DIR / "trader.log", encoding="utf-8")],
)
log = logging.getLogger("prediction-trader")


class Notifier:
    """Optional ntfy mobile notifications; disabled when no topic is set."""

    def __init__(self) -> None:
        self.topic = os.getenv("NTFY_TOPIC", "").strip()
        self.server = os.getenv("NTFY_SERVER", "https://ntfy.sh").rstrip("/")
        self.session = requests.Session()

    def send(self, title: str, message: str, priority: str = "default", tags: str = "") -> None:
        if not self.topic:
            return
        try:
            headers = {"Title": title, "Priority": priority}
            if tags:
                headers["Tags"] = tags
            response = self.session.post(
                f"{self.server}/{self.topic}",
                data=message.encode("utf-8"),
                headers=headers,
                timeout=10,
            )
            response.raise_for_status()
        except Exception as exc:
            log.warning("Notification failed: %s", exc)


def dec(value: Any, default: Decimal | None = None) -> Decimal | None:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return default


def env_decimal(name: str, default: str) -> Decimal:
    return dec(os.getenv(name, default), Decimal(default)) or Decimal(default)


def find_value(obj: Any, keys: tuple[str, ...]) -> Any:
    if isinstance(obj, dict):
        for key in keys:
            if key in obj and obj[key] not in (None, ""):
                return obj[key]
        for value in obj.values():
            found = find_value(value, keys)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for value in obj:
            found = find_value(value, keys)
            if found is not None:
                return found
    return None


def decode_json_messages(raw: str) -> list[dict[str, Any]]:
    """Decode one or more JSON objects returned in a WebSocket frame."""
    decoder = json.JSONDecoder()
    messages: list[dict[str, Any]] = []
    offset = 0
    while offset < len(raw):
        while offset < len(raw) and raw[offset].isspace():
            offset += 1
        if offset >= len(raw):
            break
        try:
            value, end = decoder.raw_decode(raw, offset)
        except json.JSONDecodeError:
            # Some gateways prefix text frames; preserve the useful JSON tail.
            start = raw.find("{", offset)
            if start < 0:
                break
            value, end = decoder.raw_decode(raw, start)
        if isinstance(value, dict):
            messages.append(value)
        offset = end
    return messages


@dataclass
class Config:
    binance_key: str = os.getenv("BINANCE_API_KEY", "")
    binance_secret: str = os.getenv("BINANCE_API_SECRET", "")
    wallet_address: str = os.getenv("BINANCE_WALLET_ADDRESS", "")
    wallet_id: str = os.getenv("BINANCE_WALLET_ID", "")
    live: bool = os.getenv("LIVE_TRADING", "false").lower() == "true"
    buy_usdt: Decimal = env_decimal("BUY_USDT", "2.00")
    entry_max: Decimal = env_decimal("ENTRY_MAX_PRICE", "0.02")
    exit_min: Decimal = env_decimal("EXIT_MIN_PRICE", "0.90")
    max_positions: int = int(os.getenv("MAX_OPEN_POSITIONS", "3"))
    max_per_market: int = int(os.getenv("MAX_ORDERS_PER_MARKET", "1"))
    market_poll: float = float(os.getenv("MARKET_POLL_SECONDS", "2"))
    order_poll: float = float(os.getenv("ORDER_POLL_SECONDS", "5"))
    stale_seconds: int = int(os.getenv("ORDER_STALE_SECONDS", "86400"))
    binance_rest: str = os.getenv("BINANCE_REST_URL", "https://api.binance.com")
    binance_ws: str = os.getenv("BINANCE_WS_URL", "wss://api.binance.com/sapi/wss")
    entry_slippage: int = int(os.getenv("ENTRY_SLIPPAGE_BPS", "50"))
    exit_slippage: int = int(os.getenv("EXIT_SLIPPAGE_BPS", "50"))
    fee_bps: int = int(os.getenv("FEE_RATE_BPS", "200"))
    account_type: str = os.getenv("ACCOUNT_TYPE", "SPOT")
    funding_source: str = os.getenv("FUNDING_SOURCE", "MPC")
    orderbook_topic_mode: str = os.getenv("ORDERBOOK_TOPIC_MODE", "rest").strip().lower()
    # Keep the first connection small and observable. Set to 0 to subscribe
    # to every discovered market, or raise it after confirming snapshots work.
    max_markets_per_connection: int = int(os.getenv("MAX_MARKETS_PER_CONNECTION", "1"))
    ws_ping_seconds: float = float(os.getenv("WS_PING_SECONDS", "25"))
    ws_recv_timeout: float = float(os.getenv("WS_RECV_TIMEOUT_SECONDS", "5"))

    @property
    def football_keywords(self) -> tuple[str, ...]:
        return tuple(x.strip().lower() for x in os.getenv("FOOTBALL_KEYWORDS", "football,soccer").split(",") if x.strip())


@dataclass
class Position:
    market_id: str
    token_id: str
    title: str
    buy_order_id: str = ""
    sell_order_id: str = ""
    filled_shares: str = "0"
    buy_price: str = "0"
    status: str = "BUY_PENDING"
    created_at: float = 0


class State:
    def __init__(self) -> None:
        self.path = DATA_DIR / "state.json"
        self.seen: set[str] = set()
        self.market_baseline_ready = False
        self.positions: dict[str, Position] = {}
        self.orders_per_market: dict[str, int] = {}
        self.lock = threading.Lock()
        self.load()

    def load(self) -> None:
        if not self.path.exists():
            return
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            self.seen = set(raw.get("seen", []))
            self.market_baseline_ready = bool(raw.get("market_baseline_ready", False))
            self.orders_per_market = {str(k): int(v) for k, v in raw.get("orders_per_market", {}).items()}
            self.positions = {k: Position(**v) for k, v in raw.get("positions", {}).items()}
        except Exception as exc:
            log.warning("Could not load state: %s", exc)

    def save(self) -> None:
        with self.lock:
            payload = {
                "seen": sorted(self.seen),
                "market_baseline_ready": self.market_baseline_ready,
                "orders_per_market": self.orders_per_market,
                "positions": {k: asdict(v) for k, v in self.positions.items()},
            }
            self.path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


class BinanceClient:
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.session = requests.Session()
        self.session.headers.update({"X-MBX-APIKEY": cfg.binance_key, "Content-Type": "application/x-www-form-urlencoded"})
        self.time_offset_ms = 0

    def sync_time(self) -> int:
        """Synchronize local timestamps with Binance's server clock."""
        started = int(time.time() * 1000)
        response = self.session.get(self.cfg.binance_rest.rstrip("/") + "/api/v3/time", timeout=10)
        response.raise_for_status()
        finished = int(time.time() * 1000)
        server_time = int(response.json()["serverTime"])
        # Use the midpoint to reduce network-latency bias.
        self.time_offset_ms = server_time - ((started + finished) // 2)
        log.info("Binance server-time offset=%+dms", self.time_offset_ms)
        return self.time_offset_ms

    def now_ms(self) -> int:
        return int(time.time() * 1000) + self.time_offset_ms

    def signed_request(self, method: str, path: str, params: dict[str, Any] | None = None, body: dict[str, Any] | None = None) -> dict[str, Any]:
        params = dict(params or {})
        params.setdefault("timestamp", self.now_ms())
        params.setdefault("recvWindow", 5000)
        body = dict(body or {})
        query = urllib.parse.urlencode(sorted((k, str(v)) for k, v in params.items()))
        encoded_body = urllib.parse.urlencode(sorted((k, str(v)) for k, v in body.items()))
        # Binance defines totalParams as the encoded query string concatenated
        # with the encoded request body for signed endpoints.
        signature_payload = query + encoded_body
        params["signature"] = hmac.new(self.cfg.binance_secret.encode(), signature_payload.encode(), hashlib.sha256).hexdigest()
        url = self.cfg.binance_rest.rstrip("/") + path
        response = self.session.request(method, url, params=params, data=body, timeout=15)
        if not response.ok:
            raise RuntimeError(f"Binance {response.status_code}: {response.text[:500]}")
        return response.json()

    def get_quote(self, token_id: str, side: str, amount_in: Decimal, price_limit: Decimal, slippage_bps: int) -> dict[str, Any]:
        body = {
            "walletAddress": self.cfg.wallet_address,
            "tokenId": token_id,
            "side": side,
            "amountIn": str(int(amount_in * Decimal(10**18))),
            "orderType": "LIMIT",
            "slippageBps": slippage_bps,
            "priceLimit": str(price_limit),
            "chainId": "56",
            "feeRateBps": self.cfg.fee_bps,
            "fundingSource": self.cfg.funding_source,
        }
        return self.signed_request("POST", "/sapi/v1/w3w/wallet/prediction/trade/get-quote", body=body)

    def get_market_price_preview(self, token_id: str, amount_usdt: Decimal) -> dict[str, Any]:
        """Request an executable market quote without creating an order."""
        body = {
            "walletAddress": self.cfg.wallet_address,
            "tokenId": token_id,
            "side": "BUY",
            "amountIn": str(int(amount_usdt * Decimal(10**18))),
            "orderType": "MARKET",
            "slippageBps": self.cfg.entry_slippage,
            "chainId": "56",
            "feeRateBps": self.cfg.fee_bps,
            "fundingSource": self.cfg.funding_source,
        }
        return self.signed_request("POST", "/sapi/v1/w3w/wallet/prediction/trade/get-quote", body=body)

    def place_limit(self, quote: dict[str, Any], price_limit: Decimal, slippage_bps: int) -> str:
        body = {
            "walletAddress": self.cfg.wallet_address,
            "walletId": self.cfg.wallet_id,
            "quoteId": quote["quoteId"],
            "timeInForce": "GTC",
            "accountType": self.cfg.account_type,
            "orderType": "LIMIT",
            "slippageBps": slippage_bps,
            "priceLimit": str(price_limit),
            "fundingSource": self.cfg.funding_source,
        }
        result = self.signed_request("POST", "/sapi/v1/w3w/wallet/prediction/trade/place-order-bundle", body=body)
        return str(result["orderId"])

    def order_history(self, market_id: str) -> list[dict[str, Any]]:
        result = self.signed_request("GET", "/sapi/v1/w3w/wallet/prediction/order/history", params={"marketId": market_id, "limit": 100})
        return result.get("orders", [])


class BinanceMarketClient:
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json", "User-Agent": "prediction-market-scanner/1.0"})

    def markets(self) -> list[dict[str, Any]]:
        try:
            body = json.loads(os.getenv("BINANCE_MARKETS_BODY_JSON", "{}"))
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"BINANCE_MARKETS_BODY_JSON is invalid JSON: {exc}")
        response = self.session.post(os.getenv("BINANCE_MARKETS_URL", ""), json=body, timeout=15)
        if not response.ok:
            raise RuntimeError(f"Binance market list {response.status_code}: {response.text[:500]}")
        payload = response.json()
        data = payload.get("data", payload)
        events = data.get("events", []) if isinstance(data, dict) else data
        if not isinstance(events, list):
            return []
        # Binance returns event records containing one or more tradable
        # ungroupedMarkets. Flatten them so marketId/tokenId map directly to
        # the order-book and trading APIs.
        flattened: list[dict[str, Any]] = []
        for event in events:
            if not isinstance(event, dict):
                continue
            grouped = event.get("gameplayGroups") or []
            children: list[dict[str, Any]] = []
            if grouped:
                for group in grouped:
                    if isinstance(group, dict):
                        children.extend(x for x in (group.get("markets") or []) if isinstance(x, dict))
            else:
                children = [x for x in (event.get("ungroupedMarkets") or []) if isinstance(x, dict)]
            if not children:
                flattened.append(event)
                continue
            for child in children:
                if isinstance(child, dict):
                    flattened.append({**event, **child, "eventId": event.get("eventId"), "eventSlug": event.get("eventSlug"), "eventTitle": event.get("title")})
        return flattened

    def market(self, market_id: str) -> dict[str, Any]:
        return {"id": market_id}


def market_text(market: dict[str, Any]) -> str:
    return " ".join(
        str(market.get(k, ""))
        for k in ("title", "question", "description", "slug", "eventTitle", "eventSlug", "eventName")
    ).lower()


def outcome_token(market: dict[str, Any]) -> tuple[str, str] | None:
    outcomes = market.get("outcomes") or market.get("tokens") or market.get("outcomeTokens") or []
    if isinstance(outcomes, dict):
        outcomes = list(outcomes.values())
    for item in outcomes:
        if isinstance(item, str):
            continue
        label = str(item.get("title") or item.get("name") or item.get("outcome") or "")
        token = item.get("tokenId") or item.get("token_id") or item.get("id")
        if token is not None and label.lower() in {"yes", "no", "over", "under"}:
            return str(token), label
    if outcomes and isinstance(outcomes[0], dict):
        item = outcomes[0]
        token = item.get("tokenId") or item.get("token_id") or item.get("id")
        if token is not None:
            return str(token), str(item.get("title") or item.get("name") or "outcome")
    return None


class LiveTrader:
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.state = State()
        self.markets_client = BinanceMarketClient(cfg)
        self.binance = BinanceClient(cfg)
        self.notifier = Notifier()
        self.stop = threading.Event()
        self.books: dict[str, dict[str, Any]] = {}
        self.market_meta: dict[str, dict[str, Any]] = {}
        self.topic_queue: queue.Queue[str] = queue.Queue()
        self.subscribed_topics: set[str] = set()
        self.last_error_notification = 0.0

    def notify_error(self, title: str, message: str) -> None:
        # Avoid flooding the phone when an upstream service is unavailable.
        now = time.time()
        if now - self.last_error_notification >= 600:
            self.last_error_notification = now
            self.notifier.send(title, message[:800], priority="high", tags="warning")

    def validate(self) -> None:
        if self.cfg.live and not all((self.cfg.binance_key, self.cfg.binance_secret, self.cfg.wallet_address, self.cfg.wallet_id)):
            raise RuntimeError("Live trading requires Binance API key, secret, wallet address, and wallet ID")
        if self.cfg.buy_usdt <= 0 or self.cfg.buy_usdt > Decimal("2.00"):
            raise RuntimeError("BUY_USDT must be > 0 and <= 2.00 for this bot")
        if not (Decimal("0") < self.cfg.entry_max < Decimal("1")):
            raise RuntimeError("ENTRY_MAX_PRICE must be between 0 and 1")
        if not (self.cfg.entry_max < self.cfg.exit_min <= Decimal("1")):
            raise RuntimeError("EXIT_MIN_PRICE must be greater than ENTRY_MAX_PRICE and <= 1")
        if self.cfg.orderbook_topic_mode not in {"rest", "aggregated", "dynamic"}:
            raise RuntimeError("ORDERBOOK_TOPIC_MODE must be rest, aggregated, or dynamic")
        if self.cfg.orderbook_topic_mode == "rest" and not all((self.cfg.binance_key, self.cfg.binance_secret, self.cfg.wallet_address)):
            raise RuntimeError("REST price previews require BINANCE_API_KEY, BINANCE_API_SECRET, and BINANCE_WALLET_ADDRESS")
        if not (0 < self.cfg.ws_ping_seconds < 30):
            raise RuntimeError("WS_PING_SECONDS must be greater than 0 and less than 30")
        if self.cfg.ws_recv_timeout <= 0:
            raise RuntimeError("WS_RECV_TIMEOUT_SECONDS must be greater than 0")

    def discover_loop(self) -> None:
        while not self.stop.is_set():
            try:
                new_markets: list[str] = []
                bootstrap_candidates: list[str] = []
                is_bootstrap = not self.state.market_baseline_ready
                for market in self.markets_client.markets():
                    market_id = str(market.get("marketId") or market.get("id") or market.get("eventId") or "")
                    if not market_id or not any(k in market_text(market) for k in self.cfg.football_keywords):
                        continue
                    is_new = market_id not in self.state.seen
                    detail = market
                    token = outcome_token(detail)
                    if not token:
                        log.warning("New football market %s has no recognizable outcome token; skipped: %s", market_id, market_text(detail)[:160])
                        continue
                    self.state.seen.add(market_id)
                    self.market_meta[market_id] = {"market": detail, "token_id": token[0], "outcome": token[1]}
                    if self.cfg.orderbook_topic_mode == "dynamic" and market_id not in self.subscribed_topics:
                        self.topic_queue.put(market_id)
                        self.subscribed_topics.add(market_id)
                        log.info("TRACKING football market id=%s outcome=%s", market_id, token[1])
                    if is_bootstrap:
                        bootstrap_candidates.append(market_id)
                        continue
                    if not is_new:
                        continue
                    title = str(detail.get("title") or detail.get("question") or market_id)
                    log.info("NEW FOOTBALL MARKET id=%s outcome=%s token=%s title=%s", market_id, token[1], token[0], title)
                    new_markets.append(f"{title} | {token[1]} | id={market_id}")
                    if self.cfg.orderbook_topic_mode == "rest":
                        self.preview_market_price(market_id, "new market")
                if is_bootstrap and bootstrap_candidates:
                    # The newest Binance internal market ID is used only as a
                    # startup verification sample; it does not create an order.
                    newest_market_id = max(bootstrap_candidates, key=lambda value: int(value) if value.isdigit() else -1)
                    if self.cfg.orderbook_topic_mode == "rest":
                        self.preview_market_price(newest_market_id, "startup verification")
                    self.state.market_baseline_ready = True
                if new_markets:
                    # A market-list refresh can contain many new markets. Send
                    # one digest so ntfy is not flooded and rate-limited.
                    shown = new_markets[:20]
                    suffix = "" if len(new_markets) <= len(shown) else f"\n…and {len(new_markets) - len(shown)} more"
                    self.notifier.send(
                        f"{len(new_markets)} new Binance football market(s)",
                        "\n".join(shown) + suffix,
                        tags="soccer",
                    )
                self.state.save()
            except Exception as exc:
                log.exception("Discovery error: %s", exc)
                self.notify_error("Scanner discovery error", str(exc))
            self.stop.wait(self.cfg.market_poll)

    def preview_market_price(self, market_id: str, reason: str) -> None:
        """Log a no-order REST price preview for one discovered market."""
        meta = self.market_meta.get(market_id)
        if not meta:
            return
        try:
            self.binance.sync_time()
            quote = self.binance.get_market_price_preview(meta["token_id"], self.cfg.buy_usdt)
            title = str(meta["market"].get("title") or meta["market"].get("question") or market_id)
            log.warning(
                "REST PRICE PREVIEW reason=%s market=%s outcome=%s average=%s last=%s chance=%s title=%s",
                reason,
                market_id,
                meta["outcome"],
                quote.get("averagePrice"),
                quote.get("lastPrice"),
                quote.get("chance"),
                title,
            )
        except Exception as exc:
            log.warning("REST price preview failed reason=%s market=%s: %s", reason, market_id, exc)
            self.notify_error("Scanner REST price preview failed", str(exc))

    def ws_loop(self) -> None:
        if self.cfg.orderbook_topic_mode == "rest":
            log.info("Order-book WebSocket disabled; using REST price previews for startup and new markets")
            return
        if not self.cfg.binance_key or not self.cfg.binance_secret:
            log.warning("BINANCE credentials missing: market discovery will run, order-book/trading will not")
            return
        while not self.stop.is_set():
            try:
                if self.cfg.orderbook_topic_mode == "aggregated":
                    # One stream covers all active prediction markets. This
                    # is the correct mode for detecting newly listed markets;
                    # discovery filters incoming books to football.
                    initial_topics = ["web3_prediction_orderbook_data"]
                    log.info("Opening aggregated order-book subscription")
                else:
                    # Dynamic mode is useful when deliberately limiting the
                    # connection to a small set of market IDs.
                    if self.topic_queue.empty():
                        self.stop.wait(1)
                        continue
                    initial_topics = []
                    topic_limit = self.cfg.max_markets_per_connection if self.cfg.max_markets_per_connection > 0 else 1024
                    while len(initial_topics) < topic_limit:
                        try:
                            initial_topics.append(self.topic_queue.get_nowait())
                        except queue.Empty:
                            break
                    log.info("Opening dynamic order-book subscription for %d market(s): %s", len(initial_topics), ",".join(initial_topics))
                self.binance.sync_time()
                timestamp = self.binance.now_ms()
                # Binance's SApi WSS docs recommend a random string (<=32
                # chars) for `random`; a UUID avoids reusing timestamp-only
                # session identifiers during rapid reconnects.
                topic = "|".join(f"web3_prediction_orderbook_{market_id}" for market_id in initial_topics)
                params = {"random": uuid.uuid4().hex, "topic": topic, "recvWindow": 30000, "timestamp": timestamp}
                # Binance's prediction-orderbook documentation requires the
                # signature payload to be sorted alphabetically by parameter
                # name. The final URL can retain the same canonical order.
                query = urllib.parse.urlencode(sorted((k, str(v)) for k, v in params.items()))
                params["signature"] = hmac.new(self.cfg.binance_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
                url = self.cfg.binance_ws + "?" + urllib.parse.urlencode(sorted((k, str(v)) for k, v in params.items()))
                ws = websocket.create_connection(
                    url,
                    header=[f"X-MBX-APIKEY: {self.cfg.binance_key}"],
                    origin="https://www.binance.com",
                    timeout=10,
                )
                # `websocket.create_connection` does not run a ping thread;
                # its ping_interval argument is ignored by websocket-client.
                # Binance closes a connection that has not received a WebSocket
                # PING frame within one minute, so send one ourselves below.
                ws.settimeout(self.cfg.ws_recv_timeout)
                last_ping = time.monotonic()
                log.info("Connected to Binance prediction order-book stream; waiting for subscription response/snapshot")
                while not self.stop.is_set():
                    additional_topics: list[str] = []
                    if self.cfg.orderbook_topic_mode == "dynamic":
                        while len(additional_topics) < 1024:
                            try:
                                additional_topics.append(self.topic_queue.get_nowait())
                            except queue.Empty:
                                break
                    if additional_topics:
                        # Use the queued ID for each topic. Do not use the
                        # stale `market_id` variable from the frame parser.
                        topic_names = [f"web3_prediction_orderbook_{topic_id}" for topic_id in additional_topics]
                        log.info("Subscribing to %d additional market(s): %s", len(topic_names), ",".join(additional_topics))
                        ws.send(json.dumps({
                            # Binance CMS uses command/value, with multiple
                            # topics concatenated by |, not the Spot WS schema.
                            "command": "SUBSCRIBE",
                            "value": "|".join(topic_names),
                        }))
                    if time.monotonic() - last_ping >= self.cfg.ws_ping_seconds:
                        ws.ping()
                        last_ping = time.monotonic()
                        log.debug("Sent Binance order-book heartbeat")
                    try:
                        raw = ws.recv()
                    except websocket.WebSocketTimeoutException:
                        # A quiet market is not a dead connection; keep the
                        # socket alive and check for newly discovered topics.
                        continue
                    if not raw:
                        log.warning(
                            "Binance closed the order-book stream without a payload code=%s reason=%s",
                            getattr(ws, "close_status", None),
                            getattr(ws, "close_reason", None),
                        )
                        break
                    log.debug("Binance WS frame: %s", str(raw)[:1000])
                    for envelope in decode_json_messages(raw):
                        if str(envelope.get("type", "")).upper() == "COMMAND" or "code" in envelope or "success" in envelope:
                            log.info(
                                "Binance subscription response type=%s action=%s code=%s status=%s",
                                envelope.get("type"),
                                envelope.get("subType"),
                                envelope.get("code"),
                                envelope.get("data") or envelope.get("message") or envelope.get("success"),
                            )
                            continue
                        data = envelope.get("data", envelope)
                        if isinstance(data, str):
                            nested = decode_json_messages(data)
                            data = nested[0] if nested else {}
                        if not isinstance(data, dict):
                            continue
                        market_id = str(data.get("marketId", ""))
                        if market_id:
                            self.books[market_id] = data
                            asks = data.get("asks") or []
                            bids = data.get("bids") or []
                            log.info(
                                "Order book snapshot/update market=%s ask=%s bid=%s",
                                market_id,
                                asks[0][0] if asks else None,
                                bids[0][0] if bids else None,
                            )
                            self.evaluate(market_id, data)
                ws.close()
            except Exception as exc:
                log.warning("Order-book stream error: %s; reconnecting", exc)
                self.notify_error("Scanner Binance connection error", str(exc))
                self.stop.wait(5)
            else:
                self.stop.wait(5)

    def evaluate(self, market_id: str, book: dict[str, Any]) -> None:
        meta = self.market_meta.get(market_id)
        if not meta:
            return
        asks = book.get("asks") or []
        bids = book.get("bids") or []
        best_ask = dec(asks[0][0]) if asks else None
        best_bid = dec(bids[0][0]) if bids else None
        log.debug("BOOK market=%s ask=%s bid=%s", market_id, best_ask, best_bid)
        if best_ask is not None and best_ask <= self.cfg.entry_max:
            self.try_buy(market_id, meta, best_ask)
        position = self.state.positions.get(market_id)
        if position and position.status == "FILLED" and best_bid is not None and best_bid >= self.cfg.exit_min:
            self.try_sell(market_id, position, best_bid)

    def try_buy(self, market_id: str, meta: dict[str, Any], price: Decimal) -> None:
        if self.state.orders_per_market.get(market_id, 0) >= self.cfg.max_per_market:
            return
        if len([p for p in self.state.positions.values() if p.status in {"BUY_PENDING", "FILLED", "SELL_PENDING"}]) >= self.cfg.max_positions:
            return
        if not self.cfg.live:
            log.warning("SIGNAL ONLY live=false BUY market=%s token=%s price=%s amount=%s", market_id, meta["token_id"], price, self.cfg.buy_usdt)
            self.notifier.send(
                "Binance entry signal",
                f"Market: {meta['market'].get('title') or meta['market'].get('question') or market_id}\nAsk: {price}\nAmount: {self.cfg.buy_usdt} USDT",
                priority="high",
                tags="chart_with_upwards_trend",
            )
            self.state.orders_per_market[market_id] = self.cfg.max_per_market
            return
        try:
            quote = self.binance.get_quote(meta["token_id"], "BUY", self.cfg.buy_usdt, price, self.cfg.entry_slippage)
            order_id = self.binance.place_limit(quote, price, self.cfg.entry_slippage)
            self.state.positions[market_id] = Position(market_id, meta["token_id"], str(meta["market"].get("title") or meta["market"].get("question") or market_id), buy_order_id=order_id, buy_price=str(price), created_at=time.time())
            self.state.orders_per_market[market_id] = self.state.orders_per_market.get(market_id, 0) + 1
            self.state.save()
            log.warning("LIVE BUY submitted market=%s order=%s price=%s amount=%s", market_id, order_id, price, self.cfg.buy_usdt)
            self.notifier.send("LIVE BUY submitted", f"Market: {market_id}\nPrice: {price}\nAmount: {self.cfg.buy_usdt} USDT", priority="high", tags="moneybag")
        except Exception as exc:
            log.exception("BUY failed market=%s: %s", market_id, exc)

    def try_sell(self, market_id: str, position: Position, price: Decimal) -> None:
        shares = dec(position.filled_shares, Decimal("0")) or Decimal("0")
        if shares <= 0 or position.sell_order_id:
            return
        if not self.cfg.live:
            log.warning("SIGNAL ONLY live=false SELL market=%s price=%s shares=%s", market_id, price, shares)
            self.notifier.send("Binance exit signal", f"Market: {market_id}\nBid: {price}\nShares: {shares}", priority="high", tags="moneybag")
            return
        try:
            quote = self.binance.get_quote(position.token_id, "SELL", shares, price, self.cfg.exit_slippage)
            order_id = self.binance.place_limit(quote, price, self.cfg.exit_slippage)
            position.sell_order_id = order_id
            position.status = "SELL_PENDING"
            self.state.save()
            log.warning("LIVE SELL submitted market=%s order=%s price=%s shares=%s", market_id, order_id, price, shares)
            self.notifier.send("LIVE SELL submitted", f"Market: {market_id}\nPrice: {price}\nShares: {shares}", priority="high", tags="moneybag")
        except Exception as exc:
            log.exception("SELL failed market=%s: %s", market_id, exc)

    def reconcile_loop(self) -> None:
        while not self.stop.is_set():
            if self.cfg.live:
                for market_id, position in list(self.state.positions.items()):
                    try:
                        orders = self.binance.order_history(market_id)
                        for order in orders:
                            order_id = str(order.get("orderId", ""))
                            if order_id == position.buy_order_id:
                                position.filled_shares = str(order.get("filledShareQty") or "0")
                                if dec(position.filled_shares, Decimal("0")) > 0:
                                    position.status = "FILLED"
                            if order_id == position.sell_order_id and str(order.get("status", "")).upper() in {"CLOSED", "FILLED", "SUCCESS"}:
                                position.status = "CLOSED"
                        if position.status == "BUY_PENDING" and time.time() - position.created_at > self.cfg.stale_seconds:
                            log.warning("Stale BUY order market=%s order=%s; cancel manually or add a cancel flow after verifying endpoint behavior", market_id, position.buy_order_id)
                        self.state.save()
                    except Exception as exc:
                        log.warning("Reconciliation failed market=%s: %s", market_id, exc)
            self.stop.wait(self.cfg.order_poll)

    def run(self) -> None:
        self.validate()
        log.warning(
            "START live=%s buy_usdt=%s entry<=%s exit>=%s orderbook_mode=%s",
            self.cfg.live,
            self.cfg.buy_usdt,
            self.cfg.entry_max,
            self.cfg.exit_min,
            self.cfg.orderbook_topic_mode,
        )
        threads = [threading.Thread(target=self.discover_loop, daemon=True), threading.Thread(target=self.ws_loop, daemon=True), threading.Thread(target=self.reconcile_loop, daemon=True)]
        for thread in threads:
            thread.start()
        try:
            while not self.stop.wait(1):
                pass
        except KeyboardInterrupt:
            pass
        finally:
            self.stop.set()
            self.state.save()
            log.warning("STOPPED")


def main() -> None:
    trader = LiveTrader(Config())
    signal.signal(signal.SIGINT, lambda *_: trader.stop.set())
    signal.signal(signal.SIGTERM, lambda *_: trader.stop.set())
    trader.run()


if __name__ == "__main__":
    main()
