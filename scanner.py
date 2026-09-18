from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import queue
import re
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
    market_scope: str = os.getenv("MARKET_SCOPE", "football").strip().lower()
    soccer_min_corner_line: Decimal = env_decimal("SOCCER_MIN_CORNER_LINE", "7.5")
    max_live_entries: int = int(os.getenv("MAX_LIVE_ENTRIES", "1"))
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
    updated_at: float = 0


class State:
    def __init__(self) -> None:
        self.path = DATA_DIR / "state.json"
        self.seen: set[str] = set()
        self.market_baseline_ready = False
        self.market_baseline_scope = ""
        self.market_baseline_started_at_ms = 0
        self.live_entries_submitted = 0
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
            self.market_baseline_scope = str(raw.get("market_baseline_scope", ""))
            self.market_baseline_started_at_ms = int(raw.get("market_baseline_started_at_ms", 0) or 0)
            self.live_entries_submitted = int(raw.get("live_entries_submitted", 0) or 0)
            self.orders_per_market = {str(k): int(v) for k, v in raw.get("orders_per_market", {}).items()}
            self.positions = {k: Position(**v) for k, v in raw.get("positions", {}).items()}
        except Exception as exc:
            log.warning("Could not load state: %s", exc)

    def save(self) -> None:
        with self.lock:
            payload = {
                "seen": sorted(self.seen),
                "market_baseline_ready": self.market_baseline_ready,
                "market_baseline_scope": self.market_baseline_scope,
                "market_baseline_started_at_ms": self.market_baseline_started_at_ms,
                "live_entries_submitted": self.live_entries_submitted,
                "orders_per_market": self.orders_per_market,
                "positions": {k: asdict(v) for k, v in self.positions.items()},
            }
            self.path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def prepare_discovery_scope(self, scope: str) -> bool:
        """Require a full no-trade baseline whenever discovery scope changes."""
        if self.market_baseline_scope == scope and self.market_baseline_started_at_ms > 0:
            return False
        self.market_baseline_scope = scope
        self.market_baseline_ready = False
        # `publishAt` values from Binance are epoch milliseconds. Persisting
        # this cutoff means an already-created event returned late by a
        # changing/paginated feed cannot become eligible after the baseline.
        self.market_baseline_started_at_ms = int(time.time() * 1000)
        self.save()
        return True


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

    def signed_request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
        raw_body: str | None = None,
    ) -> dict[str, Any]:
        params = dict(params or {})
        params.setdefault("timestamp", self.now_ms())
        params.setdefault("recvWindow", 5000)
        body = dict(body or {})
        if body and raw_body is not None:
            raise ValueError("Provide either body or raw_body, not both")
        # Binance validates the *raw* query string plus the raw form body.
        # Build each once and reuse the same encoded strings for signing and
        # transport. Passing dictionaries to requests is unsafe here because
        # requests can serialize their key order differently from the signed
        # representation, resulting in error -1022.
        query = urllib.parse.urlencode(sorted((k, str(v)) for k, v in params.items()))
        encoded_body = raw_body if raw_body is not None else urllib.parse.urlencode(sorted((k, str(v)) for k, v in body.items()))
        signature_payload = query + encoded_body
        signature = hmac.new(self.cfg.binance_secret.encode(), signature_payload.encode("utf-8"), hashlib.sha256).hexdigest()
        signed_query = f"{query}&signature={signature}" if query else f"signature={signature}"
        url = self.cfg.binance_rest.rstrip("/") + path + "?" + signed_query
        response = self.session.request(method, url, data=encoded_body or None, timeout=15)
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
        }
        return self.signed_request("POST", "/sapi/v1/w3w/wallet/prediction/trade/get-quote", body=body)

    def get_market_quote(self, token_id: str, amount_usdt: Decimal) -> dict[str, Any]:
        """Get an executable BUY quote. This endpoint does not place an order."""
        # These are the required Get Quote fields from Binance's Prediction
        # Trading REST documentation. Keep optional fields out of this
        # preflight so it validates the account/wallet setup unambiguously.
        body = {
            "walletAddress": self.cfg.wallet_address,
            "tokenId": token_id,
            "side": "BUY",
            "amountIn": str(int(amount_usdt * Decimal(10**18))),
            "orderType": "MARKET",
            "slippageBps": self.cfg.entry_slippage,
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
        result = self.signed_request(
            "GET",
            "/sapi/v1/w3w/wallet/prediction/order/history",
            # Binance documents marketId filtering for active orders, but not
            # order history. Fetch the recent documented history page and
            # match our known order IDs locally.
            params={"walletAddress": self.cfg.wallet_address, "limit": 100},
        )
        return result.get("orders", [])

    def active_orders(self, market_id: str) -> list[dict[str, Any]]:
        result = self.signed_request(
            "GET",
            "/sapi/v1/w3w/wallet/prediction/order/list",
            params={"walletAddress": self.cfg.wallet_address, "marketId": market_id, "limit": 100},
        )
        return result.get("orders", [])

    def positions(self) -> list[dict[str, Any]]:
        result = self.signed_request(
            "GET",
            "/sapi/v1/w3w/wallet/prediction/position/list",
            params={"walletAddress": self.cfg.wallet_address, "tab": "ONGOING", "limit": 100},
        )
        return result.get("positions", [])

    def order_book(self, market_id: str, token_id: str, vendor: str = "predict_fun") -> dict[str, Any]:
        return self.signed_request(
            "GET",
            "/sapi/v1/w3w/wallet/prediction/order-book",
            params={"vendor": vendor, "marketId": market_id, "tokenId": token_id},
        )

    def cancel_order(self, order_id: str) -> bool:
        # Binance documents a bracket-encoding incompatibility for this
        # endpoint: square brackets must stay literal in both the signed and
        # transmitted form body. Values remain URL encoded.
        raw_body = "&".join(
            (
                "walletAddress=" + urllib.parse.quote_plus(self.cfg.wallet_address),
                "walletId=" + urllib.parse.quote_plus(self.cfg.wallet_id),
                "cancelInfoList[0].orderId=" + urllib.parse.quote_plus(order_id),
            )
        )
        result = self.signed_request(
            "POST",
            "/sapi/v1/w3w/wallet/prediction/trade/batch-cancel",
            raw_body=raw_body,
        )
        return order_id in {str(value) for value in result.get("canceled", [])}


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
        for k in ("title", "marketTitle", "question", "description", "slug", "eventTitle", "eventSlug", "eventName", "outcomeName")
    ).lower()


def market_is_in_scope(market: dict[str, Any], cfg: Config) -> bool:
    """Apply the intentionally narrow discovery scope configured by the user."""
    if cfg.market_scope == "all":
        return True
    if cfg.market_scope == "soccer":
        category_text = " ".join(
            str(market.get(key, ""))
            for key in ("l1Category", "l2Category", "category", "sport", "sportType", "eventCategory")
        ).lower()
        text = f"{category_text} {market_text(market)}"
        return "soccer" in text and "american football" not in text
    return any(keyword in market_text(market) for keyword in cfg.football_keywords)


def market_publish_at_ms(market: dict[str, Any]) -> int:
    """Return Binance's event publication timestamp, or zero when absent."""
    return int(dec(market.get("publishAt"), Decimal("0")) or Decimal("0"))


def market_is_newer_than_baseline(market: dict[str, Any], baseline_started_at_ms: int) -> bool:
    """Only an event published after the baseline is eligible for entry."""
    return market_publish_at_ms(market) > baseline_started_at_ms


def outcome_tokens(market: dict[str, Any]) -> list[tuple[str, str]]:
    outcomes = market.get("outcomes") or market.get("tokens") or market.get("outcomeTokens") or []
    if isinstance(outcomes, dict):
        outcomes = list(outcomes.values())
    parsed: list[tuple[str, str]] = []
    for item in outcomes:
        if not isinstance(item, dict):
            continue
        label = str(item.get("title") or item.get("name") or item.get("outcome") or "")
        token = item.get("tokenId") or item.get("token_id") or item.get("id")
        if token is not None:
            parsed.append((str(token), label))
    return parsed


def outcome_token(market: dict[str, Any]) -> tuple[str, str] | None:
    """Backward-compatible primary outcome selection for generic scopes."""
    tokens = outcome_tokens(market)
    for token, label in tokens:
        if label.lower() in {"yes", "no", "over", "under"}:
            return token, label
    return tokens[0] if tokens else None


def market_kind(market: dict[str, Any]) -> str:
    text = market_text(market)
    if "total corners" in text:
        return "total_corners"
    if "second half result" in text:
        return "second_half_result"
    return "other"


def corner_line(market: dict[str, Any]) -> Decimal | None:
    match = re.search(r"(?:o/u|over\s*[/ ]?under)\s*(\d+(?:\.\d+)?)", market_text(market))
    return dec(match.group(1)) if match else None


def is_over_outcome(market: dict[str, Any], label: str) -> bool:
    """Only trade an explicit Over outcome; never infer an unknown side."""
    return label.strip().lower().startswith("over") or str(market.get("title") or "").strip().lower().startswith("over")


def outcome_public_price(market: dict[str, Any], token_id: str) -> Decimal | None:
    """Return the public REST price for the selected outcome token, if present."""
    outcomes = market.get("outcomes") or market.get("tokens") or market.get("outcomeTokens") or []
    if isinstance(outcomes, dict):
        outcomes = list(outcomes.values())
    for item in outcomes:
        if not isinstance(item, dict):
            continue
        item_token = item.get("tokenId") or item.get("token_id") or item.get("id")
        if str(item_token) == token_id:
            return dec(item.get("price"))
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
        self.startup_price_checked = False
        self.soccer_research_path = LOG_DIR / "soccer_research.jsonl"
        if self.state.prepare_discovery_scope(cfg.market_scope):
            log.warning("Discovery scope changed to %s; establishing a no-trade baseline", cfg.market_scope)

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
        if self.cfg.market_scope not in {"football", "soccer", "all"}:
            raise RuntimeError("MARKET_SCOPE must be football, soccer, or all")
        if self.cfg.market_scope == "all" and self.cfg.live:
            if self.cfg.max_positions != 1 or self.cfg.max_per_market != 1:
                raise RuntimeError("All-category live scanning requires MAX_OPEN_POSITIONS=1 and MAX_ORDERS_PER_MARKET=1")
        if self.cfg.orderbook_topic_mode not in {"rest", "aggregated", "dynamic"}:
            raise RuntimeError("ORDERBOOK_TOPIC_MODE must be rest, aggregated, or dynamic")
        if not (0 < self.cfg.ws_ping_seconds < 30):
            raise RuntimeError("WS_PING_SECONDS must be greater than 0 and less than 30")
        if self.cfg.ws_recv_timeout <= 0:
            raise RuntimeError("WS_RECV_TIMEOUT_SECONDS must be greater than 0")
        if self.cfg.soccer_min_corner_line < Decimal("7.5"):
            raise RuntimeError("SOCCER_MIN_CORNER_LINE must be at least 7.5")
        if self.cfg.max_live_entries != 1:
            raise RuntimeError("MAX_LIVE_ENTRIES must be exactly 1 for this bot")

    def log_soccer_research(self, market_id: str, market: dict[str, Any], kind: str, tokens: list[tuple[str, str]], is_new: bool) -> None:
        """Persist every discovered research market with its raw token books."""
        if not is_new or kind not in {"total_corners", "second_half_result"}:
            return
        outcomes: list[dict[str, Any]] = []
        for token_id, label in tokens:
            item: dict[str, Any] = {"token_id": token_id, "label": label, "public_price": str(outcome_public_price(market, token_id) or "")}
            if self.cfg.binance_key and self.cfg.binance_secret:
                try:
                    book = self.binance.order_book(market_id, token_id, str(market.get("vendor") or "predict_fun").lower())
                    item["order_book"] = book
                except Exception as exc:
                    item["order_book_error"] = str(exc)
            outcomes.append(item)
        record = {
            "timestamp_ms": int(time.time() * 1000),
            "market_id": market_id,
            "event_id": market.get("eventId"),
            "event_slug": market.get("eventSlug"),
            "event_title": market.get("eventTitle"),
            "kind": kind,
            "corner_line": str(corner_line(market) or ""),
            "outcomes": outcomes,
            "raw_market": market,
        }
        with self.soccer_research_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, default=str, separators=(",", ":")) + "\n")
        log.info("SOCCER RESEARCH kind=%s market=%s event=%s outcomes=%s detail_log=%s", kind, market_id, market.get("eventTitle") or market.get("title"), len(outcomes), self.soccer_research_path)

    def discover_loop(self) -> None:
        while not self.stop.is_set():
            try:
                new_markets: list[str] = []
                new_soccer_events: dict[str, set[str]] = {}
                current_candidates: list[str] = []
                is_bootstrap = not self.state.market_baseline_ready
                for market in self.markets_client.markets():
                    market_id = str(market.get("marketId") or market.get("id") or market.get("eventId") or "")
                    if not market_id or not market_is_in_scope(market, self.cfg):
                        continue
                    is_new = market_id not in self.state.seen
                    published_at = market_publish_at_ms(market)
                    eligible_for_entry = (
                        is_new
                        and not is_bootstrap
                        and market_is_newer_than_baseline(market, self.state.market_baseline_started_at_ms)
                    )
                    detail = market
                    kind = market_kind(detail)
                    tokens = outcome_tokens(detail)
                    if not tokens:
                        log.warning("New in-scope market %s has no recognizable outcome token; skipped: %s", market_id, market_text(detail)[:160])
                        continue
                    self.log_soccer_research(market_id, detail, kind, tokens, is_new)
                    trade_token: tuple[str, str] | None = outcome_token(detail)
                    trade_eligible = True
                    if self.cfg.market_scope == "soccer":
                        line = corner_line(detail)
                        trade_token = next(
                            ((token_id, label) for token_id, label in tokens if is_over_outcome(detail, label)),
                            None,
                        )
                        trade_eligible = kind == "total_corners" and line is not None and line >= self.cfg.soccer_min_corner_line and trade_token is not None
                    self.state.seen.add(market_id)
                    public_price = outcome_public_price(detail, trade_token[0]) if trade_token else None
                    self.market_meta[market_id] = {
                        "market": detail,
                        "token_id": trade_token[0] if trade_token else "",
                        "outcome": trade_token[1] if trade_token else "",
                        "vendor": str(detail.get("vendor") or "predict_fun").lower(),
                        "public_price": str(public_price) if public_price is not None else "",
                        "trade_eligible": trade_eligible,
                        "kind": kind,
                    }
                    current_candidates.append(market_id)
                    if self.cfg.orderbook_topic_mode == "rest" and trade_eligible:
                        self.evaluate_rest_order_book(market_id, eligible_for_entry)
                    if self.cfg.orderbook_topic_mode == "dynamic" and market_id not in self.subscribed_topics:
                        self.topic_queue.put(market_id)
                        self.subscribed_topics.add(market_id)
                        log.info("TRACKING in-scope market id=%s outcome=%s", market_id, trade_token[1] if trade_token else None)
                    if is_bootstrap:
                        continue
                    if not is_new:
                        continue
                    title = str(detail.get("title") or detail.get("question") or market_id)
                    if not eligible_for_entry and not is_bootstrap:
                        log.info(
                            "LATE DISCOVERY scope=%s id=%s publish_at=%s baseline_at=%s; alerting but not tradable",
                            self.cfg.market_scope,
                            market_id,
                            published_at or None,
                            self.state.market_baseline_started_at_ms,
                        )
                    log.info(
                        "NEW MARKET scope=%s eligible=%s id=%s outcome=%s token=%s title=%s",
                        self.cfg.market_scope,
                        eligible_for_entry and trade_eligible,
                        market_id,
                        trade_token[1] if trade_token else None,
                        trade_token[0] if trade_token else None,
                        title,
                    )
                    new_markets.append(f"{title} | {trade_token[1] if trade_token else 'research only'} | price={public_price} | id={market_id}")
                    if self.cfg.market_scope == "soccer" and kind in {"total_corners", "second_half_result"} and market_is_newer_than_baseline(detail, self.state.market_baseline_started_at_ms):
                        event_key = str(detail.get("eventId") or detail.get("eventSlug") or detail.get("eventTitle") or title)
                        new_soccer_events.setdefault(event_key, set()).add(kind)
                    if self.cfg.orderbook_topic_mode == "rest":
                        self.log_public_market_price(market_id, "new market discovery")
                if not self.startup_price_checked and current_candidates:
                    # The newest Binance internal market ID is used only as a
                    # startup verification sample; it does not create an
                    # order. This deliberately runs on every process start,
                    # even when the persisted discovery baseline exists.
                    newest_market_id = max(current_candidates, key=lambda value: int(value) if value.isdigit() else -1)
                    if self.cfg.orderbook_topic_mode == "rest":
                        self.log_public_market_price(newest_market_id, "startup verification")
                    self.startup_price_checked = True
                if is_bootstrap:
                    self.state.market_baseline_ready = True
                if self.cfg.orderbook_topic_mode == "rest":
                    # A tracked position can disappear from the discovery feed
                    # while its order book remains tradable. Exit checks use
                    # the documented token-level REST book directly.
                    for position in list(self.state.positions.values()):
                        if position.status == "FILLED":
                            self.evaluate_rest_exit(position)
                if self.cfg.market_scope == "soccer" and new_soccer_events:
                    corners = sum("total_corners" in kinds for kinds in new_soccer_events.values())
                    second_half = sum("second_half_result" in kinds for kinds in new_soccer_events.values())
                    self.notifier.send(
                        "New soccer items added",
                        f"Matches: {len(new_soccer_events)}\nTotal Corners: {corners}\nSecond Half Result: {second_half}\nFull details: {self.soccer_research_path.name}",
                        tags="soccer",
                    )
                elif new_markets and self.cfg.market_scope != "soccer":
                    # A market-list refresh can contain many new markets. Send
                    # one digest so ntfy is not flooded and rate-limited.
                    shown = new_markets[:20]
                    suffix = "" if len(new_markets) <= len(shown) else f"\n…and {len(new_markets) - len(shown)} more"
                    self.notifier.send(
                        f"{len(new_markets)} new Binance market(s)",
                        "\n".join(shown) + suffix,
                        tags="chart_with_upwards_trend",
                    )
                self.state.save()
            except Exception as exc:
                log.exception("Discovery error: %s", exc)
                self.notify_error("Scanner discovery error", str(exc))
            self.stop.wait(self.cfg.market_poll)

    def log_public_market_price(self, market_id: str, reason: str) -> None:
        """Log price already returned by Binance's public market-list REST call."""
        meta = self.market_meta.get(market_id)
        if not meta:
            return
        title = str(meta["market"].get("title") or meta["market"].get("question") or market_id)
        price = meta.get("public_price") or None
        if price is None:
            log.warning("PUBLIC REST PRICE unavailable reason=%s market=%s outcome=%s title=%s", reason, market_id, meta["outcome"], title)
            return
        log.warning(
            "PUBLIC REST PRICE reason=%s market=%s outcome=%s price=%s title=%s",
            reason,
            market_id,
            meta["outcome"],
            price,
            title,
        )

    @staticmethod
    def best_level(levels: Any) -> tuple[Decimal, Decimal] | None:
        """Read the first price/size level from REST or WebSocket book data."""
        if not isinstance(levels, list) or not levels:
            return None
        level = levels[0]
        if isinstance(level, dict):
            price, size = dec(level.get("price")), dec(level.get("size"))
        elif isinstance(level, (list, tuple)) and len(level) >= 2:
            price, size = dec(level[0]), dec(level[1])
        else:
            return None
        return (price, size) if price is not None and size is not None else None

    @staticmethod
    def depth_at_or_above(levels: Any, floor: Decimal) -> Decimal:
        """Sum available bid size that can fill a sell at the requested floor."""
        if not isinstance(levels, list):
            return Decimal("0")
        total = Decimal("0")
        for level in levels:
            if isinstance(level, dict):
                price, size = dec(level.get("price")), dec(level.get("size"))
            elif isinstance(level, (list, tuple)) and len(level) >= 2:
                price, size = dec(level[0]), dec(level[1])
            else:
                continue
            if price is not None and size is not None and price >= floor and size > 0:
                total += size
        return total

    @staticmethod
    def buy_notional_at_or_below(levels: Any, ceiling: Decimal) -> Decimal:
        """Sum executable USDT across ask levels without exceeding a buy cap."""
        if not isinstance(levels, list):
            return Decimal("0")
        total = Decimal("0")
        for level in levels:
            if isinstance(level, dict):
                price, size = dec(level.get("price")), dec(level.get("size"))
            elif isinstance(level, (list, tuple)) and len(level) >= 2:
                price, size = dec(level[0]), dec(level[1])
            else:
                continue
            if price is not None and size is not None and Decimal("0") < price <= ceiling and size > 0:
                total += price * size
        return total

    def evaluate_rest_order_book(self, market_id: str, is_new: bool) -> None:
        """Use Binance's signed REST book for new-entry execution checks."""
        meta = self.market_meta.get(market_id)
        if not meta or not meta.get("trade_eligible"):
            return
        if not is_new:
            return
        try:
            book = self.binance.order_book(market_id, meta["token_id"], meta["vendor"])
            best_ask = self.best_level(book.get("asks"))
            if best_ask is None:
                return
            ask_price, ask_size = best_ask
            executable_notional = self.buy_notional_at_or_below(book.get("asks"), self.cfg.entry_max)
            if ask_price <= self.cfg.entry_max and executable_notional >= self.cfg.buy_usdt:
                log.warning("ENTRY SIGNAL best_ask=%s top_size=%s executable_usdt=%s market=%s limit=%s", ask_price, ask_size, executable_notional, market_id, self.cfg.entry_max)
                self.try_buy(market_id, meta, ask_price)
        except Exception as exc:
            log.warning("REST order-book entry check failed market=%s: %s", market_id, exc)

    def evaluate_rest_exit(self, position: Position) -> None:
        """Sell only into a fresh documented REST best bid with enough size."""
        shares = dec(position.filled_shares, Decimal("0")) or Decimal("0")
        if shares <= 0 or position.sell_order_id:
            return
        try:
            book = self.binance.order_book(position.market_id, position.token_id)
            best_bid = self.best_level(book.get("bids"))
            if best_bid is None:
                return
            bid_price, bid_size = best_bid
            executable_depth = self.depth_at_or_above(book.get("bids"), self.cfg.exit_min)
            if bid_price >= self.cfg.exit_min and executable_depth >= shares:
                log.warning("EXIT SIGNAL best_bid=%s top_size=%s depth=%s market=%s limit=%s", bid_price, bid_size, executable_depth, position.market_id, self.cfg.exit_min)
                # A sell limit at the configured floor can receive a better
                # fill than the current bid while never accepting less.
                self.try_sell(position.market_id, position, self.cfg.exit_min)
        except Exception as exc:
            log.warning("REST order-book exit check failed market=%s: %s", position.market_id, exc)

    def ws_loop(self) -> None:
        if self.cfg.orderbook_topic_mode == "rest":
            log.info("Order-book WebSocket disabled; using public REST market prices for startup and new markets")
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
        if not meta.get("trade_eligible", True):
            return
        if self.state.orders_per_market.get(market_id, 0) >= self.cfg.max_per_market:
            return
        if self.cfg.live and self.state.live_entries_submitted >= self.cfg.max_live_entries:
            log.info("Live-entry cap reached (%s); ignoring market=%s", self.cfg.max_live_entries, market_id)
            return
        if len([p for p in self.state.positions.values() if p.status in {"BUY_PENDING", "FILLED", "SELL_PENDING"}]) >= self.cfg.max_positions:
            return
        if not self.cfg.live:
            log.warning("SIGNAL ONLY live=false BUY market=%s token=%s price=%s amount=%s", market_id, meta["token_id"], price, self.cfg.buy_usdt)
            return
        try:
            quote = self.binance.get_quote(meta["token_id"], "BUY", self.cfg.buy_usdt, price, self.cfg.entry_slippage)
            order_id = self.binance.place_limit(quote, price, self.cfg.entry_slippage)
            now = time.time()
            self.state.positions[market_id] = Position(
                market_id,
                meta["token_id"],
                str(meta["market"].get("eventTitle") or meta["market"].get("title") or meta["market"].get("question") or market_id),
                buy_order_id=order_id,
                buy_price=str(price),
                created_at=now,
                updated_at=now,
            )
            self.state.orders_per_market[market_id] = self.state.orders_per_market.get(market_id, 0) + 1
            self.state.live_entries_submitted += 1
            self.state.save()
            log.warning("LIVE BUY submitted market=%s order=%s price=%s amount=%s", market_id, order_id, price, self.cfg.buy_usdt)
            self.notifier.send("Live soccer order submitted", f"Market: {meta['market'].get('eventTitle') or meta['market'].get('title') or market_id}\nOutcome: {meta['outcome']}\nLimit: {price}\nAmount: {self.cfg.buy_usdt} USDT\nOrder: {order_id}", priority="high", tags="moneybag")
        except Exception as exc:
            log.exception("BUY failed market=%s: %s", market_id, exc)

    def try_sell(self, market_id: str, position: Position, price: Decimal) -> None:
        shares = dec(position.filled_shares, Decimal("0")) or Decimal("0")
        if shares <= 0 or position.sell_order_id:
            return
        if not self.cfg.live:
            log.warning("SIGNAL ONLY live=false SELL market=%s price=%s shares=%s", market_id, price, shares)
            return
        try:
            quote = self.binance.get_quote(position.token_id, "SELL", shares, price, self.cfg.exit_slippage)
            order_id = self.binance.place_limit(quote, price, self.cfg.exit_slippage)
            position.sell_order_id = order_id
            position.status = "SELL_PENDING"
            position.updated_at = time.time()
            self.state.save()
            log.warning("LIVE SELL submitted market=%s order=%s price=%s shares=%s", market_id, order_id, price, shares)
            self.notifier.send("LIVE SELL submitted", f"Market: {market_id}\nPrice: {price}\nShares: {shares}", priority="high", tags="moneybag")
        except Exception as exc:
            log.exception("SELL failed market=%s: %s", market_id, exc)

    @staticmethod
    def filled_usdt_amount(order: dict[str, Any]) -> Decimal | None:
        for key in ("filledUsdtAmount", "filledAmount", "makerUsdtAmount", "amount"):
            value = dec(order.get(key))
            if value is not None and value > 0:
                return value
        return None

    def notify_closed_position(self, market_id: str, position: Position) -> None:
        """Send a single closure notification with actual historical amounts."""
        try:
            history = self.binance.order_history(market_id)
            by_id = {str(item.get("orderId", "")): item for item in history}
            bought = self.filled_usdt_amount(by_id.get(position.buy_order_id, {}))
            sold = self.filled_usdt_amount(by_id.get(position.sell_order_id, {}))
            if bought is not None and sold is not None:
                pnl = sold - bought
                roi = (pnl / bought * Decimal("100")) if bought else Decimal("0")
                detail = f"Buy: {bought} USDT\nSell: {sold} USDT\nGross P/L: {pnl:.8f} USDT ({roi:.2f}%)"
            else:
                detail = "Historical fill amounts were unavailable; check Binance Order History."
            log.warning("LIVE SELL closed market=%s order=%s %s", market_id, position.sell_order_id, detail.replace("\n", " | "))
            self.notifier.send("Live soccer position closed", f"Market: {position.title}\nShares: {position.filled_shares}\n{detail}", priority="high", tags="white_check_mark")
        except Exception as exc:
            log.warning("Could not calculate closure P/L market=%s: %s", market_id, exc)
            self.notifier.send("Live soccer position closed", f"Market: {position.title}\nShares: {position.filled_shares}\nCheck Binance Order History for final P/L.", priority="high", tags="white_check_mark")

    def reconcile_loop(self) -> None:
        while not self.stop.is_set():
            if self.cfg.live:
                try:
                    live_positions = self.binance.positions()
                except Exception as exc:
                    log.warning("Position reconciliation failed: %s", exc)
                    self.stop.wait(self.cfg.order_poll)
                    continue
                for market_id, position in list(self.state.positions.items()):
                    try:
                        active_orders = self.binance.active_orders(market_id)
                        active_ids = {str(order.get("orderId", "")) for order in active_orders}
                        matching_position = next(
                            (
                                item for item in live_positions
                                if str(item.get("tokenId", "")) == position.token_id
                                and str(item.get("marketId", "")) == market_id
                            ),
                            None,
                        )
                        onchain_shares = dec((matching_position or {}).get("shares"), Decimal("0")) or Decimal("0")

                        if position.status == "BUY_PENDING":
                            if onchain_shares > 0:
                                # Do not leave a partially filled GTC buy open:
                                # otherwise later fills can create unmanaged shares
                                # after the first filled quantity is sold.
                                if position.buy_order_id in active_ids:
                                    if not self.binance.cancel_order(position.buy_order_id):
                                        log.warning("Partial BUY cancel not confirmed market=%s order=%s; holding exit", market_id, position.buy_order_id)
                                        continue
                                    log.warning("Partial BUY cancelled market=%s order=%s", market_id, position.buy_order_id)
                                position.filled_shares = str(onchain_shares)
                                position.status = "FILLED"
                                position.updated_at = time.time()
                                self.notifier.send(
                                    "Live soccer order filled",
                                    f"Market: {position.title}\nShares: {onchain_shares}\nEntry limit: {position.buy_price}\nOrder: {position.buy_order_id}",
                                    priority="high",
                                    tags="white_check_mark",
                                )
                            elif time.time() - position.created_at > self.cfg.stale_seconds:
                                if position.buy_order_id in active_ids and self.binance.cancel_order(position.buy_order_id):
                                    position.status = "CANCELLED"
                                    position.updated_at = time.time()
                                    log.warning("Stale BUY cancelled market=%s order=%s", market_id, position.buy_order_id)
                                    self.notifier.send("LIVE BUY cancelled", f"Market: {market_id}\nOrder: {position.buy_order_id}\nReason: stale", priority="high", tags="warning")

                        elif position.status == "SELL_PENDING":
                            if onchain_shares <= 0 and position.sell_order_id not in active_ids:
                                position.status = "CLOSED"
                                position.updated_at = time.time()
                                self.notify_closed_position(market_id, position)
                            elif time.time() - position.updated_at > self.cfg.stale_seconds and position.sell_order_id in active_ids:
                                if self.binance.cancel_order(position.sell_order_id):
                                    position.sell_order_id = ""
                                    position.filled_shares = str(onchain_shares)
                                    position.status = "FILLED"
                                    position.updated_at = time.time()
                                    log.warning("Stale SELL cancelled market=%s", market_id)

                        self.state.save()
                    except Exception as exc:
                        log.warning("Reconciliation failed market=%s: %s", market_id, exc)
            self.stop.wait(self.cfg.order_poll)

    def run(self) -> None:
        self.validate()
        log.warning(
            "START live=%s buy_usdt=%s entry<=%s exit>=%s scope=%s orderbook_mode=%s",
            self.cfg.live,
            self.cfg.buy_usdt,
            self.cfg.entry_max,
            self.cfg.exit_min,
            self.cfg.market_scope,
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
