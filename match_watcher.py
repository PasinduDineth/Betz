"""Watcher for one future Binance Prediction Market match result.

By default this service is read-only.  When explicitly opted in with
MATCH_WATCHER_LIVE_TRADING=true, it submits exactly one capped GTC limit-buy
for the configured target outcome after the exact market is found.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import signal
import threading
import time
import urllib.parse
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv


load_dotenv()

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
LOG_DIR = ROOT / "logs"
WATCH_DIR = LOG_DIR / "match_watcher"
for directory in (DATA_DIR, LOG_DIR, WATCH_DIR):
    directory.mkdir(exist_ok=True)

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler(LOG_DIR / "match_watcher.log", encoding="utf-8")],
)
log = logging.getLogger("match-watcher")


def number(value: Any, default: Decimal | None = None) -> Decimal | None:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return default


def normalize(value: str) -> str:
    return " ".join(value.casefold().replace("-", " ").split())


def market_id(market: dict[str, Any]) -> str:
    for key in ("marketId", "id", "market_id"):
        if market.get(key) not in (None, ""):
            return str(market[key])
    return ""


def outcome_tokens(market: dict[str, Any]) -> list[tuple[str, str]]:
    raw = market.get("outcomes") or market.get("tokens") or market.get("outcomeTokens") or []
    if isinstance(raw, dict):
        raw = list(raw.values())
    tokens: list[tuple[str, str]] = []
    for outcome in raw if isinstance(raw, list) else []:
        if not isinstance(outcome, dict):
            continue
        token = next((outcome.get(key) for key in ("tokenId", "token_id", "id") if outcome.get(key)), None)
        label = next((outcome.get(key) for key in ("outcomeName", "name", "title", "label", "outcome") if outcome.get(key)), "")
        if token:
            tokens.append((str(token), str(label)))
    return tokens


def rows(book: dict[str, Any], side: str) -> list[tuple[Decimal, Decimal]]:
    raw = book.get(side) or ([] if side.endswith("s") else book.get(f"{side}s")) or []
    if isinstance(raw, dict):
        raw = list(raw.values())
    result: list[tuple[Decimal, Decimal]] = []
    for item in raw if isinstance(raw, list) else []:
        if isinstance(item, (list, tuple)):
            price = item[0] if len(item) > 0 else None
            size = item[1] if len(item) > 1 else None
        elif isinstance(item, dict):
            price = item.get("price")
            size = item.get("size") or item.get("share") or item.get("quantity")
        else:
            continue
        parsed_price, parsed_size = number(price), number(size)
        if parsed_price is not None and parsed_size is not None and parsed_price > 0 and parsed_size > 0:
            result.append((parsed_price, parsed_size))
    return result


def best_ask(book: dict[str, Any]) -> Decimal | None:
    prices = [price for price, _ in rows(book, "asks")]
    return min(prices) if prices else None


def best_bid(book: dict[str, Any]) -> Decimal | None:
    prices = [price for price, _ in rows(book, "bids")]
    return max(prices) if prices else None


def ask_notional(book: dict[str, Any], ceiling: Decimal) -> Decimal:
    return sum((price * size for price, size in rows(book, "asks") if price <= ceiling), Decimal("0"))


@dataclass
class Config:
    key: str = os.getenv("BINANCE_API_KEY", "")
    secret: str = os.getenv("BINANCE_API_SECRET", "")
    wallet_address: str = os.getenv("BINANCE_WALLET_ADDRESS", "")
    wallet_id: str = os.getenv("BINANCE_WALLET_ID", "")
    rest_url: str = os.getenv("BINANCE_REST_URL", "https://api.binance.com")
    markets_url: str = os.getenv("BINANCE_MARKETS_URL", "")
    home_team: str = os.getenv("MATCH_WATCHER_HOME_TEAM", "Croatia")
    away_team: str = os.getenv("MATCH_WATCHER_AWAY_TEAM", "Spain")
    event_date: str = os.getenv("MATCH_WATCHER_EVENT_DATE", "2026-10-06")
    outcome_aliases: tuple[str, ...] = field(
        default_factory=lambda: tuple(normalize(item) for item in os.getenv("MATCH_WATCHER_OUTCOME_ALIASES", "Spain,ESP").split(",") if item.strip())
    )
    home_aliases: tuple[str, ...] = field(
        default_factory=lambda: tuple(normalize(item) for item in os.getenv("MATCH_WATCHER_HOME_ALIASES", "Croatia,CRO,HRV").split(",") if item.strip())
    )
    poll_seconds: float = float(os.getenv("MATCH_WATCHER_MARKET_POLL_SECONDS", "5"))
    book_poll_seconds: float = float(os.getenv("MATCH_WATCHER_BOOK_POLL_SECONDS", "2"))
    log_seconds: float = float(os.getenv("MATCH_WATCHER_LOG_SECONDS", "10"))
    status_log_seconds: float = float(os.getenv("MATCH_WATCHER_STATUS_LOG_SECONDS", "300"))
    catalog_page_size: int = int(os.getenv("MATCH_WATCHER_CATALOG_PAGE_SIZE", "20"))
    catalog_pages_per_poll: int = int(os.getenv("MATCH_WATCHER_CATALOG_PAGES_PER_POLL", "5"))
    entry_max: Decimal = field(default_factory=lambda: number(os.getenv("MATCH_WATCHER_ENTRY_MAX_PRICE", "0.05"), Decimal("0.05")) or Decimal("0.05"))
    desired_notional: Decimal = field(default_factory=lambda: number(os.getenv("MATCH_WATCHER_DESIRED_NOTIONAL", "5"), Decimal("5")) or Decimal("5"))
    live_trading: bool = os.getenv("MATCH_WATCHER_LIVE_TRADING", "false").strip().lower() == "true"
    buy_usdt: Decimal = field(default_factory=lambda: number(os.getenv("MATCH_WATCHER_BUY_USDT", "5"), Decimal("5")) or Decimal("5"))
    entry_retry_seconds: float = float(os.getenv("MATCH_WATCHER_ENTRY_RETRY_SECONDS", "60"))
    account_type: str = os.getenv("ACCOUNT_TYPE", "SPOT")
    funding_source: str = os.getenv("FUNDING_SOURCE", "MPC")

    def validate(self) -> None:
        if not self.key or not self.secret:
            raise RuntimeError("BINANCE_API_KEY and BINANCE_API_SECRET are required")
        if not self.markets_url:
            raise RuntimeError("BINANCE_MARKETS_URL is required")
        try:
            datetime.strptime(self.event_date, "%Y-%m-%d")
        except ValueError as exc:
            raise RuntimeError("MATCH_WATCHER_EVENT_DATE must use YYYY-MM-DD") from exc
        if not self.outcome_aliases or not self.home_aliases:
            raise RuntimeError("Match watcher aliases cannot be empty")
        if self.poll_seconds <= 0 or self.book_poll_seconds <= 0 or self.log_seconds <= 0 or self.status_log_seconds <= 0 or self.entry_retry_seconds <= 0:
            raise RuntimeError("Match watcher poll and log intervals must be positive")
        if not (1 <= self.catalog_page_size <= 20) or self.catalog_pages_per_poll <= 0:
            raise RuntimeError("Match watcher catalog page size must be 1-20 and pages per poll must be positive")
        if not (Decimal("0") < self.entry_max < Decimal("1")) or self.desired_notional <= 0 or self.buy_usdt <= 0 or self.buy_usdt > Decimal("5"):
            raise RuntimeError("Match watcher entry ceiling must be between 0 and 1 and desired notional must be positive")
        if self.live_trading and not self.wallet_address:
            raise RuntimeError("MATCH_WATCHER_LIVE_TRADING=true requires BINANCE_WALLET_ADDRESS")
        if self.live_trading and not self.wallet_id:
            raise RuntimeError("MATCH_WATCHER_LIVE_TRADING=true requires BINANCE_WALLET_ID")


class Notifier:
    def __init__(self) -> None:
        self.topic = os.getenv("NTFY_TOPIC", "").strip()
        self.server = os.getenv("NTFY_SERVER", "https://ntfy.sh").rstrip("/")
        self.session = requests.Session()

    def send(self, title: str, message: str) -> None:
        if not self.topic:
            return
        try:
            response = self.session.post(
                f"{self.server}/{self.topic}", data=message.encode("utf-8"),
                headers={"Title": title, "Priority": "high", "Tags": "mag"}, timeout=10,
            )
            response.raise_for_status()
        except Exception as exc:
            log.warning("Notification failed: %s", exc)


class ReadOnlyBinance:
    """Catalog/order-book reads and the documented quote-then-limit-order flow."""

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.session = requests.Session()
        self.session.headers.update({"X-MBX-APIKEY": cfg.key, "Content-Type": "application/x-www-form-urlencoded"})
        self.market_session = requests.Session()
        self.market_session.headers.update({"Content-Type": "application/json", "User-Agent": "prediction-match-watcher/1.0"})

    def signed_request(self, method: str, path: str, params: dict[str, Any] | None = None, body: dict[str, Any] | None = None) -> dict[str, Any]:
        values = {**(params or {}), "timestamp": int(time.time() * 1000), "recvWindow": 5000}
        query = urllib.parse.urlencode(sorted((key, str(value)) for key, value in values.items()))
        encoded_body = urllib.parse.urlencode(sorted((key, str(value)) for key, value in (body or {}).items()))
        signature = hmac.new(self.cfg.secret.encode(), (query + encoded_body).encode("utf-8"), hashlib.sha256).hexdigest()
        response = self.session.request(method, f"{self.cfg.rest_url.rstrip('/')}{path}?{query}&signature={signature}", data=encoded_body or None, timeout=15)
        if not response.ok:
            raise RuntimeError(f"Binance {path} {response.status_code}: {response.text[:300]}")
        return response.json()

    def order_book(self, market: str, token: str) -> dict[str, Any]:
        return self.signed_request("GET", "/sapi/v1/w3w/wallet/prediction/order-book", {"vendor": "predict_fun", "marketId": market, "tokenId": token})

    def get_limit_quote(self, token: str, amount_usdt: Decimal, price_limit: Decimal) -> dict[str, Any]:
        return self.signed_request("POST", "/sapi/v1/w3w/wallet/prediction/trade/get-quote", body={
            "walletAddress": self.cfg.wallet_address,
            "tokenId": token,
            "side": "BUY",
            "amountIn": str(int(amount_usdt * Decimal(10**18))),
            "orderType": "LIMIT",
            "slippageBps": 0,
            "priceLimit": str(price_limit),
        })

    def place_limit_buy(self, quote: dict[str, Any], price_limit: Decimal) -> str:
        result = self.signed_request("POST", "/sapi/v1/w3w/wallet/prediction/trade/place-order-bundle", body={
            "walletAddress": self.cfg.wallet_address,
            "walletId": self.cfg.wallet_id,
            "quoteId": quote["quoteId"],
            "timeInForce": "GTC",
            "accountType": self.cfg.account_type,
            "orderType": "LIMIT",
            "slippageBps": 0,
            "priceLimit": str(price_limit),
            "fundingSource": self.cfg.funding_source,
        })
        return str(result["orderId"])

    def markets_page(self, offset: int, limit: int) -> tuple[list[dict[str, Any]], int]:
        try:
            body = json.loads(os.getenv("BINANCE_MARKETS_BODY_JSON", "{}"))
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"BINANCE_MARKETS_BODY_JSON is invalid JSON: {exc}") from exc
        if not isinstance(body, dict):
            raise RuntimeError("BINANCE_MARKETS_BODY_JSON must contain an object")
        # Binance currently caps this endpoint at 20 events.  A rotating scan
        # is necessary because the default response exposes only page 1 while
        # the target event can be hundreds of entries later.
        body = {**body, "offset": offset, "limit": limit}
        response = self.market_session.post(self.cfg.markets_url, json=body, timeout=15)
        if not response.ok:
            raise RuntimeError(f"Binance market list {response.status_code}: {response.text[:300]}")
        payload = response.json()
        data = payload.get("data", payload)
        events = data.get("events", []) if isinstance(data, dict) else data
        flattened: list[dict[str, Any]] = []
        for event in events if isinstance(events, list) else []:
            if not isinstance(event, dict):
                continue
            groups = event.get("gameplayGroups") or []
            children = [item for group in groups if isinstance(group, dict) for item in group.get("markets", []) if isinstance(item, dict)]
            if not children:
                children = [item for item in event.get("ungroupedMarkets", []) if isinstance(item, dict)]
            for child in children:
                flattened.append({**event, **child, "eventId": event.get("eventId"), "eventSlug": event.get("eventSlug"), "eventTitle": event.get("title")})
        total = int(data.get("total", len(flattened))) if isinstance(data, dict) else len(flattened)
        return flattened, max(total, 0)


@dataclass
class State:
    selected_market_id: str = ""
    selected_token_id: str = ""
    selected_label: str = ""
    announced_listing: bool = False
    entry_alerted: bool = False
    lowest_ask: str | None = None
    entry_order_id: str = ""
    entry_submitted_at: float | None = None
    last_entry_attempt_at: float = 0.0
    last_entry_error: str = ""
    catalog_offset: int = 0


class MatchWatcher:
    def __init__(self, cfg: Config) -> None:
        cfg.validate()
        self.cfg = cfg
        self.binance = ReadOnlyBinance(cfg)
        self.notifier = Notifier()
        self.state_path = DATA_DIR / "croatia_spain_match_watcher_state.json"
        self.state = self.load_state()
        self.stop = threading.Event()
        self.last_discovery = 0.0
        self.last_book_poll = 0.0
        self.last_log = 0.0
        self.last_waiting_log = 0.0
        self.log_path: Path | None = None

    def load_state(self) -> State:
        if not self.state_path.exists():
            return State()
        try:
            raw = json.loads(self.state_path.read_text(encoding="utf-8"))
            allowed = {key: raw.get(key) for key in State.__dataclass_fields__}
            return State(**allowed)
        except Exception as exc:
            log.warning("Could not load match watcher state: %s", exc)
            return State()

    def save_state(self) -> None:
        self.state_path.write_text(json.dumps(asdict(self.state), indent=2), encoding="utf-8")

    def append(self, payload: dict[str, Any]) -> None:
        if self.log_path is None:
            date_dir = WATCH_DIR / time.strftime("%Y-%m-%d", time.gmtime())
            date_dir.mkdir(exist_ok=True)
            self.log_path = date_dir / f"{self.cfg.event_date}-{normalize(self.cfg.home_team).replace(' ', '_')}-{normalize(self.cfg.away_team).replace(' ', '_')}.jsonl"
        with self.log_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, separators=(",", ":"), default=str) + "\n")

    def event_matches(self, market: dict[str, Any]) -> bool:
        title = normalize(str(market.get("eventTitle") or market.get("title") or ""))
        if normalize(self.cfg.home_team) not in title or normalize(self.cfg.away_team) not in title:
            return False
        start = market.get("startDate")
        if start not in (None, ""):
            try:
                start_date = datetime.fromtimestamp(int(Decimal(str(start))) / 1000, tz=UTC).date().isoformat()
                if start_date != self.cfg.event_date:
                    return False
            except (ValueError, OverflowError, InvalidOperation):
                return False
        return True

    def select_target_market(self, markets: list[dict[str, Any]]) -> tuple[dict[str, Any], str, str] | None:
        for market in markets:
            if not self.event_matches(market):
                continue
            text = normalize(" ".join(str(market.get(key, "")) for key in ("marketTitle", "title", "question", "description")))
            if any(term in text for term in ("total corners", "exact score", "half", "first team to score", "more markets", "handicap", "spread")):
                continue
            tokens = outcome_tokens(market)
            target = next(((token, label) for token, label in tokens if normalize(label) in self.cfg.outcome_aliases), None)
            home_present = any(normalize(label) in self.cfg.home_aliases for _, label in tokens)
            draw_present = any(normalize(label) == "draw" for _, label in tokens)
            if target and home_present and draw_present and len(tokens) >= 3:
                return market, target[0], target[1]
        return None

    def discover(self) -> None:
        selected: tuple[dict[str, Any], str, str] | None = None
        total = 0
        for _ in range(self.cfg.catalog_pages_per_poll):
            markets, total = self.binance.markets_page(self.state.catalog_offset, self.cfg.catalog_page_size)
            selected = self.select_target_market(markets)
            self.state.catalog_offset = (self.state.catalog_offset + self.cfg.catalog_page_size) % total if total else 0
            if selected:
                break
        self.save_state()
        if not selected:
            now = time.time()
            if now - self.last_waiting_log >= self.cfg.status_log_seconds:
                log.info("MATCH WATCH waiting event=%s vs %s date=%s", self.cfg.home_team, self.cfg.away_team, self.cfg.event_date)
                self.last_waiting_log = now
            return
        market, token, label = selected
        identifier = market_id(market)
        if not identifier:
            raise RuntimeError("Target match-result market had no market ID")
        changed = identifier != self.state.selected_market_id or token != self.state.selected_token_id
        self.state.selected_market_id, self.state.selected_token_id, self.state.selected_label = identifier, token, label
        if changed:
            self.state.entry_alerted = False
            self.state.lowest_ask = None
            self.save_state()
        if not self.state.announced_listing or changed:
            event = str(market.get("eventTitle") or market.get("title") or f"{self.cfg.home_team} vs {self.cfg.away_team}")
            action = "A single live limit-buy will now be attempted." if self.cfg.live_trading else "No order will be placed."
            message = f"Found match-result market {identifier}. Watching {label}. {action}"
            self.notifier.send("Target prediction market listed", f"{event}\n{message}")
            self.append({"kind": "market_found", "observed_at": time.time(), "market_id": identifier, "token_id": token, "outcome": label, "market": market})
            self.state.announced_listing = True
            self.save_state()
            log.warning("MATCH WATCH FOUND market=%s outcome=%s token=%s", identifier, label, token)
        self.maybe_submit_entry()

    def maybe_submit_entry(self) -> None:
        """Submit one pending capped limit bid only after an explicit live opt-in."""
        if not self.cfg.live_trading or self.state.entry_order_id:
            return
        now = time.time()
        if now - self.state.last_entry_attempt_at < self.cfg.entry_retry_seconds:
            return
        self.state.last_entry_attempt_at = now
        self.save_state()
        try:
            quote = self.binance.get_limit_quote(self.state.selected_token_id, self.cfg.buy_usdt, self.cfg.entry_max)
            order_id = self.binance.place_limit_buy(quote, self.cfg.entry_max)
            self.state.entry_order_id = order_id
            self.state.entry_submitted_at = now
            self.state.last_entry_error = ""
            self.save_state()
            message = (
                f"{self.cfg.home_team} vs {self.cfg.away_team} — {self.state.selected_label}\n"
                f"Submitted one GTC LIMIT BUY: ${self.cfg.buy_usdt} capped at ${self.cfg.entry_max}.\n"
                f"Order ID: {order_id}\n"
                "It may remain pending and can fill later; inspect/cancel it manually if your plan changes."
            )
            self.notifier.send("Target limit order submitted", message)
            self.append({"kind": "limit_buy_submitted", "observed_at": now, "market_id": self.state.selected_market_id, "token_id": self.state.selected_token_id, "outcome": self.state.selected_label, "order_id": order_id, "buy_usdt": self.cfg.buy_usdt, "price_limit": self.cfg.entry_max})
            log.warning("MATCH WATCH LIVE LIMIT BUY SUBMITTED market=%s outcome=%s order=%s amount=%s price_limit=%s", self.state.selected_market_id, self.state.selected_label, order_id, self.cfg.buy_usdt, self.cfg.entry_max)
        except Exception as exc:
            self.state.last_entry_error = str(exc)[:500]
            self.save_state()
            log.warning("MATCH WATCH LIVE LIMIT BUY NOT SUBMITTED market=%s: %s", self.state.selected_market_id, exc)

    def sample_book(self) -> None:
        if not self.state.selected_market_id or not self.state.selected_token_id:
            return
        now = time.time()
        if now - self.last_book_poll < self.cfg.book_poll_seconds:
            return
        self.last_book_poll = now
        book = self.binance.order_book(self.state.selected_market_id, self.state.selected_token_id)
        ask, bid = best_ask(book), best_bid(book)
        depth = ask_notional(book, self.cfg.entry_max)
        if ask is not None and (self.state.lowest_ask is None or ask < Decimal(self.state.lowest_ask)):
            self.state.lowest_ask = str(ask)
            self.save_state()
            log.info("MATCH WATCH NEW LOW market=%s outcome=%s ask=%s bid=%s depth<=%s=%s", self.state.selected_market_id, self.state.selected_label, ask, bid, self.cfg.entry_max, depth)
        executable_entry = ask is not None and ask <= self.cfg.entry_max and depth >= self.cfg.desired_notional
        if executable_entry and not self.state.entry_alerted:
            message = (
                f"{self.cfg.home_team} vs {self.cfg.away_team} — {self.state.selected_label}\n"
                f"Best executable ask: ${ask}; best bid: ${bid if bid is not None else 'none'}\n"
                f"Ask depth at <= ${self.cfg.entry_max}: ${depth}; target notional: ${self.cfg.desired_notional}\n"
                "Order-book alert: verify the book manually before changing any order."
            )
            self.notifier.send("Potential target entry", message)
            self.state.entry_alerted = True
            self.save_state()
            log.warning("MATCH WATCH ENTRY CANDIDATE market=%s ask=%s bid=%s depth=%s", self.state.selected_market_id, ask, bid, depth)
        if now - self.last_log >= self.cfg.log_seconds:
            self.last_log = now
            self.append({
                "kind": "book_snapshot", "observed_at": now, "market_id": self.state.selected_market_id,
                "token_id": self.state.selected_token_id, "outcome": self.state.selected_label,
                "best_ask": ask, "best_bid": bid, "ask_notional_at_or_below_entry_max": depth,
                "entry_max": self.cfg.entry_max, "desired_notional": self.cfg.desired_notional, "book": book,
            })

    def run(self) -> None:
        log.warning(
            "MATCH WATCH START live_trading=%s target=%s vs %s date=%s outcome_aliases=%s entry_max=%s buy_usdt=%s",
            self.cfg.live_trading, self.cfg.home_team, self.cfg.away_team, self.cfg.event_date, ",".join(self.cfg.outcome_aliases), self.cfg.entry_max, self.cfg.buy_usdt,
        )
        while not self.stop.is_set():
            now = time.monotonic()
            try:
                if now - self.last_discovery >= self.cfg.poll_seconds:
                    self.discover()
                    self.last_discovery = now
                if self.state.selected_market_id:
                    self.sample_book()
            except Exception as exc:
                log.warning("Match watcher loop failed: %s", exc)
            # The catalog and the selected order book use independent cadence.
            self.stop.wait(0.2)
        log.warning("MATCH WATCH STOPPED")


def main() -> None:
    watcher = MatchWatcher(Config())
    signal.signal(signal.SIGTERM, lambda *_: watcher.stop.set())
    signal.signal(signal.SIGINT, lambda *_: watcher.stop.set())
    watcher.run()


if __name__ == "__main__":
    main()
