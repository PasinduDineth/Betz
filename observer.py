"""Read-only Binance Prediction Market anomaly observer.

This program deliberately contains no quote, order placement, cancellation, or
position APIs. It samples a market only when it is first discovered with a
real, executable $0.01/$0.02 ask, then records that market for ten minutes.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import re
import signal
import threading
import time
import urllib.parse
from dataclasses import asdict, dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv


load_dotenv()

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
LOG_DIR = ROOT / "logs"
OBSERVER_DIR = LOG_DIR / "observer"
for directory in (DATA_DIR, LOG_DIR, OBSERVER_DIR):
    directory.mkdir(exist_ok=True)

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler(LOG_DIR / "observer.log", encoding="utf-8")],
)
log = logging.getLogger("prediction-observer")


def decimal(value: Any, default: Decimal | None = None) -> Decimal | None:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return default


def market_id(market: dict[str, Any]) -> str:
    for key in ("marketId", "id", "market_id"):
        value = market.get(key)
        if value not in (None, ""):
            return str(value)
    return ""


def outcome_tokens(market: dict[str, Any]) -> list[tuple[str, str]]:
    raw = market.get("outcomes") or market.get("tokens") or market.get("outcomeTokens") or []
    if isinstance(raw, dict):
        raw = list(raw.values())
    if not isinstance(raw, list):
        return []
    found: list[tuple[str, str]] = []
    for outcome in raw:
        if not isinstance(outcome, dict):
            continue
        token = next((outcome.get(key) for key in ("tokenId", "token_id", "id") if outcome.get(key)), None)
        if not token:
            continue
        label = next((outcome.get(key) for key in ("outcomeName", "name", "title", "label") if outcome.get(key)), "")
        found.append((str(token), str(label)))
    return found


def template_key(market: dict[str, Any]) -> str:
    """Group repeated markets without conflating the two result sides."""
    text = " ".join(
        str(market.get(key, ""))
        for key in ("marketTitle", "question", "description", "title", "eventTitle", "slug", "eventSlug")
    ).lower()
    category = "/".join(
        str(market.get(key, "")).strip().lower()
        for key in ("l1Category", "l2Category", "category", "sport")
        if market.get(key)
    ) or "uncategorized"
    if "total corners" in text or "corners" in text and ("over" in text or "under" in text or "o/u" in text):
        return f"{category}:total_corners"
    if "second half result" in text:
        return f"{category}:second_half_result"
    if "first" in text and "score" in text:
        return f"{category}:first_to_score"
    if "up or down" in text:
        duration = re.search(r"\b(\d+)\s*(?:m|min|minute)", text)
        return f"{category}:up_down_{duration.group(1) if duration else 'unknown'}m"
    if "exact score" in text:
        return f"{category}:exact_score"
    if "spread" in text:
        return f"{category}:spread"
    if "total" in text and ("over" in text or "under" in text or "o/u" in text):
        return f"{category}:total"
    if "draw" in text or "vs." in text or " vs " in text:
        return f"{category}:match_result"
    # Keep a stable but bounded key for one-off Culture/Tech/etc. questions.
    normalized = re.sub(r"\b\d{4}[-/]\d{1,2}[-/]\d{1,2}\b|\b\d+(?:\.\d+)?\b", "#", text)
    normalized = re.sub(r"\s+", " ", normalized).strip()[:100]
    return f"{category}:{normalized or 'unknown'}"


def best_ask(book: dict[str, Any]) -> Decimal | None:
    asks = book.get("asks") or book.get("ask") or []
    if isinstance(asks, dict):
        asks = list(asks.values())
    prices: list[Decimal] = []
    for row in asks:
        value = row[0] if isinstance(row, (list, tuple)) and row else row.get("price") if isinstance(row, dict) else None
        parsed = decimal(value)
        if parsed is not None and parsed > 0:
            prices.append(parsed)
    return min(prices) if prices else None


def notional_at_or_below(book: dict[str, Any], ceiling: Decimal) -> Decimal:
    asks = book.get("asks") or book.get("ask") or []
    if isinstance(asks, dict):
        asks = list(asks.values())
    total = Decimal("0")
    for row in asks:
        if isinstance(row, (list, tuple)):
            price = row[0] if len(row) > 0 else None
            size = row[1] if len(row) > 1 else None
        elif isinstance(row, dict):
            price, size = row.get("price"), row.get("size") or row.get("share") or row.get("quantity")
        else:
            continue
        parsed_price, parsed_size = decimal(price), decimal(size)
        if parsed_price is not None and parsed_size is not None and parsed_price <= ceiling and parsed_price > 0 and parsed_size > 0:
            total += parsed_price * parsed_size
    return total


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
                headers={"Title": title, "Priority": "default", "Tags": "mag"}, timeout=10,
            )
            response.raise_for_status()
        except Exception as exc:
            log.warning("Notification failed: %s", exc)


@dataclass
class Config:
    key: str = os.getenv("BINANCE_API_KEY", "")
    secret: str = os.getenv("BINANCE_API_SECRET", "")
    rest_url: str = os.getenv("BINANCE_REST_URL", "https://api.binance.com")
    markets_url: str = os.getenv("BINANCE_MARKETS_URL", "")
    poll_seconds: float = float(os.getenv("OBSERVER_MARKET_POLL_SECONDS", "1"))
    capture_seconds: int = int(os.getenv("OBSERVER_CAPTURE_SECONDS", "600"))
    max_samples_per_template: int = int(os.getenv("OBSERVER_MAX_SAMPLES_PER_TEMPLATE", "2"))
    cheap_max: Decimal = field(default_factory=lambda: decimal(os.getenv("OBSERVER_CHEAP_MAX_PRICE", "0.02"), Decimal("0.02")) or Decimal("0.02"))

    def validate(self) -> None:
        if not self.key or not self.secret:
            raise RuntimeError("BINANCE_API_KEY and BINANCE_API_SECRET are required for read-only order-book snapshots")
        if not self.markets_url:
            raise RuntimeError("BINANCE_MARKETS_URL is required")
        if self.poll_seconds <= 0 or self.capture_seconds <= 0 or self.max_samples_per_template <= 0:
            raise RuntimeError("Observer timing and sample limits must be positive")


class ReadOnlyBinance:
    """Only catalog and order-book reads. No trading methods are present."""

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.session = requests.Session()
        self.session.headers.update({"X-MBX-APIKEY": cfg.key, "Content-Type": "application/x-www-form-urlencoded"})

    def signed_get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        values = {**params, "timestamp": int(time.time() * 1000), "recvWindow": 5000}
        query = urllib.parse.urlencode(sorted((key, str(value)) for key, value in values.items()))
        signature = hmac.new(self.cfg.secret.encode(), query.encode("utf-8"), hashlib.sha256).hexdigest()
        response = self.session.get(f"{self.cfg.rest_url.rstrip('/')}{path}?{query}&signature={signature}", timeout=15)
        if not response.ok:
            raise RuntimeError(f"Binance order-book {response.status_code}: {response.text[:300]}")
        return response.json()

    def order_book(self, market: str, token: str) -> dict[str, Any]:
        return self.signed_get(
            "/sapi/v1/w3w/wallet/prediction/order-book",
            {"vendor": "predict_fun", "marketId": market, "tokenId": token},
        )

    def markets(self) -> list[dict[str, Any]]:
        try:
            body = json.loads(os.getenv("BINANCE_MARKETS_BODY_JSON", "{}"))
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"BINANCE_MARKETS_BODY_JSON is invalid JSON: {exc}")
        response = self.session.post(self.cfg.markets_url, json=body, timeout=15)
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
        return flattened


@dataclass
class Capture:
    market_id: str
    template: str
    started_at: float
    ends_at: float
    path: str
    market: dict[str, Any]
    tokens: list[tuple[str, str]]


class ObserverState:
    def __init__(self) -> None:
        self.path = DATA_DIR / "observer_state.json"
        self.seen_markets: set[str] = set()
        self.samples_by_template: dict[str, int] = {}
        self.baseline_ready = False
        self.load()

    def load(self) -> None:
        if not self.path.exists():
            return
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            self.seen_markets = set(raw.get("seen_markets", []))
            self.samples_by_template = {str(key): int(value) for key, value in raw.get("samples_by_template", {}).items()}
            self.baseline_ready = bool(raw.get("baseline_ready", False))
        except Exception as exc:
            log.warning("Could not load observer state: %s", exc)

    def save(self) -> None:
        payload = {
            "seen_markets": sorted(self.seen_markets),
            "samples_by_template": self.samples_by_template,
            "baseline_ready": self.baseline_ready,
        }
        self.path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


class MarketObserver:
    def __init__(self, cfg: Config) -> None:
        cfg.validate()
        self.cfg = cfg
        self.binance = ReadOnlyBinance(cfg)
        self.notifier = Notifier()
        self.state = ObserverState()
        self.active: dict[str, Capture] = {}
        self.stop = threading.Event()

    @staticmethod
    def append(path: Path, payload: dict[str, Any]) -> None:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, separators=(",", ":"), default=str) + "\n")

    def capture_path(self, market: str) -> Path:
        date_dir = OBSERVER_DIR / time.strftime("%Y-%m-%d", time.gmtime())
        date_dir.mkdir(exist_ok=True)
        return date_dir / f"{int(time.time())}-{market}.jsonl"

    def initial_books(self, market: str, tokens: list[tuple[str, str]]) -> dict[str, dict[str, Any]]:
        books: dict[str, dict[str, Any]] = {}
        for token, _ in tokens:
            try:
                books[token] = self.binance.order_book(market, token)
            except Exception as exc:
                log.warning("Initial order-book read failed market=%s token=%s: %s", market, token, exc)
        return books

    def start_capture(self, market: dict[str, Any], tokens: list[tuple[str, str]], books: dict[str, dict[str, Any]], template: str) -> None:
        identifier = market_id(market)
        cheap = [(token, label, best_ask(books[token]), notional_at_or_below(books[token], self.cfg.cheap_max)) for token, label in tokens if token in books and (best_ask(books[token]) or Decimal("2")) <= self.cfg.cheap_max]
        if not cheap:
            return
        sample_count = self.state.samples_by_template.get(template, 0)
        if sample_count >= self.cfg.max_samples_per_template:
            log.info("OBSERVER SAMPLE CAP template=%s market=%s", template, identifier)
            return
        path = self.capture_path(identifier)
        now = time.time()
        capture = Capture(identifier, template, now, now + self.cfg.capture_seconds, str(path), market, tokens)
        self.active[identifier] = capture
        self.state.samples_by_template[template] = sample_count + 1
        self.state.save()
        payload = {
            "kind": "capture_started", "observed_at": now, "market_id": identifier, "template": template,
            "capture_seconds": self.cfg.capture_seconds, "market": market, "tokens": [{"token_id": token, "label": label} for token, label in tokens],
            "initial_books": books,
        }
        self.append(path, payload)
        details = "\n".join(f"{label or token}: ask ${ask} | depth at <= ${self.cfg.cheap_max}: ${depth}" for token, label, ask, depth in cheap)
        title = str(market.get("eventTitle") or market.get("title") or identifier)
        self.notifier.send("Cheap prediction detected", f"{title}\nTemplate: {template}\n{details}\nRead-only 10-minute capture started.")
        log.warning("OBSERVER CAPTURE START market=%s template=%s file=%s", identifier, template, path)

    def discover(self) -> None:
        markets = self.binance.markets()
        if not self.state.baseline_ready:
            self.state.seen_markets.update(identifier for market in markets if (identifier := market_id(market)))
            self.state.baseline_ready = True
            self.state.save()
            log.warning("OBSERVER BASELINE established markets=%s; only later additions can start captures", len(markets))
            return
        for market in markets:
            identifier = market_id(market)
            if not identifier or identifier in self.state.seen_markets:
                continue
            self.state.seen_markets.add(identifier)
            tokens = outcome_tokens(market)
            template = template_key(market)
            if tokens:
                books = self.initial_books(identifier, tokens)
                self.start_capture(market, tokens, books, template)
            self.state.save()

    def sample_active(self) -> None:
        now = time.time()
        for identifier, capture in list(self.active.items()):
            if now >= capture.ends_at:
                self.append(Path(capture.path), {"kind": "capture_finished", "observed_at": now, "market_id": identifier})
                del self.active[identifier]
                log.info("OBSERVER CAPTURE FINISHED market=%s file=%s", identifier, capture.path)
                continue
            books: dict[str, dict[str, Any]] = {}
            for token, _ in capture.tokens:
                try:
                    books[token] = self.binance.order_book(identifier, token)
                except Exception as exc:
                    books[token] = {"error": str(exc)}
            self.append(Path(capture.path), {"kind": "snapshot", "observed_at": now, "market_id": identifier, "books": books})

    def run(self) -> None:
        log.warning(
            "OBSERVER START read_only=true poll=%.2fs capture=%ss cheap_max=%s samples_per_template=%s",
            self.cfg.poll_seconds, self.cfg.capture_seconds, self.cfg.cheap_max, self.cfg.max_samples_per_template,
        )
        while not self.stop.is_set():
            started = time.monotonic()
            try:
                self.discover()
                self.sample_active()
            except Exception as exc:
                log.warning("Observer loop failed: %s", exc)
            self.stop.wait(max(0.0, self.cfg.poll_seconds - (time.monotonic() - started)))
        log.warning("OBSERVER STOPPED")


def main() -> None:
    observer = MarketObserver(Config())
    signal.signal(signal.SIGTERM, lambda *_: observer.stop.set())
    signal.signal(signal.SIGINT, lambda *_: observer.stop.set())
    observer.run()


if __name__ == "__main__":
    main()
