"""
bots/bot3_order_executor.py

OrderExecutorBot – consumes DeltaSignal events from the SignalBus,
applies four gates, submits orders, and monitors open positions for
take-profit or panic-sell conditions.

Gate pipeline per signal
─────────────────────────
  GATE 1 – Time Gate     : signal must arrive within [TIME_GATE_MIN, TIME_GATE_MAX]
                           seconds of the current 5-minute window
  GATE 2 – Price Gate    : current market price must be fetchable and not in
                           PRICE_FORBIDDEN list
  GATE 3 – Delta Gate    : |delta| in [ENTRY_RANGE_LOWER, ENTRY_RANGE_UPPER]
                           and signal != "NEUTRAL"
  CHECK   – Position Check: no existing open position for this market
  EXECUTE – sign & submit order, enter monitor loop

monitor_loop()
──────────────
  - Polls current price every tick (1 s sleep)
  - Closes if PnL >= TP_LOW (+10 %)
  - Panic-sells if signal_bus.latest().invert == True
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Optional

from config import settings
from core.dynamic_delta import DynamicDeltaManager
from core.polymarket_client import PolymarketClient
from core.state_manager import StateManager
from core.signal_bus import DeltaSignal, SignalBus
from core.wallet import PolyWallet
from utils.logger import get_logger

logger = get_logger(__name__)

# ── ANSI color constants (module-level) ────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"
SEP    = "═" * 54

# Market ID to trade – set here or pull from settings/env in a real deployment.
# For BTC Up/Down markets the token ID must be resolved from the Polymarket API.
# This placeholder is replaced at runtime once market discovery is implemented.
_BTC_MARKET_ID: str = settings.__dict__.get("BTC_MARKET_ID", "BTC_UP_DOWN_PLACEHOLDER")

# How long (seconds) to sleep between price-poll ticks in monitor_loop
_MONITOR_TICK_INTERVAL: float = 1.0

# Default order size in USDC (plain units, not 6-decimal micro-units)
_DEFAULT_ORDER_SIZE_USDC: float = 10.0


def _seconds_in_current_5min_window() -> int:
    """Return the number of seconds elapsed in the current 5-minute window."""
    return int(time.time()) % 300


class OrderExecutorBot:
    """
    Subscribes to the SignalBus, runs gate checks, submits orders, and
    monitors positions.

    Parameters
    ----------
    signal_bus : shared SignalBus instance
    client     : shared PolymarketClient instance
    wallet     : shared PolyWallet instance
    market_id  : Polymarket token/market ID to trade (BTC Up/Down)
    """

    def __init__(
        self,
        signal_bus: SignalBus,
        client: PolymarketClient,
        wallet: PolyWallet,
        market_id: str = _BTC_MARKET_ID,
    ) -> None:
        self._bus: SignalBus = signal_bus
        self._client: PolymarketClient = client
        self._wallet: PolyWallet = wallet
        self._market_id: str = market_id
        self._state: StateManager = StateManager()
        # Track active monitor tasks to avoid duplicate positions
        self._monitoring: bool = False
        # Counter for consecutive ticks where stop-loss condition is met
        self._sl_duration_count: int = 0
        # YES/NO token IDs resolved from Gamma API
        self._yes_token_id: str | None = None
        self._no_token_id: str | None = None
        self._market_end_date: str | None = None  # ISO 8601 string
        self._market_slug: str | None = None
        self._outcome_up: float | None = None   # Up (YES) price จาก outcomePrices
        self._outcome_down: float | None = None  # Down (NO) price จาก outcomePrices
        self._last_skip_reason: str | None = None  # สาเหตุล่าสุดที่ไม่เข้า
        # Task A5: Panic sell cooldown tracking
        self._consecutive_panic_sells: int = 0
        self.PANIC_COOLDOWN_TRADES = 3  # stop after 3 consecutive
        self._market_round: int = 0  # นับตลาดที่ผ่านมา
        self._last_order_signal: str | None = None  # UP/DOWN ของ order ล่าสุด
        self._clob_unavailable_since: float | None = None  # timestamp เมื่อ CLOB เริ่ม fail
        self._traded_this_market: bool = False  # 1 order ต่อตลาด — reset เมื่อตลาดใหม่
        self._dynamic_delta: DynamicDeltaManager = DynamicDeltaManager(
            base_threshold=settings.ENTRY_RANGE_LOWER,
            sustain_seconds=settings.DYNAMIC_DELTA_SUSTAIN_SECONDS,
        )

    def _send_panic_email(self) -> None:
        """Send a panic alert email using settings."""
        import smtplib
        from email.mime.text import MIMEText

        from_email = settings.EMAIL_FROM
        to_email = settings.EMAIL_TO
        smtp_host = settings.EMAIL_SMTP_HOST
        smtp_port = settings.EMAIL_SMTP_PORT
        password = settings.EMAIL_PASSWORD

        if not all([from_email, to_email, smtp_host, password]):
            logger.warning("OrderExecutorBot: email settings incomplete, skipping panic email")
            return

        try:
            msg = MIMEText(f"Bot paused after 3 consecutive panic sells at {datetime.now()}")
            msg["Subject"] = "[PolyBot] Panic sell triggered 3 times"
            msg["From"] = from_email
            msg["To"] = to_email

            with smtplib.SMTP(smtp_host, int(smtp_port)) as server:
                server.starttls()
                server.login(from_email, password)
                server.send_message(msg)
            logger.info("OrderExecutorBot: panic email sent successfully")
        except Exception as exc:
            logger.warning(f"OrderExecutorBot: failed to send panic email: {exc}")

    # ------------------------------------------------------------------
    # Market info refresh
    # ------------------------------------------------------------------

    async def _auto_discover_market(self) -> bool:
        """
        ค้นหา active BTC Up/Down market ที่ acceptingOrders=True
        set self._market_id, self._yes_token_id, self._no_token_id, self._market_end_date
        return True ถ้าสำเร็จ, False ถ้าไม่เจอ
        """
        logger.debug("OrderExecutorBot: _auto_discover_market – searching for active BTC markets")
        try:
            markets = await self._client.find_active_btc_markets(limit=20)
        except Exception as exc:
            logger.error(f"OrderExecutorBot: _auto_discover_market – find_active_btc_markets raised: {exc}")
            return False

        if not markets:
            logger.warning("OrderExecutorBot: _auto_discover_market – no BTC markets returned")
            return False

        # Sort ascending by endDate (nearest expiry first, to catch current 5-min round)
        def _parse_end_date(m: dict) -> datetime:
            end_str = m.get("endDate", "")
            try:
                return datetime.fromisoformat(end_str.replace("Z", "+00:00"))
            except Exception:
                # Push markets with unparseable dates to the end
                return datetime(9999, 12, 31, tzinfo=timezone.utc)

        sorted_markets = sorted(markets, key=_parse_end_date)

        # เอาตลาดที่ยังเหลือเวลา > 60 วินาที (skip ตลาดใกล้หมด)
        now_utc = datetime.now(timezone.utc)
        candidates = [
            m for m in sorted_markets
            if (_parse_end_date(m) - now_utc).total_seconds() > 60
        ]
        if not candidates:
            logger.info("_auto_discover_market: no active BTC markets found, will retry")
            return False

        for market in candidates:
            token_ids = self._client.extract_token_ids(market)
            if token_ids is None:
                logger.debug(
                    f"OrderExecutorBot: _auto_discover_market – skipping market "
                    f"id={market.get('id')} (extract_token_ids returned None)"
                )
                continue

            # ตรวจ CLOB liquidity ก่อนเลือก — skip ถ้า CLOB ไม่ตอบสนอง
            yes_tok, _ = token_ids
            clob_test = await self._client.get_price_clob(yes_tok)
            if clob_test is None:
                logger.debug(
                    f"OrderExecutorBot: _auto_discover_market – skipping market "
                    f"id={market.get('id')} q={market.get('question','')[:40]} (CLOB no price)"
                )
                continue

            logger.info(
                f"OrderExecutorBot: _auto_discover_market – CLOB ✓ market "
                f"id={market.get('id')} q={market.get('question','')[:50]}"
            )

            # Found a usable market
            new_market_id = str(market["id"])
            is_new = new_market_id != self._market_id
            self._market_id = new_market_id
            self._yes_token_id, self._no_token_id = token_ids
            self._market_end_date = market.get("endDate")
            self._market_slug = market.get("slug")
            # parse outcomePrices: "[\"0.09\", \"0.92\"]" → up=0.09, down=0.92
            try:
                import json as _json
                op = market.get("outcomePrices")
                if op:
                    prices = _json.loads(op) if isinstance(op, str) else op
                    self._outcome_up = float(prices[0])
                    self._outcome_down = float(prices[1])
                else:
                    self._outcome_up = self._outcome_down = None
            except Exception:
                self._outcome_up = self._outcome_down = None
            if is_new:
                self._market_round += 1
            if is_new:
                # ล้าง order/position เก่าจากตลาดที่แล้ว
                self._last_order_signal = None
                self._last_skip_reason = None
                self._monitoring = False
                self._sl_duration_count = 0
                self._traded_this_market = False
                self._bus.reset_invert()
                if settings.DRY_RUN and self._state.open_position:
                    self._state._state["open_position"] = None
                    self._state._save()
                logger.info(
                    f"OrderExecutorBot: _auto_discover_market SUCCESS – "
                    f"ตลาด 5 นาที ครั้งที่ {self._market_round} "
                    f"market_id={self._market_id} "
                    f"endDate={self._market_end_date}"
                )
            else:
                logger.debug(
                    f"OrderExecutorBot: same market rediscovered – "
                    f"market_id={self._market_id} endDate={self._market_end_date}"
                )
            return True

        logger.warning("OrderExecutorBot: _auto_discover_market – no suitable market found after scanning all candidates")
        return False

    def _is_market_expired(self) -> bool:
        """
        ตรวจว่า market หมดอายุหรือใกล้หมดแล้ว (น้อยกว่า TIME_BUFFER วิ)
        return True ถ้า expired หรือ endDate ไม่รู้
        """
        if self._market_end_date is None:
            # ไม่รู้ expiry → ถือว่ายังใช้ได้
            return False

        try:
            end_dt = datetime.fromisoformat(
                self._market_end_date.replace("Z", "+00:00")
            )
            now_dt = datetime.now(timezone.utc)
            seconds_remaining = (end_dt - now_dt).total_seconds()
            if seconds_remaining < settings.TIME_BUFFER:
                logger.debug(
                    f"OrderExecutorBot: _is_market_expired – market expires in "
                    f"{seconds_remaining:.0f}s (< TIME_BUFFER={settings.TIME_BUFFER}s)"
                )
                return True
            return False
        except Exception as exc:
            logger.warning(f"OrderExecutorBot: _is_market_expired – failed to parse endDate: {exc}")
            return False

    async def _refresh_market_info(self) -> bool:
        """
        ดึงข้อมูล market ล่าสุดจาก Gamma API
        set self._yes_token_id, self._no_token_id, self._market_end_date
        return True ถ้าสำเร็จ
        """
        market = await self._client.get_market_by_id(self._market_id)
        if market is None:
            logger.error(f"_refresh_market_info: cannot fetch market {self._market_id}")
            return False
        token_ids = self._client.extract_token_ids(market)
        if token_ids is None:
            logger.error(f"_refresh_market_info: no clobTokenIds for market {self._market_id}")
            return False
        self._yes_token_id, self._no_token_id = token_ids
        self._market_end_date = market.get("endDate")
        logger.info(
            f"_refresh_market_info: YES={self._yes_token_id[:12]}... "
            f"NO={self._no_token_id[:12]}... endDate={self._market_end_date}"
        )
        return True

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """
        Main loop – pull signals from SignalBus and process them.
        """
        logger.info(
            f"OrderExecutorBot starting – market={self._market_id} "
            f"wallet={settings.WALLET_ADDRESS}"
        )

        # Restore persisted state
        if self._state.market_id and self._state.market_id != "BTC_UP_DOWN_PLACEHOLDER":
            logger.info(f"OrderExecutorBot: restoring market_id from state: {self._state.market_id}")
            self._market_id = self._state.market_id

        if settings.DRY_RUN:
            logger.info("=" * 50)
            logger.info("[DRY RUN MODE] No real orders will be placed")
            logger.info(f"[DRY RUN] {self._state.get_summary()}")
            logger.info("=" * 50)

        # Initial market setup
        if self._market_id == "BTC_UP_DOWN_PLACEHOLDER":
            discovered = await self._auto_discover_market()
            if not discovered:
                logger.warning("OrderExecutorBot: no active BTC market found – retrying in 30s")
                await asyncio.sleep(30)
                # will retry in market rotation check
        else:
            await self._refresh_market_info()

        # เริ่ม background display loop
        asyncio.create_task(self._display_loop())

        while True:
            try:
                # Check if current market has expired or token IDs are missing
                if self._yes_token_id is None or self._no_token_id is None or self._is_market_expired():
                    if self._yes_token_id is None or self._no_token_id is None:
                        logger.info("OrderExecutorBot: token IDs not set – discovering market")
                    else:
                        logger.debug("OrderExecutorBot: market expired – discovering next market")
                    discovered = await self._auto_discover_market()
                    if not discovered:
                        # ล้าง token เก่าทิ้ง ไม่ให้ display แสดง CLOB ✗ ของตลาดเก่า
                        self._yes_token_id = None
                        self._no_token_id = None
                        self._outcome_up = None
                        self._outcome_down = None
                        logger.info("OrderExecutorBot: รอตลาดใหม่ – retry ใน 30s")
                        await asyncio.sleep(30)
                    else:
                        await asyncio.sleep(1)
                    continue

                sig: DeltaSignal = await self._bus.subscribe()
                asyncio.create_task(self._handle_signal(sig))
            except asyncio.CancelledError:
                logger.info("OrderExecutorBot received cancellation – shutting down")
                raise
            except Exception as exc:
                logger.error(f"OrderExecutorBot: error in main loop: {exc}")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _dynamic_delta_threshold(self, sig: DeltaSignal) -> float:
        return self._dynamic_delta.update(sig.delta, time.monotonic())

    # ------------------------------------------------------------------
    # Signal handling pipeline
    # ------------------------------------------------------------------

    async def _handle_signal(self, sig: DeltaSignal) -> None:
        """Apply all gates and execute order if all pass."""

        # ── GATE 0: Market Accepting Orders ──────────────────────────
        if self._yes_token_id is None or self._no_token_id is None:
            logger.debug("OrderExecutorBot: GATE 0 FAIL – token IDs not loaded yet")
            return

        # ── GATE 1: Time Gate ─────────────────────────────────────────
        if self._market_end_date:
            # ใช้ endDate จริงจาก API
            try:
                end_dt = datetime.fromisoformat(
                    self._market_end_date.replace("Z", "+00:00")
                )
                now_dt = datetime.now(timezone.utc)
                seconds_remaining = (end_dt - now_dt).total_seconds()
                elapsed = max(0, 300 - int(seconds_remaining))  # approximate elapsed
                if seconds_remaining <= settings.TIME_BUFFER:
                    logger.debug(
                        f"OrderExecutorBot: GATE 1 FAIL – market ending soon "
                        f"seconds_remaining={seconds_remaining:.0f}s"
                    )
                    return
            except Exception:
                elapsed = _seconds_in_current_5min_window()
        else:
            elapsed = _seconds_in_current_5min_window()
        if not (settings.TIME_GATE_MIN <= elapsed <= settings.TIME_GATE_MAX):
            logger.debug(
                f"OrderExecutorBot: GATE 1 FAIL – elapsed={elapsed}s "
                f"window=[{settings.TIME_GATE_MIN},{settings.TIME_GATE_MAX}]"
            )
            return
        logger.debug(f"OrderExecutorBot: GATE 1 PASS – elapsed={elapsed}s")
        _gate1_msg = f"Gate 1: +{elapsed:03d}s (in window)"

        # ── GATE 2: Price Gate (CLOB only) ───────────────────────────
        # ใช้ YES token_id เสมอ — ราคา NO = 1 - YES
        clob_price: Optional[float] = await self._client.get_price_clob(
            self._yes_token_id or self._market_id
        )
        price = clob_price

        if clob_price is None:
            if self._clob_unavailable_since is None:
                self._clob_unavailable_since = time.time()
            # ลอง last-trade-price เพื่อแสดงราคาใน display เท่านั้น
            token_for_signal = self._yes_token_id if sig.signal == "UP" else self._no_token_id
            if token_for_signal:
                ltp = await self._client.get_last_trade_price(token_for_signal)
                if ltp is not None:
                    logger.debug(f"OrderExecutorBot: CLOB ✗ last_trade_price={ltp} (display only, no trade)")
            self._last_skip_reason = "CLOB ✗ – ไม่เทรด"
            return
        else:
            self._clob_unavailable_since = None
            if self._last_skip_reason == "CLOB ✗ – ไม่เทรด":
                self._last_skip_reason = None
        if price in settings.PRICE_FORBIDDEN:
            self._last_skip_reason = f"ราคา ${price:.3f} ห้ามเข้า"
            return
        # ปรับราคาตาม signal — ถ้า DOWN ใช้ราคา NO token = 1 - YES
        if sig.signal == "DOWN":
            price = round(1 - price, 4)
        if price < settings.MIN_ENTRY_PRICE:
            self._last_skip_reason = f"ราคา ${price:.3f} ต่ำกว่า min ({settings.MIN_ENTRY_PRICE:.2f})"
            return
        potential_gain = (settings.TP_PRICE_CAP - price) / price
        if potential_gain < settings.MIN_TP_GAIN:
            self._last_skip_reason = (
                f"ราคา ${price:.3f} ใกล้ cap เกิน – gain ถึง TP แค่ {potential_gain:.1%} "
                f"(< {settings.MIN_TP_GAIN:.0%})"
            )
            return
        _gate2_msg = f"Gate 2: price=${price:.3f} (ok)"

        # ── GATE 3: Delta Gate ────────────────────────────────────────
        if sig.signal == "NEUTRAL":
            return

        if not sig.confirmed:
            effective_entry_threshold = self._dynamic_delta_threshold(sig)
            abs_delta = abs(sig.delta)
            if abs_delta < effective_entry_threshold:
                self._last_skip_reason = f"Δ={abs_delta:.4f}% ต่ำกว่า threshold {effective_entry_threshold:.3f}%"
                return

        # ── CHECK: 1 order ต่อตลาด ───────────────────────────────────
        if self._traded_this_market:
            self._last_skip_reason = "เข้าไปแล้ว 1 ครั้งในตลาดนี้"
            return

        # ── CHECK: Existing Position ──────────────────────────────────
        if self._monitoring:
            self._last_skip_reason = "มี position เปิดอยู่แล้ว"
            return

        # ── CHECK: USDC Cash Balance (skip in dry run) ───────────────
        if not settings.DRY_RUN:
            cash_wallet = settings.FUNDER or settings.WALLET_ADDRESS
            usdc_balance = await self._client.get_usdc_balance_polygon(cash_wallet)
            if usdc_balance < 5.0:
                logger.warning(f"OrderExecutorBot: CASH CHECK FAIL – balance=${usdc_balance:.2f} – attempting claim")
                claimed = await self._client.run_claim_cycle(settings.WALLET_ADDRESS, self._wallet)
                if claimed > 0:
                    usdc_balance = await self._client.get_usdc_balance_polygon(cash_wallet)
                if usdc_balance < 5.0:
                    self._last_skip_reason = f"เงินไม่พอ ${usdc_balance:.2f} (< $5)"
                    return

        existing = await self._client.get_open_positions(settings.WALLET_ADDRESS)
        open_for_market = [
            p for p in existing if p.get("market") == self._market_id
        ]
        if open_for_market:
            self._last_skip_reason = "มี position เปิดอยู่แล้ว"
            return
        logger.debug("OrderExecutorBot: POSITION CHECK PASS – no existing position")

        # ── EXECUTE ORDER ─────────────────────────────────────────────
        await self._execute_order(sig, price)

    async def _execute_order(self, sig: DeltaSignal, entry_price: float) -> None:
        """Build order_args and submit via PolymarketClient, then enter monitor loop."""

        # Lock ก่อน 3s sleep เพื่อกัน race condition (signal สองอันเข้าพร้อมกัน)
        if self._monitoring:
            return
        self._monitoring = True

        try:
            # เลือก YES หรือ NO token ตาม signal direction
            if sig.signal == "UP":
                token_id = self._yes_token_id or self._market_id
            else:  # DOWN
                token_id = self._no_token_id or self._market_id

            order_args = {
                "token_id": token_id,
                "price":    entry_price,
                "size":     _DEFAULT_ORDER_SIZE_USDC,
                "side":     "BUY",
            }

            # รอ re-confirm: confirmed=True → 3 วิ, normal → 5 วิ + EMA ต้องตรงทิศ
            wait_secs = 3 if sig.confirmed else 5
            logger.debug(f"OrderExecutorBot: waiting {wait_secs}s to confirm (confirmed={sig.confirmed})...")
            await asyncio.sleep(wait_secs)
            latest = self._bus.latest()
            if latest is None or latest.signal != sig.signal:
                self._last_skip_reason = f"spike – signal เปลี่ยนใน {wait_secs}s"
                self._monitoring = False
                return
            effective_threshold = self._dynamic_delta_threshold(latest)
            if abs(latest.delta) < effective_threshold:
                self._last_skip_reason = f"delta ร่วงใน {wait_secs}s ({abs(latest.delta):.4f}% < {effective_threshold:.3f}%)"
                self._monitoring = False
                return
            if not sig.confirmed:
                # normal path — EMA ต้องไม่ conflict
                ema_positive = latest.ema_trade_delta > 0
                signal_up = latest.signal == "UP"
                if ema_positive != signal_up:
                    self._last_skip_reason = f"EMA conflict – signal={latest.signal} ema={latest.ema_trade_delta:+.4f}"
                    self._monitoring = False
                    return

            # re-fetch ราคาล่าสุดหลัง wait — ใช้ราคาที่ถูกต้อง ณ เวลาส่ง order
            fresh_price = await self._client.get_price_clob(token_id)
            if fresh_price is not None:
                if sig.signal == "DOWN":
                    fresh_price = round(1 - fresh_price, 4)
                if (fresh_price < settings.MIN_ENTRY_PRICE
                        or (settings.TP_PRICE_CAP - fresh_price) / fresh_price < settings.MIN_TP_GAIN):
                    self._last_skip_reason = f"ราคาหลัง wait ไม่เหมาะ ${fresh_price:.3f}"
                    self._monitoring = False
                    return
                entry_price = fresh_price
                order_args["price"] = entry_price

            self._last_skip_reason = None
        except Exception:
            self._monitoring = False
            raise

        try:
            result = await self._client.submit_order(order_args)
        except Exception as exc:
            logger.error(f"OrderExecutorBot: submit_order raised: {exc}")
            self._monitoring = False
            return

        if result.get("error"):
            logger.error(f"OrderExecutorBot: order REJECTED: {result['error']}")
            self._monitoring = False
            return

        self._last_order_signal = sig.signal
        self._traded_this_market = True
        dry_tag = " [DRY RUN]" if settings.DRY_RUN else ""
        logger.info(
            f"ORDER {sig.signal}{dry_tag} @ ${entry_price:.3f} size={_DEFAULT_ORDER_SIZE_USDC} "
            f"id={result.get('orderID') or result.get('id')}"
        )

        position: dict = {
            "market":      self._market_id,
            "token_id":    token_id,
            "entry_price": entry_price,
            "signal":      sig.signal,
            "size":        _DEFAULT_ORDER_SIZE_USDC,
            "order_id":    result.get("orderID") or result.get("id"),
            "opened_at":   int(time.time()),
            "market_id":   self._market_id,
        }

        self._state.open_position(position)
        self._state.market_id = self._market_id

        try:
            await self.monitor_loop(position)
        finally:
            self._monitoring = False
            self._last_skip_reason = None

    # ------------------------------------------------------------------
    # Monitor loop
    # ------------------------------------------------------------------

    async def monitor_loop(self, position: dict) -> None:
        """
        Poll price every tick and exit on TP or invert conditions.

        Parameters
        ----------
        position : dict with at least ``entry_price``, ``market``, ``signal``
        """
        entry_price: float = position["entry_price"]
        market_id: str = position["market"]
        logger.info(
            f"OrderExecutorBot: entering monitor_loop "
            f"market={market_id} entry={entry_price}"
        )

        tick = 0
        while True:
            await asyncio.sleep(_MONITOR_TICK_INTERVAL)
            tick += 1

            # ── Fetch current price ────────────────────────────────────
            token_id_mon = position.get("token_id")
            clob_price_mon: Optional[float] = await self._client.get_price_clob(token_id_mon or market_id)
            last_trade_mon: Optional[float] = None
            if clob_price_mon is None and token_id_mon:
                last_trade_mon = await self._client.get_last_trade_price(token_id_mon)
            current_price = clob_price_mon if clob_price_mon is not None else last_trade_mon

            clob_available = clob_price_mon is not None

            if not clob_available and current_price is not None:
                logger.info(
                    f"OrderExecutorBot: CLOB unavailable – holding until expiry "
                    f"Gamma price={current_price:.3f}"
                )
                if tick % 10 == 0:
                    self._print_status(
                        clob_price=None,
                        gamma_price=current_price,
                        position=position,
                        current_price=current_price,
                    )
                continue  # CLOB ไม่มี — hold จนตลาดปิด ไม่ TP ไม่ panic sell

            if current_price is None:
                logger.warning("OrderExecutorBot: monitor_loop – price unavailable (both CLOB+Gamma), skipping tick")
                continue

            # ── Periodic terminal status display (every 10 ticks) ─────
            if tick % 10 == 0:
                self._print_status(
                    position=position,
                    current_price=current_price,
                )

            # ── Compute PnL ────────────────────────────────────────────
            if entry_price == 0:
                logger.warning("OrderExecutorBot: monitor_loop – entry_price=0, skipping PnL")
                continue

            pnl = (current_price - entry_price) / entry_price
            logger.debug(
                f"OrderExecutorBot: monitor tick market={market_id} "
                f"entry={entry_price:.4f} current={current_price:.4f} "
                f"pnl={pnl:.4f}"
            )

            # ── Stop Loss ──────────────────────────────────────────────
            loss_pct = (entry_price - current_price) / entry_price * 100
            if loss_pct >= settings.STOP_LOSS_PERCENT:
                self._sl_duration_count += 1
                if self._sl_duration_count >= settings.SL_MIN_DURATION:
                    logger.warning(
                        f"OrderExecutorBot: STOP LOSS triggered "
                        f"loss={loss_pct:.1f}% > {settings.STOP_LOSS_PERCENT}% "
                        f"(held {self._sl_duration_count} ticks)"
                    )
                    await self.close_position(position, reason="stop_loss", exit_price=current_price)
                    return
                else:
                    logger.debug(
                        f"OrderExecutorBot: SL condition tick "
                        f"{self._sl_duration_count}/{settings.SL_MIN_DURATION} "
                        f"loss={loss_pct:.1f}%"
                    )
            else:
                self._sl_duration_count = 0

            # ── Invert Panic Sell (trust bot1 signal directly) ────────
            if self._bus.check_and_clear_invert():
                logger.warning(
                    f"OrderExecutorBot: INVERT DETECTED – panic sell market={market_id}"
                )
                await self.close_position(position, reason="panic_sell")
                return

            # ── Take Profit (dynamic — target price cap) ───────────────
            if current_price >= settings.TP_PRICE_CAP:
                tp_pct = (current_price - entry_price) / entry_price
                logger.info(
                    f"OrderExecutorBot: TP reached price={current_price:.3f} "
                    f"(cap={settings.TP_PRICE_CAP}) gain={tp_pct:.2%}"
                )
                await self.close_position(position, reason="tp", exit_price=current_price)
                return

    # ------------------------------------------------------------------
    # Close / panic helpers
    # ------------------------------------------------------------------

    async def close_position(self, position: dict, reason: str, exit_price: Optional[float] = None) -> None:
        """
        Close an open position using FOK Market Sell via market_sell_fok().

        Parameters
        ----------
        position   : original position dict (must contain size, market)
        reason     : "tp_low", "panic_sell", "stop_loss"
        exit_price : known price at close time (skips re-fetch if provided)
        """
        market_id = position.get("market", self._market_id)
        token_id = position.get("token_id") or position.get("market", self._market_id)
        size = float(position.get("size", _DEFAULT_ORDER_SIZE_USDC))
        signal = position.get("signal", "UP")

        # ดึง shares จริงจาก Polygon (แทน state ที่อาจไม่ตรง)
        actual_shares = await self._client.get_position_shares_polygon(settings.WALLET_ADDRESS, token_id)
        logger.info(
            f"OrderExecutorBot: close_position reason={reason} market={market_id} "
            f"token_id={token_id} size={size} actual_shares={actual_shares:.4f}"
        )

        if actual_shares < 5.0 and actual_shares > 0:
            # shares น้อยเกินไป ขายฝั่งตรงข้ามแทน (synthetic close)
            opposite_token = self._no_token_id if signal == "UP" else self._yes_token_id
            if opposite_token:
                logger.info(
                    f"OrderExecutorBot: shares={actual_shares:.4f} < 5 – buying opposite token={opposite_token}"
                )
                clob_p = await self._client.get_price_clob(opposite_token)
                opp_price = clob_p if clob_p is not None else 0.99
                result = await self._client.market_buy_opposite(opposite_token, opp_price, size)
            else:
                logger.warning("OrderExecutorBot: no opposite token available, trying FOK anyway")
                result = await self._client.market_sell_fok(token_id=token_id, size=size)
        else:
            result = await self._client.market_sell_fok(token_id=token_id, size=size)

        if result.get("error"):
            logger.error(f"OrderExecutorBot: close_position failed: {result['error']}")
        else:
            if reason == "panic_sell":
                dry_tag = " [DRY RUN]" if settings.DRY_RUN else ""
                print(f"\n{RED}{BOLD}⚡ PANIC SELL executed{RESET}{YELLOW}{dry_tag}{RESET}  market={market_id}\n")
            logger.info(f"OrderExecutorBot: close_position FOK accepted reason={reason}")

            # Task A5: Panic sell cooldown
            if reason == 'panic_sell':
                self._consecutive_panic_sells += 1
                if self._consecutive_panic_sells >= self.PANIC_COOLDOWN_TRADES:
                    logger.warning(
                        f"OrderExecutorBot: {self.PANIC_COOLDOWN_TRADES} consecutive panic sells! "
                        "Pausing bot and sending email alert."
                    )
                    self._send_panic_email()
                    self._consecutive_panic_sells = 0
                    raise asyncio.CancelledError("Bot paused due to consecutive panic sells")
            else:
                self._consecutive_panic_sells = 0

            # ใช้ exit_price ที่ส่งมา ถ้าไม่มีให้ดึงจาก token โดยตรง
            if exit_price is None:
                try:
                    token_id_ep = position.get("token_id")
                    exit_price = await self._client.get_price_clob(token_id_ep) if token_id_ep else None
                    if exit_price is None:
                        exit_price = position.get("entry_price", 0.0)
                except Exception:
                    exit_price = position.get("entry_price", 0.0)

            self._print_status(
                position=position,
                current_price=exit_price,
                reason=reason,
            )

            self._last_order_signal = None
            trade = self._state.close_position(exit_price=exit_price, reason=reason)
            if trade is not None:
                logger.info(
                    f"[P&L] pnl={trade['pnl']:+.4f} USDC ({trade['pnl_pct']:+.2%}) reason={reason}"
                )
                logger.info(f"[STATS] {self._state.get_summary()}")

    async def _handle_market_transition(self, old_order_id: str | None) -> None:
        """
        Cancel unfilled orders and reset state when market changes.

        Parameters
        ----------
        old_order_id : order ID from the previous market session (may be None)
        """
        if old_order_id:
            cancelled = await self._client.cancel_order(old_order_id)
            logger.info(f"market transition: cancel order {old_order_id} = {cancelled}")
        self._monitoring = False
        self._sl_duration_count = 0

    # ------------------------------------------------------------------
    # Background display loop
    # ------------------------------------------------------------------

    async def _display_loop(self) -> None:
        """
        Background loop แสดงสถานะทุก 5 วินาที
        แสดง market info + ราคาปัจจุบัน + position ถ้ามี
        """
        while True:
            try:
                await asyncio.sleep(2)

                if self._yes_token_id is None and self._no_token_id is None:
                    dry_tag = f" {YELLOW}[DRY RUN]{RESET}" if settings.DRY_RUN else ""
                    print(SEP)
                    print(f" {BOLD}ตลาด 5 นาที ครั้งที่ {self._market_round}{RESET}{dry_tag}  │  {self._state.get_summary()}")
                    print(f" {YELLOW}⏳ รอตลาดใหม่...{RESET}")
                    print(SEP)
                    continue

                # คำนวณ seconds remaining ก่อนดึงราคา
                secs_remaining = 300
                if self._market_end_date:
                    try:
                        end_dt = datetime.fromisoformat(self._market_end_date.replace("Z", "+00:00"))
                        secs_remaining = max(0, int((end_dt - datetime.now(timezone.utc)).total_seconds()))
                    except Exception:
                        pass

                # หยุดดึงราคาช่วง 10 วิท้าย (ตลาดใกล้ปิด)
                token = self._yes_token_id or self._market_id
                clob_p = await self._client.get_price_clob(token) if secs_remaining > 10 else None
                slug_p = None
                if clob_p is None and secs_remaining > 10:
                    if self._outcome_up is not None:
                        slug_p = self._outcome_up  # outcomePrices fallback (no API call)
                    elif self._market_slug:
                        slug_p = await self._client.get_price_by_slug(self._market_slug)
                price = clob_p if clob_p is not None else slug_p

                # คำนวณ elapsed
                elapsed = _seconds_in_current_5min_window()
                in_gate = settings.TIME_GATE_MIN <= elapsed <= settings.TIME_GATE_MAX
                gate_str = f"{GREEN}[GATE✓]{RESET}" if in_gate else f"{RED}[GATE✗]{RESET}"

                # countdown
                countdown = ""
                # countdown — ใช้ endDate ถ้าอยู่ในช่วง 10 นาที มิฉะนั้นใช้ 5-min window clock
                try:
                    secs = 300 - _seconds_in_current_5min_window()  # clock-based default
                    if self._market_end_date:
                        end_dt = datetime.fromisoformat(self._market_end_date.replace("Z", "+00:00"))
                        api_secs = int((end_dt - datetime.now(timezone.utc)).total_seconds())
                        if 0 < api_secs <= 600:  # ใช้ API เฉพาะถ้าสมเหตุสมผล
                            secs = api_secs
                    secs = max(0, secs)
                    mm, ss = divmod(secs, 60)
                    countdown = f"  ⌛ ปิดใน: {mm:02d}:{ss:02d}"
                except Exception:
                    pass

                now_str = datetime.now().strftime("%H:%M:%S")
                stats = self._state.get_summary()

                # order status
                if self._last_order_signal == "UP":
                    order_str = f"  {GREEN}▲ ORDER: UP{RESET}"
                elif self._last_order_signal == "DOWN":
                    order_str = f"  {RED}▼ ORDER: DOWN{RESET}"
                else:
                    order_str = f"  {YELLOW}รอ signal{RESET}"

                dry_tag = f" {YELLOW}[DRY RUN]{RESET}" if settings.DRY_RUN else ""

                print(SEP)
                print(f" {BOLD}ตลาด 5 นาที ครั้งที่ {self._market_round}{RESET}{dry_tag}  │  {stats}")
                print(f" {CYAN}🕐 {now_str}{RESET}  {gate_str}  +{elapsed}s{countdown}{order_str}")

                # Up/Down display — ใช้ CLOB ก่อน ถ้าไม่มีใช้ outcomePrices
                up_display = clob_p if clob_p is not None else self._outcome_up
                down_display = (round(1 - clob_p, 3) if clob_p is not None
                                else self._outcome_down)
                if up_display is not None and down_display is not None:
                    print(f"  Up: ${up_display:.3f}  │  Down: ${down_display:.3f}")
                if clob_p is not None:
                    print(f"  {GREEN}CLOB ✓{RESET}")
                else:
                    print(f"  {RED}CLOB ✗  – last-trade-price only (ไม่เทรด){RESET}")

                # แสดง position ถ้ามี
                pos = self._state.open_position
                if callable(pos):
                    pos = None  # ป้องกัน method vs property collision
                if isinstance(pos, dict) and pos and price is not None:
                    entry = float(pos["entry_price"])
                    pnl_pct = (price - entry) / entry * 100 if entry != 0 else 0
                    sign = "📈" if pnl_pct >= 0 else "📉"
                    color = GREEN if pnl_pct >= 0 else RED
                    print(f"  ✅ Position: {pos['signal']} @ ${entry:.3f}  Size: {pos['size']}  {sign} {color}{pnl_pct:+.1f}%{RESET}")
                elif not self._monitoring:
                    latest = self._bus.latest()
                    if latest:
                        sig_color = GREEN if latest.signal == "UP" else (RED if latest.signal == "DOWN" else YELLOW)
                        print(f"  Signal: {sig_color}{latest.signal}{RESET}  Δ={latest.delta:+.4f}%")
                    if self._last_skip_reason:
                        print(f"  {YELLOW}⚠ ข้าม: {self._last_skip_reason}{RESET}")

                print(SEP)

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.debug(f"_display_loop error: {exc}")

    # ------------------------------------------------------------------
    # Terminal status display
    # ------------------------------------------------------------------

    def _print_status(
        self,
        *,
        up_price: float | None = None,
        down_price: float | None = None,
        clob_price: float | None = None,
        gamma_price: float | None = None,
        elapsed: int | None = None,
        gate_results: list[tuple[bool, str]] | None = None,
        position: dict | None = None,
        current_price: float | None = None,
        reason: str | None = None,
    ) -> None:
        """
        Print a clean terminal status block surrounded by SEP lines.

        Parameters
        ----------
        up_price      : current price for the UP (YES) token
        down_price    : current price for the DOWN (NO) token
        clob_price    : price fetched from CLOB API (None = unavailable)
        gamma_price   : price fetched from Gamma API (None = unavailable)
        elapsed       : seconds elapsed in the current 5-minute window
        gate_results  : list of (passed: bool, message: str) for each gate
        position      : open position dict (entry_price, signal, size, …)
        current_price : latest fetched market price (used for PnL when position given)
        reason        : short human-readable label shown as a plain status line
        """
        print(SEP)

        # ── Line 1: timestamp + market window (ET) ───────────────────
        from datetime import timedelta
        try:
            from zoneinfo import ZoneInfo
            _et = ZoneInfo("America/New_York")
        except ImportError:
            _et = timezone.utc
        now_str = datetime.now(_et).strftime("%H:%M:%S")
        if self._market_end_date:
            try:
                end_dt = datetime.fromisoformat(
                    self._market_end_date.replace("Z", "+00:00")
                )
                end_et = end_dt.astimezone(_et)
                start_et = end_et - timedelta(minutes=5)
                end_hhmm = end_et.strftime("%I:%M%p").lstrip("0")
                start_hhmm = start_et.strftime("%I:%M%p").lstrip("0")
                window_str = f"  {start_hhmm}→{end_hhmm} ET"
            except Exception:
                window_str = ""
        else:
            window_str = ""
        print(f" {CYAN}🕐 {now_str}{RESET}  │  Bitcoin Up or Down{window_str}")

        # ── Line 2: CLOB/Gamma prices or Up/Down fallback ─────────────
        if clob_price is not None or gamma_price is not None:
            # แสดง CLOB และ Gamma แยกกัน
            clob_str = f"${clob_price:.3f}" if clob_price is not None else f"{RED}unavailable{RESET}"
            gamma_str = f"${gamma_price:.3f}" if gamma_price is not None else f"{RED}unavailable{RESET}"
            print(f"  CLOB: {clob_str}   Gamma: {gamma_str}")
        elif up_price is not None and down_price is not None:
            # fallback แสดงแบบเดิม
            forbidden = up_price in settings.PRICE_FORBIDDEN or down_price in settings.PRICE_FORBIDDEN
            forbidden_str = f"  {RED}🔴 ห้ามเข้า{RESET}" if forbidden else ""
            print(f"  Up: ${up_price:.3f}  │  Down: ${down_price:.3f}{forbidden_str}")

        # ── Line 3: gate results / position info / reason ─────────────
        if gate_results is not None:
            for passed, msg in gate_results:
                icon = f"{GREEN}✅{RESET}" if passed else f"{RED}❌{RESET}"
                print(f"  {icon} {msg}")
        elif position is not None:
            entry = position["entry_price"]
            pnl_pct = (
                (current_price - entry) / entry * 100 if current_price else 0
            )
            sign = "📈" if pnl_pct >= 0 else "📉"
            color = GREEN if pnl_pct >= 0 else RED
            print(
                f"  ✅ Position: {position['signal']} @ ${entry:.3f}"
                f"  Size: {position['size']}"
                f"  {sign} {color}{pnl_pct:+.1f}%{RESET}"
            )
        elif reason is not None:
            print(f"  {YELLOW}{reason}{RESET}")

        # ── Line 4: countdown to market close ─────────────────────────
        if self._market_end_date:
            try:
                end_dt = datetime.fromisoformat(
                    self._market_end_date.replace("Z", "+00:00")
                )
                secs = max(
                    0,
                    int((end_dt - datetime.now(timezone.utc)).total_seconds()),
                )
                hh, rem = divmod(secs, 3600)
                mm, ss = divmod(rem, 60)
                countdown = f"⌛ ปิดใน: {hh:02d}:{mm:02d}:{ss:02d}" if hh else f"⌛ ปิดใน: {mm:02d}:{ss:02d}"
            except Exception:
                countdown = ""
            if countdown:
                print(f" {countdown}")

        print(SEP)
