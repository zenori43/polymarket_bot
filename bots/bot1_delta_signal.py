"""
bots/bot1_delta_signal.py

DeltaSignalBot – connects to Binance WebSocket @trade stream and publishes
DeltaSignal events to the shared SignalBus.

Algorithm per trade message (mirrors monitor_delta.py)
───────────────────────────────────────────────────────
1. Parse trade price, qty, and maker side from the @trade message.
2. Compute price_delta_pct relative to the start of the current 5-min window.
   - window_open_price resets whenever the 5-min window rolls over.
3. Maintain a sliding window (TRADE_WINDOW_SEC) of (timestamp, side, volume)
   tuples to compute raw buy/sell imbalance, then smooth with EMA.
4. Derive price signal from price_delta_pct vs PRICE_DELTA_THRESH.
5. Derive ema_signal from ema_trade_delta vs EMA_THRESHOLD.
6. Invert detection (same as monitor_delta):
     - If current signal differs from prev_signal → increment invert_tick_count
     - Else reset invert_tick_count to 0
     - If invert_tick_count >= INVERT_CONFIRM_TICKS → invert=True, reset count
     (no EMA confirmation required — same as monitor_delta)
7. Publish DeltaSignal when price signal is not NEUTRAL (including conflicts).
"""

from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from datetime import datetime, timedelta
from typing import Optional

import websockets
from websockets.exceptions import ConnectionClosedError, WebSocketException

from config import settings
from core.dynamic_delta import DynamicDeltaManager
from core.signal_bus import DeltaSignal, SignalBus
from utils.logger import get_logger

logger = get_logger(__name__)

# Reconnect back-off cap (seconds)
_MAX_RECONNECT_DELAY: float = 60.0


class DeltaSignalBot:
    """
    Binance @trade stream consumer that computes price-delta and EMA trade-flow
    signals, then publishes them to a SignalBus.

    Parameters
    ----------
    signal_bus : shared SignalBus instance
    """

    # ── Algorithm constants (mirrors monitor_delta.py) ──────────────────────
    _TRADE_STREAM_URL:  str   = "wss://stream.binance.com/ws/btcusdt@trade"
    _TRADE_WINDOW_SEC:  int   = 5
    _EMA_ALPHA:         float = 0.02
    _EMA_THRESHOLD:     float = 0.03    # matches monitor_delta EMA_THRESHOLD

    # -----------------------------------------------------------------------

    def __init__(self, signal_bus: SignalBus) -> None:
        self._bus: SignalBus = signal_bus

        self._dynamic_delta: DynamicDeltaManager = DynamicDeltaManager(
            base_threshold=settings.ENTRY_RANGE_LOWER,
            sustain_seconds=settings.DYNAMIC_DELTA_SUSTAIN_SECONDS,
        )

        # Invert detection state
        self._prev_signal: Optional[str] = None
        self._invert_tick_count: int = 0

        # Trade-stream state
        self._window_open_price: float = 0.0
        self._prev_window_start: Optional[datetime] = None
        self._trade_window: deque = deque()          # (timestamp_sec, side, volume)
        self._ema_trade_delta: float = 0.0

        # Confirm path: delta >= CONFIRM_THRESHOLD + EMA ตรงทิศ + ยืน 3 วิ
        self._confirm_above_since: Optional[float] = None
        self._confirm_signal: Optional[str] = None  # ทิศที่กำลัง track อยู่

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _get_window_start(now: datetime) -> datetime:
        """Return the start of the current 5-minute window for *now*."""
        floored = (now.minute // 5) * 5
        return now.replace(minute=floored, second=0, microsecond=0)

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """
        Main loop – connects to Binance @trade WS and processes messages
        indefinitely.  Reconnects with exponential back-off on disconnect.
        """
        delay: float = 1.0
        logger.info(f"DeltaSignalBot starting – connecting to {self._TRADE_STREAM_URL}")

        while True:
            try:
                await self._connect_and_stream()
                # _connect_and_stream returned normally → reset back-off
                delay = 1.0
            except asyncio.CancelledError:
                logger.info("DeltaSignalBot received cancellation – shutting down")
                raise
            except (ConnectionClosedError, WebSocketException, OSError) as exc:
                logger.warning(
                    f"DeltaSignalBot WS disconnected: {exc} – reconnecting in {delay:.1f}s"
                )
            except Exception as exc:
                logger.error(
                    f"DeltaSignalBot unexpected error: {exc} – reconnecting in {delay:.1f}s"
                )

            await asyncio.sleep(delay)
            delay = min(delay * 2, _MAX_RECONNECT_DELAY)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _connect_and_stream(self) -> None:
        """Open WS connection to the @trade stream and process messages."""
        async with websockets.connect(
            self._TRADE_STREAM_URL,
            ping_interval=20,
            ping_timeout=20,
        ) as ws:
            logger.info("DeltaSignalBot connected to Binance @trade WebSocket")
            async for raw in ws:
                await self._process_message(raw)

    async def _process_message(self, raw: str | bytes) -> None:
        """
        Parse one @trade message and publish a DeltaSignal.

        Binance @trade message format::

            {
              "e": "trade",
              "p": "74655.26",   // price (string)
              "q": "0.001",      // qty   (string)
              "m": false         // false = buyer initiated (BUY pressure)
                                 // true  = seller initiated (SELL pressure)
            }
        """
        try:
            data: dict = json.loads(raw)

            # ── Guard: only process trade events ──────────────────────────
            if data.get("e") != "trade":
                return

            # ── Parse fields ───────────────────────────────────────────────
            current_price: float = float(data["p"])
            qty:           float = float(data["q"])
            volume:        float = current_price * qty
            side:          str   = "BUY" if data["m"] is False else "SELL"
            now:           datetime = datetime.now()
            now_ts:        float = time.time()

            # ── Step 1: 5-min window detection ────────────────────────────
            window_start = self._get_window_start(now)

            if self._prev_window_start is None or window_start != self._prev_window_start:
                # New window – reset open price
                self._window_open_price = current_price
                self._prev_window_start = window_start
                logger.debug(
                    f"DeltaSignalBot: new 5-min window @ {window_start.strftime('%H:%M')} "
                    f"open_price={current_price:.2f}"
                )

            # ── Step 2: price_delta_pct ────────────────────────────────────
            if self._window_open_price == 0.0:
                # Safety: initialise on very first trade
                self._window_open_price = current_price

            price_delta_pct: float = (
                (current_price - self._window_open_price) / self._window_open_price * 100
            )

            prev_threshold = self._dynamic_delta.threshold
            current_threshold = self._dynamic_delta.update(price_delta_pct, time.monotonic())
            if current_threshold != prev_threshold:
                logger.info(
                    f"DeltaSignalBot: dynamic threshold bumped "
                    f"{prev_threshold:.4f} → {current_threshold:.4f} "
                    f"(price_delta={price_delta_pct:+.4f}%)"
                )

            # ── Step 3: sliding trade window + EMA ────────────────────────
            # Append new trade
            self._trade_window.append((now_ts, side, volume))

            # Evict trades older than TRADE_WINDOW_SEC
            cutoff = now_ts - self._TRADE_WINDOW_SEC
            while self._trade_window and self._trade_window[0][0] < cutoff:
                self._trade_window.popleft()

            buy_vol:  float = sum(vol for _, s, vol in self._trade_window if s == "BUY")
            sell_vol: float = sum(vol for _, s, vol in self._trade_window if s == "SELL")
            total:    float = buy_vol + sell_vol
            raw_imbalance: float = (buy_vol - sell_vol) / total if total > 0 else 0.0

            # Exponential moving average
            self._ema_trade_delta = (
                self._EMA_ALPHA * raw_imbalance
                + (1 - self._EMA_ALPHA) * self._ema_trade_delta
            )

            # ── Step 4: signal from price_delta_pct ───────────────────────
            if price_delta_pct > current_threshold:
                signal_str = "UP"
            elif price_delta_pct < -current_threshold:
                signal_str = "DOWN"
            else:
                signal_str = "NEUTRAL"

            # ── Step 4b: ema_signal from ema_trade_delta ──────────────────
            if self._ema_trade_delta > self._EMA_THRESHOLD:
                ema_signal = "UP"
            elif self._ema_trade_delta < -self._EMA_THRESHOLD:
                ema_signal = "DOWN"
            else:
                ema_signal = "NEUTRAL"

            # ── Step 5: invert detection (mirrors monitor_delta logic) ───────
            # Any signal change increments counter — no EMA gate required
            invert: bool = False
            if self._prev_signal is not None and signal_str != self._prev_signal:
                self._invert_tick_count += 1
                if self._invert_tick_count >= settings.INVERT_CONFIRM_TICKS:
                    invert = True
                    self._invert_tick_count = 0
                    logger.info(
                        f"DeltaSignalBot: INVERT CONFIRMED "
                        f"({self._prev_signal}→{signal_str}, ema={ema_signal})"
                    )
            else:
                self._invert_tick_count = 0

            self._prev_signal = signal_str

            # ── Step 6: publish when price signal is not NEUTRAL ──────────
            if signal_str == "NEUTRAL":
                self._confirm_above_since = None
                logger.debug(f"DeltaSignalBot: skip publish – price signal NEUTRAL")
                return

            # ── Confirm path: delta >= CONFIRM_THRESHOLD + EMA ตรงทิศ + ยืน 3 วิ ──
            ema_agrees = ema_signal == signal_str
            now_mono = time.monotonic()
            if abs(price_delta_pct) >= settings.CONFIRM_THRESHOLD and ema_agrees:
                if self._confirm_above_since is None or self._confirm_signal != signal_str:
                    self._confirm_above_since = now_mono
                    self._confirm_signal = signal_str
                confirmed = (now_mono - self._confirm_above_since) >= settings.DYNAMIC_DELTA_SUSTAIN_SECONDS
                if confirmed:
                    logger.info(
                        f"DeltaSignalBot: CONFIRM signal={signal_str} "
                        f"delta={price_delta_pct:+.4f}% ema={self._ema_trade_delta:+.4f}"
                    )
            else:
                self._confirm_above_since = None
                self._confirm_signal = None
                confirmed = False
            sig = DeltaSignal(
                signal=signal_str,
                delta=price_delta_pct,
                ema_trade_delta=self._ema_trade_delta,
                timestamp=int(now_ts),
                invert=invert,
                confirmed=confirmed,
            )
            await self._bus.publish(sig)

            logger.debug(
                f"DeltaSignalBot: price={current_price:.2f} "
                f"price_delta={price_delta_pct:+.4f}% "
                f"ema_delta={self._ema_trade_delta:+.6f} "
                f"signal={signal_str} ema_signal={ema_signal} "
                f"confirmed={confirmed} invert={invert}"
            )

        except json.JSONDecodeError as exc:
            logger.warning(f"DeltaSignalBot: failed to parse message: {exc}")
        except Exception as exc:
            logger.error(f"DeltaSignalBot: error processing message: {exc}")
