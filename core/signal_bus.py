"""
core/signal_bus.py

Provides:
  - DeltaSignal  : dataclass carrying the latest delta computation result.
  - SignalBus    : asyncio.Queue-based pub/sub channel between bots.
"""

import asyncio
import time
from dataclasses import dataclass, field
from typing import Optional

from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class DeltaSignal:
    """
    Represents a single delta-based market signal derived from Binance order book depth.

    Attributes
    ----------
    signal    : "UP" | "DOWN" | "NEUTRAL"
    delta     : float in [-1, 1] – (bid_weight - ask_weight) / (bid_weight + ask_weight)
    ema_trade_delta : float representing the smoothed trade-flow imbalance
    timestamp : unix timestamp (seconds) when the signal was produced
    invert    : True if a confirmed signal inversion was detected this tick
    """
    signal: str            # "UP" | "DOWN" | "NEUTRAL"
    delta: float
    ema_trade_delta: float = 0.0
    timestamp: int = field(default_factory=lambda: int(time.time()))
    invert: bool = False
    confirmed: bool = False  # True = delta >= CONFIRM_THRESHOLD + EMA ตรงทิศ + ยืน 3 วิ


class SignalBus:
    """
    Asyncio queue-based signal bus.

    Bot1 (producer) calls ``publish()`` to push new signals.
    Bot3 (consumer) calls ``subscribe()`` to block-wait for the next signal.
    Any component can call ``latest()`` for a non-blocking peek at the most
    recent signal without consuming it from the queue.
    """

    def __init__(self) -> None:
        self._queue: asyncio.Queue[DeltaSignal] = asyncio.Queue()
        self._latest: Optional[DeltaSignal] = None
        self._invert_pending: bool = False  # sticky — stays True until consumed

    async def publish(self, signal: DeltaSignal) -> None:
        """
        Push a new DeltaSignal onto the bus.
        Always updates the cached latest value.
        If signal.invert is True, sets the sticky invert flag.
        """
        self._latest = signal
        if signal.invert:
            self._invert_pending = True
        await self._queue.put(signal)
        logger.debug(
            f"SignalBus published | signal={signal.signal} delta={signal.delta:.4f} "
            f"invert={signal.invert} ts={signal.timestamp}"
        )

    def reset_invert(self) -> None:
        """Clear stale invert flag — call when a new market starts."""
        self._invert_pending = False

    def check_and_clear_invert(self) -> bool:
        """
        Return True if an invert was pending, then clear the flag.
        Bot3 calls this instead of latest().invert to avoid missing fast inversions.
        """
        if self._invert_pending:
            self._invert_pending = False
            return True
        return False

    async def subscribe(self) -> DeltaSignal:
        """
        Blocking get – suspends until a DeltaSignal is available.
        Intended to be called in a loop by consumer bots.
        """
        sig = await self._queue.get()
        logger.debug(
            f"SignalBus consumed  | signal={sig.signal} delta={sig.delta:.4f} "
            f"invert={sig.invert} ts={sig.timestamp}"
        )
        return sig

    def latest(self) -> Optional[DeltaSignal]:
        """
        Non-blocking peek at the most recently published signal.
        Returns None if no signal has been published yet.
        """
        return self._latest
