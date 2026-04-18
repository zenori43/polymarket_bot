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

    async def publish(self, signal: DeltaSignal) -> None:
        """
        Push a new DeltaSignal onto the bus.
        Always updates the cached latest value.
        """
        self._latest = signal
        await self._queue.put(signal)
        logger.debug(
            f"SignalBus published | signal={signal.signal} delta={signal.delta:.4f} "
            f"invert={signal.invert} ts={signal.timestamp}"
        )

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
