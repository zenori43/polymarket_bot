"""
main.py

Polymarket Bitcoin Up/Down Trading Bot – entry point.

Startup order
─────────────
1. Create SignalBus
2. Create ClobClient (py-clob-client, handles signing for orders)
3. Create PolymarketClient (wraps ClobClient)
4. Create PolyWallet (used by ClaimBot only)
5. Instantiate Bot1 (DeltaSignalBot), Bot2 (ClaimBot), Bot3 (OrderExecutorBot)
6. Run all three bots concurrently via asyncio.gather()

Shutdown
────────
KeyboardInterrupt (Ctrl+C) cancels all tasks and closes HTTP connections gracefully.
"""

from __future__ import annotations

import asyncio
import sys

from config import settings
from core.clob_client_factory import create_clob_client
from core.polymarket_client import PolymarketClient
from core.signal_bus import SignalBus
from core.wallet import PolyWallet
from bots.bot1_delta_signal import DeltaSignalBot
from bots.bot2_claim import ClaimBot
from bots.bot3_order_executor import OrderExecutorBot
from utils.logger import get_logger

logger = get_logger(__name__)


async def main() -> None:
    """Initialise all components and run the three bots concurrently."""

    # ── Validate required config ──────────────────────────────────────
    if not settings.WALLET_ADDRESS:
        logger.error("WALLET_ADDRESS is not set – please configure .env")
        sys.exit(1)
    if not settings.PRIVATE_KEY:
        logger.error("PRIVATE_KEY is not set – please configure .env")
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("Polymarket Bot starting up")
    logger.info(f"  Wallet  : {settings.WALLET_ADDRESS}")
    logger.info(f"  CLOB    : {settings.CLOB_URL}")
    logger.info(f"  Gamma   : {settings.GAMMA_URL}")
    logger.info(f"  DataAPI : {settings.DATA_API_URL}")
    logger.info(f"  BinanceWS: {settings.BINANCE_WS_URL}")
    logger.info("=" * 60)

    # ── 1. SignalBus ──────────────────────────────────────────────────
    signal_bus = SignalBus()
    logger.info("SignalBus initialised")

    # ── 2. ClobClient (py-clob-client, handles signing) ──────────────
    if settings.DRY_RUN:
        clob_client = None
        logger.info("[DRY RUN] ClobClient skipped – no real orders will be placed")
    else:
        clob_client = create_clob_client()

    # ── 3. PolymarketClient (wraps ClobClient + httpx fallback) ──────
    client = PolymarketClient(clob_client=clob_client)
    logger.info("PolymarketClient initialised (ClobClient injected)")

    # ── 4. PolyWallet (kept for bot2 ClaimBot) ────────────────────────
    if settings.DRY_RUN:
        wallet = PolyWallet(
            private_key="0x0000000000000000000000000000000000000000000000000000000000000001",
            wallet_address=settings.WALLET_ADDRESS or "0x0000000000000000000000000000000000000001",
        )
        logger.info("[DRY RUN] PolyWallet using dummy key")
    else:
        wallet = PolyWallet(
            private_key=settings.PRIVATE_KEY,
            wallet_address=settings.WALLET_ADDRESS,
        )
    logger.info("PolyWallet initialised (used by ClaimBot)")

    # ── 5. Bots ───────────────────────────────────────────────────────
    bot1 = DeltaSignalBot(signal_bus=signal_bus)
    bot2 = ClaimBot(client=client, wallet=wallet)
    bot3 = OrderExecutorBot(signal_bus=signal_bus, client=client, wallet=wallet)
    logger.info("All bots instantiated – starting concurrent execution")

    # ── 6. Run concurrently ───────────────────────────────────────────
    try:
        await asyncio.gather(
            bot1.run(),
            bot2.run(),
            bot3.run(),
        )
    except asyncio.CancelledError:
        logger.info("asyncio.gather cancelled – bots stopping")
    finally:
        logger.info("Shutting down – closing HTTP client")
        await client.close()
        logger.info("Polymarket Bot shut down cleanly")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received – exiting")
        sys.exit(0)
