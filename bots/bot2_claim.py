"""
bots/bot2_claim.py

ClaimBot – periodically checks for resolved, unclaimed Polymarket positions
and submits EIP-712-signed redeem transactions.

Schedule: every CLAIM_INTERVAL_SECONDS (default 300 s / 5 minutes)
"""

from __future__ import annotations

import asyncio

from config import settings
from core.polymarket_client import PolymarketClient
from core.wallet import PolyWallet
from utils.logger import get_logger

logger = get_logger(__name__)


class ClaimBot:
    """
    Periodically claims rewards for resolved Polymarket positions.

    Parameters
    ----------
    client  : shared PolymarketClient instance
    wallet  : shared PolyWallet instance
    """

    def __init__(self, client: PolymarketClient, wallet: PolyWallet) -> None:
        self._client: PolymarketClient = client
        self._wallet: PolyWallet = wallet

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """
        Main loop – run a claim cycle immediately, then repeat every
        CLAIM_INTERVAL_SECONDS.
        """
        logger.info(
            f"ClaimBot starting – interval={settings.CLAIM_INTERVAL_SECONDS}s "
            f"wallet={settings.WALLET_ADDRESS}"
        )
        while True:
            await self._claim_cycle()
            logger.debug(f"ClaimBot sleeping {settings.CLAIM_INTERVAL_SECONDS}s until next cycle")
            await asyncio.sleep(settings.CLAIM_INTERVAL_SECONDS)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _claim_cycle(self) -> None:
        """
        Single claim cycle:
        1. Fetch all resolved & claimable positions.
        2. For each position: sign redeem → submit claim → log result.
        """
        logger.info("ClaimBot: starting claim cycle")
        try:
            claimable = await self._client.get_resolved_claimable(settings.WALLET_ADDRESS)
        except Exception as exc:
            logger.error(f"ClaimBot: failed to fetch claimable positions: {exc}")
            return

        if not claimable:
            logger.info("ClaimBot: no claimable positions found")
            return

        logger.info(f"ClaimBot: {len(claimable)} position(s) to claim")

        for position in claimable:
            await self._process_claim(position)

    async def _process_claim(self, position: dict) -> None:
        """
        Sign and submit a single claim.

        Expected position fields (from data API):
          condition_id   : str  – bytes32 hex condition identifier
          size           : float/int – token amount held
          market         : str  – market identifier (for logging)
        """
        condition_id: str = position.get("condition_id") or position.get("conditionId", "")
        market_id: str = position.get("market", condition_id)
        amount_raw = position.get("size", 0)

        try:
            amount: int = int(float(amount_raw))
        except (ValueError, TypeError):
            logger.warning(
                f"ClaimBot: could not parse amount '{amount_raw}' "
                f"for conditionId={condition_id} – skipping"
            )
            return

        if not condition_id:
            logger.warning(f"ClaimBot: position missing condition_id – skipping: {position}")
            return

        logger.info(
            f"ClaimBot: claiming market={market_id} conditionId={condition_id} amount={amount}"
        )

        try:
            # Sign the redeem transaction
            signed_tx = self._wallet.sign_redeem(condition_id, amount)

            # Submit to CLOB
            result = await self._client.claim_reward(condition_id, signed_tx)

            if result.get("error"):
                logger.error(
                    f"ClaimBot: claim FAILED for conditionId={condition_id}: {result['error']}"
                )
            else:
                logger.info(
                    f"ClaimBot: claim SUCCESS for conditionId={condition_id} "
                    f"amount={amount} result={result}"
                )
        except Exception as exc:
            logger.error(
                f"ClaimBot: unexpected error claiming conditionId={condition_id}: {exc}"
            )
