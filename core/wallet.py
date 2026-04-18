"""
core/wallet.py

EIP-712 wallet wrapper built on eth_account.

Provides signing for:
  - CLOB limit orders  (sign_order)
  - Redeem/claim txns  (sign_redeem)

NOTE: The exact EIP-712 domain and type-hash values follow the Polymarket CLOB
      specification.  Adjust the domain fields (name, version, chainId,
      verifyingContract) to match the deployed contract if they change.
"""

from __future__ import annotations

from eth_account import Account
from eth_account.messages import encode_typed_data

from utils.logger import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Polymarket EIP-712 domain constants
# (Polygon mainnet – update if contract is redeployed)
# ---------------------------------------------------------------------------
_EIP712_DOMAIN = {
    "name": "Polymarket CTF Exchange",
    "version": "1",
    "chainId": 137,                          # Polygon mainnet
    "verifyingContract": "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
}

# EIP-712 type definitions for a CLOB order
_ORDER_TYPES = {
    "EIP712Domain": [
        {"name": "name",              "type": "string"},
        {"name": "version",           "type": "string"},
        {"name": "chainId",           "type": "uint256"},
        {"name": "verifyingContract", "type": "address"},
    ],
    "Order": [
        {"name": "salt",          "type": "uint256"},
        {"name": "maker",         "type": "address"},
        {"name": "signer",        "type": "address"},
        {"name": "taker",         "type": "address"},
        {"name": "tokenId",       "type": "uint256"},
        {"name": "makerAmount",   "type": "uint256"},
        {"name": "takerAmount",   "type": "uint256"},
        {"name": "expiration",    "type": "uint256"},
        {"name": "nonce",         "type": "uint256"},
        {"name": "feeRateBps",    "type": "uint256"},
        {"name": "side",          "type": "uint8"},
        {"name": "signatureType", "type": "uint8"},
    ],
}

# EIP-712 type definitions for a redeem request
_REDEEM_TYPES = {
    "EIP712Domain": _ORDER_TYPES["EIP712Domain"],
    "Redeem": [
        {"name": "conditionId", "type": "bytes32"},
        {"name": "amount",      "type": "uint256"},
        {"name": "redeemer",    "type": "address"},
    ],
}


class PolyWallet:
    """
    Thin wrapper around eth_account for EIP-712 signing.

    Parameters
    ----------
    private_key    : hex private key (with or without '0x' prefix)
    wallet_address : checksummed Ethereum address
    """

    def __init__(self, private_key: str, wallet_address: str) -> None:
        self._private_key: str = private_key
        self.wallet_address: str = wallet_address
        # Validate that the key corresponds to the address
        try:
            derived = Account.from_key(private_key).address
            if derived.lower() != wallet_address.lower():
                logger.warning(
                    f"PolyWallet: derived address {derived} does not match "
                    f"configured WALLET_ADDRESS {wallet_address}"
                )
            else:
                logger.info(f"PolyWallet initialised for {wallet_address}")
        except Exception as exc:
            logger.error(f"PolyWallet key validation error: {exc}")

    # ------------------------------------------------------------------
    # Public signing methods
    # ------------------------------------------------------------------

    def sign_order(self, order_params: dict) -> dict:
        """
        EIP-712 sign a Polymarket CLOB order.

        Parameters
        ----------
        order_params : dict with fields matching the Order type above.
                       Must include at minimum: salt, tokenId, makerAmount,
                       takerAmount, expiration, nonce, feeRateBps, side,
                       signatureType.

        Returns
        -------
        dict : original order_params enriched with ``maker``, ``signer``,
               ``taker`` (zero address), and ``signature`` fields.
        """
        try:
            # Fill in wallet-specific fields
            order_params.setdefault("maker", self.wallet_address)
            order_params.setdefault("signer", self.wallet_address)
            order_params.setdefault("taker", "0x0000000000000000000000000000000000000000")

            structured_data = {
                "types": _ORDER_TYPES,
                "primaryType": "Order",
                "domain": _EIP712_DOMAIN,
                "message": order_params,
            }

            encoded = encode_typed_data(full_message=structured_data)
            signed = Account.sign_message(encoded, private_key=self._private_key)

            signed_order = {
                **order_params,
                "signature": signed.signature.hex(),
            }
            logger.info(f"sign_order: order signed for tokenId={order_params.get('tokenId')}")
            return signed_order
        except Exception as exc:
            logger.error(f"sign_order error: {exc}")
            raise

    def sign_redeem(self, condition_id: str, amount: int) -> dict:
        """
        EIP-712 sign a Polymarket redeem request.

        Parameters
        ----------
        condition_id : hex bytes32 condition identifier (as a hex string)
        amount       : integer token amount to redeem

        Returns
        -------
        dict : signed transaction dict with ``conditionId``, ``amount``,
               ``redeemer``, and ``signature`` fields.
        """
        try:
            message = {
                "conditionId": condition_id,
                "amount": amount,
                "redeemer": self.wallet_address,
            }

            structured_data = {
                "types": _REDEEM_TYPES,
                "primaryType": "Redeem",
                "domain": _EIP712_DOMAIN,
                "message": message,
            }

            encoded = encode_typed_data(full_message=structured_data)
            signed = Account.sign_message(encoded, private_key=self._private_key)

            signed_tx = {
                **message,
                "signature": signed.signature.hex(),
            }
            logger.info(f"sign_redeem: redeem signed for conditionId={condition_id} amount={amount}")
            return signed_tx
        except Exception as exc:
            logger.error(f"sign_redeem error for conditionId={condition_id}: {exc}")
            raise
