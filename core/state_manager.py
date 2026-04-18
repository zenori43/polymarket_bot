"""
core/state_manager.py

Persist bot state to a JSON file so dry-run sessions survive restarts.

State schema
────────────
{
  "market_id": str,
  "open_position": {           # null when no position open
    "token_id": str,
    "entry_price": float,
    "signal": str,             # "UP" | "DOWN"
    "size": float,
    "order_id": str | null,
    "opened_at": int           # unix timestamp
  } | null,
  "trades": [                  # completed trades (closed positions)
    {
      "market_id": str,
      "signal": str,
      "entry_price": float,
      "exit_price": float,
      "size": float,
      "pnl": float,            # absolute USDC
      "pnl_pct": float,        # e.g. 0.12 = +12%
      "reason": str,           # "tp_low" | "panic_sell" | "stop_loss"
      "opened_at": int,
      "closed_at": int
    }
  ],
  "stats": {
    "total_trades": int,
    "wins": int,
    "losses": int,
    "total_pnl": float
  }
}
"""

from __future__ import annotations

import json
import os
import time
from typing import Any

from config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


class StateManager:
    """Read/write bot state to a JSON file."""

    _EMPTY_STATE: dict = {
        "market_id": "",
        "open_position": None,
        "trades": [],
        "stats": {
            "total_trades": 0,
            "wins": 0,
            "losses": 0,
            "panic_sells": 0,
            "total_pnl": 0.0,
        },
    }

    def __init__(self, path: str = settings.STATE_FILE) -> None:
        self._path = path
        self._state: dict = self._load()

    # ------------------------------------------------------------------

    def _load(self) -> dict:
        if os.path.exists(self._path):
            try:
                with open(self._path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                logger.info(f"StateManager: loaded state from {self._path}")
                return data
            except Exception as exc:
                logger.warning(f"StateManager: failed to load state ({exc}) – starting fresh")
        import copy
        return copy.deepcopy(self._EMPTY_STATE)

    def _save(self) -> None:
        try:
            with open(self._path, "w", encoding="utf-8") as f:
                json.dump(self._state, f, indent=2, ensure_ascii=False)
        except Exception as exc:
            logger.error(f"StateManager: failed to save state: {exc}")

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------

    @property
    def market_id(self) -> str:
        return self._state.get("market_id", "")

    @market_id.setter
    def market_id(self, value: str) -> None:
        self._state["market_id"] = value
        self._save()

    @property
    def open_position(self) -> dict | None:
        return self._state.get("open_position")

    # ------------------------------------------------------------------
    # Position lifecycle
    # ------------------------------------------------------------------

    def open_position(self, position: dict) -> None:
        """Call when an order is accepted. position must have all required fields."""
        pos = {**position, "opened_at": position.get("opened_at", int(time.time()))}
        self._state["open_position"] = pos
        self._save()
        logger.info(f"StateManager: opened position {pos}")

    def close_position(self, exit_price: float, reason: str) -> dict | None:
        """
        Call when position is closed.
        Computes pnl, appends to trades, updates stats.
        Returns the completed trade dict or None if no open position.
        """
        pos = self._state.get("open_position")
        if pos is None:
            return None

        entry = float(pos["entry_price"])
        size = float(pos["size"])
        pnl_pct = (exit_price - entry) / entry if entry != 0 else 0.0
        pnl_abs = pnl_pct * size

        trade = {
            "market_id": pos.get("market_id", self._state["market_id"]),
            "signal": pos["signal"],
            "entry_price": entry,
            "exit_price": exit_price,
            "size": size,
            "pnl": round(pnl_abs, 4),
            "pnl_pct": round(pnl_pct, 6),
            "reason": reason,
            "opened_at": pos.get("opened_at", 0),
            "closed_at": int(time.time()),
        }

        self._state["trades"].append(trade)
        self._state["open_position"] = None

        stats = self._state["stats"]
        stats["total_pnl"] = round(stats["total_pnl"] + pnl_abs, 4)
        if reason == "panic_sell":
            stats.setdefault("panic_sells", 0)
            stats["panic_sells"] += 1
        else:
            stats["total_trades"] += 1
            if pnl_abs >= 0:
                stats["wins"] += 1
            else:
                stats["losses"] += 1

        self._save()
        logger.info(
            f"StateManager: closed trade reason={reason} "
            f"entry={entry} exit={exit_price} "
            f"pnl={pnl_abs:+.4f} ({pnl_pct:+.2%})"
        )
        return trade

    def reset_stats(self) -> None:
        """Reset trade stats and trade history (for dry run per-round reset)."""
        self._state["trades"] = []
        self._state["stats"] = {
            "total_trades": 0,
            "wins": 0,
            "losses": 0,
            "panic_sells": 0,
            "total_pnl": 0.0,
        }
        self._save()

    def get_summary(self) -> str:
        """Return a human-readable P&L summary string."""
        s = self._state["stats"]
        total = s["total_trades"]
        wins = s["wins"]
        losses = s["losses"]
        panics = s.get("panic_sells", 0)
        pnl = s["total_pnl"]
        wr = (wins / total * 100) if total > 0 else 0.0
        panic_str = f" Panic={panics}" if panics > 0 else ""
        return (
            f"Trades={total} W={wins} L={losses}{panic_str} "
            f"WR={wr:.1f}% TotalPnL={pnl:+.4f} USDC"
        )
