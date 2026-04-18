"""
config/settings.py

Load all configuration from environment variables using python-dotenv.
All runtime constants are defined here and imported by other modules.
"""

import os
from dotenv import load_dotenv

# Load .env file from project root
load_dotenv()

# ---------------------------------------------------------------------------
# Wallet / Auth
# ---------------------------------------------------------------------------
WALLET_ADDRESS: str = os.getenv("WALLET_ADDRESS", "")
PRIVATE_KEY: str = os.getenv("PRIVATE_KEY", "")

# ---------------------------------------------------------------------------
# External endpoints
# ---------------------------------------------------------------------------
BINANCE_WS_URL: str = "wss://stream.binance.com/ws/btcusdt@depth"
CLOB_URL: str = "https://clob.polymarket.com"
GAMMA_URL: str = "https://gamma-api.polymarket.com"
DATA_API_URL: str = "https://data-api.polymarket.com"

# ---------------------------------------------------------------------------
# Delta signal thresholds
# Will be overridden by backtest results in the future.
# ---------------------------------------------------------------------------
ENTRY_RANGE_LOWER: float = float(os.getenv("ENTRY_RANGE_LOWER", "0.03"))
ENTRY_RANGE_UPPER: float = float(os.getenv("ENTRY_RANGE_UPPER", "0.07"))

# Number of consecutive invert ticks required before invert=True is signalled
INVERT_CONFIRM_TICKS: int = int(os.getenv("INVERT_CONFIRM_TICKS", "3"))

# ---------------------------------------------------------------------------
# Time gate  (seconds from the start of the current 5-minute window)
# ---------------------------------------------------------------------------
TIME_GATE_MIN: int = int(os.getenv("TIME_GATE_MIN", "30"))    # seconds
TIME_GATE_MAX: int = int(os.getenv("TIME_GATE_MAX", "220"))   # seconds

# ---------------------------------------------------------------------------
# Price gate – prices at which entry is forbidden
# ---------------------------------------------------------------------------
PRICE_FORBIDDEN: list[float] = [0.00, 0.01, 0.99, 1.00]

# ---------------------------------------------------------------------------
# Take-profit levels
# ---------------------------------------------------------------------------
TP_LOW: float = float(os.getenv("TP_LOW", "0.10"))    # +10% → full close
TP_HIGH: float = float(os.getenv("TP_HIGH", "0.15"))  # +15%

# ---------------------------------------------------------------------------
# Timeouts / intervals
# ---------------------------------------------------------------------------
CLOB_TIMEOUT_MS: int = int(os.getenv("CLOB_TIMEOUT_MS", "500"))        # milliseconds
CLAIM_INTERVAL_SECONDS: int = int(os.getenv("CLAIM_INTERVAL_SECONDS", "300"))  # 5 minutes

# ---------------------------------------------------------------------------
# Poly Proxy Wallet
# signature_type=2 คือ Polymarket proxy wallet (ไม่ใช่ EOA)
# ---------------------------------------------------------------------------
SIGNATURE_TYPE: int = int(os.getenv("SIGNATURE_TYPE", "2"))
FUNDER: str = os.getenv("FUNDER", "")           # Polymarket proxy wallet address
CHAIN_ID: int = int(os.getenv("CHAIN_ID", "137"))   # Polygon mainnet
RPC: str = os.getenv("RPC", "https://polygon.drpc.org")

# ---------------------------------------------------------------------------
# Stop loss
# แทน panic sell แบบเดิมที่ใช้ invert only
# ---------------------------------------------------------------------------
STOP_LOSS_PERCENT: float = float(os.getenv("STOP_LOSS_PERCENT", "45.0"))
SL_MIN_DURATION: int = int(os.getenv("SL_MIN_DURATION", "5"))   # วิ ที่ต้อง hold ก่อน trigger SL

# ---------------------------------------------------------------------------
# Time buffer ก่อน market ปิด (วิ) - ห้ามเข้า
# ---------------------------------------------------------------------------
TIME_BUFFER: int = int(os.getenv("TIME_BUFFER", "15"))

# ---------------------------------------------------------------------------
# Markets ก่อนหยุดพัก
# ---------------------------------------------------------------------------
MARKETS_BEFORE_PAUSE: int = int(os.getenv("MARKETS_BEFORE_PAUSE", "5"))
PAUSE_DURATION_SECONDS: int = int(os.getenv("PAUSE_DURATION_SECONDS", "300"))

# ---------------------------------------------------------------------------
# BTC Up/Down Market ID (integer ID จาก Gamma API)
# ถ้าไม่ได้ set จะ fallback เป็น auto-discovery
# ---------------------------------------------------------------------------
BTC_MARKET_ID: str = os.getenv("BTC_MARKET_ID", "BTC_UP_DOWN_PLACEHOLDER")

# ---------------------------------------------------------------------------
# Dry Run / State
# ---------------------------------------------------------------------------
DRY_RUN: bool = os.getenv("DRY_RUN", "false").lower() == "true"
STATE_FILE: str = os.getenv("STATE_FILE", "state.json")
