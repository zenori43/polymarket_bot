"""
monitor_delta.py — Standalone Binance WebSocket price-delta monitor.

Connects to a single Binance trade stream and displays:
  - Price delta (%) relative to window-open price  (primary signal)
  - Trade buy/sell pressure EMA                     (secondary signal)
  - Confirmation / conflict between both signals
with ANSI color highlighting.
"""

import asyncio
import json
import time
from collections import deque
from datetime import datetime, timedelta

import websockets

# ── Constants ────────────────────────────────────────────────────────────────
WS_TRADE_URL = "wss://stream.binance.com/ws/btcusdt@trade"
WS_DEPTH_URL = "wss://stream.binance.com/ws/btcusdt@depth"

INVERT_CONFIRM_TICKS  = 3
STATS_INTERVAL        = 50
TRADE_WINDOW_SECONDS  = 5      # sliding window width for trade aggregation

EMA_ALPHA: float     = 0.02    # smoothing factor for trade pressure EMA
EMA_THRESHOLD: float = 0.03    # ±0.03 threshold for trade signal UP/DOWN

PRICE_DELTA_THRESHOLD = 0.03   # % threshold for price-delta signal (±0.03 %)

# 5-minute window gate boundaries (seconds elapsed within the window)
TIME_GATE_MIN = 30
TIME_GATE_MAX = 220

# ANSI colors
GREEN   = "\033[92m"
RED     = "\033[91m"
YELLOW  = "\033[93m"
MAGENTA = "\033[95m"
RESET   = "\033[0m"

# ── State ─────────────────────────────────────────────────────────────────────
prev_signal        = None
invert_tick_count  = 0
tick_count         = 0
stats = {"UP": 0, "DOWN": 0, "NEUTRAL": 0, "Inverts": 0, "Conflicts": 0}

# Window tracking state
prev_window_start: datetime | None = None

# Price state
window_open_price: float = 0.0   # price of the first trade in the current window
current_price: float     = 0.0   # most recent trade price

# Trade pressure state (sliding window + EMA)
trade_window: deque     = deque()  # (timestamp_float, side: str, volume: float)
ema_trade_delta: float  = 0.0      # smoothed trade pressure delta

# Order book delta state (from @depth stream)
ob_delta: float  = 0.0          # latest order book delta
ob_signal: str   = "NEUTRAL"    # "UP" | "DOWN" | "NEUTRAL"


# ── Helpers ───────────────────────────────────────────────────────────────────

def signal_color(signal: str) -> str:
    return {"UP": GREEN, "DOWN": RED, "NEUTRAL": YELLOW}.get(signal, RESET)


def signal_arrow(signal: str) -> str:
    return {"UP": "▲", "DOWN": "▼", "NEUTRAL": "●"}.get(signal, " ")


def get_window(now: datetime) -> tuple[datetime, datetime, int]:
    """
    Compute the current 5-minute clock-aligned window.

    Returns:
        window_start: datetime truncated to nearest 5-minute boundary
        window_end:   window_start + 5 minutes
        elapsed_sec:  seconds elapsed since window_start (0–299)
    """
    floored_minute = (now.minute // 5) * 5
    window_start = now.replace(minute=floored_minute, second=0, microsecond=0)
    window_end   = window_start + timedelta(minutes=5)
    elapsed_sec  = int((now - window_start).total_seconds())
    return window_start, window_end, elapsed_sec


# ── Depth stream handler ──────────────────────────────────────────────────────

def handle_depth_message(data: dict) -> None:
    """
    Parse Binance @depth message, compute order book delta, update globals.
    Format: {"bids": [["price","qty"],...], "asks": [["price","qty"],...]}
    """
    global ob_delta, ob_signal
    bids = data.get("bids", [])
    asks = data.get("asks", [])
    if not bids and not asks:
        return
    bid_w = sum(float(p) * float(q) for p, q in bids)
    ask_w = sum(float(p) * float(q) for p, q in asks)
    total = bid_w + ask_w
    if total == 0:
        return
    ob_delta = (bid_w - ask_w) / total
    if ob_delta > 0:
        ob_signal = "UP"
    elif ob_delta < 0:
        ob_signal = "DOWN"
    else:
        ob_signal = "NEUTRAL"


# ── Trade stream handler ───────────────────────────────────────────────────────

def handle_trade_message(data: dict) -> None:
    """
    Parse a Binance @trade message.

    Updates:
      - current_price        (latest trade price)
      - window_open_price    (first trade price of the current window)
      - ema_trade_delta      (smoothed buy/sell pressure)

    Then produces one output line per trade tick.

    Binance field meanings:
        p  — price (string)
        q  — quantity (string)
        m  — True  → seller is market maker → buyer  initiated → BUY
             False → buyer  is market maker → seller initiated → SELL
    """
    global current_price, window_open_price
    global ema_trade_delta
    global prev_signal, invert_tick_count, tick_count, prev_window_start

    if data.get("e") != "trade":
        return

    price  = float(data["p"])
    qty    = float(data["q"])
    volume = price * qty
    # m=False → buyer initiated the trade → BUY pressure
    side   = "BUY" if not data["m"] else "SELL"

    # ── Update current price ──────────────────────────────────────────────
    current_price = price

    # ── 5-minute window calculation ───────────────────────────────────────
    now = datetime.now()
    window_start, window_end, elapsed_sec = get_window(now)

    # Detect window change — reset open price and print separator
    if prev_window_start is not None and window_start != prev_window_start:
        ws_str = window_start.strftime("%H:%M")
        we_str = window_end.strftime("%H:%M")
        print(
            f"━━━ New Window: {ws_str}→{we_str}  "
            f"OPEN: ${current_price:,.2f} ━━━━━━━━━━━━━━━━━━━━━━━━━━"
        )
        # Reset open price for the new window
        window_open_price = current_price

    # Set open price when it is not yet initialised (very first trade)
    if window_open_price == 0.0:
        window_open_price = current_price

    prev_window_start = window_start

    # ── Trade pressure — sliding window + EMA ────────────────────────────
    now_ts = time.monotonic()
    cutoff = now_ts - TRADE_WINDOW_SECONDS

    while trade_window and trade_window[0][0] < cutoff:
        trade_window.popleft()

    trade_window.append((now_ts, side, volume))

    buy_vol  = sum(vol for _, s, vol in trade_window if s == "BUY")
    sell_vol = sum(vol for _, s, vol in trade_window if s == "SELL")
    total    = buy_vol + sell_vol
    trade_delta_raw = (buy_vol - sell_vol) / total if total > 0 else 0.0
    ema_trade_delta = EMA_ALPHA * trade_delta_raw + (1 - EMA_ALPHA) * ema_trade_delta

    # ── Price delta (primary signal) ─────────────────────────────────────
    if window_open_price != 0.0:
        price_delta_pct = (current_price - window_open_price) / window_open_price * 100
    else:
        price_delta_pct = 0.0

    if price_delta_pct > PRICE_DELTA_THRESHOLD:
        signal = "UP"
    elif price_delta_pct < -PRICE_DELTA_THRESHOLD:
        signal = "DOWN"
    else:
        signal = "NEUTRAL"

    # ── Trade pressure signal (secondary) ────────────────────────────────
    if ema_trade_delta > EMA_THRESHOLD:
        trade_signal = "UP"
    elif ema_trade_delta < -EMA_THRESHOLD:
        trade_signal = "DOWN"
    else:
        trade_signal = "NEUTRAL"

    # ── Confirmation logic ────────────────────────────────────────────────
    if signal == trade_signal and signal != "NEUTRAL":
        confirm       = "✓ CONFIRM"
        confirm_color = GREEN
    elif signal != trade_signal:
        confirm       = "✗ CONFLICT"
        confirm_color = RED
    else:
        confirm       = "~ NEUTRAL"
        confirm_color = YELLOW

    # ── Invert detection (based on price-delta signal) ────────────────────
    invert_confirmed = False
    if prev_signal is not None and signal != prev_signal:
        invert_tick_count += 1
        if invert_tick_count >= INVERT_CONFIRM_TICKS:
            invert_confirmed = True
            stats["Inverts"] += 1
            invert_tick_count = 0
    else:
        invert_tick_count = 0

    old_signal  = prev_signal
    prev_signal = signal

    # ── Stats tracking ────────────────────────────────────────────────────
    stats[signal] += 1
    if confirm == "✗ CONFLICT":
        stats["Conflicts"] += 1
    tick_count += 1

    # ── Gate status ───────────────────────────────────────────────────────
    in_gate  = TIME_GATE_MIN <= elapsed_sec <= TIME_GATE_MAX
    gate_str = f"{GREEN}[GATE✓]{RESET}" if in_gate else f"{RED}[GATE✗]{RESET}"

    # ── Format output line ────────────────────────────────────────────────
    now_str   = now.strftime("%H:%M:%S")
    ws_str    = window_start.strftime("%H:%M")
    we_str    = window_end.strftime("%H:%M")

    price_color = signal_color(signal)
    price_arrow = signal_arrow(signal)
    delta_sign  = "+" if price_delta_pct >= 0 else ""

    td_color    = signal_color(trade_signal)
    td_arrow    = signal_arrow(trade_signal)
    td_sign     = "+" if ema_trade_delta >= 0 else ""

    line = (
        f"[{now_str}] "
        f"W:{ws_str}→{we_str} "
        f"+{elapsed_sec:>3}s "
        f"{gate_str}  "
        f"OPEN:{window_open_price:>11,.2f}  "
        f"NOW:{current_price:>11,.2f}  "
        f"Δ={price_color}{delta_sign}{price_delta_pct:.4f}%{RESET}  "
        f"{price_color}{price_arrow} {signal:<7}{RESET}"
        f"| "
        f"BUY/SELL: ema={td_color}{td_sign}{ema_trade_delta:.4f}{RESET} "
        f"{td_color}{td_arrow} {trade_signal:<7}{RESET}"
        f"→ {confirm_color}{confirm:<12}{RESET}"
    )
    print(line)

    # ── Invert confirmed banner ───────────────────────────────────────────
    if invert_confirmed:
        print(f"{MAGENTA}⚡ INVERT CONFIRMED: {old_signal} → {signal}{RESET}")

    # ── Stats every STATS_INTERVAL ticks ─────────────────────────────────
    if tick_count % STATS_INTERVAL == 0:
        print(
            f"── Stats ({STATS_INTERVAL} ticks) ── "
            f"UP: {stats['UP']} | "
            f"DOWN: {stats['DOWN']} | "
            f"NEUTRAL: {stats['NEUTRAL']} | "
            f"Inverts: {stats['Inverts']} | "
            f"Conflicts: {stats['Conflicts']} ──"
        )


# ── WebSocket coroutine ────────────────────────────────────────────────────────

async def stream_trades() -> None:
    """Listen to a single trade stream and dispatch to handle_trade_message."""
    print(f"Connecting to trade stream: {WS_TRADE_URL} …")
    async with websockets.connect(WS_TRADE_URL) as ws:
        print("Trade stream connected.\n")
        async for raw in ws:
            try:
                data = json.loads(raw)
                handle_trade_message(data)
            except Exception as exc:  # noqa: BLE001
                print(f"[TRADE ERROR] {exc}")


async def monitor() -> None:
    """Run the single trade stream."""
    print("Streaming BTC/USDT — Price Delta + Trade Pressure (Ctrl+C to exit)\n")
    await stream_trades()


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    try:
        asyncio.run(monitor())
    except KeyboardInterrupt:
        print("\nStopped by user.")
        print(
            f"Final stats — UP: {stats['UP']} | DOWN: {stats['DOWN']} | "
            f"NEUTRAL: {stats['NEUTRAL']} | Inverts: {stats['Inverts']} | "
            f"Conflicts: {stats['Conflicts']}"
        )


if __name__ == "__main__":
    main()
