"""
test_prices.py — ทดสอบดึงราคา Gamma + CLOB สำหรับตลาด 5-min up/down

Usage:
    python test_prices.py                        # ดึงตลาด active ล่าสุด
    python test_prices.py <condition_id>         # ระบุ condition ID เอง
    python test_prices.py <yes_token> <no_token> # ระบุ token ID เลย
"""

import asyncio
import json
import sys
import time

import httpx

CLOB_URL   = "https://clob.polymarket.com"
GAMMA_URL  = "https://gamma-api.polymarket.com"


# ──────────────────────────────────────────────────────────────────────────────
# Gamma helpers
# ──────────────────────────────────────────────────────────────────────────────

async def gamma_find_active_5min(client: httpx.AsyncClient) -> dict | None:
    """
    ค้นหาตลาด BTC 5-min Up/Down ที่ active — ใช้ end_date_min/max filter
    แบบเดียวกับที่ bot ใช้จริง
    """
    from datetime import datetime, timezone, timedelta
    import random

    now_utc = datetime.now(timezone.utc)
    cutoff  = now_utc + timedelta(minutes=30)
    url     = f"{GAMMA_URL}/markets"
    params  = {
        "closed":       "false",
        "limit":        50,
        "order":        "endDate",
        "ascending":    "true",
        "end_date_min": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end_date_max": cutoff.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "_t":           int(now_utc.timestamp()),
        "_r":           random.randint(1000, 9999),
    }
    r = await client.get(url, params=params, timeout=10.0)
    r.raise_for_status()
    markets = r.json()
    if not isinstance(markets, list):
        markets = markets.get("markets", [])

    for m in markets:
        q = m.get("question", "").lower()
        if ("up" in q and "down" in q) and ("btc" in q or "bitcoin" in q):
            return m

    # ถ้าไม่เจอใน 30 นาที ขยายเป็น 2 ชั่วโมง
    params["end_date_max"] = (now_utc + timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    r = await client.get(url, params=params, timeout=10.0)
    r.raise_for_status()
    markets = r.json()
    if not isinstance(markets, list):
        markets = markets.get("markets", [])
    for m in markets:
        q = m.get("question", "").lower()
        if ("up" in q and "down" in q) and ("btc" in q or "bitcoin" in q):
            return m

    return None


async def gamma_get_prices(
    client: httpx.AsyncClient,
    condition_id: str,
    market: dict | None = None,
) -> dict:
    """
    ดึงราคา Up/Down จาก Gamma
    ถ้ามี market dict อยู่แล้วจะใช้เลย ไม่ fetch ซ้ำ
    Return: {"up": float|None, "down": float|None, "raw": dict}
    """
    if market is None:
        url = f"{GAMMA_URL}/markets"
        r = await client.get(url, params={"id": condition_id}, timeout=10.0)
        r.raise_for_status()
        data = r.json()
        market = data[0] if isinstance(data, list) and data else data

    up = down = None

    # outcomePrices = ["0.58", "0.42"]  →  [0]=Up [1]=Down
    op = market.get("outcomePrices")
    if op is not None:
        try:
            prices = json.loads(op) if isinstance(op, str) else op
            if isinstance(prices, list) and len(prices) >= 2:
                up   = round(float(prices[0]), 4)
                down = round(float(prices[1]), 4)
        except (ValueError, TypeError):
            pass

    # fallback: bestBid/bestAsk mid (only gives YES)
    if up is None:
        bid = market.get("bestBid")
        ask = market.get("bestAsk")
        if bid is not None and ask is not None:
            up = round((float(bid) + float(ask)) / 2, 4)
            down = round(1 - up, 4) if up is not None else None

    return {"up": up, "down": down, "raw": market}


# ──────────────────────────────────────────────────────────────────────────────
# CLOB helpers
# ──────────────────────────────────────────────────────────────────────────────

async def clob_get_midpoint(client: httpx.AsyncClient, token_id: str) -> float | None:
    """POST /midpoints → midpoint price สำหรับ token เดียว"""
    r = await client.post(
        f"{CLOB_URL}/midpoints",
        json=[{"token_id": token_id}],
        timeout=5.0,
    )
    r.raise_for_status()
    data = r.json()
    val = data.get(token_id)
    return float(val) if val is not None else None


async def clob_get_orderbook(client: httpx.AsyncClient, token_id: str) -> dict:
    """GET /book?token_id=... → top-of-book bid/ask"""
    r = await client.get(
        f"{CLOB_URL}/book",
        params={"token_id": token_id},
        timeout=5.0,
    )
    r.raise_for_status()
    book = r.json()
    bids = book.get("bids", [])
    asks = book.get("asks", [])
    best_bid = float(bids[0]["price"]) if bids else None
    best_ask = float(asks[0]["price"]) if asks else None
    mid = round((best_bid + best_ask) / 2, 4) if best_bid and best_ask else None
    return {"bid": best_bid, "ask": best_ask, "mid": mid}


async def clob_get_prices(
    client: httpx.AsyncClient, yes_token: str, no_token: str
) -> dict:
    """ดึงทั้ง midpoint และ orderbook ของ yes/no token พร้อมกัน"""
    mid_yes, mid_no, ob_yes, ob_no = await asyncio.gather(
        clob_get_midpoint(client, yes_token),
        clob_get_midpoint(client, no_token),
        clob_get_orderbook(client, yes_token),
        clob_get_orderbook(client, no_token),
        return_exceptions=True,
    )
    return {
        "yes_mid":       mid_yes if not isinstance(mid_yes, Exception) else None,
        "no_mid":        mid_no  if not isinstance(mid_no,  Exception) else None,
        "yes_orderbook": ob_yes  if not isinstance(ob_yes,  Exception) else None,
        "no_orderbook":  ob_no   if not isinstance(ob_no,   Exception) else None,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Token ID helpers (ดึงจาก Gamma market data)
# ──────────────────────────────────────────────────────────────────────────────

def extract_tokens(market: dict) -> tuple[str | None, str | None]:
    """
    Gamma market มี clobTokenIds = '["<yes_token>","<no_token>"]'
    Return (yes_token, no_token)
    """
    raw = market.get("clobTokenIds")
    if raw is None:
        return None, None
    try:
        tokens = json.loads(raw) if isinstance(raw, str) else raw
        if isinstance(tokens, list) and len(tokens) >= 2:
            return tokens[0], tokens[1]
    except (ValueError, TypeError):
        pass
    return None, None


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def fmt(val: float | None) -> str:
    return f"{val:.4f}" if val is not None else "N/A"


async def main() -> None:
    async with httpx.AsyncClient() as client:

        # ── 1. หา market ──────────────────────────────────────────────
        yes_token = no_token = condition_id = None

        if len(sys.argv) == 3:
            yes_token, no_token = sys.argv[1], sys.argv[2]
            print(f"[manual tokens]  YES={yes_token[:12]}…  NO={no_token[:12]}…\n")

        else:
            if len(sys.argv) == 2:
                condition_id = sys.argv[1]
                print(f"[condition_id]  {condition_id}\n")
            else:
                print(f"[auto] ค้นหาตลาด 5-min active จาก Gamma …")
                market = await gamma_find_active_5min(client)
                if market is None:
                    print("  ไม่พบตลาด active")
                    return
                condition_id = market.get("conditionId") or market.get("id")
                end_date     = market.get("endDate", "?")
                question     = market.get("question", "?")
                print(f"  question : {question}")
                print(f"  end_date : {end_date}")
                print(f"  condition: {condition_id}\n")
                yes_token, no_token = extract_tokens(market)
                # เก็บ market dict ไว้ใช้เลย ไม่ต้อง fetch ซ้ำ
                prefetched_market = market

        # ── 2. Gamma prices ───────────────────────────────────────────
        print("=" * 55)
        print("GAMMA")
        print("=" * 55)
        g: dict = {"up": None, "down": None, "raw": {}}
        if "prefetched_market" in dir():
            # ใช้ข้อมูลที่มีอยู่แล้ว ไม่ fetch ซ้ำ
            t0 = time.perf_counter()
            g = await gamma_get_prices(client, prefetched_market.get("id", condition_id), prefetched_market)
            ms = (time.perf_counter() - t0) * 1000
            print(f"  Up   (outcomePrices[0]) : {fmt(g['up'])}")
            print(f"  Down (outcomePrices[1]) : {fmt(g['down'])}")
            print(f"  latency : {ms:.0f} ms")
        elif condition_id:
            t0 = time.perf_counter()
            g = await gamma_get_prices(client, condition_id)
            ms = (time.perf_counter() - t0) * 1000
            print(f"  Up   (outcomePrices[0]) : {fmt(g['up'])}")
            print(f"  Down (outcomePrices[1]) : {fmt(g['down'])}")
            print(f"  latency : {ms:.0f} ms")
            if yes_token is None:
                yes_token, no_token = extract_tokens(g["raw"])
        else:
            print("  (ข้าม – ไม่มี condition_id)")

        # ── 3. CLOB prices ────────────────────────────────────────────
        print()
        print("=" * 55)
        print("CLOB")
        print("=" * 55)
        if yes_token and no_token:
            print(f"  YES token : {yes_token[:20]}…")
            print(f"  NO  token : {no_token[:20]}…")
            t0 = time.perf_counter()
            try:
                c = await clob_get_prices(client, yes_token, no_token)
                ms = (time.perf_counter() - t0) * 1000
                print(f"\n  midpoints:")
                print(f"    YES mid : {fmt(c['yes_mid'])}")
                print(f"    NO  mid : {fmt(c['no_mid'])}")
                if isinstance(c["yes_orderbook"], dict):
                    ob = c["yes_orderbook"]
                    print(f"\n  YES orderbook (top):")
                    print(f"    bid={fmt(ob['bid'])}  ask={fmt(ob['ask'])}  mid={fmt(ob['mid'])}")
                if isinstance(c["no_orderbook"], dict):
                    ob = c["no_orderbook"]
                    print(f"\n  NO orderbook (top):")
                    print(f"    bid={fmt(ob['bid'])}  ask={fmt(ob['ask'])}  mid={fmt(ob['mid'])}")
                print(f"\n  latency : {ms:.0f} ms")
            except Exception as exc:
                print(f"  ERROR: {repr(exc)}")
        else:
            print("  (ข้าม – ไม่มี token IDs)")

        # ── 4. สรุป ───────────────────────────────────────────────────
        print()
        print("=" * 55)
        print("SUMMARY")
        print("=" * 55)
        if condition_id:
            g_up   = g.get("up")
            g_down = g.get("down")
        else:
            g_up = g_down = None
        if yes_token and no_token and isinstance(c.get("yes_orderbook"), dict):
            clob_up   = c["yes_orderbook"]["mid"]
            clob_down = c["no_orderbook"]["mid"] if isinstance(c.get("no_orderbook"), dict) else None
        else:
            clob_up = clob_down = None
        print(f"  {'':15s}  {'Gamma':>8}  {'CLOB':>8}")
        print(f"  {'Up':15s}  {fmt(g_up):>8}  {fmt(clob_up):>8}")
        print(f"  {'Down':15s}  {fmt(g_down):>8}  {fmt(clob_down):>8}")


if __name__ == "__main__":
    asyncio.run(main())
