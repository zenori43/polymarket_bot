"""
core/polymarket_client.py

Async HTTP client for all Polymarket REST endpoints.

Endpoints used:
  CLOB    → https://clob.polymarket.com
  Gamma   → https://gamma-api.polymarket.com
  Data    → https://data-api.polymarket.com
"""

from __future__ import annotations

import time
from typing import Optional

import httpx

from config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

# Convert milliseconds → seconds for httpx timeout
_CLOB_TIMEOUT_S: float = settings.CLOB_TIMEOUT_MS / 1000.0


class PolymarketClient:
    """
    Async client for the Polymarket REST APIs.
    An httpx.AsyncClient is created lazily and reused across calls.
    Call ``close()`` (or use as an async context manager) to release connections.
    """

    def __init__(self, clob_client: "ClobClient | None" = None) -> None:
        self._clob = clob_client   # py-clob-client instance (optional)
        self._http = httpx.AsyncClient(timeout=settings.CLOB_TIMEOUT_MS / 1000)
        self._client: Optional[httpx.AsyncClient] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def _get_client(self) -> httpx.AsyncClient:
        """Return (and lazily create) the shared async HTTP client."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(10.0),  # default; overridden per-request where needed
                follow_redirects=True,
            )
        return self._client

    async def close(self) -> None:
        """Close underlying HTTP connections."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            logger.info("PolymarketClient HTTP client closed")

    async def __aenter__(self) -> "PolymarketClient":
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Price helpers
    # ------------------------------------------------------------------

    async def get_price_clob(self, market_id: str) -> Optional[float]:
        """
        Fetch best mid-price from the CLOB API.

        GET https://clob.polymarket.com/prices?token_id={market_id}

        Returns None on timeout or any error.
        """
        url = f"{settings.CLOB_URL}/prices"
        params = {"token_id": market_id}
        try:
            client = await self._get_client()
            resp = await client.get(
                url,
                params=params,
                timeout=httpx.Timeout(_CLOB_TIMEOUT_S),
            )
            resp.raise_for_status()
            data = resp.json()
            # Expected shape: {"price": "0.52"} or {"mid": "0.52"}
            price_str: Optional[str] = data.get("price") or data.get("mid")
            if price_str is None:
                logger.warning(f"CLOB price response missing 'price'/'mid' key for {market_id}: {data}")
                return None
            price = float(price_str)
            logger.debug(f"CLOB price for {market_id}: {price}")
            return price
        except httpx.TimeoutException:
            logger.warning(f"CLOB price request timed out for market_id={market_id}")
            return None
        except Exception as exc:
            logger.error(f"CLOB price fetch error for market_id={market_id}: {exc}")
            return None

    async def get_price_gamma(self, market_id: str) -> Optional[float]:
        """
        Fetch price from the Gamma API as a fallback.

        GET https://gamma-api.polymarket.com/markets?id={market_id}

        Returns None on any error.
        """
        url = f"{settings.GAMMA_URL}/markets"
        params = {"id": market_id}
        try:
            client = await self._get_client()
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
            # data may be a list or a single dict depending on the endpoint version
            market = data[0] if isinstance(data, list) and data else data
            # Try common field names used by the Gamma API
            for key in ("bestAsk", "bestBid", "price", "outcomePrices"):
                val = market.get(key)
                if val is not None:
                    # outcomePrices is sometimes a JSON string like '["0.52","0.48"]'
                    if isinstance(val, str):
                        try:
                            import json
                            parsed = json.loads(val)
                            if isinstance(parsed, list) and parsed:
                                price = float(parsed[0])
                            else:
                                price = float(val)
                        except (ValueError, TypeError):
                            price = float(val)
                    elif isinstance(val, list) and val:
                        price = float(val[0])
                    else:
                        price = float(val)
                    logger.debug(f"Gamma price for {market_id}: {price}")
                    return price
            logger.warning(f"Gamma price response has no known price field for {market_id}: {market}")
            return None
        except Exception as exc:
            logger.error(f"Gamma price fetch error for market_id={market_id}: {exc}")
            return None

    async def get_price(self, market_id: str) -> Optional[float]:
        """
        Fetch price with CLOB → Gamma fallback.

        Returns None if both endpoints fail.
        """
        price = await self.get_price_clob(market_id)
        if price is not None:
            return price
        logger.info(f"CLOB price unavailable for {market_id}, falling back to Gamma")
        return await self.get_price_gamma(market_id)

    # ------------------------------------------------------------------
    # Position helpers
    # ------------------------------------------------------------------

    async def get_open_positions(self, wallet: str) -> list[dict]:
        """
        Return all open positions for the given wallet address.

        GET https://data-api.polymarket.com/positions?user={wallet}
        """
        url = f"{settings.DATA_API_URL}/positions"
        params = {"user": wallet}
        try:
            client = await self._get_client()
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            positions: list[dict] = resp.json()
            logger.debug(f"get_open_positions: {len(positions)} positions for {wallet}")
            return positions
        except Exception as exc:
            logger.error(f"get_open_positions error for wallet={wallet}: {exc}")
            return []

    async def get_resolved_claimable(self, wallet: str) -> list[dict]:
        """
        Return positions that are resolved and not yet redeemed.

        Filters get_open_positions() for:
          - outcome_index is not None  (outcome is known)
          - redeemed == False          (not yet claimed)
        """
        try:
            positions = await self.get_open_positions(wallet)
            claimable = [
                p for p in positions
                if p.get("outcome_index") is not None and p.get("redeemed") is False
            ]
            logger.info(f"get_resolved_claimable: {len(claimable)} claimable positions for {wallet}")
            return claimable
        except Exception as exc:
            logger.error(f"get_resolved_claimable error for wallet={wallet}: {exc}")
            return []

    # ------------------------------------------------------------------
    # Order / Claim submission
    # ------------------------------------------------------------------

    async def submit_order(self, order_args: dict) -> dict:
        """
        ส่ง order โดยใช้ py-clob-client ถ้ามี, fallback httpx ถ้าไม่มี
        order_args ต้องมี: token_id, price, size, side ("BUY"/"SELL")
        """
        # Dry run – simulate accepted order
        if settings.DRY_RUN:
            fake_id = f"DRY-{int(time.time())}"
            logger.info(f"[DRY RUN] submit_order simulated – fake_id={fake_id} args={order_args}")
            return {"orderID": fake_id, "status": "matched", "dry_run": True}

        if self._clob is not None:
            try:
                from py_clob_client.clob_types import OrderArgs, OrderType
                from py_clob_client.constants import BUY, SELL
                side = BUY if order_args["side"] == "BUY" else SELL
                args = OrderArgs(
                    token_id=order_args["token_id"],
                    price=order_args["price"],
                    size=order_args["size"],
                    side=side,
                )
                signed = self._clob.create_order(args)
                result = self._clob.post_order(signed, OrderType.GTC)
                logger.info(f"submit_order (clob) response: {result}")
                return result if isinstance(result, dict) else {"status": "ok", "result": result}
            except Exception as exc:
                logger.error(f"submit_order (clob) error: {exc}")
                return {"error": str(exc)}

        # fallback: httpx POST (signed_order passed directly as body)
        url = f"{settings.CLOB_URL}/order"
        try:
            client = await self._get_client()
            resp = await client.post(
                url,
                json=order_args,
                timeout=httpx.Timeout(_CLOB_TIMEOUT_S),
            )
            resp.raise_for_status()
            result_http: dict = resp.json()
            logger.info(f"submit_order (httpx) response: {result_http}")
            return result_http
        except httpx.TimeoutException:
            logger.error("submit_order timed out")
            return {"error": "timeout"}
        except Exception as exc:
            logger.error(f"submit_order error: {exc}")
            return {"error": str(exc)}

    async def market_sell_fok(self, token_id: str, size: float) -> dict:
        """
        FOK Market Sell — ใช้สำหรับ stop loss และ panic sell
        Fill-or-Kill ทันที ไม่รอ
        """
        if settings.DRY_RUN:
            logger.info(f"[DRY RUN] market_sell_fok simulated – token_id={token_id} size={size}")
            return {"status": "filled", "dry_run": True}

        if self._clob is not None:
            try:
                from py_clob_client.clob_types import MarketOrderArgs
                from py_clob_client.constants import SELL
                args = MarketOrderArgs(token_id=token_id, amount=size, side=SELL)
                signed = self._clob.create_market_order(args)
                result = self._clob.post_order(signed, order_type="FOK")
                logger.info(f"market_sell_fok response: {result}")
                return result if isinstance(result, dict) else {"status": "ok"}
            except Exception as exc:
                logger.error(f"market_sell_fok error: {exc}")
                return {"error": str(exc)}
        logger.error("market_sell_fok requires ClobClient — not available")
        return {"error": "no_clob_client"}

    async def cancel_order(self, order_id: str) -> bool:
        """Cancel unfilled order"""
        if self._clob is not None:
            try:
                self._clob.cancel(order_id)
                return True
            except Exception as exc:
                logger.error(f"cancel_order failed: {exc}")
        return False

    async def has_filled_position(self, market_id: str) -> bool:
        """
        ตรวจว่ามี filled position จริงหรือไม่
        เช็คทั้ง get_positions() และ filled orders
        """
        # เช็ค open positions
        positions = await self.get_open_positions(settings.WALLET_ADDRESS)
        for p in positions:
            if p.get("asset_id") == market_id or p.get("market") == market_id:
                if float(p.get("size", 0)) > 0:
                    return True
        # เช็ค filled orders จาก CLOB
        if self._clob is not None:
            try:
                orders = self._clob.get_orders()
                if isinstance(orders, list):
                    for o in orders:
                        if (o.get("asset_id") == market_id or o.get("market") == market_id):
                            if o.get("status") in ("FILLED", "MATCHED"):
                                return True
            except Exception as exc:
                logger.warning(f"has_filled_position orders check failed: {exc}")
        return False

    # ------------------------------------------------------------------
    # Market discovery
    # ------------------------------------------------------------------

    async def get_market_by_id(self, market_id: str) -> dict | None:
        """
        GET https://gamma-api.polymarket.com/markets/{id}
        return dict หรือ None ถ้า error/404
        """
        url = f"{settings.GAMMA_URL}/markets/{market_id}"
        try:
            client = await self._get_client()
            resp = await client.get(url, timeout=httpx.Timeout(5.0))
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            logger.error(f"get_market_by_id({market_id}) error: {exc}")
            return None

    async def find_active_btc_markets(self, limit: int = 50) -> list[dict]:
        """
        ค้นหา BTC 5-min Up/Down markets
        filter: question มี 'up or down' + 'bitcoin'/'btc', acceptingOrders=True
        และ endDate อยู่ภายใน 10 นาทีข้างหน้า (5-min round)
        """
        import json as _json
        from datetime import datetime, timezone, timedelta
        url = f"{settings.GAMMA_URL}/markets"
        now_utc = datetime.now(timezone.utc)
        cutoff = now_utc + timedelta(minutes=10)
        params = {"active": "true", "limit": limit, "order": "endDate", "ascending": "true"}
        try:
            client = await self._get_client()
            resp = await client.get(url, params=params, timeout=httpx.Timeout(5.0))
            resp.raise_for_status()
            markets: list[dict] = resp.json()
            filtered = []
            for m in markets:
                q = m.get("question", "").lower()
                # ต้องมีคำว่า up/down และ btc/bitcoin
                if not (("up" in q and "down" in q) and ("btc" in q or "bitcoin" in q)):
                    continue
                if not m.get("acceptingOrders", False):
                    continue
                # ต้องหมดภายใน 10 นาที
                end_str = m.get("endDate", "")
                try:
                    end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                    if not (now_utc < end_dt <= cutoff):
                        continue
                except Exception:
                    continue
                filtered.append(m)
            logger.info(f"find_active_btc_markets: found {len(filtered)} BTC 5-min markets")
            return filtered
        except Exception as exc:
            logger.error(f"find_active_btc_markets error: {exc}")
            return []

    def extract_token_ids(self, market: dict) -> tuple[str, str] | None:
        """
        แยก YES และ NO token ID จาก market dict
        clobTokenIds[0] = YES token
        clobTokenIds[1] = NO token
        return (yes_token_id, no_token_id) หรือ None ถ้าไม่มีข้อมูล
        """
        token_ids = market.get("clobTokenIds") or market.get("tokens") or []
        if isinstance(token_ids, str):
            # บางครั้ง API return เป็น JSON string
            import json as _json
            try:
                token_ids = _json.loads(token_ids)
            except Exception:
                pass
        if isinstance(token_ids, list) and len(token_ids) >= 2:
            return str(token_ids[0]), str(token_ids[1])
        return None

    async def claim_reward(self, condition_id: str, signed_tx: dict) -> dict:
        """
        Claim / redeem a resolved market position.

        POST https://clob.polymarket.com/redeem
        Body : {"conditionId": condition_id, ...signed_tx}
        Returns response JSON dict.
        """
        url = f"{settings.CLOB_URL}/redeem"
        body = {"conditionId": condition_id, **signed_tx}
        try:
            client = await self._get_client()
            resp = await client.post(url, json=body)
            resp.raise_for_status()
            result: dict = resp.json()
            logger.info(f"claim_reward conditionId={condition_id} response: {result}")
            return result
        except Exception as exc:
            logger.error(f"claim_reward error for conditionId={condition_id}: {exc}")
            return {"error": str(exc)}

    # ------------------------------------------------------------------
    # Polygon blockchain helpers
    # ------------------------------------------------------------------

    async def get_usdc_balance_polygon(self, wallet: str) -> float:
        """ดึง USDC.e balance จาก Polygon blockchain (Polymarket ใช้ USDC.e)"""
        try:
            addr = wallet.lower().replace("0x", "").zfill(64)
            data = f"0x70a08231{addr}"
            payload = {
                "jsonrpc": "2.0", "method": "eth_call",
                "params": [{"to": "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174", "data": data}, "latest"],
                "id": 1,
            }
            client = await self._get_client()
            resp = await client.post(settings.RPC, json=payload, timeout=httpx.Timeout(5.0))
            resp.raise_for_status()
            result = resp.json().get("result") or "0x0"
            return int(result, 16) / 1_000_000.0
        except Exception as exc:
            logger.warning(f"get_usdc_balance_polygon error for {wallet}: {exc}")
            return 0.0

    async def get_position_shares_polygon(self, wallet: str, token_id: str) -> float:
        """ดึงจำนวน shares ของ position จาก Polygon CTF contract โดยตรง"""
        try:
            addr = wallet.lower().replace("0x", "").zfill(64)
            tid = hex(int(token_id)).replace("0x", "").zfill(64)
            data = f"0x00fdd58e{addr}{tid}"
            payload = {
                "jsonrpc": "2.0", "method": "eth_call",
                "params": [{"to": "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045", "data": data}, "latest"],
                "id": 1,
            }
            client = await self._get_client()
            resp = await client.post(settings.RPC, json=payload, timeout=httpx.Timeout(5.0))
            resp.raise_for_status()
            result = resp.json().get("result") or "0x0"
            return int(result, 16) / 1_000_000.0
        except Exception as exc:
            logger.warning(f"get_position_shares_polygon error for {wallet} token={token_id}: {exc}")
            return 0.0

    async def run_claim_cycle(self, wallet: str, wallet_obj: object) -> int:
        """
        Claim ทุก resolved position สำหรับ wallet นี้
        return จำนวน position ที่ claim สำเร็จ
        """
        claimed = 0
        try:
            claimable = await self.get_resolved_claimable(wallet)
            if not claimable:
                logger.info("run_claim_cycle: nothing to claim")
                return 0
            for position in claimable:
                condition_id = position.get("condition_id") or position.get("conditionId", "")
                amount = int(float(position.get("size", 0)))
                if not condition_id or amount == 0:
                    continue
                try:
                    signed_tx = wallet_obj.sign_redeem(condition_id, amount)
                    result = await self.claim_reward(condition_id, signed_tx)
                    if not result.get("error"):
                        claimed += 1
                        logger.info(f"run_claim_cycle: claimed conditionId={condition_id}")
                except Exception as exc:
                    logger.error(f"run_claim_cycle: failed conditionId={condition_id}: {exc}")
        except Exception as exc:
            logger.error(f"run_claim_cycle error: {exc}")
        return claimed

    async def market_buy_opposite(self, opposite_token_id: str, price: float, size: float) -> dict:
        """ซื้อ token ฝั่งตรงข้ามเพื่อ synthetic close เมื่อถือ shares < 5"""
        order_args = {
            "token_id": opposite_token_id,
            "price":    price,
            "size":     size,
            "side":     "BUY",
        }
        logger.info(f"market_buy_opposite: buying opposite token={opposite_token_id} size={size}")
        return await self.submit_order(order_args)
