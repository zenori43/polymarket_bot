from __future__ import annotations

import time

from config import settings


class DynamicDeltaManager:
    def __init__(self, base_threshold: float, sustain_seconds: float = settings.DYNAMIC_DELTA_SUSTAIN_SECONDS) -> None:
        self._base: float = base_threshold
        self._sustain: float = sustain_seconds
        self._threshold: float = base_threshold
        self._stage_start: float = time.monotonic()

    @property
    def threshold(self) -> float:
        return self._threshold

    def update(self, price_delta_pct: float, now_ts: float) -> float:
        abs_delta = abs(price_delta_pct)

        if abs_delta < 0.2:
            self._threshold = self._base
            self._stage_start = now_ts
            return self._threshold

        if abs_delta >= 0.4:
            step = 0.15
        elif abs_delta >= 0.3:
            step = 0.10
        else:
            step = 0.05

        if now_ts - self._stage_start >= self._sustain:
            self._threshold += step
            self._stage_start = now_ts

        return self._threshold

    def is_confirmed(self, price_delta_pct: float) -> bool:
        return abs(price_delta_pct) >= settings.CONFIRM_THRESHOLD
