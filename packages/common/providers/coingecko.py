from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests


@dataclass(frozen=True)
class CoinGeckoQuote:
    coin_id: str
    vs_currency: str
    price: float
    last_updated_at: int | None  # unix seconds (provider field)


class CoinGeckoClient:
    def __init__(self, base_url: str, timeout_s: float = 20.0) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout_s = timeout_s

    def fetch_simple_prices(
        self,
        *,
        coin_ids: list[str],
        vs_currency: str,
        include_last_updated_at: bool = True,
    ) -> list[CoinGeckoQuote]:
        if not coin_ids:
            return []

        url = f"{self._base_url}/simple/price"
        params = {
            "ids": ",".join(coin_ids),
            "vs_currencies": vs_currency,
        }
        if include_last_updated_at:
            params["include_last_updated_at"] = "true"

        resp = requests.get(url, params=params, timeout=self._timeout_s)
        resp.raise_for_status()
        payload: dict[str, Any] = resp.json()

        quotes: list[CoinGeckoQuote] = []
        for coin_id in coin_ids:
            data = payload.get(coin_id)
            if not isinstance(data, dict):
                continue
            raw_price = data.get(vs_currency)
            if raw_price is None:
                continue
            try:
                price = float(raw_price)
            except (TypeError, ValueError):
                continue

            lua = data.get("last_updated_at")
            last_updated_at: int | None
            try:
                last_updated_at = int(lua) if lua is not None else None
            except (TypeError, ValueError):
                last_updated_at = None

            quotes.append(
                CoinGeckoQuote(
                    coin_id=coin_id,
                    vs_currency=vs_currency,
                    price=price,
                    last_updated_at=last_updated_at,
                )
            )

        return quotes

