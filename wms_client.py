import asyncio
import json
import logging
from typing import Any, Dict, List, Tuple

import aiohttp


class WMSClient:
    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        verify_ssl: bool = True,
        concurrency: int = 10,
        timeout_seconds: float = 30.0,
        retries: int = 3,
        backoff_base: float = 0.5,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.verify_ssl = verify_ssl
        self.concurrency = concurrency
        self.timeout_seconds = timeout_seconds
        self.retries = retries
        self.backoff_base = backoff_base

        # Client session resources are created in async context within fetch_all

    async def _fetch_total_pages(self, session: aiohttp.ClientSession, entity: str) -> int:
        url = f"{self.base_url}/wms/lgfapi/v10/entity/{entity}"
        async with session.get(url, params={"page": 1}, ssl=self.verify_ssl) as response:
            response.raise_for_status()
            data = await response.json()
            return int(data.get("page_count", 1))

    async def _fetch_page(
        self,
        session: aiohttp.ClientSession,
        entity: str,
        page: int,
    ) -> Tuple[int, List[Dict[str, Any]]]:
        url = f"{self.base_url}/wms/lgfapi/v10/entity/{entity}"
        attempt = 0
        while True:
            attempt += 1
            try:
                async with session.get(url, params={"page": page}, ssl=self.verify_ssl) as response:
                    if response.status >= 500:
                        raise aiohttp.ClientResponseError(
                            request_info=response.request_info,
                            history=response.history,
                            status=response.status,
                            message=f"Server error {response.status}",
                        )
                    response.raise_for_status()
                    data = await response.json()
                    results = data.get("results", [])
                    return page, results
            except (aiohttp.ClientConnectorError, aiohttp.ClientResponseError, asyncio.TimeoutError) as e:
                if attempt > self.retries:
                    logging.error("Falha página %s após %s tentativas (%s): %s", page, self.retries, entity, e)
                    return page, []
                sleep_s = self.backoff_base * (2 ** (attempt - 1))
                logging.warning(
                    "Erro ao buscar %s página %s (tentativa %s/%s): %s. Retentando em %.1fs",
                    entity,
                    page,
                    attempt,
                    self.retries,
                    e,
                    sleep_s,
                )
                await asyncio.sleep(sleep_s)

    async def fetch_all(self, entity: str, limit_pages: int | None = None) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
        connector = aiohttp.TCPConnector(limit=self.concurrency)
        async with aiohttp.ClientSession(
            auth=aiohttp.BasicAuth(self.username, self.password),
            timeout=timeout,
            connector=connector,
        ) as session:
            total_pages = await self._fetch_total_pages(session, entity)
            if limit_pages is not None:
                total_pages = min(total_pages, limit_pages)

            tasks = [
                asyncio.create_task(self._fetch_page(session=session, entity=entity, page=page))
                for page in range(1, total_pages + 1)
            ]

            for coro in asyncio.as_completed(tasks):
                try:
                    _page, page_items = await coro
                except Exception as e:
                    logging.error("Exceção em tarefa de página %s: %s", entity, e)
                    page_items = []
                if page_items:
                    items.extend(page_items)
        return items
