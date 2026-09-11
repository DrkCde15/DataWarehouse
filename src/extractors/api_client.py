"""Cliente API genérico com suporte a paginação e rate limiting."""

import time
import logging
from typing import Any, Generator

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class RateLimiter:
    """Controla taxa de requisições para evitar bloqueio pela API."""

    def __init__(self, max_requests_per_second: int = 10) -> None:
        self.min_interval = 1.0 / max_requests_per_second
        self.last_request_time = 0.0

    def wait_if_needed(self) -> None:
        """Aguarda se necessário para respeitar o rate limit."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_interval:
            sleep_time = self.min_interval - elapsed
            logger.debug(f"Rate limit: aguardando {sleep_time:.3f}s")
            time.sleep(sleep_time)
        self.last_request_time = time.time()


class APIClient:
    """Cliente HTTP genérico com paginação, rate limiting e retry."""

    def __init__(
        self,
        base_url: str,
        api_key: str | None = None,
        rate_limit: int = 10,
        max_retries: int = 3,
        timeout: int = 30,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.rate_limiter = RateLimiter(rate_limit)

        self.session = requests.Session()
        self.session.headers.update(headers or {})

        if api_key:
            self.session.headers["Authorization"] = f"Bearer {api_key}"

        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        self._request_count = 0
        self._total_records = 0

    def get(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Realiza uma requisição GET e retorna a resposta JSON."""
        self.rate_limiter.wait_if_needed()

        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        logger.info(f"GET {url} | params={params}")

        response = self.session.get(url, params=params, timeout=self.timeout)
        self._request_count += 1

        response.raise_for_status()
        data = response.json()
        logger.debug(f"Resposta recebida: {len(str(data))} bytes")
        return data

    def _paginate_none(
        self,
        endpoint: str,
        page_size: int,
        params: dict[str, Any] | None = None,
        results_key: str = "results",
    ) -> Generator[list[dict[str, Any]], None, None]:
        """Sem paginação — busca única e retorna os registros."""
        params = params or {}
        response = self.get(endpoint, params=params)

        if isinstance(response, list):
            results = response
        elif isinstance(response, dict):
            results = response.get(results_key, [response])
            if isinstance(results, dict):
                results = [results]
        else:
            results = [response]

        if results:
            yield results

    def _paginate_offset(
        self,
        endpoint: str,
        page_size: int,
        params: dict[str, Any] | None = None,
        results_key: str = "results",
    ) -> Generator[list[dict[str, Any]], None, None]:
        """Paginação por offset/limit."""
        offset = 0
        params = params or {}

        while True:
            page_params = {**params, "offset": offset, "limit": page_size}
            response = self.get(endpoint, params=page_params)

            if isinstance(response, list):
                results = response
            else:
                results = response.get(results_key, [])
            if not results:
                break

            yield results
            offset += len(results)

            if isinstance(response, list):
                break

            total = response.get("total") if isinstance(response, dict) else None
            if total is not None and offset >= total:
                break

    def _paginate_cursor(
        self,
        endpoint: str,
        page_size: int,
        params: dict[str, Any] | None = None,
        cursor_key: str = "next_cursor",
        results_key: str = "results",
    ) -> Generator[list[dict[str, Any]], None, None]:
        """Paginação por cursor."""
        params = params or {}
        cursor = None

        while True:
            page_params = {**params, "limit": page_size}
            if cursor:
                page_params["cursor"] = cursor

            response = self.get(endpoint, params=page_params)

            if isinstance(response, list):
                results = response
            else:
                results = response.get(results_key, [])
            if not results:
                break

            yield results

            cursor = response.get(cursor_key) if isinstance(response, dict) else None
            if not cursor:
                break

    def _paginate_link_header(
        self,
        endpoint: str,
        page_size: int,
        params: dict[str, Any] | None = None,
    ) -> Generator[list[dict[str, Any]], None, None]:
        """Paginação via Link headers (padrão GitHub)."""
        params = params or {}
        params["per_page"] = page_size

        url = endpoint
        while url:
            self.rate_limiter.wait_if_needed()

            full_url = f"{self.base_url}/{url.lstrip('/')}" if not url.startswith("http") else url
            logger.info(f"GET {full_url}")

            response = self.session.get(full_url, params=params, timeout=self.timeout)
            self._request_count += 1
            response.raise_for_status()

            results = response.json()
            if isinstance(results, dict):
                results = results.get("results", results.get("data", []))

            yield results

            url = response.links.get("next", {}).get("url")
            params = {}

    def get_all_pages(
        self,
        endpoint: str,
        pagination_type: str = "offset",
        page_size: int = 100,
        params: dict[str, Any] | None = None,
        results_key: str = "results",
    ) -> list[dict[str, Any]]:
        """
        Busca todos os registros de um endpoint com paginação.

        Args:
            endpoint: Caminho do endpoint (ex: /usuarios)
            pagination_type: Tipo de paginação ('offset', 'cursor', 'link')
            page_size: Tamanho da página
            params: Parâmetros adicionais de query string
            results_key: Chave onde estão os resultados na resposta

        Returns:
            Lista com todos os registros
        """
        generators = {
            "none": lambda: self._paginate_none(endpoint, page_size, params, results_key),
            "offset": lambda: self._paginate_offset(endpoint, page_size, params, results_key),
            "cursor": lambda: self._paginate_cursor(endpoint, page_size, params, results_key=results_key),
            "link": lambda: self._paginate_link_header(endpoint, page_size, params),
        }

        if pagination_type not in generators:
            raise ValueError(f"Tipo de paginação inválido: {pagination_type}")

        all_records: list[dict[str, Any]] = []
        for page in generators[pagination_type]():
            self._total_records += len(page)
            all_records.extend(page)
            logger.info(f"Acumulado: {self._total_records} registros")

        logger.info(f"Total extraído: {self._total_records} registros de {endpoint}")
        return all_records

    def get_paginated_generator(
        self,
        endpoint: str,
        pagination_type: str = "offset",
        page_size: int = 100,
        params: dict[str, Any] | None = None,
        results_key: str = "results",
    ) -> Generator[list[dict[str, Any]], None, None]:
        """
        Retorna um generator que yield cada página.
        Útil para processar dados em streaming sem carregar tudo na memória.
        """
        generators = {
            "none": self._paginate_none,
            "offset": self._paginate_offset,
            "cursor": self._paginate_cursor,
            "link": self._paginate_link_header,
        }

        if pagination_type not in generators:
            raise ValueError(f"Tipo de paginação inválido: {pagination_type}")

        yield from generators[pagination_type](endpoint, page_size, params, results_key=results_key)

    @property
    def stats(self) -> dict[str, int]:
        """Retorna estatísticas de uso do cliente."""
        return {
            "requests_realizados": self._request_count,
            "total_registros_extraidos": self._total_records,
        }
