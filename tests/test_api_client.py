"""Testes unitarios para APIClient."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from src.extractors.api_client import APIClient, RateLimiter


class TestRateLimiter:
    def test_init_default(self):
        rl = RateLimiter()
        assert rl.min_interval == 0.1

    def test_init_custom(self):
        rl = RateLimiter(max_requests_per_second=5)
        assert rl.min_interval == 0.2


class TestAPIClient:
    def setup_method(self):
        self.client = APIClient(base_url="https://api.example.com", rate_limit=100)

    def test_init_strips_trailing_slash(self):
        client = APIClient(base_url="https://api.example.com/")
        assert client.base_url == "https://api.example.com"

    def test_init_with_api_key(self):
        client = APIClient(base_url="https://api.example.com", api_key="test-key")
        assert client.session.headers["Authorization"] == "Bearer test-key"

    def test_init_without_api_key(self):
        assert "Authorization" not in self.client.session.headers

    @patch("src.extractors.api_client.requests.Session")
    def test_get_success(self, mock_session_class):
        mock_session = MagicMock()
        mock_response = Mock()
        mock_response.json.return_value = {"data": "test"}
        mock_response.raise_for_status = Mock()
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session

        client = APIClient(base_url="https://api.example.com")
        result = client.get("/test", params={"key": "value"})

        assert result == {"data": "test"}
        assert client._request_count == 1

    def test_get_all_pages_invalid_pagination(self):
        with pytest.raises(ValueError, match="Tipo de paginação inválido"):
            self.client.get_all_pages("/test", pagination_type="invalid")

    @patch.object(APIClient, "get")
    def test_paginate_none_list_response(self, mock_get):
        mock_get.return_value = [{"id": 1}, {"id": 2}]
        result = self.client.get_all_pages("/test", pagination_type="none")
        assert result == [{"id": 1}, {"id": 2}]

    @patch.object(APIClient, "get")
    def test_paginate_none_dict_with_key(self, mock_get):
        mock_get.return_value = {"coins": [{"id": 1}, {"id": 2}]}
        result = self.client.get_all_pages(
            "/test", pagination_type="none", results_key="coins"
        )
        assert result == [{"id": 1}, {"id": 2}]

    @patch.object(APIClient, "get")
    def test_paginate_offset_list_response(self, mock_get):
        mock_get.return_value = [{"id": 1}, {"id": 2}]
        result = self.client.get_all_pages("/test", pagination_type="offset")
        assert result == [{"id": 1}, {"id": 2}]

    @patch.object(APIClient, "get")
    def test_paginate_offset_dict_with_total(self, mock_get):
        mock_get.side_effect = [
            {"results": [{"id": 1}], "total": 2},
            {"results": [{"id": 2}], "total": 2},
        ]
        result = self.client.get_all_pages("/test", pagination_type="offset")
        assert result == [{"id": 1}, {"id": 2}]

    @patch.object(APIClient, "get")
    def test_paginate_cursor(self, mock_get):
        mock_get.side_effect = [
            {"results": [{"id": 1}], "next_cursor": "abc"},
            {"results": [{"id": 2}], "next_cursor": None},
        ]
        result = self.client.get_all_pages("/test", pagination_type="cursor")
        assert result == [{"id": 1}, {"id": 2}]

    @patch.object(APIClient, "get")
    def test_stats(self, mock_get):
        mock_get.return_value = [{"id": 1}]
        self.client.get_all_pages("/test", pagination_type="none")
        stats = self.client.stats
        assert stats["total_registros_extraidos"] == 1

    @patch.object(APIClient, "get")
    def test_empty_response(self, mock_get):
        mock_get.return_value = []
        result = self.client.get_all_pages("/test", pagination_type="none")
        assert result == []
