import json
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from app.main import fast_api_app


@pytest.fixture
def return_data():
    return [
        {
            "exchange_product_id": "DE15YAI065F",
            "exchange_product_name": "ДТ ЕВРО класс 1 (ДТ-З-К5) минус 26, ст. Яничкино (ст. отправления)",
            "delivery_basis_name": "ст. Яничкино",
            "volume": 650,
            "total": 45961630,
            "count": 10,
        }
    ]


@pytest.mark.asyncio
@patch("app.routers.client")
async def test_get_last_trading_dates(mock_client):
    mock_client.get.return_value = json.dumps(["2025-12-10"]).encode("utf-8")
    client = TestClient(fast_api_app)
    response = client.get("/last_dates")
    assert response.status_code == 200
    mock_client.get.assert_called()
    data = response.json()
    assert data == [{"date": "2025-12-10"}]


@pytest.mark.asyncio
@patch("app.routers.client")
async def test_get_dynamics(mock_client, return_data):
    mock_client.get.return_value = json.dumps(return_data).encode("utf-8")
    client = TestClient(fast_api_app)
    response = client.get("/get_dynamics")
    assert response.status_code == 200
    mock_client.get.assert_called()
    data = response.json()
    assert data == return_data


@pytest.mark.asyncio
@patch("app.routers.client")
async def test_get_trading_results(mock_client, return_data):
    mock_client.get.return_value = json.dumps(return_data).encode("utf-8")
    client = TestClient(fast_api_app)
    response = client.get("/get_trading_results")
    assert response.status_code == 200
    mock_client.get.assert_called()
    assert response.json() == return_data
