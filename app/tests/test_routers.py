import json
from datetime import date
from parser.async_download.db_depends import get_async_db
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from app.main import fast_api_app
from app.routers import get_redis


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


@pytest.fixture(autouse=True)
def cleanup_overrides():
    yield
    fast_api_app.dependency_overrides.clear()

@pytest.mark.skip
@pytest.mark.asyncio
@patch("app.routers.is_after_1411", return_value=False)
async def test_get_last_trading_dates(mock_is_after):
    mock_redis = AsyncMock()
    mock_redis.get.return_value = json.dumps(["2025-12-10"])

    fast_api_app.dependency_overrides[get_redis] = lambda: mock_redis
    async with AsyncClient(
        transport=ASGITransport(app=fast_api_app), base_url="http://test"
    ) as ac:
        response = await ac.get("/last_dates")

    assert response.status_code == 200
    assert response.json() == [{"date": "2025-12-10"}]


@pytest.mark.asyncio
@patch("app.routers.is_after_1411", return_value=True)
async def test_get_last_trading_dates_with_is_after_1411(mock_is_after):
    async def mock_db_scalars(*args):
        result = MagicMock()
        result.all.return_value = [date(2025, 12, 10)]
        return result

    mock_db = AsyncMock()
    mock_db.scalars = mock_db_scalars

    mock_cache = AsyncMock()

    # 3. Переопределяем ОБЕ зависимости
    fast_api_app.dependency_overrides[get_async_db] = lambda: mock_db
    fast_api_app.dependency_overrides[get_redis] = lambda: mock_cache

    async with AsyncClient(
        transport=ASGITransport(app=fast_api_app), base_url="http://test"
    ) as ac:
        response = await ac.get("/last_dates", params={"limit_days": 10})

    assert response.status_code == 200
    assert response.json() == [{"date": "2025-12-10"}]


@pytest.mark.asyncio
@patch("app.routers.is_after_1411", return_value=False)
async def test_get_dynamics(mock_is_after, return_data):
    mock_redis = AsyncMock()
    mock_redis.get.return_value = json.dumps(return_data)

    fast_api_app.dependency_overrides[get_redis] = lambda: mock_redis
    async with AsyncClient(
        transport=ASGITransport(app=fast_api_app), base_url="http://test"
    ) as ac:
        response = await ac.get("/get_dynamics")
    assert response.status_code == 200
    data = response.json()
    assert data == return_data


@pytest.mark.asyncio
@patch("app.routers.is_after_1411", return_value=True)
async def test_get_dynamics_with_is_after_1411(mock_is_after, return_data):
    async def mock_db_scalars(*args):
        result = MagicMock()
        result.all.return_value = return_data
        return result

    mock_db = AsyncMock()
    mock_db.scalars = mock_db_scalars

    mock_cache = AsyncMock()

    fast_api_app.dependency_overrides[get_async_db] = lambda: mock_db
    fast_api_app.dependency_overrides[get_redis] = lambda: mock_cache

    async with AsyncClient(
        transport=ASGITransport(app=fast_api_app), base_url="http://test"
    ) as ac:
        response = await ac.get("/get_dynamics")

    assert response.status_code == 200
    assert response.json() == return_data


@pytest.mark.asyncio
@patch("app.routers.is_after_1411", return_value=False)
async def test_get_trading_results(mock_is_after, return_data):
    mock_redis = AsyncMock()
    mock_redis.get.return_value = json.dumps(return_data)

    fast_api_app.dependency_overrides[get_redis] = lambda: mock_redis

    async with AsyncClient(
        transport=ASGITransport(app=fast_api_app), base_url="http://test"
    ) as ac:
        response = await ac.get("/get_trading_results")

    assert response.status_code == 200
    assert response.json() == return_data


@pytest.mark.asyncio
@patch("app.routers.is_after_1411", return_value=True)
async def test_get_trading_results_with_is_after_1411(mock_is_after, return_data):
    async def mock_db_scalars(*args):
        result = MagicMock()
        result.all.return_value = return_data
        return result

    mock_db = AsyncMock()
    mock_db.scalars = mock_db_scalars

    mock_cache = AsyncMock()

    fast_api_app.dependency_overrides[get_async_db] = lambda: mock_db
    fast_api_app.dependency_overrides[get_redis] = lambda: mock_cache

    async with AsyncClient(
        transport=ASGITransport(app=fast_api_app), base_url="http://test"
    ) as ac:
        response = await ac.get("/get_trading_results")

    assert response.status_code == 200
    assert response.json() == return_data
