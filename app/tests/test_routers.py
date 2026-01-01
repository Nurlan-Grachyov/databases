import pytest

import app
from app.routers import get_last_trading_dates
from app.schemas import Dates
from parser.async_download.db_depends import get_async_db


@pytest.mark.asyncio
async def test_get_last_trading_dates(mocker):
    mocker.patch("app.routers.get_last_trading_dates", return_value=[Dates(date="2025-12-10")])

    async for db in get_async_db():
        result = await app.routers.get_last_trading_dates(db=db)
        assert result == [
            Dates(date="2025-12-10"),
        ]

