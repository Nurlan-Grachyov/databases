import json
from datetime import date
from parser.async_download.db_depends import get_async_db
from parser.async_download.models import Data

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.schemas import Dates, Trades
from app.utils import decimal_default, is_after_1411, to_dict

router = APIRouter()


async def get_redis(request: Request):
    return request.state.redis


@router.get("/last_dates", response_model=list[Dates])
async def get_last_trading_dates(
    limit_days: int = 10,
    db: AsyncSession = Depends(get_async_db),
    cache=Depends(get_redis),
):
    """
    Получает последние уникальные даты торгов за указанное количество записей.

    Параметры:
    - limit_days: Количество последних дат, которые нужно получить (по умолчанию 10).
    - db: Асинхронная сессия базы данных, внедряемая через Depends.

    Возвращает:
    - Список дат в порядке убывания.
    """
    if is_after_1411():
        query = (
            select(Data.date).distinct().order_by(Data.date.desc()).limit(limit_days)
        )
        result = await db.scalars(query)
        list_dates = result.all()

        # Преобразуем datetime в строку ISO
        list_dates_str = [date.isoformat() for date in list_dates]

        # Сохраняем в кеш
        await cache.delete("last_trading_dates")
        data_dicts = [to_dict(item) for item in list_dates_str]
        data_json = json.dumps(data_dicts)
        await cache.set("last_trading_dates", data_json)

    else:
        list_dates_json = await cache.get("last_trading_dates")
        list_dates_str = json.loads(list_dates_json) if list_dates_json else []

    # Объединяем оба варианта: строки с ISO датами
    return [Dates(date=d) for d in list_dates_str]


@router.get("/get_dynamics", response_model=list[Trades])
async def get_dynamics(
    start_date: date = Query(default=date(2025, 1, 1), description="Дата начала"),
    end_date: date = Query(default=date(2025, 12, 1), description="Дата окончания"),
    oil_id: int | None = Query(None, description="ID вида нефти для фильтрации"),
    delivery_type_id: int | None = Query(None, description="ID типа поставки"),
    delivery_basis_id: int | None = Query(None, description="ID основы доставки"),
    db: AsyncSession = Depends(get_async_db),
    cache=Depends(get_redis),
):
    """
    Получает динамику данных за указанный диапазон дат с возможностью фильтрации.

    Параметры:
    - start_date (date): Начальная дата диапазона (в формате "YYYY-MM-DD").
    - end_date (date): Конечная дата диапазона (в формате "YYYY-MM-DD").
    - oil_id (int | None): ID вида нефти для фильтрации.
    - delivery_type_id (int | None): ID типа поставки.
    - delivery_basis_id (int | None): ID основы доставки.
    - db (AsyncSession): Сессия БД.

    Результат:
    возвращает торги, удовлетворяющие условиям
    """

    list_filters = [Data.date >= start_date, Data.date <= end_date]

    if oil_id:
        list_filters.append(Data.oil_id == oil_id)
    if delivery_type_id:
        list_filters.append(Data.delivery_type_id == delivery_type_id)
    if delivery_basis_id:
        list_filters.append(Data.delivery_basis_id == delivery_basis_id)

    if is_after_1411():
        query = select(Data).where(*list_filters)
        results = await db.scalars(query)
        datas_str = results.all()

        # Сохраняем в кеш
        await cache.delete("dynamics")
        data_dicts = [to_dict(item) for item in datas_str]
        data_json = json.dumps(data_dicts, default=decimal_default)
        await cache.set("dynamics", data_json)

    else:
        datas_json = await cache.get("dynamics")
        data_dicts = json.loads(datas_json) if datas_json else []

    return [
        Trades(
            id=item.get("id"),
            exchange_product_id=item.get("exchange_product_id"),
            exchange_product_name=item.get("exchange_product_name"),
            delivery_basis_name=item.get("delivery_basis_name"),
            volume=round(float(item.get("volume")), 2),
            total=round(float(item.get("total")), 2),
            count=round(float(item.get("count")), 2),
        )
        for item in data_dicts
        if item.get("exchange_product_id")
        and item.get("exchange_product_name")
        and item.get("delivery_basis_name")
        and item.get("volume")
        and item.get("total")
        and item.get("count")
    ]


@router.get("/get_trading_results", response_model=list[Trades])
async def get_trading_results(
    limit_trades: int = Query(10, description="Количество последних операций"),
    oil_id: int | None = Query(None, description="ID вида нефти для фильтрации"),
    delivery_type_id: int | None = Query(None, description="ID типа поставки"),
    delivery_basis_id: int | None = Query(None, description="ID основы доставки"),
    db: AsyncSession = Depends(get_async_db),
    cache=Depends(get_redis),
):
    """
    Получает последние операции трейдинга с возможностью фильтрации и ограничением.
    Также пересохраняет результаты в Redis, если время после 14:11.

    Параметры:
    - limit_trades: Максимальное число операций (по умолчанию 10).
    - oil_id: ID вида нефти для фильтрации (опционально).
    - delivery_type_id: ID типа поставки (опционально).
    - delivery_basis_id: ID основы доставки (опционально).
    - db: Асинхронная сессия базы данных, внедряемая через Depends.

    Возвращает:
    - список объектов Data.
    """
    # Формируем фильтры
    list_filters = []
    if oil_id:
        list_filters.append(Data.oil_id == oil_id)
    if delivery_type_id:
        list_filters.append(Data.delivery_type_id == delivery_type_id)
    if delivery_basis_id:
        list_filters.append(Data.delivery_basis_id == delivery_basis_id)

    if is_after_1411():
        query = select(Data).where(*list_filters).limit(limit_trades)
        results = await db.scalars(query)
        data_list = results.all()

        # Сохраняем в кеш
        await cache.delete("trading_results")
        data_dicts = [to_dict(item) for item in data_list]
        data_json = json.dumps(data_dicts, default=decimal_default)
        await cache.set("trading_results", data_json)

    else:
        datas_json = await cache.get("trading_results")
        data_dicts = json.loads(datas_json) if datas_json else []

    return [
        Trades(
            id=item.get("id"),
            exchange_product_id=item.get("exchange_product_id"),
            exchange_product_name=item.get("exchange_product_name"),
            delivery_basis_name=item.get("delivery_basis_name"),
            volume=round(float(item.get("volume")), 2),
            total=round(float(item.get("total")), 2),
            count=round(float(item.get("count")), 2),
        )
        for item in data_dicts
        if item.get("exchange_product_id")
        and item.get("exchange_product_name")
        and item.get("delivery_basis_name")
        and item.get("volume")
        and item.get("total")
        and item.get("count")
    ]
