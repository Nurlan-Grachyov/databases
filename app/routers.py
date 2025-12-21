import json
from datetime import date

from app.schemas import Dates, Trades
from parser.async_download.db_depends import get_async_db
from parser.async_download.models import Data

import redis
from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.utils import is_after_1411, parse_flexible_date, to_dict

router = APIRouter()

client = redis.Redis(host="localhost", port=6379, db=0)


@router.get("/last_dates", response_model=list[Dates])
async def get_last_trading_dates(
        limit_days: int = 10, db: AsyncSession = Depends(get_async_db)
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
        query = select(Data.date).distinct().order_by(Data.date.desc()).limit(limit_days)
        result = await db.scalars(query)
        list_dates = result.all()

        await client.delete("last_trading_dates")
        data_dicts = [to_dict(item) for item in list_dates]
        data_json = json.dumps(data_dicts)
        await client.set("last_trading_dates", data_json)

    else:
        list_dates = await client.get("last_trading_dates")

    return [Dates(date=some_value) for some_value in list_dates]


@router.get("/get_dynamics", response_model=Trades)
async def get_dynamics(
        start_date: date = Depends(parse_flexible_date),
        end_date: date = Depends(parse_flexible_date),
        oil_id: int | None = Query(None, description="ID вида нефти для фильтрации"),
        delivery_type_id: int | None = Query(None, description="ID типа поставки"),
        delivery_basis_id: int | None = Query(None, description="ID основы доставки"),
        db: AsyncSession = Depends(get_async_db),
):
    """
    Получает динамику данных за указанный диапазон дат с возможностью фильтрации.

    Параметры:
    - start_date (date): Начальная дата диапазона (в формате YYYY-MM-DD).
    - end_date (date): Конечная дата диапазона (в формате YYYY-MM-DD).
    - oil_id (int | None): ID вида нефти для фильтрации.
    - delivery_type_id (int | None): ID типа поставки.
    - delivery_basis_id (int | None): ID основы доставки.
    - db (AsyncSession): Сессия БД.

    Результат:
    возвращает торги, удовлетворяющие условиям
    """

    list_filters = [start_date <= Data.date <= end_date]

    if oil_id:
        list_filters.append(Data.oil_id == oil_id)
    if delivery_type_id:
        list_filters.append(Data.delivery_type_id == delivery_type_id)
    if delivery_basis_id:
        list_filters.append(Data.delivery_basis_id == delivery_basis_id)

    if is_after_1411():
        query = select(Data).where(*list_filters)
        results = await db.scalars(query)
        datas = results.all()

        await client.delete("dynamics")
        data_dicts = [to_dict(item) for item in datas]
        data_json = json.dumps(data_dicts)
        await client.set("dynamics", data_json)

    else:
        datas = await client.get("dynamics")

    return [Trades(
        id=item.id,
        exchange_product_id=item.exchange_product_id,
        exchange_product_name=item.exchange_product_name,
        delivery_basis_name=item.delivery_basis_name,
        volume=int(item.volume) if item.volume.is_integer() else item.volume,
        total=int(item.total) if item.total.is_integer() else item.total,
        count=item.count
    ) for item in datas]


@router.get("/get_trading_results", response_model=Trades)
async def get_trading_results(
        limit_trades: int = Query(10, description="Количество последних операций"),
        oil_id: int | None = Query(None, description="ID вида нефти для фильтрации"),
        delivery_type_id: int | None = Query(None, description="ID типа поставки"),
        delivery_basis_id: int | None = Query(None, description="ID основы доставки"),
        db: AsyncSession = Depends(get_async_db),
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

        await client.delete("trading_results")
        data_dicts = [to_dict(item) for item in data_list]
        data_json = json.dumps(data_dicts)
        await client.set("trading_results", data_json)

    else:
        data_list = await client.get("trading_results")

    return [Trades(
        id=item.id,
        exchange_product_id=item.exchange_product_id,
        exchange_product_name=item.exchange_product_name,
        delivery_basis_name=item.delivery_basis_name,
        volume=int(item.volume) if item.volume.is_integer() else item.volume,
        total=int(item.total) if item.total.is_integer() else item.total,
        count=item.count
    ) for item in data_list]
