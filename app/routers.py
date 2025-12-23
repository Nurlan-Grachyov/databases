import json
from parser.async_download.db_depends import get_async_db
from parser.async_download.models import Data

import redis
from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.schemas import Dates, Trades
from app.utils import (decimal_default, is_after_1411, parse_flexible_date,
                       to_dict)

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
        query = (
            select(Data.date).distinct().order_by(Data.date.desc()).limit(limit_days)
        )
        result = await db.scalars(query)
        list_dates = result.all()

        # Преобразуем datetime в строку ISO
        list_dates_str = [date.isoformat() for date in list_dates]

        # Сохраняем в кеш
        client.delete("last_trading_dates")
        data_dicts = [to_dict(item) for item in list_dates_str]
        data_json = json.dumps(data_dicts)
        client.set("last_trading_dates", data_json)

    else:
        list_dates_bytes = client.get("last_trading_dates")
        list_dates_json = list_dates_bytes.decode("utf-8")
        list_dates_str = json.loads(list_dates_json)  # список строк

    # Объединяем оба варианта: строки с ISO датами
    return [Dates(date=d) for d in list_dates_str]


@router.get("/get_dynamics", response_model=list[Trades])
async def get_dynamics(
    start_date: str = Query(description="Дата начала", default="2025-11-21"),
    end_date: str = Query(description="Дата окончания", default="2025-12-01"),
    oil_id: int | None = Query(None, description="ID вида нефти для фильтрации"),
    delivery_type_id: int | None = Query(None, description="ID типа поставки"),
    delivery_basis_id: int | None = Query(None, description="ID основы доставки"),
    db: AsyncSession = Depends(get_async_db),
):
    """
    Получает динамику данных за указанный диапазон дат с возможностью фильтрации.

    Параметры:
    - start_date (date): Начальная дата диапазона (в форматах "YYYY.MM.DD", "DD.MM.YYYY", "YYYY-MM-DD" или "DD-MM-YYYY").
    - end_date (date): Конечная дата диапазона (в форматах "YYYY.MM.DD", "DD.MM.YYYY", "YYYY-MM-DD" или "DD-MM-YYYY").
    - oil_id (int | None): ID вида нефти для фильтрации.
    - delivery_type_id (int | None): ID типа поставки.
    - delivery_basis_id (int | None): ID основы доставки.
    - db (AsyncSession): Сессия БД.

    Результат:
    возвращает торги, удовлетворяющие условиям
    """

    start_date = parse_flexible_date(start_date)
    end_date = parse_flexible_date(end_date)

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

        client.delete("dynamics")
        data_dicts = [to_dict(item) for item in datas_str]
        data_json = json.dumps(data_dicts, default=decimal_default)
        client.set("dynamics", data_json)
    #
    else:
        datas_bytes = client.get("dynamics")
        datas_json = datas_bytes.decode("utf-8")
        datas_str = json.loads(datas_json)

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
        for item in datas_str
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

        client.delete("trading_results")
        data_dicts = [to_dict(item) for item in data_list]
        data_json = json.dumps(data_dicts, default=decimal_default)
        client.set("trading_results", data_json)

    else:
        data_list_bytes = client.get("trading_results")
        data_list_json = data_list_bytes.decode("utf-8")
        data_list = json.loads(data_list_json)

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
        for item in data_list
        if item.get("exchange_product_id")
        and item.get("exchange_product_name")
        and item.get("delivery_basis_name")
        and item.get("volume")
        and item.get("total")
        and item.get("count")
    ]
