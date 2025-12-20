import json
from datetime import date, datetime
from parser.async_download.db_depends import get_async_db
from parser.async_download.models import Data

import redis
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()


@router.get("/last_dates")
async def get_last_trading_dates(
    limit: int = 10, db: AsyncSession = Depends(get_async_db)
):
    """
    Получает последние уникальные даты торгов за указанное количество записей.

    Параметры:
    - limit: Количество последних дат, которые нужно получить (по умолчанию 10).
    - db: Асинхронная сессия базы данных, внедряемая через Depends.

    Возвращает:
    - Список дат в порядке убывания.
    """
    query = select(Data.date).distinct().order_by(Data.date.desc()).limit(limit)
    result = await db.scalars(query)
    list_dates = result.all()
    return list_dates


async def parse_flexible_date(dates_str: str):
    """
    Парсит строку дат в формат даты.
    Предполагается, что строка содержит одну дату в формате "%Y.%m.%d".

    Аргументы:
    - dates_str: строка с датой.

    Возвращает:
    - объект date, если формат корректен.

    Исключает:
    - HTTPException с кодом 400 при неправильном формате.
    """
    for date_str in dates_str:
        try:
            return datetime.strptime(date_str, "%Y.%m.%d").date()
        except ValueError:
            raise HTTPException(
                status_code=400, detail=f"Некорректный формат даты: {date_str}"
            )


@router.get("/get_dynamics")
async def get_dynamics(
    dates: tuple[date, date] = Depends(parse_flexible_date),
    oil_id: int | None = Query(None, description="ID вида нефти для фильтрации"),
    delivery_type_id: int | None = Query(None, description="ID типа поставки"),
    delivery_basis_id: int | None = Query(None, description="ID основы доставки"),
    db: AsyncSession = Depends(get_async_db),
):
    """
    Получает динамику данных за указанный диапазон дат с возможностью фильтрации.

    Параметры:
    - dates: кортеж из двух дат (начальная и конечная).
    - oil_id: ID вида нефти для фильтрации (опционально).
    - delivery_type_id: ID типа поставки (опционально).
    - delivery_basis_id: ID основы доставки (опционально).
    - db: Асинхронная сессия базы данных, внедряемая через Depends.

    Возвращает:
    - список объектов Data, соответствующих фильтрам.
    """
    start_date, end_date = dates
    list_filters = [start_date <= Data.date <= end_date]

    if oil_id:
        list_filters.append(Data.oil_id == oil_id)
    if delivery_type_id:
        list_filters.append(Data.delivery_type_id == delivery_type_id)
    if delivery_basis_id:
        list_filters.append(Data.delivery_basis_id == delivery_basis_id)

    db_data = select(Data).where(*list_filters)
    data = await db.scalars(db_data)
    return data.all()


def is_after_1411():
    """
    Проверяет, после ли 14:11 текущего дня.

    Возвращает:
    - True, если время позже 14:11.
    - False в противном случае.
    """
    now = datetime.now()
    return (now.hour > 14) or (now.hour == 14 and now.minute >= 11)


def to_dict(instance):
    """
    Преобразует объект SQLAlchemy модели в словарь.

    Аргументы:
    - instance: объект модели Data.

    Возвращает:
    - словарь с ключами-именами колонок и соответствующими значениями.
    """
    return {
        column.name: getattr(instance, column.name)
        for column in instance.__table__.columns
    }


@router.get("/get_trading_results")
async def get_trading_results(
    limit: int = Query(10, description="Количество последних операций"),
    oil_id: int | None = Query(None, description="ID вида нефти для фильтрации"),
    delivery_type_id: int | None = Query(None, description="ID типа поставки"),
    delivery_basis_id: int | None = Query(None, description="ID основы доставки"),
    db: AsyncSession = Depends(get_async_db),
):
    """
    Получает последние операции трейдинга с возможностью фильтрации и ограничением.
    Также сохраняет результаты в Redis, если время после 14:11.

    Параметры:
    - limit: Максимальное число операций (по умолчанию 10).
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

    # Выполняем запрос
    query = select(Data).where(*list_filters).limit(limit)
    results = await db.scalars(query)
    data_list = results.all()

    # Работа с Redis
    client = redis.Redis(host="localhost", port=6379, db=0)
    if is_after_1411():
        await client.flushdb()
    data_dicts = [to_dict(item) for item in data_list]
    data_json = json.dumps(data_dicts)
    await client.set("data", data_json)

    return data_list
