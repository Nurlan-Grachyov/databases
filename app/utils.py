from datetime import datetime

from fastapi import HTTPException


async def parse_flexible_date(date_str: str):
    """
    Парсит строку дат в формат даты.
    Предполагается, что строка содержит одну дату в формате "%Y.%m.%d".

    Аргументы:
    - date_str: строка с датой.

    Возвращает:
    - объект date, если формат корректен.

    Исключает:
    - HTTPException с кодом 400 при неправильном формате.
    """
    try:
        return datetime.strptime(date_str, "%Y.%m.%d").date()
    except ValueError:
        raise HTTPException(
            status_code=400, detail=f"Некорректный формат даты: {date_str}"
        )


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
