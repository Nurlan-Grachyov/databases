from datetime import date, datetime
from decimal import Decimal

from fastapi import HTTPException


def parse_flexible_date(date_str: str):
    """
    Парсит строку дат в формат даты.
    Предполагается, что строка содержит одну дату в форматах "YYYY.MM.DD", "DD.MM.YYYY", "YYYY-MM-DD" или "DD-MM-YYYY".

    Аргументы:
    - date_str: строка с датой.

    Возвращает:
    - объект date, если формат корректен.

    Исключает:
    - HTTPException с кодом 400 при неправильном формате.
    """
    for fmt in ("%Y.%m.%d", "%d.%m.%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    # Если ни один формат не подошел — выбрасываем исключение
    raise HTTPException(status_code=400, detail=f"Некорректный формат даты: {date_str}")


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
    if hasattr(instance, "__table__"):
        return {
            column.name: getattr(instance, column.name)
            for column in instance.__table__.columns
        }
    elif isinstance(instance, (date, datetime)):
        return instance.isoformat()
    else:
        return instance


def decimal_default(obj):
    if isinstance(obj, Decimal):
        return str(obj)
