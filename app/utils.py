from datetime import date, datetime
from decimal import Decimal


def is_after_1411():
    """
    Проверяет, после ли 14:11 текущего дня.

    Возвращает:
    - True, если время позже 14:11.
    - False в противном случае.
    """
    now = datetime.now()
    return now.hour >= 14 and now.minute > 11


def to_dict(instance):
    """
    Преобразовать экземпляр модели SQLAlchemy в словарь.

    Args:
        instance: Экземпляр модели SQLAlchemy.

    Returns:
        Словарь, представляющий экземпляр.
    """
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
    """
    Преобразовать Decimal в строку.

    Args:
        obj: Decimal-объект.

    Returns:
        Строковое представление Decimal-объекта.
    """
    if isinstance(obj, Decimal):
        return str(obj)
