from datetime import datetime

from fastapi import HTTPException
from pydantic import BaseModel, field_validator


class Dates(BaseModel):
    date: str

    @field_validator("date")
    @classmethod
    def parse_flexible_date(cls, value):
        for fmt in ("%Y.%m.%d", "%d.%m.%Y", "%Y-%m-%d", "%d-%m-%Y"):
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                continue
        # Если ни один формат не подошел — выбрасываем исключение
        raise HTTPException(
            status_code=400, detail=f"Некорректный формат даты: {value}"
        )


class Trades(BaseModel):
    exchange_product_id: str
    exchange_product_name: str
    delivery_basis_name: str
    volume: float
    total: float
    count: float
