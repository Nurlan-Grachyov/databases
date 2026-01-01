from datetime import datetime

from fastapi import HTTPException
from pydantic import BaseModel, field_validator


class Dates(BaseModel):
    date: str


class Trades(BaseModel):
    exchange_product_id: str
    exchange_product_name: str
    delivery_basis_name: str
    volume: float
    total: float
    count: float
