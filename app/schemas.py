from pydantic import BaseModel


class Dates(BaseModel):
    date: str

class Trades(BaseModel):
    id: int
    exchange_product_id: str
    exchange_product_name: str
    delivery_basis_name: str
    volume: float
    total: float
    count: float
