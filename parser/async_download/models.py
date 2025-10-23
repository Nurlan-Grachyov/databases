import asyncio

from sqlalchemy import Column, Integer, String, Numeric, DateTime

from parser.async_download.database import engine

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class Data(Base):
    __tablename__ = "spimex_trading_results"

    id = Column(Integer, primary_key=True)
    exchange_product_id = Column(String)
    exchange_product_name = Column(String)
    oil_id = Column(String)
    delivery_basis_id = Column(String)
    delivery_basis_name = Column(String)
    delivery_type_id = Column(String)
    volume = Column(Numeric(30, 8))
    total = Column(Numeric(30, 8))
    count = Column(Numeric(30, 8))
    date = Column(DateTime)
    created_on = Column(DateTime)
    updated_on = Column(DateTime)

    def __repr__(self):
        return str(
            {
                "exchange_product_id": self.exchange_product_id,
                "exchange_product_name": self.exchange_product_name,
                "delivery_basis_name": self.delivery_basis_name,
                "volume": self.volume,
                "total": self.total,
                "count": self.count,
            }
        )

    def __eq__(self, other):
        if not isinstance(other, Data):
            return False
        return (
                self.exchange_product_id == other.exchange_product_id
                and self.exchange_product_name == other.exchange_product_name
                and self.oil_id == other.oil_id
                and self.delivery_basis_id == other.delivery_basis_id
                and self.delivery_basis_name == other.delivery_basis_name
                and self.delivery_type_id == other.delivery_type_id
                and self.volume == other.volume
                and self.total == other.total
                and self.count == other.count
        )


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


asyncio.run(main())
