import asyncio
import logging
import math
import os
from datetime import datetime
from parser.async_download.data_parser import main_load
from parser.async_download.database import async_session
from parser.async_download.models import Data, start_db
from parser.async_download.read_data import data_dir, read_files_in_dir
from time import time

from sqlalchemy import select

current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
current_dir = os.path.dirname(__file__)
log_file = os.path.join(current_dir, "..", "..", "logs", "send_errors.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8", mode="w"),
        logging.StreamHandler(),
    ],
)

ignore_fields = [
    "volume",
    "oil_id",
    "count",
    "date",
    "created_on",
    "updated_on",
    "id",
    "_sa_instance_state",
    "delivery_basis_id",
    "delivery_type_id",
]

count_operation = 0

semaphore = asyncio.BoundedSemaphore(1000)

stop_event = asyncio.Event()


async def get_data(data_dict):
    volume = data_dict.get("volume", 0)
    total = data_dict.get("total", 0)
    count = data_dict.get("count", 0)

    # Проверяем и заменяем nan на 0
    if isinstance(volume, float) and math.isnan(volume):
        volume = 0
    if isinstance(total, float) and math.isnan(total):
        total = 0
    if isinstance(count, float) and math.isnan(count):
        count = 0

    exchange_product_id = data_dict.get("exchange_product_id")
    if exchange_product_id and isinstance(exchange_product_id, str):
        oil_id = exchange_product_id[:4]
        delivery_basis_id = exchange_product_id[4:7]
        delivery_type_id = exchange_product_id[-1]
    else:
        oil_id = None
        delivery_basis_id = None
        delivery_type_id = None

    data = Data(
        exchange_product_id=exchange_product_id,
        exchange_product_name=data_dict.get("exchange_product_name", None),
        oil_id=oil_id,
        delivery_basis_id=delivery_basis_id,
        delivery_basis_name=data_dict.get("delivery_basis_name", None),
        delivery_type_id=delivery_type_id,
        volume=volume,
        total=total,
        count=count,
        date=datetime.now(),
        created_on=datetime.now(),
        updated_on=datetime.now(),
    )
    return data


async def get_existing_dict(data):
    async with semaphore:
        async with async_session() as session:
            existing = await session.execute(
                select(Data).filter_by(exchange_product_id=data.exchange_product_id)
            )
            existing_obj = existing.scalars().first()
        if existing_obj:
            # Получаем словарь из объекта ORM
            existing_dict = {
                k: v for k, v in existing.__dict__.items() if k not in ignore_fields
            }
            # Теперь сравниваем

            return existing_dict
        return None


async def send_data():
    global count_operation
    objects_to_save = []
    while not stop_event.is_set():
        tasks = []
        # datas_from_file = await read_files_in_dir(data_dir)
        print("читаем excel файлы")
        async for data_dict in read_files_in_dir(data_dir):
            data = await get_data(data_dict)
            if stop_event.is_set():
                print("Обнаружено событие остановки, выходим из цикла чтения файлов")
                break
            if count_operation == 10000:
                stop_event.set()
            if count_operation % 1000 == 0:
                print(count_operation)
                print(data)
            count_operation += 1
            task = asyncio.create_task(get_existing_dict(data))
            tasks.append(task)
        if stop_event.is_set():
            print("Цикл остановлен по событию")
            break

    existing_dicts = await asyncio.gather(*tasks)
    print("ждем получения существующих записей")

    async for data_dict in read_files_in_dir(data_dir):
        data_dict_need = {
            k: (
                int(v)
                if k == "total" and isinstance(v, str) and v.isdigit()
                else (None if isinstance(v, float) and math.isnan(v) else v)
            )
            for k, v in data_dict.items()
            if k not in ignore_fields
        }
        if data_dict_need in existing_dicts:
            print("Объекты идентичны")
            logging.info("Объекты идентичны, записи не будет")
        else:
            objects_to_save.append(data_dict_need)
    async with async_session() as session:
        try:
            if objects_to_save:
                # Предположим, что Data — ORM модель
                session.add_all([Data(**obj) for obj in objects_to_save])
                await session.commit()
                print(
                    f"данные сохранены в базу в количестве {count_operation} экземпляров"
                )
            else:
                print("нет новых данных")
        except Exception as e:
            print(f"Ошибка при коммите сессии: {e}")
            logging.error(e)
            await session.rollback()


async def main():
    await main_load()
    await start_db()
    t0 = time()
    await send_data()
    print(time() - t0)


if __name__ == "__main__":
    asyncio.run(main())
