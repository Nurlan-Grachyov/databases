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

current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
current_dir = os.path.dirname(__file__)
log_file_error = os.path.join(current_dir, "..", "..", "logs", "send_errors.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file_error, encoding="utf-8", mode="w"),
        logging.StreamHandler(),
    ],
)

log_file_report = os.path.join(current_dir, "..", "..", "logs", "report.log")
logger_report = logging.getLogger("anotherLogger")
logger_report.setLevel(logging.INFO)

file_handler = logging.FileHandler(log_file_report, encoding="utf-8", mode="w")
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()

logger_report.addHandler(file_handler)
logger_report.addHandler(stream_handler)

ignore_fields = [
    "oil_id",
    "id",
    "_sa_instance_state",
    "delivery_basis_id",
    "delivery_type_id",
]

semaphore = asyncio.BoundedSemaphore(100)

stop_event = asyncio.Event()

objects_to_save = []


async def get_data(data_dict):
    volume = data_dict.get("volume", 0)
    total = data_dict.get("total", 0)
    count = data_dict.get("count", 0)
    date_str = data_dict.get("date")
    date_obj = datetime.strptime(date_str, "%d.%m.%Y").date()

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
        date=date_obj,
        created_on=datetime.now(),
        updated_on=datetime.now(),
    )
    yield data


async def send_data():
    async with async_session() as session:
        async with semaphore:
            print("читаем excel файлы")
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
                async for data in get_data(data_dict_need):
                    objects_to_save.append(data)
                try:
                    if objects_to_save:
                        # print(objects_to_save)
                        session.add_all(objects_to_save)
                        objects_to_save.clear()
                    else:
                        print("нет новых данных")
                except Exception as e:
                    print(f"Ошибка при добавлении данных в сессию: {e}")
                    logging.error(e)
        try:
            await session.commit()
        except Exception as e:
            print(f"Ошибка при коммите данных: {e}")
            logging.error(e)
            await session.rollback()
        else:
            print(
                f"данные сохранены в базу в количестве {len(session.new)} экземпляров"
            )


async def main():
    t0 = time()
    await main_load()
    logger_report.info(f"after main_load {time() - t0}")

    await start_db()
    logger_report.info(f"after start_db {time() - t0}")

    await send_data()
    logger_report.info(f"after send_data {time() - t0}")


if __name__ == "__main__":
    asyncio.run(main())
