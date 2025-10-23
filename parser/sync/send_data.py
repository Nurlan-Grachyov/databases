import logging
import math
import os
from datetime import datetime
from parser.sync.database import session
from parser.sync.models import Data
from parser.sync.read_files import read_file

current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
current_dir = os.path.dirname(__file__)
log_file = os.path.join(current_dir, "..", "logs", "send_errors.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8", mode='w'),
        logging.StreamHandler(),
    ],
)


def send_data():
    count_operation = 0
    for data_dict in read_file():
        count_operation += 1
        try:
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

            if count_operation == 20000:
                break

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

            # Проверка на существование в бд
            existing = (
                session.query(Data)
                .filter_by(exchange_product_id=exchange_product_id)
                .first()
            )
            if existing:
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

                # Получаем словарь из объекта ORM
                existing_dict = {
                    k: v for k, v in existing.__dict__.items() if k not in ignore_fields
                }
                # Теперь сравниваем
                data_dict_need = {
                    k: (
                        int(v)
                        if k == "total" and isinstance(v, str) and v.isdigit()
                        else v
                    )
                    for k, v in data_dict.items()
                    if k not in ignore_fields
                }
                if existing_dict == data_dict_need:
                    print("Объекты идентичны")
                    logging.info("Объекты идентичны, записи не будет")

            session.add(data)
            print(f"data добавлена в сессию, {count_operation}")

        except Exception as e:
            print(f"Ошибка при обработке данных: {e}")
            logging.error(f"Ошибка: {e}")

    try:
        session.commit()
        print("данные сохранены в базу")
    except Exception as e:
        print(f"Ошибка при коммите сессии: {e}")
        logging.error(e)
        session.rollback()


if __name__ == "__main__":
    send_data()
