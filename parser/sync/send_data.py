import logging
import math
import os
from datetime import datetime
from parser.sync.data_parser import load_file
from parser.sync.database import session
from parser.sync.models import Data
from parser.sync.read_files import read_file
from time import time

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

#
# def get_data(data_dict):
#     volume = data_dict.get("volume", 0)
#     total = data_dict.get("total", 0)
#     count = data_dict.get("count", 0)
#
#     # Проверяем и заменяем nan на 0
#     if isinstance(volume, float) and math.isnan(volume):
#         volume = 0
#     if isinstance(total, float) and math.isnan(total):
#         total = 0
#     if isinstance(count, float) and math.isnan(count):
#         count = 0
#
#     # if count_operation == 20000:
#     #     break
#
#     exchange_product_id = data_dict.get("exchange_product_id")
#     if exchange_product_id and isinstance(exchange_product_id, str):
#         oil_id = exchange_product_id[:4]
#         delivery_basis_id = exchange_product_id[4:7]
#         delivery_type_id = exchange_product_id[-1]
#     else:
#         oil_id = None
#         delivery_basis_id = None
#         delivery_type_id = None
#
#     data = Data(
#         exchange_product_id=exchange_product_id,
#         exchange_product_name=data_dict.get("exchange_product_name", None),
#         oil_id=oil_id,
#         delivery_basis_id=delivery_basis_id,
#         delivery_basis_name=data_dict.get("delivery_basis_name", None),
#         delivery_type_id=delivery_type_id,
#         volume=volume,
#         total=total,
#         count=count,
#         date=datetime.now(),
#         created_on=datetime.now(),
#         updated_on=datetime.now(),
#     )
#     return data
#
#
# def check_data(data, data_dict):
#     existing = (
#         session.query(Data)
#         .filter_by(exchange_product_id=data.exchange_product_id)
#         .first()
#     )
#     if existing:
#         ignore_fields = [
#             "volume",
#             "oil_id",
#             "count",
#             "date",
#             "created_on",
#             "updated_on",
#             "id",
#             "_sa_instance_state",
#             "delivery_basis_id",
#             "delivery_type_id",
#         ]
#
#         # Получаем словарь из объекта ORM
#         existing_dict = {
#             k: v for k, v in existing.__dict__.items() if k not in ignore_fields
#         }
#         # Теперь сравниваем
#         data_dict_need = {
#             k: (
#                 int(v)
#                 if k == "total" and isinstance(v, str) and v.isdigit()
#                 else v
#             )
#             for k, v in data_dict.items()
#             if k not in ignore_fields
#         }
#         return existing_dict, data_dict_need
#     return None, None
#
#
# def send_data():
#     count_operation = 0
#     objects_to_save = []
#     for data_dict in read_file():
#         count_operation += 1
#         try:
#
#             data = get_data(data_dict)
#
#             existing_dict, data_dict_need = check_data(data, data_dict)
#             if count_operation == 10000:
#                 break
#             if existing_dict == data_dict_need and existing_dict is not None and data_dict_need is not None:
#                 print("Объекты идентичны")
#                 logging.info("Объекты идентичны, записи не будет")
#
#             else:
#                 objects_to_save.append(data)
#                 print(f"data добавлена в сессию, {count_operation}")
#
#         except Exception as e:
#             print(f"Ошибка при обработке данных: {e}")
#             logging.error(f"Ошибка: {e}")
#
#     try:
#         if objects_to_save:
#             session.add(objects_to_save)
#             session.commit()
#             print(f"данные сохранены в базу в количестве {count_operation} экземпляров")
#         else:
#             print("нет новых данных")
#     except Exception as e:
#         print(f"Ошибка при коммите сессии: {e}")
#         logging.error(e)
#         session.rollback()


def send_data():
    count_operation = 0
    for data_dict in read_file():
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
            print(data)

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

                else:
                    count_operation += 1
                    session.add(data)
                    print(
                        f"данные добавлены в сессию в количестве {count_operation} экземпляров"
                    )
            else:
                count_operation += 1
                session.add(data)
                print(
                    f"данные добавлены в сессию в количестве {count_operation} экземпляров"
                )
        except Exception as e:
            print(f"Ошибка при обработке данных: {e}")
            logging.error(f"Ошибка: {e}")

    try:
        session.commit()
        print(f"данные сохранены в базу в количестве {count_operation} экземпляров")
    except Exception as e:
        print(f"Ошибка при коммите сессии: {e}")
        logging.error(e)
        session.rollback()


if __name__ == "__main__":
    t0 = time()
    load_file()
    send_data()
    print(time() - t0)
