import logging
import os

import pandas as pd

current_dir = os.path.dirname(__file__)
data_dir = os.path.join(current_dir, "..", "..", "data", "sync_files")
log_file = os.path.join(current_dir, "..", "..", "logs", "read_files_errors.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8", mode="w"),
        logging.StreamHandler(),
    ],
)


def read_file():
    operations = []
    for filename in os.listdir(data_dir):
        filepath = os.path.join(data_dir, filename)
        if os.path.isfile(filepath):
            try:
                df = pd.read_excel(filepath, skiprows=6)
            except Exception as e:
                logging.error(f"Ошибка при чтении файла {filename}: {e}")
                continue

            try:
                df["Количество\nДоговоров,\nшт."] = pd.to_numeric(
                    df["Количество\nДоговоров,\nшт."], errors="coerce"
                ).fillna(0)
            except Exception as e:
                logging.error(f"Ошибка при обработке данных в файле {filename}: {e}")
                continue

            try:
                filtered_df = df[df["Количество\nДоговоров,\nшт."] > 0]
            except Exception as e:
                logging.error(f"Ошибка при фильтрации данных в файле {filename}: {e}")
                continue

            if not filtered_df.empty:
                for _, row in filtered_df.iterrows():
                    try:
                        operation = {
                            "exchange_product_id": row["Код\nИнструмента"],
                            "exchange_product_name": row["Наименование\nИнструмента"],
                            "delivery_basis_name": row["Базис\nпоставки"],
                            "volume": row["Объем\nДоговоров\nв единицах\nизмерения"],
                            "total": row["Обьем\nДоговоров,\nруб."],
                            "count": row["Количество\nДоговоров,\nшт."],
                        }
                        operations.append(operation)

                    except Exception as e:
                        logging.error(
                            f"Ошибка при обработке строки в файле {filename}: {e}"
                        )
                        continue
            print(operations)
            return operations


if __name__ == "__main__":
    print(read_file())
