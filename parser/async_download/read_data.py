import asyncio
import functools
import logging
import os

import pandas as pd

current_dir = os.path.dirname(__file__)
data_dir = os.path.join(current_dir, "..", "..", "data", "async_files")
log_file = os.path.join(current_dir, "..", "..", "logs", "read_files_errors.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8", mode="w"),
        logging.StreamHandler(),
    ],
)


async def read_excel_file(filepath):
    loop = asyncio.get_event_loop()
    try:
        df = await loop.run_in_executor(
            None, functools.partial(pd.read_excel, filepath, skiprows=6)
        )

        df["Количество\nДоговоров,\nшт."] = pd.to_numeric(
            df["Количество\nДоговоров,\nшт."], errors="coerce"
        ).fillna(0)

        filtered_df = df[df["Количество\nДоговоров,\nшт."] > 0]

        if not filtered_df.empty:
            for _, row in filtered_df.iterrows():
                yield {
                    "exchange_product_id": row["Код\nИнструмента"],
                    "exchange_product_name": row["Наименование\nИнструмента"],
                    "delivery_basis_name": row["Базис\nпоставки"],
                    "volume": row["Объем\nДоговоров\nв единицах\nизмерения"],
                    "total": row["Обьем\nДоговоров,\nруб."],
                    "count": row["Количество\nДоговоров,\nшт."],
                }

    except Exception as e:
        logging.error(f"Ошибка при обработке строки в файле {filepath}: {e}")


async def read_files_in_dir(data_dir):
    for filename in os.listdir(data_dir):
        if not filename:
            print("Директория пуста")
            logging.info("Директория пуста")
        filepath = os.path.join(data_dir, filename)

        if os.path.isfile(filepath):
            async for data in read_excel_file(filepath):
                yield data


if __name__ == "__main__":

    async def main():
        async for data in read_files_in_dir(data_dir):
            print(data)

    asyncio.run(main())
