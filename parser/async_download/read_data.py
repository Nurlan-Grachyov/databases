import asyncio
import logging
import os
import re

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
    def process():
        try:
            # Читаем первую строку для извлечения числа
            df_header = pd.read_excel(filepath, header=None, skiprows=3, nrows=1)
            text_with_number = df_header.iloc[0, 1]

            # Извлекаем число
            number_match = re.search(r"\d{2}\.\d{2}\.\d{4}", str(text_with_number))

            if number_match:
                number = number_match.group()
            else:
                number = None

            # Читаем весь DataFrame с пропусками строк
            df = pd.read_excel(filepath, skiprows=6)

            df["Количество\nДоговоров,\nшт."] = pd.to_numeric(
                df["Количество\nДоговоров,\nшт."], errors="coerce"
            ).fillna(0)

            filtered_df = df[df["Количество\nДоговоров,\nшт."] > 0]

            results = []

            for _, row in filtered_df.iterrows():
                results.append(
                    {
                        "date": number,
                        "exchange_product_id": row["Код\nИнструмента"],
                        "exchange_product_name": row["Наименование\nИнструмента"],
                        "delivery_basis_name": row["Базис\nпоставки"],
                        "volume": row["Объем\nДоговоров\nв единицах\nизмерения"],
                        "total": row["Обьем\nДоговоров,\nруб."],
                        "count": row["Количество\nДоговоров,\nшт."],
                    }
                )

            return results

        except Exception as e:
            logging.error(f"Ошибка при обработке файла {filepath}: {e}")
            return []

    # Вызов всей обработки в одном потоке
    results = await asyncio.to_thread(process)
    # Генерируем результаты по одному
    for item in results:
        yield item


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
            continue

    asyncio.run(main())
