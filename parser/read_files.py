import os
import pandas as pd

current_dir = os.path.dirname(__file__)
data_dir = os.path.join(current_dir, '..', 'data')


def read_file():
    for filename in os.listdir(data_dir):
        filepath = os.path.join(data_dir, filename)
        if os.path.isfile(filepath):
            df = pd.read_excel(filepath, skiprows=6)
            df["Количество\nДоговоров,\nшт."] = pd.to_numeric(df["Количество\nДоговоров,\nшт."], errors='coerce').fillna(0)
            filtered_df = df[df["Количество\nДоговоров,\nшт."] > 0]
            if not filtered_df.empty:
                exchange_product_id = filtered_df["Код\nИнструмента"]
                exchange_product_name = filtered_df["Наименование\nИнструмента"]
                delivery_basis_name = filtered_df["Базис\nпоставки"]
                volume = filtered_df["Объем\nДоговоров\nв единицах\nизмерения"]
                total = filtered_df["Обьем\nДоговоров,\nруб."]
                count = filtered_df["Количество\nДоговоров,\nшт."]
                print(count)


read_file()
