import os
import pandas as pd

current_dir = os.path.dirname(__file__)
data_dir = os.path.join(current_dir, '..', 'data')


def read_file():
    for filename in os.listdir(data_dir):
        filepath = os.path.join(data_dir, filename)
        if os.path.isfile(filepath):
            df = pd.read_excel(filepath)
            df["Количество Договоров, шт."] = pd.to_numeric(df["Количество Договоров, шт."], errors='coerce').fillna(0)
            count = df[df["Количество Договоров, шт."] > 0]
            if count:
                exchange_product_id = df["Код Инструмента"]
                exchange_product_name = df["Наименование Инструмента"]
                delivery_basis_name = df["Базис поставки"]
                volume = df["Объем Договоров в единицах измерения"]
                total = df["Объем Договоров, руб."]



read_file()
