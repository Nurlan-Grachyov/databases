import os

current_dir = os.path.dirname(__file__)
data_dir = os.path.join(current_dir, '..', 'data')


def read_file():
    for filename in os.listdir(data_dir):
        filepath = os.path.join(data_dir, filename)
        if os.path.isfile(filepath):
            print("1")


read_file()
