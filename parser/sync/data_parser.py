import logging
import os
import re
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

current_dir = os.path.dirname(__file__)
log_file = os.path.join(current_dir, "..", "..", "logs", "parser_errors.log")
data_dir = os.path.join(current_dir, "..", "..", "data", "sync_files")
os.makedirs(data_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8", mode="w"),
        logging.StreamHandler(),
    ],
)

st_accept = "text/html"
st_useragent = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_3_1) AppleWebKit/605.1.15 (KHTML, like Gecko) "
    "Version/15.4 Safari/605.1.15"
)
headers = {"Accept": st_accept, "User-Agent": st_useragent}

base_url = os.getenv("BASE_URL")

t0 = time.time()


def try_request(url, headers, max_retries=3, delay=2):
    """Попытка выполнить GET-запрос с несколькими повторными попытками."""
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, headers=headers)

            return response
        except requests.RequestException as e:
            print(f"Ошибка при запросе {url}: {e}. Попытка {attempt} из {max_retries}.")
            if attempt < max_retries:
                time.sleep(delay)
            else:
                return None


def load_file(year=2023):
    count = 0
    page_number = 1
    while True:
        url = f"{base_url}?page=page-{page_number}"
        response = try_request(url, headers)
        if response is None or response.status_code != 200:
            print(
                f"Ошибка доступа к странице {page_number}: {response.status_code if response else 'нет ответа'}"
            )
            break

        soup = BeautifulSoup(response.text, "html.parser")
        links = soup.find_all("a", class_="accordeon-inner__item-title link xls")

        if not links:
            print(f"На странице {page_number} ссылок не найдено. Завершение обхода.")
            break

        for link in links:
            href = link.get("href", "")
            match = re.search(r"oil_xls_(\d{8})(\d{6})\.xls", href)
            # исходный код внутри вашего цикла:
            if match:
                date_str = match.group(1)  # например, '20230401'
                time_str = match.group(2)  # например, '123456'
                try:
                    # Парсим дату и время
                    date_obj = datetime.strptime(date_str + time_str, "%Y%m%d%H%M%S")
                    if date_obj.year >= year:
                        full_url = "https://spimex.com" + href
                        # Формируем более читаемое название файла
                        readable_date = date_obj.strftime("%Y-%m-%d_%H-%M-%S")
                        filename = f"oil_{readable_date}.xls"
                        file_path = os.path.join(data_dir, filename)

                        if os.path.exists(file_path):
                            print(f"Файл уже существует: {file_path}")
                            continue

                        response_file = try_request(full_url, headers)
                        if response_file and response_file.status_code == 200:
                            with open(file_path, "wb") as f:
                                for chunk in response_file.iter_content(
                                    chunk_size=8192
                                ):
                                    if chunk:
                                        f.write(chunk)
                            print(f"Файл сохранен: {file_path}", count)
                        # count += 1
                        # if count == 10:
                        #     print(time.time() - t0)
                        #     exit()
                        else:
                            print(
                                f"Ошибка скачивания файла: "
                                f"{response_file.status_code if response_file else 'нет ответа'}"
                            )
                            logging.error(
                                f"Ошибка скачивания файла: "
                                f"{response_file.status_code if response_file else 'нет ответа'}"
                            )
                    else:
                        print(f"Файл ранее {year} года, скачивание завершается")
                        logging.warning(
                            f"Файл ранее {year} года, скачивание завершается"
                        )
                        return
                except Exception as e:
                    print(f"Ошибка при обработке файла {href}: {e}")
        page_number += 1


if __name__ == "__main__":
    load_file()
    print("good")
