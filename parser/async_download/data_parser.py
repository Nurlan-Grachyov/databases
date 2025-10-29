import asyncio
import logging
import os
import re
from datetime import datetime
from time import time

import aiofiles
import aiohttp
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

END_YEAR = 2023

current_dir = os.path.dirname(__file__)
log_file = os.path.join(current_dir, "..", "..", "logs", "parser_errors.log")
data_dir = os.path.join(current_dir, "..", "..", "data", "async_files")
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

stop_event = asyncio.Event()

count_files = 0


async def try_request(session, url, headers, max_retries=3, delay=0):
    """Попытка выполнить GET-запрос с несколькими повторными попытками."""
    for attempt in range(1, max_retries + 1):
        try:
            response = await session.get(url, headers=headers)
            return response
        except aiohttp.ClientConnectionError as e:
            print(
                f"Ошибка соединения при запросе {url}: {e}. Попытка {attempt} из {max_retries}."
            )
            if attempt < max_retries:
                if delay:
                    await asyncio.sleep(delay)
                print("Неудачно. Пробуем скачать файл еще раз.")
                continue
            else:
                break
        except aiohttp.ClientError as e:
            print(f"Общая ошибка клиента при запросе {url}: {e}")
            return None


async def process_link(session, link, headers, year):
    """
    Обрабатывает ссылку: скачивает файл, если он соответствует условиям.
    """
    global count_files
    href = link.get("href", "")
    match = re.search(r"oil_xls_(\d{8})(\d{6})\.xls", href)
    if match:
        date_str = match.group(1)
        time_str = match.group(2)
        try:
            date_obj = datetime.strptime(date_str + time_str, "%Y%m%d%H%M%S")
            # if count_files == 50:
            #     stop_event.set()
            if date_obj.year >= year:
                full_url = "https://spimex.com" + href
                readable_date = date_obj.strftime("%Y-%m-%d_%H-%M-%S")
                filename = f"oil_{readable_date}.xls"
                file_path = os.path.join(data_dir, filename)

                if os.path.exists(file_path):
                    print(f"Файл уже существует: {file_path}")
                    return

                response_file = await try_request(session, full_url, headers)
                if response_file and response_file.status == 200:
                    content = await response_file.read()
                    async with aiofiles.open(file_path, "wb") as f:
                        await f.write(content)
                    count_files += 1
                    print(f"Файл сохранен: {file_path}, {count_files}")
                else:
                    print(
                        f"Ошибка скачивания файла: "
                        f"{response_file.status if response_file else 'нет ответа'}"
                    )
            else:
                print(f"Файл ранее {year} года, скачивание завершается")
                logging.warning(f"Файл ранее {year} года, скачивание завершается")
                stop_event.set()

        except Exception as e:
            print(f"Ошибка при обработке файла {href}: {e}")


async def load_page(session, page_number, headers):
    """
    Загружает страницу и возвращает список ссылок.
    """
    url = f"{base_url}?page=page-{page_number}"
    response = await try_request(session, url, headers)
    if response is None or response.status != 200:
        print(
            f"Ошибка доступа к странице {page_number}: {response.status if response else 'нет ответа'}"
        )
    content = await response.text()
    soup = BeautifulSoup(content, "html.parser")
    links = soup.find_all("a", class_="accordeon-inner__item-title link xls")
    return links, response


async def load_file(year=2023):
    """
    Основная функция для скачивания файлов за указанный год.
    """
    page_number = 0
    async with aiohttp.ClientSession() as session:
        while not stop_event.is_set():
            links, response = await load_page(session, page_number, headers)
            if stop_event.is_set():
                if response.closed:
                    print("Соединение уже закрыто")
                return None
            if not links:
                print(f"На странице {page_number} ссылок не найдено или ошибка.")
                response.release()
                break

            tasks = [process_link(session, link, headers, year) for link in links]
            await asyncio.gather(*tasks, return_exceptions=False)

            page_number += 1


async def main_load():
    task = asyncio.create_task(load_file(END_YEAR))
    await asyncio.gather(task)


if __name__ == "__main__":
    t0 = time()
    asyncio.run(main_load())
    print(time() - t0)