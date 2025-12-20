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
semaphore = asyncio.Semaphore(100)
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
# st_useragent = (
st_useragent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 12.3; rv:102.0) Gecko/20100101 Firefox/102.0"
headers = {"Accept": st_accept, "User-Agent": st_useragent}

base_url = os.getenv("BASE_URL")

stop_event = asyncio.Event()

count_files = 0


async def try_request(session, url, headers, max_retries=3, delay=0):
    """Попытка выполнить GET-запрос с несколькими повторными попытками."""
    # async with semaphore:
    for attempt in range(1, max_retries + 1):
        try:
            response = await session.get(url, headers=headers)
            yield response
            break
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
            yield None


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

                async for response_file in try_request(session, full_url, headers):
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
                logging.warning(f"Файл ранее {year} года, скачивание завершается")
                stop_event.set()

        except Exception as e:
            print(f"{e}")


async def load_page(session, page_number, headers):
    """
    Загружает страницу и возвращает список ссылок.
    """
    url = f"{base_url}?page=page-{page_number}"
    async for response in try_request(session, url, headers):
        if response is None:
            # ошибка, дальнейшие действия
            continue
        if response.status != 200:
            print(
                f"Ошибка доступа: статус {response.status}, ошибка {await response.text()}"
            )
            continue
        content = await response.text()
        soup = BeautifulSoup(content, "html.parser")
        links = soup.find_all("a", class_="accordeon-inner__item-title link xls")
        yield links, response


async def load_file(year=2023):
    """
    Основная функция для скачивания файлов за указанный год.
    """
    page_number = 1
    async with aiohttp.ClientSession() as session:
        while True:
            async for links, response in load_page(session, page_number, headers):
                if stop_event.is_set():
                    if response.closed:
                        print("Соединение уже закрыто")
                    return None
                if not links:
                    print(f"На странице {page_number} ссылок не найдено или ошибка.")
                    response.release()
                    break

            tasks = []
            count = 0
            for link in links:
                tasks.append(
                    asyncio.create_task(process_link(session, link, headers, year))
                )
                count += 1
                if count == 10:
                    break
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=False)
            page_number += 1


async def main_load():
    await load_file(END_YEAR)


if __name__ == "__main__":
    t0 = time()
    asyncio.run(main_load())
    print(time() - t0)
