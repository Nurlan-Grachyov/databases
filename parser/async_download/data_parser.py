import asyncio
import logging
import os
import re
from datetime import datetime

import aiohttp
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

current_dir = os.path.dirname(__file__)
log_file = os.path.join(current_dir, "..", "..", "logs", "parser_errors.log")
data_dir = os.path.join(current_dir, "..", "..", "data", "async_files")
os.makedirs(data_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8", mode='w'),
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


async def try_request(session, url, headers, max_retries=3, delay=2):
    """Попытка выполнить GET-запрос с несколькими повторными попытками."""

    for attempt in range(1, max_retries + 1):
        try:
            async with session.get(url, headers=headers) as response:
                return response
        except aiohttp.ClientConnectionError as e:
            print(f"Ошибка соединения при запросе {url}: {e}. Попытка {attempt} из {max_retries}.")
            if attempt < max_retries:
                await asyncio.sleep(delay)
            else:
                return None
        except aiohttp.ClientError as e:
            print(f"Общая ошибка клиента при запросе {url}: {e}")
            return None


async def load_file(year=2023):
    page_number = 1
    async with aiohttp.ClientSession() as client_session:
        while True:
            url = f"{base_url}?page=page-{page_number}"
            response = await try_request(client_session, url, headers)
            if response is None or response.status != 200:
                print(
                    f"Ошибка доступа к странице {page_number}: {response.status_code if response else 'нет ответа'}"
                )
                break
            print(response)
            content = await response.text()
            if response is None:
                print(f"Не удалось получить ответ по URL: {url}")
                break
            print("есть response")
            soup = BeautifulSoup(content, "html.parser")
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

                            response_file = await try_request(client_session, full_url, headers)
                            if response_file and response_file.status == 200:
                                content = await response_file.read()
                                with open(file_path, "wb") as f:
                                    f.write(content)
                                print(f"Файл сохранен: {file_path}")
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
                    except aiohttp.ClientError as e:
                        print(f"Ошибка при обработке файла {href}: {e}")
        page_number += 1


if __name__ == "__main__":
    async def main():
        task = asyncio.create_task(load_file())
        await task

    asyncio.run(main())
    print("good")
