import requests
from bs4 import BeautifulSoup

st_accept = "text/html"  # говорим веб-серверу,
# что хотим получить html
# имитируем подключение через браузер Mozilla на macOS
st_useragent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_3_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.4 Safari/605.1.15"
# формируем хеш заголовков
headers = {
    "Accept": st_accept,
    "User-Agent": st_useragent
}

# отправляем запрос с заголовками по нужному адресу
req = requests.get("https://spimex.com/markets/oil_products/trades/results/", headers=headers)
# считываем текст HTML-документа
src = req.text

# инициализируем html-код страницы
soup = BeautifulSoup(src, 'html.parser')
# считываем заголовок страницы
title = soup.title.string
print(title)
