from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
import os
import requests
import zipfile
import csv
import io
import pandas as pd
import pandas_ta as ta
import backtesting
from backtesting.lib import crossover

# основные параметры
symbol = 'BTCUSDT'
interval = '1m'
start_date = '2020-01'
end_date = '2024-9'

# функция для генерации месяцев в заданном интервале
def generate_months(start_date, end_date):
    # получаем стартовую и конечную дату
    start_date = datetime.strptime(start_date, '%Y-%m')
    end_date = datetime.strptime(end_date, '%Y-%m')
    months = []
    # перебираем месяцы до конечной даты
    while start_date <= end_date:
        months.append(start_date.strftime('%Y-%m'))
        # добавляем месяц
        start_date = start_date + relativedelta(months=1)
    return months

months = generate_months(start_date, end_date)


# создаем папки если их нет
if not os.path.exists('klines'):
    os.mkdir('klines')

klines = {
    'Date': [],
    'Open': [],
    'High': [],
    'Low': [],
    'Close': [],
    'Volume': []
}

for month in months:
    filename = f"{symbol}-{interval}-{month}.zip"
    # если нет klines, то скачиваем
    if not os.path.exists(f"klines/{filename}"):
        url = f"https://data.binance.vision/data/futures/um/monthly/klines/{symbol}/{interval}/{filename}"
        r = requests.get(url, allow_redirects=True)
        open(f"klines/{filename}", 'wb').write(r.content)
    # открываем zip
    with zipfile.ZipFile(f"klines/{filename}", 'r') as zip_file:
        # читаем csv внутри zip
        with zip_file.open(f"{symbol}-{interval}-{month}.csv", 'r') as csv_file:
            csv_reader = csv.reader(io.TextIOWrapper(csv_file, 'utf-8'))
            for row in csv_reader:
                # если строка содержит число (отбрасываем первую строку)
                if row[0].isdigit():
                    # заполняем списки с данными свечи
                    klines['Date'].append(datetime.fromtimestamp(int(row[0]) / 1000, tz=timezone.utc))
                    klines['Open'].append(float(row[1]))
                    klines['High'].append(float(row[2]))
                    klines['Low'].append(float(row[3]))
                    klines['Close'].append(float(row[4]))
                    klines['Volume'].append(float(row[5]))
