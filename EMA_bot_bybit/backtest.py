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
