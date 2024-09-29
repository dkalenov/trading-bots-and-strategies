import sloping
import io
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
import os
import requests
import zipfile
import csv
import plotly.graph_objects as go



symbol = 'BTCUSDT'
interval = '1h'
start_date = '2024-04'
end_date = '2024-08'
lenght = 50

def generate_months(start_date, end_date):
    # get start and end dates
    start_date = datetime.strptime(start_date, '%Y-%m')
    end_date = datetime.strptime(end_date, '%Y-%m')
    months = []
    # loop through months
    while start_date <= end_date:
        months.append(start_date.strftime('%Y-%m'))
        # add month
        start_date = start_date + relativedelta(months=1)
    return months

months = generate_months(start_date, end_date)

# create folders if they don't exist
if not os.path.exists('klines'):
    os.mkdir('klines')
if not os.path.exists('plots'):
    os.mkdir('plots')
