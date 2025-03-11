
import pandas as pd
import time
import requests
from datetime import datetime, timedelta, timezone

csv_file = "tradingview_signals_server.csv"
start_date = "2025-03-01"
end_date = "2025-03-10"
interval = "30m"

def download_klines_daily(symbol, interval, start_date, end_date):
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')

    klines = {
        'Date': [], 'Open': [], 'High': [], 'Low': [], 'Close': [], 'Volume': [], 'Symbol': []
    }

    limit = 1500
    current_date = start_date

    while current_date <= end_date:
        start_time = int(current_date.timestamp() * 1000)
        end_time = int(end_date.timestamp() * 1000)

        url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&startTime={start_time}&endTime={end_time}&limit={limit}"

        try:
            r = requests.get(url)
            r.raise_for_status()
            data = r.json()
            if not data:
                print(f"⚠️ Нет данных для {symbol} в {current_date.strftime('%Y-%m-%d')}")
                break

            for row in data:
                klines['Date'].append(datetime.fromtimestamp(row[0] / 1000, tz=timezone.utc))
                klines['Open'].append(float(row[1]))
                klines['High'].append(float(row[2]))
                klines['Low'].append(float(row[3]))
                klines['Close'].append(float(row[4]))
                klines['Volume'].append(float(row[5]))
                klines['Symbol'].append(symbol)

# Update `current_date`, moving forward by `limit` intervals
            current_date += timedelta(hours=limit)
            time.sleep(0.01)

        except Exception as e:
            print(f"❌ Download error {symbol}: {e}")
            break

    if not klines['Date']:
        print(f"⚠️ No data for {symbol} from {start_date} to {end_date}")
        return pd.DataFrame()

    df = pd.DataFrame(klines)
    df['Date'] = pd.to_datetime(df['Date'])
    df.set_index('Date', inplace=True)

    return df


signals_df = pd.read_csv(csv_file)
symbols = signals_df['symbol'].unique().tolist()

dfs = []
for symbol in symbols:
    print(f"📥 Downloading data for {symbol}...")
    df = download_klines_daily(symbol, interval, start_date, end_date)
    if not df.empty:
        dfs.append(df)


if dfs:
    df_all = pd.concat(dfs)
    df_all.to_csv(f"klines_data_{interval}.csv")
    print(f"✅ Save data: klines_data_{interval}.csv")
else:
    print("⚠️ NO DATA.")