import psycopg2
from psycopg2 import sql
from datetime import datetime, timezone, timedelta
from config import DB_CONFIG

VIETNAM_TZ = timezone(timedelta(hours=7))


def connect_db():
    return psycopg2.connect(**DB_CONFIG)


def save_signal(symbol, timeframe, signal, entry_price):
    conn = connect_db()
    cur = conn.cursor()

    now_utc = datetime.now(timezone.utc)
    now_local = now_utc.astimezone(VIETNAM_TZ)
    unix_timestamp = int(now_utc.timestamp())

    query = sql.SQL("""
        INSERT INTO public.tradingview_signals (symbol, timeframe, signal, 
        entry_price, unix_timestamp, utc_time, local_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
    """)
    cur.execute("SET search_path TO public;")

    cur.execute(query, (symbol, timeframe, signal, entry_price,
                        unix_timestamp, now_utc, now_local))

    conn.commit()
    cur.close()
    conn.close()

    print(f"Saved signal: {symbol} - {signal} at {entry_price} ({now_utc} UTC, {now_local} Local)")