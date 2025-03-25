def save_candlestick_plot(df, symbol, interval, hours_window):
    df_symbol = df.copy()

    df_symbol['hour_only'] = df_symbol.index.floor(f'{hours_window}h')

    unique_hours = sorted(df_symbol['hour_only'].unique())

    intervals = [unique_hours[i: i + 2] for i in range(len(unique_hours) - 1)]

    print(f"📊 Графики для {symbol}: {len(intervals)} интервалов по {hours_window} часов")

    base_dir = os.path.join("plots_new", symbol, f"{symbol}_{interval}")
    os.makedirs(base_dir, exist_ok=True)

    for idx, interval_hours in enumerate(intervals):
        start, end = interval_hours
        df_window = df_symbol.loc[(df_symbol.index >= start) & (df_symbol.index < end)]

        if df_window.empty:
            continue

        fig, ax = plt.subplots(figsize=(16, 8))

        for date, row in df_window.iterrows():
            color = 'green' if row['close'] >= row['open'] else 'red'
            ax.plot([date, date], [row['low'], row['high']], color='black')  # Тень
            ax.plot([date, date], [row['open'], row['close']], color=color, linewidth=6)  # Тело

        ax.scatter(df_window.index[df_window['signal'] == 1], df_window['close'][df_window['signal'] == 1],
                   color='lime', marker='^', s=200, label='LONG')

        ax.scatter(df_window.index[df_window['signal'] == -1], df_window['close'][df_window['signal'] == -1],
                   color='red', marker='v', s=200, label='SHORT')

        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%d %b %Y\n%H:%M'))
        plt.xticks(rotation=30)
        plt.grid(True, linestyle='--', alpha=0.5)
        plt.title(f"{symbol} {interval} — Интервал {idx + 1}")
        plt.xlabel("Date")
        plt.ylabel("Price")
        plt.legend(loc="upper left")

        plot_path = os.path.join(base_dir, f"{symbol}_{interval}_part{idx + 1}_long.png")
        plt.savefig(plot_path, dpi=300)
        plt.close()

        print(f"✅ График сохранён: {plot_path}")