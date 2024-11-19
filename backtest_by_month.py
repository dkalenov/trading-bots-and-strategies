from hyperopt import fmin, tpe, hp, Trials
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import backtesting
import os

# Функция для загрузки данных по месяцам
def generate_monthly_intervals(start_date, end_date, interval_count=12):
    intervals = []
    current_date = datetime.strptime(start_date, "%Y-%m")
    end_date = datetime.strptime(end_date, "%Y-%m")

    while current_date < end_date and len(intervals) < interval_count:
        next_date = current_date + relativedelta(months=1)
        intervals.append((current_date.strftime("%Y-%m"), next_date.strftime("%Y-%m")))
        current_date = next_date

    return intervals

# Основная функция для бэктеста по месяцам
def monthly_backtest(strategy, data, intervals, initial_capital=1_000_000):
    total_pnl = 0
    profit_months = 0
    loss_months = 0
    monthly_results = []

    # Убедимся, что индекс данных - это datetime
    data.index = pd.to_datetime(data.index)

    for start, end in intervals:
        # Фильтруем данные по индексам
        monthly_data = data[(data.index >= start) & (data.index < end)]
        if len(monthly_data) == 0:
            continue
        
        # Запуск бэктеста для каждого месяца
        test = backtesting.Backtest(monthly_data, strategy, cash=initial_capital, commission=0.0005)
        res = test.run()

        pnl_month = round((res._trades['PnL'].sum() / initial_capital) * 100, 2)
        monthly_results.append((start, end, pnl_month))

        total_pnl += pnl_month
        if pnl_month > 0:
            profit_months += 1
        else:
            loss_months += 1

    return total_pnl, profit_months, loss_months, monthly_results

# Оптимизация стратегии с использованием next_evals
def optimize_strategy_with_next_evals(data, symbol, interval, start_date, end_date, max_evals=5, next_evals=20):
    """
    Функция оптимизации стратегии с возможностью добавления итераций через next_evals.
    """
    intervals = generate_monthly_intervals(start_date, end_date)
    trials = Trials()  # Trials сохраняет результаты между вызовами fmin
    
    # Пространство поиска параметров
    space = {
        'window_length': hp.quniform('window_length', 20, 50, 1),  # Уменьшите диапазон
        'min_space': hp.quniform('min_space', 5, 15, 1),
        'sloping_atr_length': hp.quniform('sloping_atr_length', 10, 30, 1),
        'take_profit_multiplier': hp.uniform('take_profit_multiplier', 1.1, 2.0),  # Меньший диапазон
        'stop_loss_multiplier': hp.uniform('stop_loss_multiplier', 1.1, 2.0)
    }

    # Функция оценки
    def objective(params):
        # Установка параметров стратегии
        SlopingStrategy.window_length = int(params['window_length'])
        SlopingStrategy.min_space = int(params['min_space'])
        SlopingStrategy.sloping_atr_length = int(params['sloping_atr_length'])
        SlopingStrategy.take_profit_multiplier = params['take_profit_multiplier']
        SlopingStrategy.stop_loss_multiplier = params['stop_loss_multiplier']

        # Бэктест на месячных интервалах
        total_pnl, profit_months, loss_months, monthly_results = monthly_backtest(
            strategy=SlopingStrategy,
            data=data,
            intervals=intervals,
            initial_capital=1_000_000
        )

        # Логирование промежуточных результатов
        print(f"\nПараметры: {params}")
        print(f"Общий профит: {total_pnl:.2f}%, Профитные месяцы: {profit_months}, Убыточные месяцы: {loss_months}")
        print(f"Результаты по месяцам: {monthly_results}")

        # Возвращаем словарь с ключами 'loss', 'status' и 'params' 
        # для корректной работы Hyperopt
        return {'loss': -total_pnl, 'status': 'ok', 'params': params}  

    # Первый запуск оптимизации
    print(f"Запуск первой оптимизации с max_evals={max_evals}")
    fmin(objective, space, algo=tpe.suggest, max_evals=max_evals, trials=trials)

    # Повторные запуски через next_evals
    if next_evals > 0:
        print(f"\nДобавляем {next_evals} дополнительных итераций.")
        fmin(objective, space, algo=tpe.suggest, max_evals=max_evals + next_evals, trials=trials)

    # Получение лучших параметров
    best_params = trials.best_trial['result']['params']
    best_loss = trials.best_trial['result']['loss']

    print(f"\nЛучшие параметры: {best_params}")
    print(f"Наибольший профит / Наименьший убыток: {-best_loss}")
    return best_params, trials
