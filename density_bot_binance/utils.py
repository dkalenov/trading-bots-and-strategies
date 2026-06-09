import math
import time


# функция для получения текущего времени
def get_ts():
    return int(time.time() * 1000)


# функция для округления вверх до нужного количества знаков
def round_up(num, decimals=0):
    multiplier = 10 ** decimals
    return math.ceil(num * multiplier) / multiplier


# функция для округления вниз до нужного количества знаков
def round_down(num, decimals=0):
    multiplier = 10 ** decimals
    return math.floor(num * multiplier) / multiplier


# функция для округления до нужного количества знаков
def round_price(num, decimals):
    return f"{num:.{decimals}f}"
