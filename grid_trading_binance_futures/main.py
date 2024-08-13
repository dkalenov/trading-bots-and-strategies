from binance_bot import Bot
from threading import Thread


symbol = "BTCUSDT" # trading pair symbol
no_of_decimals = 1 # number of decimal places to round the price to
volume = 0.05 # trade volume
proportion = 0.03 # proportion of price change when placing orders
tp = 5 # target profit (take profit) in percent
n = 10 # number of orders placed on each side

symbol2 = "ETHUSDT"
no_of_decimals2 = 1
volume2 = 0.09
proportion2 = 0.03
tp2 = 5
n2 = 10



if __name__ == "__main__":
    bot1 = Bot(symbol, no_of_decimals, volume, proportion, tp, n)
    bot2 = Bot(symbol2, no_of_decimals2, volume2, proportion2, tp2, n2)

    def b1():
        bot1.run()

    def b2(): 
        bot2.run() 

    t1 = Thread(target=b1) 
    t2 = Thread(target=b2) 

    t1.start() 
    t2.start()
