import requests
import json
import backtrader as bt
import pandas as pd
import os

base_url = 'https://query1.finance.yahoo.com/v8/finance/chart'
params = {'period1': -2208988800, 'period2': 1605176449, 'interval': '1d', 'includePrePost': False, 'events': 'div,splits'}

class MarketTimingModel(bt.Strategy):
    ticker = 'MSFT'
    STARTING_CASH = 10000
    POSITION_SIZE_PCNT = 1
    def __init__(self):
        self.portfolio_shares = 0
        self.cash = self.STARTING_CASH
        self.sma = bt.ind.SimpleMovingAverage(period=20)
        self.dataclose = self.datas[0].close
        
    def log(self, txt, dt=None):
        ''' Logging function for this strategy'''
        dt = dt or self.datas[0].datetime.date(0)
        print('%s\n%s' % (dt.isoformat(), txt))
        
    def calculateSize(self, price):
        position_value = self.POSITION_SIZE_PCNT * self.STARTING_CASH
        return position_value / price

    def next(self):
        self.log('Close: %.2f' % self.dataclose[0])
#         if self.sma > self.data.close:
# # TO DO
#         elif self.sma < self.data.close:
# TODO

                
    def stop(self):
        print('*** BT End ***')
        print('Todays share price', self.dataclose[0])
        print('Final portfolio shares', self.portfolio_shares)
        print('Final portfolio value', self.dataclose[0] * self.portfolio_shares)
        print('Final cash', self.cash)

        
def getData():
    response = requests.request('GET', '{}/{}'.format(base_url, 'MSFT'), params=params)
    # up to here
    data = response.json()
    print(data)
    ticker = data['name']
#     store in a file anyway
    with open(os.path.join('./stock-data', f'{ticker}.txt'), 'w') as f:
        f.write(json.dumps(data))
    df = pd.DataFrame.from_dict(data['history']).transpose()
    df.index = pd.to_datetime(df.index, format="%Y-%m-%d")
    df = df.applymap(float)
    return bt.feeds.PandasData(dataname=df)
    
        
if __name__ == "__main__":
    cerebro = bt.Cerebro()
    cerebro.addstrategy(MarketTimingModel)

    data = getData()
    cerebro.adddata(data)
    print('Starting Portfolio Value: %.2f' % cerebro.broker.get_value())
    
    cerebro.broker.setcash(10000)

    cerebro.run()
    
    print('Final Broker Portfolio Value: %.2f' % cerebro.broker.get_value())
    print('Final Broker Cash: %.2f' % cerebro.broker.get_cash())

    cerebro.plot()
