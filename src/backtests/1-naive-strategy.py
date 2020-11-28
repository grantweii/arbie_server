import requests
import json
import backtrader as bt
import pandas as pd
import os
from src import db as database
import csv

''' This is a naive strategy for testing purposes. If todays price > yesterdays price, buy, otherwise sell '''
class Backtest(bt.Strategy):
    params = dict(ticker='')
    STARTING_CASH = 10000
    POSITION_SIZE_PCNT = 1
    def __init__(self):
        self.portfolio_shares = 0
        self.cash = self.STARTING_CASH
        self.sma = bt.ind.SimpleMovingAverage(period=20)
        self.open = self.datas[0].open
        self.high = self.datas[0].high
        self.low = self.datas[0].low
        self.close = self.datas[0].close
        self.ticker = self.params.ticker
        
    def log(self, txt, dt=None):
        ''' Logging function for this strategy'''
        dt = dt or self.datas[0].datetime.date(0)
        print('%s\n%s' % (dt.isoformat(), txt))
        
    def calculateSize(self, price):
        position_value = self.POSITION_SIZE_PCNT * self.STARTING_CASH
        return position_value / price

    def next(self):
        # self.log('Close: %.2f' % self.close[0])
        size = self.calculateSize(self.close[0])
        
        if self.sma > self.data.close and self.cash > 0:
            # we buy
            size = self.calculateSize(self.close[0])
            self.portfolio_shares = size
            self.cash = 0
        elif self.sma < self.data.close and self.portfolio_shares > 0:
            cash = self.portfolio_shares * self.close[0]
            self.portfolio_shares = 0
            self.cash = cash

    def stop(self):
        print('*** BT End for %s ***' % self.ticker)
        print('Todays share price', self.close[0])
        print('Final portfolio shares', self.portfolio_shares)
        print('Final portfolio value', self.close[0] * self.portfolio_shares)
        print('Final cash', self.cash)
