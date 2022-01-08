from src import db as database
import os
import time
from datetime import datetime
import importlib
from src.backtest import backtest
import backtrader as bt
import json
import pandas as pd
from src.yahoo.utils import getFreshPriceData

# This runner should 
# 1. initialise the db connection
# 2. given a list of tickers, industries or sectors, and an exchange, pull fresh historicals from yahoo, and append to datafile if entry doesnt exist
# 3. if parameter "store" is set to True, store results of run in db 


default_tickers = ['PAB', 'VTI']
default_industries = []
default_sectors = []
default_exchange = 'ASX'

default_strategy = '1-naive-strategy.py'

def psqlArray(arr):
    arraySubQueryStr = ""
    for i, item in enumerate(arr):
        apostrophiedStr = '\'%s\'' % item
        arraySubQueryStr += apostrophiedStr
        if i < len(arr)-1:
            arraySubQueryStr += ','
    return arraySubQueryStr

def checkExistance(data, inputs, key):
    arr = [item[key] for item in data]
    incorrectInputs = [item for item in inputs if item not in arr]
    if len(incorrectInputs): raise Exception('%s are incorrect inputs for exchange %s' % (', '.join(incorrectInputs), exchange)) 

def getTickersFromIndustries(db, industries, exchange):
    query = "SELECT DISTINCT industry from stock where exchange = '%s'" % exchange
    distinctData = db.selectQuery(query)
    checkExistance(distinctData, industries, 'industry')

    industryStr = psqlArray(industries)
    query = "SELECT * from stock where industry in (%s) and exchange = '%s'" % (industryStr, exchange)
    print('**** Industries provided: %s. Exchange: %s ****' % (industries, exchange))
    return query

def getTickersFromTickers(db, tickers, exchange):
    query = "SELECT DISTINCT ticker from stock where exchange = '%s'" % exchange
    distinctData = db.selectQuery(query)
    checkExistance(distinctData, tickers, 'ticker')

    tickerStr = psqlArray(tickers)
    query = "SELECT * from stock where ticker in (%s) and exchange = '%s'" % (tickerStr, exchange)
    print('**** Tickers provided: %s. Exchange: %s ****' % (tickers, exchange))
    return query

def getTickersFromSectors(db, sectors, exchange):
    query = "SELECT DISTINCT sector from stock where exchange = '%s'" % exchange
    distinctData = db.selectQuery(query)
    checkExistance(distinctData, sectors, 'sector')

    sectorStr = psqlArray(sectors)
    query = "SELECT * from stock where sector in (%s) and exchange = '%s'" % (sectorStr, exchange)
    print('**** Sectors provided: %s. Exchange: %s ****' % (sectors, exchange))
    return query

def validateParams(tickers, industries, sectors, exchange):
    # check parameters
    if not len(tickers) and not len(industries) and not len(sectors):
        raise ValueError('Need to provide at least 1 ticker, industry OR sector')

    if not exchange:
        raise ValueError('Exchange must be provided')

    if ((len(industries) and len(sectors)) or
        (len(tickers) and len(industries)) or
        (len(tickers) and len(sectors))):
        raise ValueError('Please provide only tickers, industries OR sectors')

def getData(ticker, exchange):
    datafilePath = os.path.abspath('/Users/grantwei/datafiles/price/{}/{}.csv'.format(exchange.lower(), ticker))
    df = pd.read_csv(datafilePath)
    df = df.drop(['Dividends', 'Stock Splits', 'Close'], 'columns')
    df = df.rename(columns={'Adj Close': 'Close'})
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.set_index('Date')
    return bt.feeds.PandasData(dataname=df)

# Needs to be able to be initiated from script or front end
class BacktestRunner():


    # UP TO: JUST REREAD AND FIX THIS WHOLE METHOD, need to refactor rest of file as well...
    def __init__(self, run_type='script', strategy=default_strategy, **kwargs):
        print('strategy***', strategy)
        self.run_type = run_type
        self.strategy = importlib.import_module('src.backtest.strategies.%s' % strategy.replace('.py', ''))
        # script wont have any parameters, it will use default for all
        if run_type == 'script':
            self.tickers = default_tickers
            self.industries = default_industries
            self.sectors = default_sectors
            self.exchange = default_exchange
        elif run_type == 'front-end':
            # front end can provide the script and only 1 ticker currently
            self.tickers = [kwargs.get('ticker')]
            self.exchange = kwargs.get('exchange')
            self.industries = []
            self.sectors = []
        else:
            raise ValueError('Run type must be of either script or front-end')


    def run(self, store=None):
        ''' Entry method into the script '''
        start_time = time.time()
        validateParams(self.tickers, self.industries, self.sectors, self.exchange)

        # initialise DB connection
        db = database.DB()

        # for industries, sectors as input need to get relevant tickers
        # check existance of the entry before querying
        if len(self.industries):
            query = getTickersFromIndustries(db, self.industries, self.exchange)
        elif len(self.tickers):
            query = getTickersFromTickers(db, self.tickers, self.exchange)
        elif len(self.sectors):
            query = getTickersFromSectors(db, self.sectors, self.exchange)


        data = db.selectQuery(query)
        tickersToPull = [entry['ticker'] for entry in data]
        print('Found %s stocks' % len(tickersToPull))
        print('List:', tickersToPull)

        backtest = self.initBacktester(db, tickersToPull)

        print("--- %s seconds ---" % (time.time() - start_time))


    def initBacktester(self, db, tickers):
        if not len(tickers):
            raise ValueError('Need to provide at least 1 ticker to run a backtest')

        tickerResults = {}
        for ticker in tickers:
            print('***** Running backtest for %s *****' % ticker)
            # retrieve price data
            df = getFreshPriceData(ticker, self.exchange)
            data = bt.feeds.PandasData(dataname=df)

            cerebro = bt.Cerebro()
            cerebro.addstrategy(self.strategy.Backtest, ticker=ticker)

            cerebro.adddata(data)
            
            cerebro.broker.setcash(10000)

            strat = cerebro.run()

            tickerResults = { **tickerResults, ticker: strat[0].getResults() }

        self.performance = tickerResults

