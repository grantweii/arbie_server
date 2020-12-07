from src import db as database
import os
import yfinance as yf
import time
from datetime import timedelta, date, datetime
import importlib
from src.backtests import backtest
import backtrader as bt
import json
import pandas as pd

# This runner should 
# 1. initialise the db connection
# 2. given a list of tickers, industries or sectors, and an exchange, pull fresh historicals from yahoo, and append to datafile if entry doesnt exist
# 3. if parameter "store" is set to True, store results of run in db 


tickers = ['PAB', 'VTI']
industries = []
sectors = []
exchange = 'ASX'

script = importlib.import_module('src.backtests.1-naive-strategy')

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

def getTickersFromIndustries(db):
    query = "SELECT DISTINCT industry from stock where exchange = '%s'" % exchange
    distinctData = db.selectQuery(query)
    checkExistance(distinctData, industries, 'industry')

    industryStr = psqlArray(industries)
    query = "SELECT * from stock where industry in (%s) and exchange = '%s'" % (industryStr, exchange)
    print('**** Industries provided: %s. Exchange: %s ****' % (industries, exchange))
    return query

def getTickersFromTickers(db):
    query = "SELECT DISTINCT ticker from stock where exchange = '%s'" % exchange
    distinctData = db.selectQuery(query)
    checkExistance(distinctData, tickers, 'ticker')

    tickerStr = psqlArray(tickers)
    query = "SELECT * from stock where ticker in (%s) and exchange = '%s'" % (tickerStr, exchange)
    print('**** Tickers provided: %s. Exchange: %s ****' % (tickers, exchange))
    return query

def getTickersFromSectors(db):
    query = "SELECT DISTINCT sector from stock where exchange = '%s'" % exchange
    distinctData = db.selectQuery(query)
    checkExistance(distinctData, sectors, 'sector')

    sectorStr = psqlArray(sectors)
    query = "SELECT * from stock where sector in (%s) and exchange = '%s'" % (sectorStr, exchange)
    print('**** Sectors provided: %s. Exchange: %s ****' % (sectors, exchange))
    return query

def validateParams():
    # check parameters
    if not len(tickers) and not len(industries) and not len(sectors):
        raise ValueError('Need to provide at least 1 ticker, industry OR sector')

    if not exchange:
        raise ValueError('Exchange must be provided')

    if ((len(industries) and len(sectors)) or
        (len(tickers) and len(industries)) or
        (len(tickers) and len(sectors))):
        raise ValueError('Please provide only tickers, industries OR sectors')


# date should be a datetime
def getPreviousBusinessDay(date=datetime.today()):
    offset = max(1, (date.weekday() + 6) % 7 - 3)
    delta = timedelta(offset)
    lastBusinessDay = date - delta
    return lastBusinessDay


''' This method is used to pull the historical prices for a list of tickers and append to their datafile if necessary. We make the assumption that if the last entry is the last business day, we dont need to pull fresh data '''
def pullHistorical(tickers):
    for ticker in tickers:
        datafilesPath = os.path.abspath('/Users/grantwei/datafiles/price/{}/{}.csv'.format(exchange.lower(), ticker))
        # add .AX for asx, must be after the datafilesPath instantiation
        if exchange == 'ASX': 
            ticker += '.AX'

        with open(datafilesPath, 'r+', newline='') as dataFile:
            # get the last entry in the datafile
            lines = dataFile.read().splitlines()
            lastLine = lines[-1]
            dateString = lastLine.split(',')[0]

            # check if datafile is up to date
            lastBusinessDay = getPreviousBusinessDay()
            lastStoredDate = datetime.strptime(dateString, '%Y-%m-%d')
            daysDelta = (lastBusinessDay - lastStoredDate).days
            # daysDelta can be -1, 0, 1+, where 1+ should retrieve and store
            # if -1, that means we have stored data for today, hence lastBusinessDay (yesterday) - today = -1
            if daysDelta <= 0: 
                print('Skipping %s. Already stored' % ticker)
                continue

            # need to add one day to get start date
            date = datetime.strptime(dateString, '%Y-%m-%d')
            startDate = date + timedelta(days=1)
            startDateString = startDate.strftime('%Y-%m-%d')
            df = yf.Ticker(ticker).history(start=startDateString)
            print('Storing %s. Last date stored: %s' % (ticker, dateString))
            # Lets just round to 6 dp, should be enough. Write to file as well
            dataFile.write(df.round(6).to_csv(header=False))

        time.sleep(0.5)

''' Entry method into the script '''
def run(store=None):
    start_time = time.time()
    validateParams()

    # initialise DB connection
    db = database.DB()

    # for industries, sectors as input need to get relevant tickers
    # check existance of the entry before querying
    if len(industries):
        query = getTickersFromIndustries(db)
    elif len(tickers):
        query = getTickersFromTickers(db)
    elif len(sectors):
        query = getTickersFromSectors(db)

    data = db.selectQuery(query)
    tickersToPull = [entry['ticker'] for entry in data]
    print('Found %s stocks' % len(tickersToPull))
    print('List:', tickersToPull)
    
    # pull fresh prices
    pullHistorical(tickersToPull)

    initBacktester(db, tickersToPull, exchange)

    print("--- %s seconds ---" % (time.time() - start_time))

def initBacktester(db, tickers, exchange):
    if not len(tickers):
        raise ValueError('Need to provide at least 1 ticker to run a backtest')

    for ticker in tickers:
        print('***** Running backtest for %s *****' % ticker)
        # initialise DB connection 
        data = getData(db, ticker, exchange)

        cerebro = bt.Cerebro()
        cerebro.addstrategy(script.Backtest, ticker=ticker)

        cerebro.adddata(data)
        print('Starting Portfolio Value: %.2f' % cerebro.broker.get_value())
        
        cerebro.broker.setcash(10000)

        strat = cerebro.run()
        object_methods = [method_name for method_name in dir(strat)
                  if callable(getattr(strat, method_name))]
        print('strat is', strat[0].getResults())
    
    print('Final Broker Portfolio Value: %.2f' % cerebro.broker.get_value())
    print('Final Broker Cash: %.2f' % cerebro.broker.get_cash())

def getData(database, ticker, exchange):
    datafilePath = os.path.abspath('/Users/grantwei/datafiles/price/{}/{}.csv'.format(exchange.lower(), ticker))
    df = pd.read_csv(datafilePath)
    df = df.drop(['Dividends', 'Stock Splits', 'Close'], 'columns')
    df = df.rename(columns={'Adj Close': 'Close'})
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.set_index('Date')
    return bt.feeds.PandasData(dataname=df)