import yfinance as yf
import re
import csv
import time
from yahoo.utils import store_premium_financials
import os

# This script stores premium fundamental data for every stock in the file

def run(db):
    exchange = 'asx'
    freq = 'annual'
    startingDate = '2020-07-02'

    datafilePath = os.path.abspath('/Users/grantwei/datafiles/tickerlist/{}.csv'.format(exchange))
    with open(datafilePath, newline='') as file:
        reader = csv.DictReader(file, delimiter=',')
        i = 0
        for row in reader:
            ticker = row['Symbol']

            if re.search('-', ticker) is None: # and re.search('\.', ticker) is None:
                i += 1
                store_premium_financials(ticker, freq, startingDate, exchange)
                time.sleep(2.5)