
import csv
import os
import time
import requests
import json
import re
import yfinance as yf

# This script pulls historical data from yahoo and stores into csv

exchange = 'asx'

def run(db):
    # get tickers
    tickerListPath = os.path.abspath('/Users/grantwei/datafiles/tickerlist/{}.csv'.format(exchange))
    successFile = open(os.path.abspath('/Users/grantwei/datafiles/tickeroutcome/{}/price_success.csv'.format(exchange)), 'a', newline='')
    errorFile = open(os.path.abspath('/Users/grantwei/datafiles/tickeroutcome/{}/price_error.csv'.format(exchange)), 'a', newline='')
    successWriter = csv.writer(successFile)
    errorWriter = csv.writer(errorFile)
    
    with open(tickerListPath, newline='') as file:
        reader = csv.DictReader(file, delimiter=',')
        i = 0
        for row in reader:
            ticker = row['Symbol']

            # skip all the random tickers with '-' and '.' in them
            try:
                if re.search('\-', ticker) is None and re.search('\.', ticker) is None:
                    datafilesPath = os.path.abspath('/Users/grantwei/datafiles/price/{}/{}.csv'.format(exchange, ticker))                        
                    # add .AX for asx 
                    if exchange == 'asx': 
                        ticker += '.AX'

                    # get data from yfinance
                    df = yf.Ticker(ticker).history(period='max')

                    with open(datafilesPath, 'w', newline='') as dataFile:
                        print('Storing', ticker)
                        # lets just round to 6 dp, should be enough
                        dataFile.write(df.round(6).to_csv())
                        successWriter.writerow([ticker])
                        i +=1

                    time.sleep(1)
            except Exception as e:
                print('An error has occurred for %s' % ticker, str(e))
                errorWriter.writerow([ticker, str(e)])

    successFile.close()
    errorFile.close()