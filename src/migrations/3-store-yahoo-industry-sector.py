import yfinance as yf
import time
import re
import csv
import os

# This script retrieves and upserts the industry and sector from yahoo for every ticker in the file
# uses the 'info' method of yfinance

def run(db):
    print('wtf going on', os.listdir())
    errorFile = open('../../../datafiles/tickeroutcome/asx/industry_error.csv', 'a', newline='') # change exchange
    successFile = open('../../../datafiles/tickeroutcome/asx/industry_success.csv', 'a', newline='') # change exchange
    failedFile = open('../../../datafiles/tickeroutcome/asx/industry_fails_4.csv', 'a', newline='') # change exchange
    errorWriter = csv.writer(errorFile)
    successWriter = csv.writer(successFile)
    failedWriter = csv.writer(failedFile)
    
    # 1. for all stocks in file
    with open('../../../datafiles/tickeroutcome/asx/industry_fails_3.csv', newline='') as file: # change exchange
        reader = csv.DictReader(file, delimiter=',')
        i = 0
        for row in reader:
            try: 
                yahooTicker = row['Symbol']
                ticker = yahooTicker.replace('.AX', '') # change: exchange
                # skip all the random tickers with '-' and '.' in them
                if re.search('\-', ticker) is None and re.search('\.', ticker) is None:
                    print('Storing', ticker)
                    i += 1
                    # 2. get stock from db because we need the ID
                    findStockIdQuery = "SELECT id from stock where ticker = '%s' and exchange = 'ASX'" % (ticker) # change: exchange
                    data = db.selectQuery(findStockIdQuery)
                    stock_id = data[0][0]

                    # 3. get stock from yfinance
                    yfStock = yf.Ticker(yahooTicker)
                    info = yfStock.info

                    sector = info.get('sector')
                    industry = info.get('industry')
                    if sector is None:
                        print('Ticker:', ticker, 'has no SECTOR')
                    if industry is None:
                        print('Ticker:', ticker, 'has no INDUSTRY')
                    # print('Ticker:', ticker, 'Id:', stock_id, 'Sector:' sector, 'Industry': industry)
                    insertQuery = "UPDATE stock SET industry = '%s', sector = '%s' WHERE id = %s" % (industry, sector, stock_id)
                    db.upsertQuery(insertQuery)

                    successWriter.writerow([ticker])
                    time.sleep(1)
            except Exception as e:
                print('An error has occurred for %s' % ticker)
                errorWriter.writerow([ticker, str(e)])
                failedWriter.writerow([ticker, row['Company name']])
        print('Tickers stored', i)

    errorFile.close()
    successFile.close()
    
