
import csv
import os
import time
import requests
import json
import re

# This script pulls the industry and sector for the ones that failed


def run(db):
    with open(os.path.abspath('/Users/grantwei/Downloads/stocks_no_industry.csv'), newline='') as file:
        reader = csv.DictReader(file, delimiter=',')
        for row in reader:
            stockId = row['id']
            ticker = row['ticker']
            exchange = row['exchange']

            try: 
                # skip all the random tickers with '-' and '.' in them
                if re.search('\-', ticker) is None and re.search('\.', ticker) is None:
                    print('Storing', ticker)
                    time.sleep(2)
                    if exchange == 'ASX': # append .AX for ASX stocks
                        ticker += '.AX'

                    endpoint = 'https://query2.finance.yahoo.com/v10/finance/quoteSummary/{}?formatted=true&crumb=QfjdVraMeWH&lang=en-AU&region=AU&modules=assetProfile%2CsecFilings&corsDomain=au.finance.yahoo.com'.format(ticker)
                    data = requests.get(url=endpoint)

                    jsonData = json.loads(data.content)
                    quoteSummary = jsonData.get('quoteSummary')
                    if quoteSummary is None:
                        print('    No quoteSummary')
                        continue

                    result = quoteSummary.get('result')
                    if result is None:
                        print('    No result')
                        continue

                    assetProfile = result[0].get('assetProfile')
                    if assetProfile is None:
                        print('    No assetProfile')
                        continue

                    sector = assetProfile.get('sector')
                    industry = assetProfile.get('industry')
                    if sector is None:
                        print('    Ticker:', ticker, 'has no SECTOR')
                    if industry is None:
                        print('    Ticker:', ticker, 'has no INDUSTRY')

                    if sector is None and industry is None:
                        print('    Both none skipping')
                        continue

                    print('    Ticker:', ticker, '| Stock Id:', stockId, '| Sector:', sector, '| Industry:', industry)
                    insertQuery = "UPDATE stock SET industry = '%s', sector = '%s' WHERE id = %s" % (industry, sector, stockId)
                    db.upsertQuery(insertQuery)

                    with open(os.path.abspath('/Users/grantwei/datafiles/tickeroutcome/industry_success.csv'), 'a', newline='') as successFile:
                        successWriter = csv.writer(successFile)
                        successWriter.writerow([ticker])
            except Exception as e:
                print('An error has occurred for %s' % ticker, str(e))
                with open(os.path.abspath('/Users/grantwei/datafiles/tickeroutcome/industry_error.csv'), 'a', newline='') as errorFile:
                    errorWriter = csv.writer(errorFile)
                    errorWriter.writerow([ticker, str(e)])