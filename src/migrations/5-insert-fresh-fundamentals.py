
import csv
import os
from inflection import underscore
from datetime import datetime
import time

# current existing entries in db
fieldDict = {
    'annualNetIncome': 1,
    'annualTotalRevenue': 1,
    'annualBasicEPS': 1,
    'annualDilutedEPS': 1,
    'annualBasicAverageShares': 1, 
    'annualDilutedAverageShares': 1,
    'annualNormalizedEBITDA': 1,
    'annualEBIT': 1,
    'annualOperatingIncome': 1,
    'annualOperatingRevenue': 1,
    'annualNormalizedIncome': 1,
    'annualGrossProfit': 1,
    'quarterlyNetIncome': 1,
    'quarterlyTotalRevenue': 1,
    'quarterlyBasicEPS': 1,
    'quarterlyDilutedEPS': 1,
    'quarterlyBasicAverageShares': 1, 
    'quarterlyDilutedAverageShares': 1,
    'quarterlyNormalizedEBITDA': 1,
    'quarterlyEBIT': 1,
    'quarterlyOperatingIncome': 1,
    'quarterlyOperatingRevenue': 1,
    'quarterlyNormalizedIncome': 1,
    'quarterlyGrossProfit': 1,
}


# This script upserts the fundamentals stored in csv files from yahoo, into the DB



def run(db):
    start_time = time.time()
    exchange = 'nasdaq'
    freq = 'quarterly'
    startingDateAsString = '2020-05-31'
    count = 0
    totalRows = 0

    errorFile = open(os.path.abspath('/Users/grantwei/datafiles/tickeroutcome/{}/{}_upsert_error.csv'.format(exchange, freq)), 'a', newline='')
    successFile = open(os.path.abspath('/Users/grantwei/datafiles/tickeroutcome/{}/{}_upsert_success.csv'.format(exchange, freq)), 'a', newline='')
    errorWriter = csv.writer(errorFile)
    successWriter = csv.writer(successFile)

    with open(os.path.abspath('/Users/grantwei/datafiles/tickeroutcome/{}/{}_success.csv'.format(exchange, freq)), newline='') as successes:
        reader = csv.DictReader(successes, delimiter=',')

        for row in reader:
            ticker = row['ticker']
    # for subdir, dirs, files in os.walk('/Users/grantwei/datafiles/fundamentals/{}/{}'.format(exchange, freq)):
        # for file in files:
            # if totalRows > 0:
            #     break
            print(ticker)
            # ticker = file.replace('.csv', '') # removes for all stocks
            count += 1
            try:
                with open(os.path.abspath('/Users/grantwei/datafiles/fundamentals/{}/{}/{}.csv'.format(exchange, freq, ticker)), newline='') as f:
                    ticker = ticker.replace('.AX', '') # will only remove for asx
                    reader = csv.DictReader(f, delimiter=',')
                    findStockIdQuery = "SELECT id from stock where ticker = '%s' and exchange = '%s'" % (ticker, exchange.upper())
                    stock = db.selectQuery(findStockIdQuery)
                    stock_id = stock[0][0]
                    internalCount = 1
                    for row in reader:
                        dateAsString = row['date']
                        entry = row['entry']
                        value = row['figure']
                        date = datetime.strptime(dateAsString, '%Y-%m-%d')
                        startingDate = datetime.strptime(startingDateAsString, '%Y-%m-%d')
                        if fieldDict.get(entry) is not None and date > startingDate:
                            entry = underscore(entry)
                            entry = entry.replace('%s_' % freq, '')
                            # check if entry exists, this is a guard but it shouldnt return any data theoretically
                            findExistingEntryQuery = "SELECT * from %sfinancial where stock_id = '%s' and entry = '%s' and date = '%s'" % (freq, stock_id, entry, dateAsString)
                            existingEntry = db.selectQuery(findExistingEntryQuery)
                            if not len(existingEntry): # only insert if entry doesnt already exist
                                totalRows +=1
                                insertQuery = "INSERT INTO %sfinancial (stock_id, date, entry, value) VALUES (%s, '%s'::date, '%s', %s)" % (freq, stock_id, dateAsString, entry, value)
                                print('    ', internalCount, '| stock_id:', stock_id, '| entry:', entry, '| value:', value, '| date:', dateAsString)
                                db.upsertQuery(insertQuery)
                                internalCount += 1
                            else:
                                print('****** {} has an existing entry'.format(ticker))
                    successWriter.writerow([ticker])
            except Exception as e:
                print('Error:', str(e))
                errorWriter.writerow([ticker, str(e)])
        

    errorFile.close()
    successFile.close()

    print('Total Tickers:', count)
    print('Total Rows:', totalRows)
    print("--- %s seconds ---" % (time.time() - start_time))

