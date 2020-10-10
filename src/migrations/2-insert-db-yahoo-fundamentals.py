
import csv
import os
from inflection import underscore


# This script upserts the fundamentals stored in csv files from yahoo, into the DB


def run(db):
    count = 0
    totalRows = 0
    for file in os.listdir('../../../../datafiles/fundamentals/asx/annual'): # change: folder and exchange
        ticker = file.replace('.csv', '')
        ticker = ticker.replace('.AX', '') # change: exchange
        count += 1
        if file == '.DS_Store':
            continue
        with open('../../../../datafiles/fundamentals/asx/annual/{}'.format(file), newline='') as f: # change: folder and exchange
            reader = csv.DictReader(f, delimiter=',')
            findStockIdQuery = "SELECT id from stock where ticker = '%s' and exchange = 'ASX'" % (ticker) # change: exchange
            data = db.selectQuery(findStockIdQuery)
            stock_id = data[0][0]
            for row in reader:
                date = row[0]
                entry = row[1]
                value = row[2]
                if fieldDict.get(entry) is not None:
                    totalRows +=1
                    entry = underscore(entry)
                    entry = entry.replace('annual_', '') # change: timeperiod
                    # change: table
                    insertQuery = "INSERT INTO annualfinancial (stock_id, date, entry, value) VALUES (%s, '%s'::date, '%s', %s)" % (stock_id, date, entry, value)
                    db.upsertQuery(insertQuery)

        print('Stored', ticker)
    print('Total Tickers:', count)
    print('Total Rows:', totalRows)

            
