import yfinance as yf
import re
import csv
import time


# This script stores premium fundamental data for every stock in the file

def run(db):
    base = yf.Base(username='rudyfinch255@gmail.com', password='0E@Of4BBb4u#')
    # open here cos i dont want to do this file I/O for every ticker when its not necessary 
    errorFile = open('../../../../datafiles/tickeroutcome/nasdaq/quarterly_error.csv', 'a', newline='')
    successFile = open('../../../../datafiles/tickeroutcome/nasdaq/quarterly_success.csv', 'a', newline='')
    errorWriter = csv.writer(errorFile)
    successWriter = csv.writer(successFile)

    with open('../../../../datafiles/tickerlist/nasdaq.csv', newline='') as file:
        reader = csv.DictReader(file, delimiter=',')
        i = 0
        for row in reader:
            # if i == 400:
            #     break
            stonk = row['Symbol']
            if re.search('-', stonk) is None:
                i += 1
                stonk = yf.Ticker(stonk, base.driver, base.session)
                stonk.store_premium_quarterly_financials(errorWriter=errorWriter, successWriter=successWriter) # change between annual and quarterly
                # store_premium_quarterly_financials does the writing to file
                time.sleep(2)
        print(i)
        
    errorFile.close()
    successFile.close()