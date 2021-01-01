# Standard Python Libraries
import requests
import json
import os
from datetime import datetime, timedelta, date
import csv
import time
import yfinance as yf
import os
import pandas as pd

# date should be a datetime
def getPreviousBusinessDay(date=datetime.today()):
    offset = max(1, (date.weekday() + 6) % 7 - 3)
    delta = timedelta(offset)
    lastBusinessDay = date - delta
    return lastBusinessDay


# This probably shouldnt be in this yahoo utils folder...
def getFreshPriceData(ticker, exchange, jsonify=False):
    ''' Pulls fresh fundamentals and returns price data from locally stored files '''
    pullHistorical(ticker, exchange)
    datafilePath = os.path.abspath('/Users/grantwei/datafiles/price/{}/{}.csv'.format(exchange.lower(), ticker))
    df = pd.read_csv(datafilePath)
    df = df.drop(['Dividends', 'Stock Splits', 'Close'], 'columns')
    df = df.rename(columns={'Adj Close': 'Close'})
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.set_index('Date')
    if jsonify:
        print('jsonified**', df.to_json())
        return df.to_json()
    print('df ****8', df)
    return df

def pullHistorical(ticker, exchange):
    ''' This method is used to pull the historical prices for a ticker and append to their datafile if necessary. We make the assumption that if the last entry is the last business day, we dont need to pull fresh data '''
    print('Pulling fresh price data for %s' % ticker)
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
            return

        # need to add one day to get start date
        date = datetime.strptime(dateString, '%Y-%m-%d')
        startDate = date + timedelta(days=1)
        startDateString = startDate.strftime('%Y-%m-%d')
        df = yf.Ticker(ticker).history(start=startDateString)
        print('Storing fresh price data for %s. Last date stored: %s' % (ticker, dateString))
        # Lets just round to 6 dp, should be enough. Write to file as well
        dataFile.write(df.round(6).to_csv(header=False))

    time.sleep(0.5)

# Python LIbraries
import numpy as np
import pandas as pd

# Local Libraries
from inflection import underscore
import yfinance as yf

def cleanData(data):
    jsonData = json.loads(data)
    timeseries = jsonData.get('timeseries').get('result')
    list = []
    for item in timeseries:
        key = item.get('meta').get('type')[0]
        values = item.get(key)
        cleanKey = underscore(key.replace('annual', '').replace('quarterly', ''))
        if values is not None:
            for entry in values:
                if entry is not None:
                    entryDate = entry.get('asOfDate')
                    entryFigure = entry.get('reportedValue').get('raw')
                    list.append({ 'entry': cleanKey, 'date': entryDate, 'value': entryFigure })
    return list

def get_cashflow(ticker, exchange, freq='yearly'):
    ''' Directly accesses yahoo api to retrieve the cashflow statement '''
    if (exchange == 'ASX'):
        ticker += '.AX'

    epochTime = int(time.time())
    if freq == 'yearly':
        cashflowUrl = 'https://query2.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/%s?lang=en-AU&region=AU&symbol=%s&padTimeSeries=true&type=annualFreeCashFlow,trailingFreeCashFlow,annualCapitalExpenditure,trailingCapitalExpenditure,annualOperatingCashFlow,trailingOperatingCashFlow,annualEndCashPosition,trailingEndCashPosition,annualBeginningCashPosition,trailingBeginningCashPosition,annualChangeInCashSupplementalAsReported,trailingChangeInCashSupplementalAsReported,annualCashFlowFromContinuingFinancingActivities,trailingCashFlowFromContinuingFinancingActivities,annualNetOtherFinancingCharges,trailingNetOtherFinancingCharges,annualCashDividendsPaid,trailingCashDividendsPaid,annualRepurchaseOfCapitalStock,trailingRepurchaseOfCapitalStock,annualCommonStockIssuance,trailingCommonStockIssuance,annualRepaymentOfDebt,trailingRepaymentOfDebt,annualInvestingCashFlow,trailingInvestingCashFlow,annualNetOtherInvestingChanges,trailingNetOtherInvestingChanges,annualSaleOfInvestment,trailingSaleOfInvestment,annualPurchaseOfInvestment,trailingPurchaseOfInvestment,annualPurchaseOfBusiness,trailingPurchaseOfBusiness,annualOtherNonCashItems,trailingOtherNonCashItems,annualChangeInAccountPayable,trailingChangeInAccountPayable,annualChangeInInventory,trailingChangeInInventory,annualChangesInAccountReceivables,trailingChangesInAccountReceivables,annualChangeInWorkingCapital,trailingChangeInWorkingCapital,annualStockBasedCompensation,trailingStockBasedCompensation,annualDeferredIncomeTax,trailingDeferredIncomeTax,annualDepreciationAndAmortization,trailingDepreciationAndAmortization,annualNetIncome,trailingNetIncome&merge=false&period1=493590046&period2=%s&corsDomain=au.finance.yahoo.com' % (ticker, ticker, epochTime)
    else:
        cashflowUrl = 'https://query1.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/%s?lang=en-AU&region=AU&symbol=%s&padTimeSeries=true&type=quarterlyFreeCashFlow,trailingFreeCashFlow,quarterlyCapitalExpenditure,trailingCapitalExpenditure,quarterlyOperatingCashFlow,trailingOperatingCashFlow,quarterlyEndCashPosition,trailingEndCashPosition,quarterlyBeginningCashPosition,trailingBeginningCashPosition,quarterlyChangeInCashSupplementalAsReported,trailingChangeInCashSupplementalAsReported,quarterlyCashFlowFromContinuingFinancingActivities,trailingCashFlowFromContinuingFinancingActivities,quarterlyNetOtherFinancingCharges,trailingNetOtherFinancingCharges,quarterlyCashDividendsPaid,trailingCashDividendsPaid,quarterlyRepurchaseOfCapitalStock,trailingRepurchaseOfCapitalStock,quarterlyCommonStockIssuance,trailingCommonStockIssuance,quarterlyRepaymentOfDebt,trailingRepaymentOfDebt,quarterlyInvestingCashFlow,trailingInvestingCashFlow,quarterlyNetOtherInvestingChanges,trailingNetOtherInvestingChanges,quarterlySaleOfInvestment,trailingSaleOfInvestment,quarterlyPurchaseOfInvestment,trailingPurchaseOfInvestment,quarterlyPurchaseOfBusiness,trailingPurchaseOfBusiness,quarterlyOtherNonCashItems,trailingOtherNonCashItems,quarterlyChangeInAccountPayable,trailingChangeInAccountPayable,quarterlyChangeInInventory,trailingChangeInInventory,quarterlyChangesInAccountReceivables,trailingChangesInAccountReceivables,quarterlyChangeInWorkingCapital,trailingChangeInWorkingCapital,quarterlyStockBasedCompensation,trailingStockBasedCompensation,quarterlyDeferredIncomeTax,trailingDeferredIncomeTax,quarterlyDepreciationAndAmortization,trailingDepreciationAndAmortization,quarterlyNetIncome,trailingNetIncome&merge=false&period1=493590046&period2=%s&corsDomain=au.finance.yahoo.com' % (ticker, ticker, epochTime)
    
    data = requests.get(url=cashflowUrl).text
    cashflow = cleanData(data)

    return cashflow

def get_balance_sheet(ticker, exchange, freq='yearly'):
    ''' Directly accesses yahoo api to retrieve the balance sheet '''
    if (exchange == 'ASX'):
        ticker += '.AX'

    epochTime = int(time.time())
    if freq == 'yearly':
        balanceSheetUrl = 'https://query2.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/%s?lang=en-AU&region=AU&symbol=%s&padTimeSeries=true&type=annualTotalAssets,trailingTotalAssets,annualStockholdersEquity,trailingStockholdersEquity,annualGainsLossesNotAffectingRetainedEarnings,trailingGainsLossesNotAffectingRetainedEarnings,annualRetainedEarnings,trailingRetainedEarnings,annualCapitalStock,trailingCapitalStock,annualTotalLiabilitiesNetMinorityInterest,trailingTotalLiabilitiesNetMinorityInterest,annualTotalNonCurrentLiabilitiesNetMinorityInterest,trailingTotalNonCurrentLiabilitiesNetMinorityInterest,annualOtherNonCurrentLiabilities,trailingOtherNonCurrentLiabilities,annualNonCurrentDeferredRevenue,trailingNonCurrentDeferredRevenue,annualNonCurrentDeferredTaxesLiabilities,trailingNonCurrentDeferredTaxesLiabilities,annualLongTermDebt,trailingLongTermDebt,annualCurrentLiabilities,trailingCurrentLiabilities,annualOtherCurrentLiabilities,trailingOtherCurrentLiabilities,annualCurrentDeferredRevenue,trailingCurrentDeferredRevenue,annualCurrentAccruedExpenses,trailingCurrentAccruedExpenses,annualIncomeTaxPayable,trailingIncomeTaxPayable,annualAccountsPayable,trailingAccountsPayable,annualCurrentDebt,trailingCurrentDebt,annualTotalNonCurrentAssets,trailingTotalNonCurrentAssets,annualOtherNonCurrentAssets,trailingOtherNonCurrentAssets,annualOtherIntangibleAssets,trailingOtherIntangibleAssets,annualGoodwill,trailingGoodwill,annualInvestmentsAndAdvances,trailingInvestmentsAndAdvances,annualNetPPE,trailingNetPPE,annualAccumulatedDepreciation,trailingAccumulatedDepreciation,annualGrossPPE,trailingGrossPPE,annualCurrentAssets,trailingCurrentAssets,annualOtherCurrentAssets,trailingOtherCurrentAssets,annualInventory,trailingInventory,annualAccountsReceivable,trailingAccountsReceivable,annualCashCashEquivalentsAndShortTermInvestments,trailingCashCashEquivalentsAndShortTermInvestments,annualOtherShortTermInvestments,trailingOtherShortTermInvestments,annualCashAndCashEquivalents,trailingCashAndCashEquivalents&merge=false&period1=493590046&period2=%s&corsDomain=au.finance.yahoo.com' % (ticker, ticker, epochTime)
    else:
        balanceSheetUrl = 'https://query1.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/%s?lang=en-AU&region=AU&symbol=%s&padTimeSeries=true&type=quarterlyTotalAssets,trailingTotalAssets,quarterlyStockholdersEquity,trailingStockholdersEquity,quarterlyGainsLossesNotAffectingRetainedEarnings,trailingGainsLossesNotAffectingRetainedEarnings,quarterlyRetainedEarnings,trailingRetainedEarnings,quarterlyCapitalStock,trailingCapitalStock,quarterlyTotalLiabilitiesNetMinorityInterest,trailingTotalLiabilitiesNetMinorityInterest,quarterlyTotalNonCurrentLiabilitiesNetMinorityInterest,trailingTotalNonCurrentLiabilitiesNetMinorityInterest,quarterlyOtherNonCurrentLiabilities,trailingOtherNonCurrentLiabilities,quarterlyNonCurrentDeferredRevenue,trailingNonCurrentDeferredRevenue,quarterlyNonCurrentDeferredTaxesLiabilities,trailingNonCurrentDeferredTaxesLiabilities,quarterlyLongTermDebt,trailingLongTermDebt,quarterlyCurrentLiabilities,trailingCurrentLiabilities,quarterlyOtherCurrentLiabilities,trailingOtherCurrentLiabilities,quarterlyCurrentDeferredRevenue,trailingCurrentDeferredRevenue,quarterlyCurrentAccruedExpenses,trailingCurrentAccruedExpenses,quarterlyIncomeTaxPayable,trailingIncomeTaxPayable,quarterlyAccountsPayable,trailingAccountsPayable,quarterlyCurrentDebt,trailingCurrentDebt,quarterlyTotalNonCurrentAssets,trailingTotalNonCurrentAssets,quarterlyOtherNonCurrentAssets,trailingOtherNonCurrentAssets,quarterlyOtherIntangibleAssets,trailingOtherIntangibleAssets,quarterlyGoodwill,trailingGoodwill,quarterlyInvestmentsAndAdvances,trailingInvestmentsAndAdvances,quarterlyNetPPE,trailingNetPPE,quarterlyAccumulatedDepreciation,trailingAccumulatedDepreciation,quarterlyGrossPPE,trailingGrossPPE,quarterlyCurrentAssets,trailingCurrentAssets,quarterlyOtherCurrentAssets,trailingOtherCurrentAssets,quarterlyInventory,trailingInventory,quarterlyAccountsReceivable,trailingAccountsReceivable,quarterlyCashCashEquivalentsAndShortTermInvestments,trailingCashCashEquivalentsAndShortTermInvestments,quarterlyOtherShortTermInvestments,trailingOtherShortTermInvestments,quarterlyCashAndCashEquivalents,trailingCashAndCashEquivalents&merge=false&period1=493590046&period2=%s&corsDomain=au.finance.yahoo.com' % (ticker, ticker, epochTime)
    
    data = requests.get(url=balanceSheetUrl).text
    balanceSheet = cleanData(data)
    
    return balanceSheet

def get_roe(ticker, exchange, freq='yearly'):
    ''' Retrieves the balance sheet and financials from yfinance. Calculates the average shareholder equity and
    determines the Return on Equity '''

    def match_dates(frame1, frame2):
        """ Returns the intersection of dates between two input frames and the smallest date """
        dates_frame1 = frame1.index.strftime('%Y-%m-%d')
        print(dates_frame1)
        dates_array1 = pd.Index.ravel(dates_frame1, order='C')
        print(dates_array1)
        dates_frame2 = frame2.index.strftime('%Y-%m-%d')
        dates_array2 = pd.Index.ravel(dates_frame2, order='C')
        return_array = np.intersect1d(dates_array1, dates_array2)
        minimum = np.amin(return_array)
        return return_array, minimum 

    if (exchange == 'ASX'):
        ticker += '.AX'

    stock = yf.Ticker(ticker)

    if freq == 'yearly':
        netIncomeDf = stock.financials.loc['Net Income'].to_frame()
        netIncomeDf = netIncomeDf.iloc[::-1]
        shareholderEquityDf = stock.balance_sheet.loc['Total Stockholder Equity'].to_frame()
        shareholderEquityDf = shareholderEquityDf.iloc[::-1]
    else:
        netIncomeDf = stock.quarterly_financials.loc['Net Income'].to_frame()
        netIncomeDf = netIncomeDf.iloc[::-1]
        shareholderEquityDf = stock.quarterly_balance_sheet.loc['Total Stockholder Equity'].to_frame()
        shareholderEquityDf = shareholderEquityDf.iloc[::-1]
    
    averageShareHolderEquity = []
    returnOnEquity = []
    dates, minDate = match_dates(netIncomeDf, shareholderEquityDf)

    for index, _ in enumerate(dates):
        currDate = dates[index]
        prevDate = dates[index - 1]
        if currDate == minDate:
            # we cannot calculate the average for the first date
            averageShareHolderEquity.append(0)
            returnOnEquity.append(0)
            continue

        currNetIncome = netIncomeDf.loc[np.datetime64(currDate)]['Net Income']
        currSE = shareholderEquityDf.loc[np.datetime64(currDate)]['Total Stockholder Equity']
        prevSE = shareholderEquityDf.loc[np.datetime64(prevDate)]['Total Stockholder Equity']
        
        avgSE = (prevSE + currSE) / 2
        averageShareHolderEquity.append(avgSE)
        
        roe = currNetIncome / avgSE
        returnOnEquity.append(roe)
    
    # Create dataframe to be processed
    data = list(zip(averageShareHolderEquity, returnOnEquity))
    finalDf = pd.DataFrame(data, columns=['average_shareholder_equity', 'return_on_equity'], index=dates)
    
    roeDict = json.loads(finalDf.transpose().to_json())

    roeArray = []
    # front end requires the data to be in this format
    for date in roeDict.keys():
        roeArray.append({
            'entry': 'return_on_equity',
            'date': date,
            'value': roeDict.get(date).get('return_on_equity'),
        })
    
    return roeArray

# we will ony store values >= the startDate
def store_premium_financials(ticker, freq, startingDate, exchange):

    def store(data):
        print('Storing %s %s' % (freq, ticker))
        startDate = datetime.strptime(startingDate, '%Y-%m-%d')
        jsonData = json.loads(data)
        timeseries = jsonData.get('timeseries').get('result')
        datafilePath = os.path.abspath('/Users/grantwei/datafiles/fundamentals/{}/{}/{}.csv'.format(exchange, freq, ticker))
        successFilePath = os.path.abspath('/Users/grantwei/datafiles/tickeroutcome/{}/{}_success.csv'.format(exchange, freq))
        nodataFilePath = os.path.abspath('/Users/grantwei/datafiles/tickeroutcome/{}/{}_nodata.csv'.format(exchange, freq))
        fileExists = os.path.exists(datafilePath)
        with open(datafilePath, 'a', newline='') as csvFile:
            writer = csv.writer(csvFile)
            if not fileExists:
                writer.writerow(['date', 'entry', 'figure'])
            i = 0
            j = 0
            for item in timeseries:
                key = item.get('meta').get('type')[0]
                # this timestamps logic is to check if yahoo actually returns any data for this ticker
                # ensure this only runs for annual entries, not trailing entries
                if key.startswith(freq):
                    timestamps = item.get('timestamp')
                    if timestamps is not None and len(timestamps) > 0: 
                        i += 1
                values = item.get(key)
                if values is not None and key.startswith(freq) and len(values) > 0:
                    for entry in values:
                        if entry is not None:
                            entryDateAsString = entry.get('asOfDate') # date as a string
                            entryDate = datetime.strptime(entryDateAsString, '%Y-%m-%d')
                            if entryDate > startDate:
                                j += 1
                                entryFigure = entry.get('reportedValue').get('raw')
                                writer.writerow([entryDateAsString, key, entryFigure])
            if i == 0:
                with open(nodataFilePath, 'a', newline='') as nodataCsv:
                    print('%s has no data' % ticker)
                    nodataWriter = csv.writer(nodataCsv)
                    nodataWriter.writerow([ticker])
            elif j == 0:
                with open(nodataFilePath, 'a', newline='') as nodataCsv:
                    print('%s has no NEW data to store' % ticker)
                    nodataWriter = csv.writer(nodataCsv)
                    nodataWriter.writerow([ticker])
            else:
                with open(successFilePath, 'a', newline='') as successCsv:
                    print('***** %s successfully stored NEW data *****' % ticker)
                    successWriter = csv.writer(successCsv)
                    successWriter.writerow([ticker])

    if ticker is None or startingDate is None or exchange is None:
        raise Exception('Please ensure, ticker, startingDate and exchange provided as params')

    if freq not in ['annual', 'quarterly']:
        raise Exception('Freq must be either annual or quarterly')

    if exchange not in ['nasdaq', 'nyse', 'asx']:
        raise Exception('Exchange must be one of nasdaq, nyse, asx')

    epochTime = int(time.time())
    # public, non-premium API
    annual_url = 'https://query2.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/%s?lang=en-US&region=US&symbol=%s&padTimeSeries=true&type=annualTaxEffectOfUnusualItems,trailingTaxEffectOfUnusualItems,annualTaxRateForCalcs,trailingTaxRateForCalcs,annualNormalizedEBITDA,trailingNormalizedEBITDA,annualNormalizedDilutedEPS,trailingNormalizedDilutedEPS,annualNormalizedBasicEPS,trailingNormalizedBasicEPS,annualTotalUnusualItems,trailingTotalUnusualItems,annualTotalUnusualItemsExcludingGoodwill,trailingTotalUnusualItemsExcludingGoodwill,annualNetIncomeFromContinuingOperationNetMinorityInterest,trailingNetIncomeFromContinuingOperationNetMinorityInterest,annualReconciledDepreciation,trailingReconciledDepreciation,annualReconciledCostOfRevenue,trailingReconciledCostOfRevenue,annualEBITDA,trailingEBITDA,annualEBIT,trailingEBIT,annualNetInterestIncome,trailingNetInterestIncome,annualInterestExpense,trailingInterestExpense,annualInterestIncome,trailingInterestIncome,annualContinuingAndDiscontinuedDilutedEPS,trailingContinuingAndDiscontinuedDilutedEPS,annualContinuingAndDiscontinuedBasicEPS,trailingContinuingAndDiscontinuedBasicEPS,annualNormalizedIncome,trailingNormalizedIncome,annualNetIncomeFromContinuingAndDiscontinuedOperation,trailingNetIncomeFromContinuingAndDiscontinuedOperation,annualTotalExpenses,trailingTotalExpenses,annualRentExpenseSupplemental,trailingRentExpenseSupplemental,annualReportedNormalizedDilutedEPS,trailingReportedNormalizedDilutedEPS,annualReportedNormalizedBasicEPS,trailingReportedNormalizedBasicEPS,annualTotalOperatingIncomeAsReported,trailingTotalOperatingIncomeAsReported,annualDividendPerShare,trailingDividendPerShare,annualDilutedAverageShares,trailingDilutedAverageShares,annualBasicAverageShares,trailingBasicAverageShares,annualDilutedEPS,trailingDilutedEPS,annualDilutedEPSOtherGainsLosses,trailingDilutedEPSOtherGainsLosses,annualTaxLossCarryforwardDilutedEPS,trailingTaxLossCarryforwardDilutedEPS,annualDilutedAccountingChange,trailingDilutedAccountingChange,annualDilutedExtraordinary,trailingDilutedExtraordinary,annualDilutedDiscontinuousOperations,trailingDilutedDiscontinuousOperations,annualDilutedContinuousOperations,trailingDilutedContinuousOperations,annualBasicEPS,trailingBasicEPS,annualBasicEPSOtherGainsLosses,trailingBasicEPSOtherGainsLosses,annualTaxLossCarryforwardBasicEPS,trailingTaxLossCarryforwardBasicEPS,annualBasicAccountingChange,trailingBasicAccountingChange,annualBasicExtraordinary,trailingBasicExtraordinary,annualBasicDiscontinuousOperations,trailingBasicDiscontinuousOperations,annualBasicContinuousOperations,trailingBasicContinuousOperations,annualDilutedNIAvailtoComStockholders,trailingDilutedNIAvailtoComStockholders,annualAverageDilutionEarnings,trailingAverageDilutionEarnings,annualNetIncomeCommonStockholders,trailingNetIncomeCommonStockholders,annualOtherunderPreferredStockDividend,trailingOtherunderPreferredStockDividend,annualPreferredStockDividends,trailingPreferredStockDividends,annualNetIncome,trailingNetIncome,annualMinorityInterests,trailingMinorityInterests,annualNetIncomeIncludingNoncontrollingInterests,trailingNetIncomeIncludingNoncontrollingInterests,annualNetIncomeFromTaxLossCarryforward,trailingNetIncomeFromTaxLossCarryforward,annualNetIncomeExtraordinary,trailingNetIncomeExtraordinary,annualNetIncomeDiscontinuousOperations,trailingNetIncomeDiscontinuousOperations,annualNetIncomeContinuousOperations,trailingNetIncomeContinuousOperations,annualEarningsFromEquityInterestNetOfTax,trailingEarningsFromEquityInterestNetOfTax,annualTaxProvision,trailingTaxProvision,annualPretaxIncome,trailingPretaxIncome,annualOtherIncomeExpense,trailingOtherIncomeExpense,annualOtherNonOperatingIncomeExpenses,trailingOtherNonOperatingIncomeExpenses,annualSpecialIncomeCharges,trailingSpecialIncomeCharges,annualGainOnSaleOfPPE,trailingGainOnSaleOfPPE,annualGainOnSaleOfBusiness,trailingGainOnSaleOfBusiness,annualOtherSpecialCharges,trailingOtherSpecialCharges,annualWriteOff,trailingWriteOff,annualImpairmentOfCapitalAssets,trailingImpairmentOfCapitalAssets,annualRestructuringAndMergernAcquisition,trailingRestructuringAndMergernAcquisition,annualSecuritiesAmortization,trailingSecuritiesAmortization,annualEarningsFromEquityInterest,trailingEarningsFromEquityInterest,annualGainOnSaleOfSecurity,trailingGainOnSaleOfSecurity,annualNetNonOperatingInterestIncomeExpense,trailingNetNonOperatingInterestIncomeExpense,annualTotalOtherFinanceCost,trailingTotalOtherFinanceCost,annualInterestExpenseNonOperating,trailingInterestExpenseNonOperating,annualInterestIncomeNonOperating,trailingInterestIncomeNonOperating,annualOperatingIncome,trailingOperatingIncome,annualOperatingExpense,trailingOperatingExpense,annualOtherOperatingExpenses,trailingOtherOperatingExpenses,annualOtherTaxes,trailingOtherTaxes,annualProvisionForDoubtfulAccounts,trailingProvisionForDoubtfulAccounts,annualDepreciationAmortizationDepletionIncomeStatement,trailingDepreciationAmortizationDepletionIncomeStatement,annualDepletionIncomeStatement,trailingDepletionIncomeStatement,annualDepreciationAndAmortizationInIncomeStatement,trailingDepreciationAndAmortizationInIncomeStatement,annualAmortization,trailingAmortization,annualAmortizationOfIntangiblesIncomeStatement,trailingAmortizationOfIntangiblesIncomeStatement,annualDepreciationIncomeStatement,trailingDepreciationIncomeStatement,annualResearchAndDevelopment,trailingResearchAndDevelopment,annualSellingGeneralAndAdministration,trailingSellingGeneralAndAdministration,annualSellingAndMarketingExpense,trailingSellingAndMarketingExpense,annualGeneralAndAdministrativeExpense,trailingGeneralAndAdministrativeExpense,annualOtherGandA,trailingOtherGandA,annualInsuranceAndClaims,trailingInsuranceAndClaims,annualRentAndLandingFees,trailingRentAndLandingFees,annualSalariesAndWages,trailingSalariesAndWages,annualGrossProfit,trailingGrossProfit,annualCostOfRevenue,trailingCostOfRevenue,annualTotalRevenue,trailingTotalRevenue,annualExciseTaxes,trailingExciseTaxes,annualOperatingRevenue,trailingOperatingRevenue&merge=false&period1=493590046&period2=%s&corsDomain=finance.yahoo.com' % (ticker, ticker, epochTime)
    quarterly_url = 'https://query1.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/%s?lang=en-US&region=US&symbol=%s&padTimeSeries=true&type=quarterlyTaxEffectOfUnusualItems,trailingTaxEffectOfUnusualItems,quarterlyTaxRateForCalcs,trailingTaxRateForCalcs,quarterlyNormalizedEBITDA,trailingNormalizedEBITDA,quarterlyNormalizedDilutedEPS,trailingNormalizedDilutedEPS,quarterlyNormalizedBasicEPS,trailingNormalizedBasicEPS,quarterlyTotalUnusualItems,trailingTotalUnusualItems,quarterlyTotalUnusualItemsExcludingGoodwill,trailingTotalUnusualItemsExcludingGoodwill,quarterlyNetIncomeFromContinuingOperationNetMinorityInterest,trailingNetIncomeFromContinuingOperationNetMinorityInterest,quarterlyReconciledDepreciation,trailingReconciledDepreciation,quarterlyReconciledCostOfRevenue,trailingReconciledCostOfRevenue,quarterlyEBITDA,trailingEBITDA,quarterlyEBIT,trailingEBIT,quarterlyNetInterestIncome,trailingNetInterestIncome,quarterlyInterestExpense,trailingInterestExpense,quarterlyInterestIncome,trailingInterestIncome,quarterlyContinuingAndDiscontinuedDilutedEPS,trailingContinuingAndDiscontinuedDilutedEPS,quarterlyContinuingAndDiscontinuedBasicEPS,trailingContinuingAndDiscontinuedBasicEPS,quarterlyNormalizedIncome,trailingNormalizedIncome,quarterlyNetIncomeFromContinuingAndDiscontinuedOperation,trailingNetIncomeFromContinuingAndDiscontinuedOperation,quarterlyTotalExpenses,trailingTotalExpenses,quarterlyRentExpenseSupplemental,trailingRentExpenseSupplemental,quarterlyReportedNormalizedDilutedEPS,trailingReportedNormalizedDilutedEPS,quarterlyReportedNormalizedBasicEPS,trailingReportedNormalizedBasicEPS,quarterlyTotalOperatingIncomeAsReported,trailingTotalOperatingIncomeAsReported,quarterlyDividendPerShare,trailingDividendPerShare,quarterlyDilutedAverageShares,trailingDilutedAverageShares,quarterlyBasicAverageShares,trailingBasicAverageShares,quarterlyDilutedEPS,trailingDilutedEPS,quarterlyDilutedEPSOtherGainsLosses,trailingDilutedEPSOtherGainsLosses,quarterlyTaxLossCarryforwardDilutedEPS,trailingTaxLossCarryforwardDilutedEPS,quarterlyDilutedAccountingChange,trailingDilutedAccountingChange,quarterlyDilutedExtraordinary,trailingDilutedExtraordinary,quarterlyDilutedDiscontinuousOperations,trailingDilutedDiscontinuousOperations,quarterlyDilutedContinuousOperations,trailingDilutedContinuousOperations,quarterlyBasicEPS,trailingBasicEPS,quarterlyBasicEPSOtherGainsLosses,trailingBasicEPSOtherGainsLosses,quarterlyTaxLossCarryforwardBasicEPS,trailingTaxLossCarryforwardBasicEPS,quarterlyBasicAccountingChange,trailingBasicAccountingChange,quarterlyBasicExtraordinary,trailingBasicExtraordinary,quarterlyBasicDiscontinuousOperations,trailingBasicDiscontinuousOperations,quarterlyBasicContinuousOperations,trailingBasicContinuousOperations,quarterlyDilutedNIAvailtoComStockholders,trailingDilutedNIAvailtoComStockholders,quarterlyAverageDilutionEarnings,trailingAverageDilutionEarnings,quarterlyNetIncomeCommonStockholders,trailingNetIncomeCommonStockholders,quarterlyOtherunderPreferredStockDividend,trailingOtherunderPreferredStockDividend,quarterlyPreferredStockDividends,trailingPreferredStockDividends,quarterlyNetIncome,trailingNetIncome,quarterlyMinorityInterests,trailingMinorityInterests,quarterlyNetIncomeIncludingNoncontrollingInterests,trailingNetIncomeIncludingNoncontrollingInterests,quarterlyNetIncomeFromTaxLossCarryforward,trailingNetIncomeFromTaxLossCarryforward,quarterlyNetIncomeExtraordinary,trailingNetIncomeExtraordinary,quarterlyNetIncomeDiscontinuousOperations,trailingNetIncomeDiscontinuousOperations,quarterlyNetIncomeContinuousOperations,trailingNetIncomeContinuousOperations,quarterlyEarningsFromEquityInterestNetOfTax,trailingEarningsFromEquityInterestNetOfTax,quarterlyTaxProvision,trailingTaxProvision,quarterlyPretaxIncome,trailingPretaxIncome,quarterlyOtherIncomeExpense,trailingOtherIncomeExpense,quarterlyOtherNonOperatingIncomeExpenses,trailingOtherNonOperatingIncomeExpenses,quarterlySpecialIncomeCharges,trailingSpecialIncomeCharges,quarterlyGainOnSaleOfPPE,trailingGainOnSaleOfPPE,quarterlyGainOnSaleOfBusiness,trailingGainOnSaleOfBusiness,quarterlyOtherSpecialCharges,trailingOtherSpecialCharges,quarterlyWriteOff,trailingWriteOff,quarterlyImpairmentOfCapitalAssets,trailingImpairmentOfCapitalAssets,quarterlyRestructuringAndMergernAcquisition,trailingRestructuringAndMergernAcquisition,quarterlySecuritiesAmortization,trailingSecuritiesAmortization,quarterlyEarningsFromEquityInterest,trailingEarningsFromEquityInterest,quarterlyGainOnSaleOfSecurity,trailingGainOnSaleOfSecurity,quarterlyNetNonOperatingInterestIncomeExpense,trailingNetNonOperatingInterestIncomeExpense,quarterlyTotalOtherFinanceCost,trailingTotalOtherFinanceCost,quarterlyInterestExpenseNonOperating,trailingInterestExpenseNonOperating,quarterlyInterestIncomeNonOperating,trailingInterestIncomeNonOperating,quarterlyOperatingIncome,trailingOperatingIncome,quarterlyOperatingExpense,trailingOperatingExpense,quarterlyOtherOperatingExpenses,trailingOtherOperatingExpenses,quarterlyOtherTaxes,trailingOtherTaxes,quarterlyProvisionForDoubtfulAccounts,trailingProvisionForDoubtfulAccounts,quarterlyDepreciationAmortizationDepletionIncomeStatement,trailingDepreciationAmortizationDepletionIncomeStatement,quarterlyDepletionIncomeStatement,trailingDepletionIncomeStatement,quarterlyDepreciationAndAmortizationInIncomeStatement,trailingDepreciationAndAmortizationInIncomeStatement,quarterlyAmortization,trailingAmortization,quarterlyAmortizationOfIntangiblesIncomeStatement,trailingAmortizationOfIntangiblesIncomeStatement,quarterlyDepreciationIncomeStatement,trailingDepreciationIncomeStatement,quarterlyResearchAndDevelopment,trailingResearchAndDevelopment,quarterlySellingGeneralAndAdministration,trailingSellingGeneralAndAdministration,quarterlySellingAndMarketingExpense,trailingSellingAndMarketingExpense,quarterlyGeneralAndAdministrativeExpense,trailingGeneralAndAdministrativeExpense,quarterlyOtherGandA,trailingOtherGandA,quarterlyInsuranceAndClaims,trailingInsuranceAndClaims,quarterlyRentAndLandingFees,trailingRentAndLandingFees,quarterlySalariesAndWages,trailingSalariesAndWages,quarterlyGrossProfit,trailingGrossProfit,quarterlyCostOfRevenue,trailingCostOfRevenue,quarterlyTotalRevenue,trailingTotalRevenue,quarterlyExciseTaxes,trailingExciseTaxes,quarterlyOperatingRevenue,trailingOperatingRevenue&merge=false&period1=493590046&period2=%s&corsDomain=finance.yahoo.com' % (ticker, ticker, epochTime)
    errorFilePath = os.path.abspath('/Users/grantwei/datafiles/tickeroutcome/{}/{}_error.csv'.format(exchange, freq))
    failFilePath = os.path.abspath('/Users/grantwei/datafiles/tickeroutcome/{}/{}_fail.csv'.format(exchange,freq))
    try:
        url = annual_url
        if freq == 'quarterly':
            url = quarterly_url
        
        data = requests.get(url).text
        store(data)
    except Exception as e:
        print('----- %s failed to store. Error:' % (ticker), str(e))
        with open(errorFilePath, 'a', newline='') as errorCsv:
            errorWriter = csv.writer(errorCsv)
            errorWriter.writerow([ticker, str(e)])

        with open(failFilePath, 'a', newline='') as failCsv:
            failWriter = csv.writer(failCsv)
            failWriter.writerow(ticker)