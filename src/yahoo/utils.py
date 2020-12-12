import requests
import json
from inflection import underscore
import os
from datetime import datetime
import csv
import time
import yfinance as yf
import numpy as np


def cleanData(data):
    """ 
    """
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

def get_roe(ticker, exchange, freq='yearly'):
    if (exchange == 'ASX'):
        ticker += '.AX'

    stock = yf.Ticker(ticker)

    if freq == 'yearly':
        netIncomeSeries = stock.financials.loc['Net Income']
        ShareholderEquityDf = stock.balance_sheet.loc['Total Stockholder Equity'].to_frame()
        ShareholderEquityDf = ShareholderEquityDf.iloc[::-1]
    else:
        netIncomeSeries = stock.quarterly_financials.loc['Net Income']
        ShareholderEquityDf = stock.quarterly_balance_sheet.loc['Total Stockholder Equity'].to_frame()
        ShareholderEquityDf = ShareholderEquityDf.iloc[::-1]

    averageShareHolderEquity = []
    for index, value in enumerate(ShareholderEquityDf.values):
        if index == 0:
            # we cannot calculate the average for the first date
            averageShareHolderEquity.append(0)
            continue
        avgSE = (ShareholderEquityDf.values[index - 1] + value) / 2
        averageShareHolderEquity.append(avgSE[0])
    ShareholderEquityDf['average_shareholder_equity'] = averageShareHolderEquity

    roe = []
    for index, value in enumerate(ShareholderEquityDf['average_shareholder_equity'].values):
        if index == 0:
            roe.append(0)
            continue
        roeTmp = netIncomeSeries.iloc[index] / value
        roe.append(roeTmp)
    ShareholderEquityDf['return_on_equity'] = roe

    newIndex = ShareholderEquityDf.index.strftime('%Y-%m-%d')
    ShareholderEquityDf['Index'] = newIndex
    returnDf = ShareholderEquityDf.drop(columns=['Total Stockholder Equity'])
    returnDf = returnDf.set_index('Index')
    return returnDf.to_json()


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

if __name__ == '__main__':
    get_roe("MSFT", 'NASDAQ')