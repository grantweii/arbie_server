import requests
import json
from inflection import underscore

def get_cashflow(ticker, freq='yearly'):
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

    if freq == 'yearly':
        cashflowUrl = 'https://query2.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/%s?lang=en-AU&region=AU&symbol=%s&padTimeSeries=true&type=annualFreeCashFlow,trailingFreeCashFlow,annualCapitalExpenditure,trailingCapitalExpenditure,annualOperatingCashFlow,trailingOperatingCashFlow,annualEndCashPosition,trailingEndCashPosition,annualBeginningCashPosition,trailingBeginningCashPosition,annualChangeInCashSupplementalAsReported,trailingChangeInCashSupplementalAsReported,annualCashFlowFromContinuingFinancingActivities,trailingCashFlowFromContinuingFinancingActivities,annualNetOtherFinancingCharges,trailingNetOtherFinancingCharges,annualCashDividendsPaid,trailingCashDividendsPaid,annualRepurchaseOfCapitalStock,trailingRepurchaseOfCapitalStock,annualCommonStockIssuance,trailingCommonStockIssuance,annualRepaymentOfDebt,trailingRepaymentOfDebt,annualInvestingCashFlow,trailingInvestingCashFlow,annualNetOtherInvestingChanges,trailingNetOtherInvestingChanges,annualSaleOfInvestment,trailingSaleOfInvestment,annualPurchaseOfInvestment,trailingPurchaseOfInvestment,annualPurchaseOfBusiness,trailingPurchaseOfBusiness,annualOtherNonCashItems,trailingOtherNonCashItems,annualChangeInAccountPayable,trailingChangeInAccountPayable,annualChangeInInventory,trailingChangeInInventory,annualChangesInAccountReceivables,trailingChangesInAccountReceivables,annualChangeInWorkingCapital,trailingChangeInWorkingCapital,annualStockBasedCompensation,trailingStockBasedCompensation,annualDeferredIncomeTax,trailingDeferredIncomeTax,annualDepreciationAndAmortization,trailingDepreciationAndAmortization,annualNetIncome,trailingNetIncome&merge=false&period1=493590046&period2=1597921829&corsDomain=au.finance.yahoo.com' % (ticker, ticker)
    else:
        cashflowUrl = 'https://query1.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/%s?lang=en-AU&region=AU&symbol=%s&padTimeSeries=true&type=quarterlyFreeCashFlow,trailingFreeCashFlow,quarterlyCapitalExpenditure,trailingCapitalExpenditure,quarterlyOperatingCashFlow,trailingOperatingCashFlow,quarterlyEndCashPosition,trailingEndCashPosition,quarterlyBeginningCashPosition,trailingBeginningCashPosition,quarterlyChangeInCashSupplementalAsReported,trailingChangeInCashSupplementalAsReported,quarterlyCashFlowFromContinuingFinancingActivities,trailingCashFlowFromContinuingFinancingActivities,quarterlyNetOtherFinancingCharges,trailingNetOtherFinancingCharges,quarterlyCashDividendsPaid,trailingCashDividendsPaid,quarterlyRepurchaseOfCapitalStock,trailingRepurchaseOfCapitalStock,quarterlyCommonStockIssuance,trailingCommonStockIssuance,quarterlyRepaymentOfDebt,trailingRepaymentOfDebt,quarterlyInvestingCashFlow,trailingInvestingCashFlow,quarterlyNetOtherInvestingChanges,trailingNetOtherInvestingChanges,quarterlySaleOfInvestment,trailingSaleOfInvestment,quarterlyPurchaseOfInvestment,trailingPurchaseOfInvestment,quarterlyPurchaseOfBusiness,trailingPurchaseOfBusiness,quarterlyOtherNonCashItems,trailingOtherNonCashItems,quarterlyChangeInAccountPayable,trailingChangeInAccountPayable,quarterlyChangeInInventory,trailingChangeInInventory,quarterlyChangesInAccountReceivables,trailingChangesInAccountReceivables,quarterlyChangeInWorkingCapital,trailingChangeInWorkingCapital,quarterlyStockBasedCompensation,trailingStockBasedCompensation,quarterlyDeferredIncomeTax,trailingDeferredIncomeTax,quarterlyDepreciationAndAmortization,trailingDepreciationAndAmortization,quarterlyNetIncome,trailingNetIncome&merge=false&period1=493590046&period2=1597926642&corsDomain=au.finance.yahoo.com' % (ticker, ticker)
    
    data = requests.get(url=cashflowUrl).text
    cashflow = cleanData(data)

    return cashflow