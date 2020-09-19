
from .models import Stock, AnnualFinancial, QuarterlyFinancial
from flask import Blueprint, request, g
from app import session
from flask_classful import FlaskView, route
from .route_helpers import output_json
import yfinance as yf
from .yahoo import get_cashflow
from .lists import exchanges
from iexfinance.stocks import Stock as IEXStock
from .config import Config
import os

def institutionalHoldersAsDict(df):
    df['Date Reported'] = df['Date Reported'].astype(str)
    df['% Out'] = df['% Out'].round(4)
    dict = df.to_dict('records')
    return dict

def majorHoldersAsDict(df):
    dict = {}
    for index, row in df.iterrows():
        dict[row[1]] = row[0]
    return dict

class StocksView(FlaskView):
    representations = {'application/json': output_json}

    # retrieves all stocks
    def index(self):
        results = Stock.getStocks()
        return { 'result': results }

    # searches for stocks by ticker only
    @route('/search')
    def search(self):
        ticker = request.args.get('ticker')
        results = Stock.findStocks(ticker)
        return { 'result': results }

    @route('/list')
    def stockList(self):
        sector = request.args.get('sector')
        industry = request.args.get('industry')
        exchange = request.args.get('exchange')
        results = Stock.getStockList(sector, industry, exchange)
        return { 'result': results }

    @route('/<id>')
    def getStock(self, id):
        results = Stock.getStock(id)
        return { 'result': results }

    @route('/<id>/major-holders')
    def getMajorHolders(self, id):
        stock = Stock.getStock(id)
        yfStock = yf.Ticker(stock.get('ticker'))
        df = yfStock.major_holders
        dict = majorHoldersAsDict(df)
        return { 'result': dict }

    @route('/<id>/institutional-holders')
    def getInstitutionalHolders(self, id):
        stock = Stock.getStock(id)
        yfStock = yf.Ticker(stock.get('ticker'))
        df = yfStock.institutional_holders
        dict = institutionalHoldersAsDict(df)
        return { 'result': dict }

    @route('/industries')
    def getIndustries(self):
        sector = request.args.get('sector')
        results = Stock.getIndustries(sector)
        return { 'result': results }

    @route('/sectors')
    def getSectors(self):
        industry = request.args.get('industry')
        results = Stock.getSectors(industry)
        return { 'result': results }

    @route('/exchanges')
    def getExchanges(self):
        return { 'result': exchanges }

    @route('/<id>/price')
    def getEODPrice(self, id):
        stock = Stock.getStock(id)
        if stock.get('exchange') == 'ASX':
            return { 'result': None }
        iex = IEXStock(stock.get('ticker'), token=os.getenv('IEX_SANDBOX_TOKEN'))
        price = iex.get_previous_day_prices()
        return { 'result': price }
         

class AnnualFinancialsView(FlaskView):
    representations = {'application/json': output_json}

    def index(self):
        stock_id = request.args.get('stock_id')
        fieldParam = request.args.get('fields')
        fields = request.args.get('fields').split(',') if fieldParam is not None else None
        results = AnnualFinancial.getAnnualFinancial(stock_id, fields)
        return { 'result': results }

    @route('/cashflow')
    def getCashflows(self):
        stock_id = request.args.get('stock_id')
        stock = Stock.getStock(stock_id)
        results = get_cashflow(stock.get('ticker'), stock.get('exchange'), 'yearly')
        return { 'result': results } 
    

class QuarterlyFinancialsView(FlaskView):
    representations = {'application/json': output_json}

    def index(self):
        stock_id = request.args.get('stock_id')
        fieldParam = request.args.get('fields')
        fields = request.args.get('fields').split(',') if fieldParam is not None else None
        results = QuarterlyFinancial.getQuarterlyFinancial(stock_id, fields)
        return { 'result': results }

    @route('/cashflow')
    def getCashflows(self):
        stock_id = request.args.get('stock_id')
        stock = Stock.getStock(stock_id)
        results = get_cashflow(stock.get('ticker'), stock.get('exchange'), 'quarterly')
        return { 'result': results } 