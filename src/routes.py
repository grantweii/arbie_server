
from models import Stock, AnnualFinancial, QuarterlyFinancial
from flask import Blueprint, request, g
from app import session
from flask_classful import FlaskView, route
from route_helpers import output_json
import yfinance as yf

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

    @route('/<id>')
    def getStock(self, id):
        results = Stock.getStock(id)
        return { 'result': results }

    @route('/<id>/holders')
    def getHolders(self, id):
        stock = Stock.getStock(id)
        yfStock = yf.Ticker(stock.get('ticker'))
        df = yfStock.institutional_holders
        df['Date Reported'] = df['Date Reported'].astype(str)
        df['% Out'] = df['% Out'].round(4)
        dict = df.to_dict('records')
        return { 'result': dict }

class AnnualFinancialsView(FlaskView):
    representations = {'application/json': output_json}

    def index(self):
        stock_id = request.args.get('stock_id')
        fieldParam = request.args.get('fields')
        fields = request.args.get('fields').split(',') if fieldParam != 'undefined' and fieldParam is not None else None
        results = AnnualFinancial.getAnnualFinancial(stock_id, fields)
        return { 'result': results }

class QuarterlyFinancialView(FlaskView):
    representations = {'application/json': output_json}

    def index(self):
        stock_id = request.args.get('stock_id')
        fieldParam = request.args.get('fields')
        fields = request.args.get('fields').split(',') if fieldParam != 'undefined' and fieldParam is not None else None
        results = QuarterlyFinancial.getQuarterlyFinancial(stock_id, fields)
        return { 'result': results }
