
from models import Stock, AnnualFinancial, QuarterlyFinancial
from app import app, session
from flask import Blueprint, jsonify, request
from sqlalchemy import or_

routesBlueprint = Blueprint('routes', __name__)

@routesBlueprint.route('/')
def hello():
    return {"hello": "world"}

@routesBlueprint.route('/stocks')
def stocks():
    try:
        stocks = session.query(Stock).all()
        return jsonify([s.to_dict() for s in stocks])
    except Exception as e:
        raise

@routesBlueprint.route('/annualfinancial')
def annualfinancial():
    ticker = request.args.get('ticker')
    exchange = request.args.get('exchange')
    fields = request.args.get('fields').split(',') if request.args.get('fields') is not None else None
    try:
        q = session.query(AnnualFinancial)\
            .join(Stock, AnnualFinancial.stock_id == Stock.id)\
            .filter(Stock.ticker == ticker)

        if exchange is not None:
            q = q.filter(Stock.exchange == exchange)

        if fields is not None and len(fields) > 0:
            q = q.filter(or_(AnnualFinancial.entry == field for field in fields))
        
        results = q.all()
        return jsonify([f.to_dict() for f in results])
    except Exception as e:
        raise

@routesBlueprint.route('/quarterlyfinancisal')
def quarterlyfinancial():
    ticker = request.args.get('ticker')
    exchange = request.args.get('exchange')
    fields = request.args.get('fields').split(',') if request.args.get('fields') is not None else None
    try:
        q = session.query(QuarterlyFinancial)\
            .join(Stock, QuarterlyFinancial.stock_id == Stock.id)\
            .filter(Stock.ticker == ticker)
        
        if exchange is not None:
            q = q.filter(Stock.exchange == exchange)

        if fields is not None and len(fields) > 0:
            q = q.filter(or_(AnnualFinancial.entry == field for field in fields))
        
        results = q.all()
        return jsonify([f.to_dict() for f in results])
    except Exception as e:
        raise

