from sqlalchemy import Column, Integer, String, ForeignKey, Date, Numeric
from sqlalchemy_serializer import SerializerMixin
import json
from flask_sqlalchemy import SQLAlchemy
from app import session, db
from sqlalchemy import or_

class Stock(db.Model, SerializerMixin):
    __tablename__ = 'stock'

    id = Column(Integer, primary_key = True)
    name = Column(String(length=255))
    ticker = Column(String(length=255))
    exchange = Column(String(length=255))
    industry = Column(String(length=255))
    sector = Column(String(length=255))

    def __repr__(self):
        return json.dumps({
            'id': self.id,
            'name': self.name,
            'ticker': self.ticker,
            'exchange': self.exchange,
            'industry': self.industry,
            'sector': self.sector,
        })

    def getStocks():
        results = session.query(Stock).all()
        return [entry.to_dict() for entry in results]

    def getStock(id):
        results = session.query(Stock).filter(Stock.id == id).one()
        return results.to_dict()

    def findStocks(ticker):
        q = session.query(Stock).filter(Stock.ticker == ticker.upper())
        results = q.all()
        return [entry.to_dict() for entry in results]

class AnnualFinancial(db.Model, SerializerMixin):
    __tablename__ = 'annualfinancial'

    id = Column(Integer, primary_key=True)
    stock_id = Column(Integer, ForeignKey('stock.id'))
    date = Column(Date)
    entry = Column(String(length=255))
    value = Column(Numeric(precision=28, scale=6))

    def __repr__(self):
        return json.dumps({
            'stock_id': self.stock_id,
            'date': self.date.strftime('%Y-%m-%d'),
            'entry': self.entry,
            'value': str(self.value),
        })

    def getAnnualFinancial(stock_id, fields):
        try:
            q = session.query(AnnualFinancial).filter(AnnualFinancial.stock_id == stock_id)

            if fields is not None and len(fields) > 0:
                q = q.filter(or_(AnnualFinancial.entry == field for field in fields))

            results = q.all()
            return [entry.to_dict() for entry in results]
        except Exception as e:
            raise

    # def getAnnualFinancial(ticker, exchange, fields):
    #     try:
    #         q = session.query(AnnualFinancial)\
    #             .join(Stock, AnnualFinancial.stock_id == Stock.id)\
    #             .filter(Stock.ticker == ticker)

    #         if exchange is not None:
    #             q = q.filter(Stock.exchange == exchange)

    #         if fields is not None and len(fields) > 0:
    #             q = q.filter(or_(AnnualFinancial.entry == field for field in fields))
            
    #         results = q.all()
    #         return [entry.to_dict() for entry in results]
    #     except Exception as e:
    #         raise

class QuarterlyFinancial(db.Model, SerializerMixin):
    __tablename__ = 'quarterlyfinancial'

    id = Column(Integer, primary_key=True)
    stock_id = Column(Integer, ForeignKey('stock.id'))
    date = Column(Date)
    entry = Column(String(length=255))
    value = Column(Numeric(precision=28, scale=6))

    def __repr__(self):
        return json.dumps({
            'stock_id': self.stock_id,
            'date': self.date.strftime('%Y-%m-%d'),
            'entry': self.entry,
            'value': str(self.value),
        })

    def getQuarterlyFinancial(stock_id, fields):
        try:
            q = session.query(QuarterlyFinancial).filter(QuarterlyFinancial.stock_id == stock_id)

            if fields is not None and len(fields) > 0:
                q = q.filter(or_(QuarterlyFinancial.entry == field for field in fields))

            results = q.all()
            return [entry.to_dict() for entry in results]
        except Exception as e:
            raise

    
    # def getQuarterlyFinancial(ticker, exchange, fields):
    #     try:
    #         q = session.query(QuarterlyFinancial)\
    #             .join(Stock, QuarterlyFinancial.stock_id == Stock.id)\
    #             .filter(Stock.ticker == ticker)

    #         if exchange is not None:
    #             q = q.filter(Stock.exchange == exchange)

    #         if fields is not None and len(fields) > 0:
    #             q = q.filter(or_(QuarterlyFinancial.entry == field for field in fields))
            
    #         results = q.all()
    #         return [entry.to_dict() for entry in results]
    #     except Exception as e:
    #         raise
