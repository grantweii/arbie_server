from flask import g
from app import db
from sqlalchemy import Column, Integer, String, ForeignKey, Date, Numeric
from sqlalchemy_serializer import SerializerMixin
import json
from datetime import datetime
from decimal import Decimal

class Stock(db.Model, SerializerMixin):
    __tablename__ = 'stock'

    id = Column(Integer, primary_key = True)
    name = Column(String(length=255))
    ticker = Column(String(length=255))
    exchange = Column(String(length=255))

    def __repr__(self):
        return json.dumps({
            'id': self.id,
            'name': self.name,
            'ticker': self.ticker,
            'exchange': self.exchange,
        })

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
