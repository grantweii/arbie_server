from flask import Flask
from src.config import DevelopmentConfig
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# initialise flask
app = Flask(__name__)

app.config.from_object(DevelopmentConfig)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# initialise db
db = SQLAlchemy(app) 
some_engine = create_engine('postgres+psycopg2://oaqque:53090830Aa@localhost:5432/db')
# some_engine = create_engine('postgres+psycopg2://postgres:w6e8N6w3LI3M@arbie.crgx3ghlstn1.ap-southeast-2.rds.amazonaws.com:5432/db')
Session = sessionmaker(bind=some_engine)
session = Session()

# initialise routes

from src.routes import StocksView, AnnualFinancialsView, QuarterlyFinancialsView

StocksView.register(app, route_base='/stocks', trailing_slash=False)
AnnualFinancialsView.register(app, route_base='/annual-financials', trailing_slash=False)
QuarterlyFinancialsView.register(app, route_base='/quarterly-financials', trailing_slash=False)

if __name__ == '__main__':
    app.run()
