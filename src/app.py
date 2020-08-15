from flask import Flask
from config import DevelopmentConfig
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# initialise flask
app = Flask(__name__)

app.config.from_object(DevelopmentConfig)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# initialise db
db = SQLAlchemy(app) 
some_engine = create_engine('postgres+psycopg2://grantwei:$2B6MvzvshKl@localhost:5432/db')
Session = sessionmaker(bind=some_engine)
session = Session()

# initialise routes

from routes import StocksView, AnnualFinancialsView

StocksView.register(app, route_base='/stocks', trailing_slash=False)
AnnualFinancialsView.register(app, route_base='/annual-financials', trailing_slash=False)

if __name__ == '__main__':
    app.run()
