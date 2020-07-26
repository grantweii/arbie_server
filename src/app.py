from flask import Flask
from config import DevelopmentConfig
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

#893dc2 fuchsia purple

app = Flask(__name__)

app.config.from_object(DevelopmentConfig)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

some_engine = create_engine('postgres+psycopg2://grantwei:$2B6MvzvshKl@localhost:5432/db')
Session = sessionmaker(bind=some_engine)

session = Session()

from routes import routesBlueprint
app.register_blueprint(routesBlueprint)

if __name__ == '__main__':
    app.run()