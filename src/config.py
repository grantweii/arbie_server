import os
basedir = os.path.abspath(os.path.dirname(__file__))

class Config(object):
    DEBUG = False
    TESTING = False
    CSRF_ENABLED = True
    SECRET_KEY = b'\xb0\xadw@\x91\x15\x02\xcb^!f[!XE\x05'
    SQLALCHEMY_DATABASE_URI = 'postgres+psycopg2://grantwei:$2B6MvzvshKl@localhost:5432/db'


class ProductionConfig(Config):
    DEBUG = False

class DevelopmentConfig(Config):
    DEVELOPMENT = True
    DEBUG = True


class TestingConfig(Config):
    TESTING = True