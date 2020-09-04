#!/usr/bin/python
import psycopg2

class DB():
    def __init__(self):
        self.connection = self.connectDB()

    def connectDB(self):
        """ Connect to the PostgreSQL database server """
        conn = None
        try:
            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            conn = psycopg2.connect(user = "grantwei",
                                    password = "$2B6MvzvshKl",
                                    host = "localhost",
                                    port = "5432",
                                    database = "db")
            
            # create a cursor
            cur = conn.cursor()
            
            # execute a statement
            print('PostgreSQL database version:')
            cur.execute('SELECT version()')

            # display the PostgreSQL database server version
            db_version = cur.fetchone()
            print(db_version)

            print('Ready for querying...')
            cur.close()
            return conn
        except (Exception, psycopg2.DatabaseError) as error:
            print('PostgreSQL error:', error)
        


    def selectQuery(self, query):
        if self.connection is not None:
            cur = self.connection.cursor()
            cur.execute(query)
            data = cur.fetchall()
            cur.close()
            return data
        

    def upsertQuery(self, query):
        if self.connection is not None:
            cur = self.connection.cursor()
            cur.execute(query)
            self.connection.commit()
            cur.close()
