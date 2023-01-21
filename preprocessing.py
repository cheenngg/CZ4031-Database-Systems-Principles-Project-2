import psycopg2
from psycopg2 import sql
from configparser import ConfigParser

class Preprocessing:
    def __init__(self, database, user, password):
        self.conn = None
        try:
            print('Connecting to PostgreSQL ....')
            self.conn = psycopg2.connect(dbname= database, user= user, password= password)
            print('Connected.')
            self.cur = self.conn.cursor()

        except(Exception, psycopg2.DatabaseError) as error:
            print(error)

    def getQEP(self,query):
        self.cur.execute("Explain (Format json) {}".format(query))
        self.qep = self.cur.fetchall()
        self.qep = self.qep[0][0][0]['Plan']
        print(self.qep)
        return self.qep
