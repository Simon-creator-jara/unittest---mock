# -*- coding: utf-8 -*-
"""
Created on Wed Jun  5 16:15:05 2024

@author: Simon Jaramillo
"""

import pyodbc
import sqlalchemy as sa
import unittest

server = "(local)\,1433"
database= "Datamart_EIA"

def cnx(server,database):
    pyodbc.autocommit=True
    engine = sa.create_engine('mssql+pyodbc://@'+server+'/'+database+'?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server',fast_executemany=True)

    return engine

def sql_statement(statement,con):
    cursor=con.raw_connection().cursor()
    cursor.execute(statement)
    result = cursor.fetchall()
    return result


class TestDataBase(unittest.TestCase):
    def setUp(self):
        self.server="(local)\,1433"
        self.database= "Datamart_EIA"
    
    def test_connection(self):
        try:
            conn=cnx(self.server,self.database)
            response=True
        except:
            response=False
        
        self.assertEqual(response, True)
    
    def test_statement(self):
        conn=cnx(self.server,self.database)
        result=sql_statement("SELECT 1", conn)
        
        self.assertEqual(result[0][0], 1)
        
        
        
        
            
unittest.main()


