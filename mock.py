# -*- coding: utf-8 -*-
"""
Created on Thu Jun  6 08:19:36 2024

@author: Simon Jaramillo
"""

import pyodbc
import sqlalchemy as sa
import unittest
from unittest.mock import MagicMock, patch
import pandas as pd

server = "(local)\,1433"
database= "Datamart_EIA"

def cnx_emr(odbc):
    pyodbc.autocommit=True
    conn= pyodbc.connect('DSN={}'.format(odbc), autocommit=True)
    return conn

def cnx_DM(server,database):
    
    engine = sa.create_engine('mssql+pyodbc://@'+server+'/'+database+'?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server',fast_executemany=True)
    return engine
    
def read_sql(sql_statement):
    with open(sql_statement, 'r') as file:
        sql_query = file.read()
    return sql_query

def get_emr_data(cnx,query):
    data= pd.read_sql(query,cnx)
    return data

def close_cnx(cnx):
    cnx.close()
    
class TestCnxOdbcDatabase(unittest.TestCase):
    @patch('pyodbc.connect')
    def test_cnx_emr(self, mock_connect):
        mock_connection = MagicMock()
        mock_connect.return_value=mock_connection 
        
        conn=cnx_emr("testdef")
        
        mock_connect.assert_called_once_with('DSN=testdef', autocommit=True)
        
        self.assertEqual(conn, mock_connection)
    
    def test_read_sql(self):
        
        str_desire=read_sql(r"C:\Users\Simon Jaramillo\Downloads\pruebas\unittest.sql")
        
        str_expected = "SELECT Pais_Nombre FROM Datamart_EIA.dbo.Dim_Geografia WHERE IdGeografia =351"
        
        self.assertEqual(str_desire, str_expected)
        
    
class TestDatabaseOperation(unittest.TestCase):
    @patch('pandas.read_sql')
    @patch('pyodbc.connect')
    def test_get_emr_data(self,mock_connect,mock_read_sql):
        mock_connection = MagicMock()
        mock_connect.return_value= mock_connection 
        
        expected_df = pd.DataFrame({
            'Pais_Nombre' : 'FRANCE'
            }, index=[0])
        mock_read_sql.return_value=expected_df
        
        conn=cnx_emr("testdef")
        sql=read_sql(r"C:\Users\Simon Jaramillo\Downloads\pruebas\unittest.sql")
        data=get_emr_data(conn, sql)
        
        mock_read_sql.assert_called_once_with(sql,mock_connection)
        
        pd.testing.assert_frame_equal(data,expected_df)

class TestDatabaseClose(unittest.TestCase):
    
    @patch('pyodbc.connect')
    def test_close_cnx(self, mock_connect):
        mock_connection = MagicMock()
        mock_connect.return_value= mock_connection 
        
        conn=cnx_emr("testdef")
        
        close_cnx(conn)
        
        mock_connection.close.assert_called_once()
        
class TestDatabaseSAConnection(unittest.TestCase):
    
    @patch('sqlalchemy.create_engine')
    def test_cnx_DM(self,mock_create_engine):
        mock_engine = MagicMock()
        mock_create_engine.return_value= mock_engine 
        
        engine=cnx_DM(server,database)
        
        mock_create_engine.assert_called_once_with('mssql+pyodbc://@'+server+'/'+database+'?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server',fast_executemany=True)
        
        self.assertEqual(engine, mock_engine)
        

if __name__=='__main__':
    unittest.main()
#conn=cnx_emr("testdef")
#sql=read_sql(r"C:\Users\Simon Jaramillo\Downloads\pruebas\unittest.sql")
#data=get_emr_data(conn, sql)