# -*- coding: utf-8 -*-
"""
Created on Tue Sep 10 14:16:11 2019

@author: Meryll
"""

from sqlalchemy import create_engine
import pandas.io.sql as psql
import sqlalchemy
import sys

import config
#------------------------------------------------------------------------------


def connect(dbc='pg'):
    """Connects to known database: PostgreSQL or MySQL.

    Args:
        dbc (str): Database connection; known dbc are mysql and pg. 
                   Defaults to pg.

    Returns:
        connection (sqlalchemy.engine.base.Engine): Database connection.

    """ 
    
    if dbc not in ['mysql', 'pg']:
        print ('unknown connection')
        return
    
    dbc_cfg = {'mysql': {'dialect': 'mysql', 'driver': 'pymysql'},
               'pg': {'dialect': 'postgresql', 'driver': 'psycopg2'}}

    cfg = config.server_config()
    dialect = dbc_cfg[dbc]['dialect']
    driver = dbc_cfg[dbc]['driver']
    user = cfg[dbc]['user']
    password = cfg[dbc]['password']
    host = cfg[dbc]['host']
    port = cfg[dbc]['port']
    dbname = cfg[dbc]['dbname']
    
    try:
        engine = create_engine('{}+{}://{}:{}@{}:{}/{}'.format(dialect, driver,
                               user, password, host, port, dbname))
        return engine
    except sqlalchemy.exc.OperationalError:
        print (">> Error in connetion")
        return False


def read_df(query, dbc='pg'):
    """Retrieves data from known database: PostgreSQL or MySQL.
    
    Args:
        query (str): Query statement. For PostgreSQL, include schema of tables
                     (e.g. public.table_name)
        dbc (str): Database connection; known dbc are mysql and pg. 
                   Defaults to pg.

    Returns:
        df (pandas.DataFrame): Dataframe to be retrieved.

    """ 

    if dbc not in ['mysql', 'pg']:
        print('unknown connection: {}'.format(dbc))
        return

    connection = connect(dbc).connect()
    ret_val = None
    try:
        df = psql.read_sql_query(query, connection)
        ret_val = df
    except KeyboardInterrupt:
        print('Exception detected in accessing database')
        sys.exit()
    except psql.DatabaseError:
        print('Error getting query: {}'.format(query))
        ret_val = None
    finally:
        return ret_val


def execute(query, dbc='pg'):
    """Executes query from known database: PostgreSQL or MySQL.
    
    Args:
        query (str): Query statement. For PostgreSQL, include schema of tables
                     (e.g. public.table_name)
        dbc (str): Database connection; known dbc are mysql and pg. 
                   Defaults to pg.
    
    """

    if dbc not in ['mysql', 'pg']:
        print('unknown connection: {}'.format(dbc))
        return

    connection = connect(dbc).connect()
    try:
        connection.execute(query)
        print('Query executed')
    except Exception as e:
        if(connection):
            print("\n\nFailed to execute query: {}\n\n".format(e))
    finally:
        #closing database connection.
        if(connection):
            connection.close()
            print("Connection closed")
            
            
def write_df(df, table_name, schema='public', index=False, dbc='pg'):
    """Writes df to known database: PostgreSQL or MySQL.
    
    Args:
        df (pandas.DataFrame): Dataframe to be written to database.
        table_name (str): Name of table df to be written to. 
                          Includes schema for PostgreSQL (e.g. public.table_name)
        index (bool): If to include index in writing to database. 
                      Defaults to False
        dbc (str): Database connection; known dbc are mysql and pg. 
                   Defaults to pg.
   
    """

    if dbc not in ['mysql', 'pg']:
        print('unknown connection: {}'.format(dbc))
        return
    
    if index:
        df2 = df.reset_index()
    else:
        df2 = df
    
    df2 = df2.drop_duplicates()
    columns = ', '.join(df2.columns)
    tuple_list = map(lambda x: tuple(map(str, x)), df2.values)
    values = ', '.join(list(map(str, tuple_list))).replace('"', '').replace("'nan'", 'NULL').replace("'None'", 'NULL')

    query = "INSERT INTO {} ({}) VALUES {} ".format(table_name, columns, values)

    if dbc == 'pg':
        query = query.replace("INSERT INTO {}".format(table_name),
                              "INSERT INTO {}.{}".format(schema, table_name))
        update_columns = ', '.join(map(lambda x: '{} = EXCLUDED.{}'.format(x, x),
                                       df2.columns))
        query += """ON CONFLICT ON CONSTRAINT {}_uq 
                DO UPDATE SET {}""".format(table_name, update_columns)
    
    elif dbc == 'mysql':
        update_columns = ', '.join(map(lambda x: '{} = VALUES({})'.format(x, x),
                                       df2.columns))
        query += "ON DUPLICATE KEY UPDATE {}".format(update_columns)
    
    execute(query, dbc=dbc)