# -*- coding: utf-8 -*-
"""
Created on Tue Sep 10 14:16:11 2019

@author: Meryll

Creates PostgreSQL tables

naming convention:
    singular
    lowercase
    word separator: underscore
    auto-increment column as pk: [table_name]_id
    primary key constraint: [table_name]_pk
    unique key constraint: [table_name]_uq
    foreign key constraint: [table_name]_fk_[referenced_table_name]

"""

import querydb as qdb


def create_schema(schema):
    """Creates schema for Landslide Risk Knowledge Base in PostgreSQL/PostGIS.

    """ 

    query = "CREATE SCHEMA IF NOT EXISTS {}".format(schema)
    qdb.execute(query)


def create_tables():
    """Creates tables for Landslide Risk Knowledge Base in PostgreSQL/PostGIS.

    """ 
        
    pk_contraint = "CONSTRAINT {}_pk PRIMARY KEY ({})"
    uq_contraint = "CONSTRAINT {}_uq UNIQUE ({})"
    fk_query = """CONSTRAINT {}_fk_{} 
                      FOREIGN KEY ({}) 
                      REFERENCES {}({}) 
                      ON UPDATE CASCADE 
                      ON DELETE RESTRICT
                      """
                      
    create_dict = {}
    index = 1


    ############################## public SCHEMA ##############################
    
    schema = 'public'
    create_schema(schema)

    #################### site ####################
    table_name = 'site'
    pk_id = 'site_id'
    uq_list = ['site_code']
    fk_dict = {}
    query = """CREATE TABLE IF NOT EXISTS {}.{} (
          {} SERIAL,   
          site_code CHAR(3),
          purok VARCHAR,
          sitio VARCHAR,
          barangay VARCHAR,
          municipality VARCHAR,
          province VARCHAR,
          region VARCHAR,
          psgc INTEGER,
          active BOOLEAN NOT NULL DEFAULT TRUE,
          season SMALLINT,
          {}, {} {}
        );
        """    
    create_dict[index] = {'schema': schema,
                          'table_name': table_name,
                          'query': query,
                          'pk_id': pk_id,
                          'uq_list': uq_list,
                          'fk_dict': fk_dict}
    index += 1


    ############################## spatial SCHEMA ##############################
    
    schema = 'spatial'
    create_schema(schema)
    
    #################### exposure ####################
    table_name = 'exposure'
    pk_id = 'exp_id'
    uq_list = ['exp_name']
    fk_dict = {}
    query = """CREATE TABLE IF NOT EXISTS {}.{} (
          {} SERIAL,   
          exp_name VARCHAR,
          {}, {} {}
        );
        """   
    create_dict[index] = {'schema': schema,
                          'table_name': table_name,
                          'query': query,
                          'pk_id': pk_id,
                          'uq_list': uq_list,
                          'fk_dict': fk_dict}
    index += 1
    
    #################### site_exposure ####################
    table_name = 'site_exposure'
    pk_id = 'se_id'
    uq_list = ['site_id', 'exp_id', 'geom']
    fk_dict = {'site_id': {'ref_schema': 'public', 'ref_table': 'site'},
               'exp_id': {'ref_schema': 'spatial', 'ref_table': 'exposure'}}
    query = """CREATE TABLE IF NOT EXISTS {}.{} (
          {} SERIAL,   
          site_id INTEGER,
          exp_id INTEGER,
          label_name VARCHAR,
          geom GEOMETRY,
          activated DATE NOT NULL DEFAULT CURRENT_DATE,
          deactivated DATE,
          {}, {} {}
        );
        """   
    create_dict[index] = {'schema': schema,
                          'table_name': table_name,
                          'query': query,
                          'pk_id': pk_id,
                          'uq_list': uq_list,
                          'fk_dict': fk_dict}
    index += 1
    
    #################### feature ####################
    table_name = 'feature'
    pk_id = 'feat_id'
    uq_list = ['feat_name']
    fk_dict = {}
    query = """CREATE TABLE IF NOT EXISTS {}.{} (
          {} SERIAL,   
          feat_name VARCHAR,
          {}, {} {}
        );
        """   
    create_dict[index] = {'schema': schema,
                          'table_name': table_name,
                          'query': query,
                          'pk_id': pk_id,
                          'uq_list': uq_list,
                          'fk_dict': fk_dict}
    index += 1

    #################### site_feature ####################
    table_name = 'site_feature'
    pk_id = 'sf_id'
    uq_list = ['site_id', 'feat_id', 'geom']
    fk_dict = {'site_id': {'ref_schema': 'public', 'ref_table': 'site'},
               'feat_id': {'ref_schema': 'spatial', 'ref_table': 'feature'}}
    query = """CREATE TABLE IF NOT EXISTS {}.{} (
          {} SERIAL,   
          site_id INTEGER,
          feat_id INTEGER,
          geom GEOMETRY,
          activated DATE NOT NULL DEFAULT CURRENT_DATE,
          deactivated DATE,
          {}, {} {}
        );
        """   
    create_dict[index] = {'schema': schema,
                          'table_name': table_name,
                          'query': query,
                          'pk_id': pk_id,
                          'uq_list': uq_list,
                          'fk_dict': fk_dict}
    index += 1

    #################### hazard_zone ####################
    table_name = 'hazard_zone'
    pk_id = 'hz_id'
    uq_list = ['site_id, geom']
    fk_dict = {'site_id': {'ref_schema': 'public', 'ref_table': 'site'}}
    query = """CREATE TABLE IF NOT EXISTS {}.{} (
          {} SERIAL,   
          site_id INTEGER,
          geom GEOMETRY,
          activated DATE NOT NULL DEFAULT CURRENT_DATE,
          deactivated DATE,
          {}, {} {}
        );
        """
    create_dict[index] = {'schema': schema,
                          'table_name': table_name,
                          'query': query,
                          'pk_id': pk_id,
                          'uq_list': uq_list,
                          'fk_dict': fk_dict}
    index += 1

    #################### monitoring ####################
    table_name = 'monitoring'
    pk_id = 'mon_id'
    uq_list = ['mon_name']
    fk_dict = {}
    query = """CREATE TABLE IF NOT EXISTS {}.{} (
          {} SERIAL,   
          mon_name VARCHAR,
          {}, {} {}
        );
        """   
    create_dict[index] = {'schema': schema,
                          'table_name': table_name,
                          'query': query,
                          'pk_id': pk_id,
                          'uq_list': uq_list,
                          'fk_dict': fk_dict}
    index += 1

    #################### site_monitoring ####################
    table_name = 'site_monitoring'
    pk_id = 'sm_id'
    uq_list = ['site_id', 'mon_id', 'geom']
    fk_dict = {'site_id': {'ref_schema': 'public', 'ref_table': 'site'},
               'mon_id': {'ref_schema': 'spatial', 'ref_table': 'monitoring'}}
    query = """CREATE TABLE IF NOT EXISTS {}.{} (
          {} SERIAL,   
          site_id INTEGER,
          mon_id INTEGER,
          label_name VARCHAR,
          geom GEOMETRY,
          activated DATE NOT NULL DEFAULT CURRENT_DATE,
          deactivated DATE,
          {}, {} {}
        );
        """   
    create_dict[index] = {'schema': schema,
                          'table_name': table_name,
                          'query': query,
                          'pk_id': pk_id,
                          'uq_list': uq_list,
                          'fk_dict': fk_dict}
    index += 1


    ############################### comm SCHEMA ###############################
    
    schema = 'comm'
    create_schema(schema)

    #################### gsm_server ####################
    table_name = 'gsm_server'
    pk_id = 'server_id'
    uq_list = ['server_name']
    fk_dict = {}
    query = """CREATE TABLE IF NOT EXISTS {}.{} (
          {} SERIAL,   
          server_name VARCHAR,
          platform_type VARCHAR,
          version SMALLINT,
          {}, {} {}
        );
        """   
    create_dict[index] = {'schema': schema,
                          'table_name': table_name,
                          'query': query,
                          'pk_id': pk_id,
                          'uq_list': uq_list,
                          'fk_dict': fk_dict}
    index += 1

    #################### server_port ####################
    table_name = 'server_port'
    pk_id = 'port_id'
    uq_list = ['server_id', 'port']
    fk_dict = {'server_id': {'ref_schema': 'comm', 'ref_table': 'gsm_server'}}
    query = """CREATE TABLE IF NOT EXISTS {}.{} (
          {} SERIAL,   
          server_id INTEGER,
          port BOOLEAN,
          ser_port VARCHAR,
          pwr_on_pin SMALLINT,
          ring_pin SMALLINT,
          module_type SMALLINT,
          {}, {} {}
        );
        """
    query += """ COMMENT ON TABLE {}.{} IS 
          '0- left
           1- right'
         ;""".format(schema, table_name)
    create_dict[index] = {'schema': schema,
                          'table_name': table_name,
                          'query': query,
                          'pk_id': pk_id,
                          'uq_list': uq_list,
                          'fk_dict': fk_dict}
    index += 1

    #################### network_type ####################
    table_name = 'network_type'
    pk_id = 'prefix'
    uq_list = ['prefix']
    fk_dict = {}
    query = """CREATE TABLE IF NOT EXISTS {}.{} (
          {} VARCHAR(3),   
          carrier SMALLINT,
          {}, {} {}
        );
        """
    query += """ COMMENT ON TABLE {}.{} IS 
          '1- globe
           2- smart
           3- landline'
         ;""".format(schema, table_name)
    create_dict[index] = {'schema': schema,
                          'table_name': table_name,
                          'query': query,
                          'pk_id': pk_id,
                          'uq_list': uq_list,
                          'fk_dict': fk_dict}
    index += 1

    #################### gsm_module ####################
    table_name = 'gsm_module'
    pk_id = 'gsm_id'
    uq_list = ['prefix', 'num', 'activated']
    fk_dict = {'prefix': {'ref_schema': 'comm', 'ref_table': 'network_type'},
               'port_id': {'ref_schema': 'comm', 'ref_table': 'server_port'}}
    query = """CREATE TABLE IF NOT EXISTS {}.{} (
          {} SERIAL,   
          prefix VARCHAR(3),
          num CHAR(7),
          activated DATE NOT NULL DEFAULT CURRENT_DATE,
          port_id INTEGER,
          {}, {} {}
        );
        """
    create_dict[index] = {'schema': schema,
                          'table_name': table_name,
                          'query': query,
                          'pk_id': pk_id,
                          'uq_list': uq_list,
                          'fk_dict': fk_dict}
    index += 1


    ############################# temporal SCHEMA #############################
    
    schema = 'temporal'
    create_schema(schema)

    #################### marker_observation ####################
    table_name = 'marker_observation'
    pk_id = 'mo_id'
    uq_list = ['site_id', 'ts']
    fk_dict = {'site_id': {'ref_schema': 'public', 'ref_table': 'site'}}
    query = """CREATE TABLE IF NOT EXISTS {}.{} (
          {} SERIAL,   
          site_id INTEGER,
          ts TIMESTAMP,
          meas_type VARCHAR(7),
          weather VARCHAR,
          observer_name VARCHAR,
          {}, {} {}
        );
        """   
    create_dict[index] = {'schema': schema,
                          'table_name': table_name,
                          'query': query,
                          'pk_id': pk_id,
                          'uq_list': uq_list,
                          'fk_dict': fk_dict}
    index += 1

    #################### marker_history ####################
    table_name = 'marker_history'
    pk_id = 'hist_id'
    uq_list = ['sm_id', 'ts', 'event']
    fk_dict = {'sm_id': {'ref_schema': 'spatial', 'ref_table': 'site_monitoring'}}
    query = """CREATE TABLE IF NOT EXISTS {}.{} (
          {} SERIAL,
          sm_id BIGINT,
          ts TIMESTAMP,
          event BOOLEAN,
          label_name VARCHAR,
          {}, {} {}
        );
        """
    query += """ COMMENT ON TABLE {}.{} IS 
          '0- rename
           1- reposition'
         ;""".format(schema, table_name)
    create_dict[index] = {'schema': schema,
                          'table_name': table_name,
                          'query': query,
                          'pk_id': pk_id,
                          'uq_list': uq_list,
                          'fk_dict': fk_dict}
    index += 1

    #################### marker_data ####################
    table_name = 'marker_data'
    pk_id = 'data_id'
    uq_list = ['sm_id', 'mo_id']
    fk_dict = {'sm_id': {'ref_schema': 'spatial', 'ref_table': 'site_monitoring'},
               'mo_id': {'ref_schema': 'temporal', 'ref_table': 'marker_observation'}}
    query = """CREATE TABLE IF NOT EXISTS {}.{} (
          {} SERIAL,
          mo_id BIGINT,
          sm_id BIGINT,
          measurement NUMERIC(5,1),
          {}, {} {}
        );
        """   
    create_dict[index] = {'schema': schema,
                          'table_name': table_name,
                          'query': query,
                          'pk_id': pk_id,
                          'uq_list': uq_list,
                          'fk_dict': fk_dict}
    index += 1

    #################### marker_alert ####################
    table_name = 'marker_alert'
    pk_id = 'alert_id'
    uq_list = ['data_id']
    fk_dict = {'data_id': {'ref_schema': 'temporal', 'ref_table': 'marker_data'}}
    query = """CREATE TABLE IF NOT EXISTS {}.{} (
          {} SERIAL,
          data_id BIGINT,
          displacement NUMERIC(4,1),
          time_delta FLOAT,
          alert_level SMALLINT,
          {}, {} {}
        );
        """   
    create_dict[index] = {'schema': schema,
                          'table_name': table_name,
                          'query': query,
                          'pk_id': pk_id,
                          'uq_list': uq_list,
                          'fk_dict': fk_dict}
    index += 1

    #################### logger_model ####################
    table_name = 'logger_model'
    pk_id = 'model_id'
    uq_list = ['has_tilt', 'has_rain', 'has_piezo', 'has_soms', 'logger_type']
    fk_dict = {}
    query = """CREATE TABLE IF NOT EXISTS {}.{} (
          {} SERIAL,   
          has_tilt BOOLEAN,
          has_rain BOOLEAN,
          has_piezo BOOLEAN,
          has_soms BOOLEAN,
          logger_type SMALLINT,
          {}, {} {}
        );
        """   
    create_dict[index] = {'schema': schema,
                          'table_name': table_name,
                          'query': query,
                          'pk_id': pk_id,
                          'uq_list': uq_list,
                          'fk_dict': fk_dict}
    index += 1

    #################### logger ####################
    table_name = 'logger'
    pk_id = 'logger_id'
    uq_list = ['sm_id']
    fk_dict = {'sm_id': {'ref_schema': 'spatial', 'ref_table': 'site_monitoring'},
               'model_id': {'ref_schema': 'temporal', 'ref_table': 'logger_model'}}
    query = """CREATE TABLE IF NOT EXISTS {}.{} (
          {} SERIAL,   
          sm_id BIGINT,
          model_id INTEGER,
          {}, {} {}
        );
        """   
    create_dict[index] = {'schema': schema,
                          'table_name': table_name,
                          'query': query,
                          'pk_id': pk_id,
                          'uq_list': uq_list,
                          'fk_dict': fk_dict}
    index += 1
    
    #################### logger_mobile ####################
    table_name = 'logger_mobile'
    pk_id = 'mobile_id'
    uq_list = ['logger_id', 'activated']
    fk_dict = {'logger_id': {'ref_schema': 'temporal', 'ref_table': 'logger'},
               'gsm_id': {'ref_schema': 'comm', 'ref_table': 'gsm_module'}}
    query = """CREATE TABLE IF NOT EXISTS {}.{} (
          {} SERIAL,
          logger_id INTEGER,
          activated DATE NOT NULL DEFAULT CURRENT_DATE,
          sim_num VARCHAR(12),
          gsm_id INTEGER,
          {}, {} {}
        );
        """   
    create_dict[index] = {'schema': schema,
                          'table_name': table_name,
                          'query': query,
                          'pk_id': pk_id,
                          'uq_list': uq_list,
                          'fk_dict': fk_dict}
    index += 1


    #################### EXECUTE QUERY TO CREATE TABLES ####################
    for index in create_dict.keys():
        dct = create_dict[index]
        schema = dct['schema']
        table_name = dct['table_name']
        query = dct['query']
        pk_id = dct['pk_id']
        uq_list = dct['uq_list']
        fk_dict = dct['fk_dict']
        if len(fk_dict.keys()) == 0:
            fk_constraint = ''
        else:
            fk_constraint_list = ['']
            for fk_id in fk_dict.keys():
                ref_schema = fk_dict.get(fk_id)['ref_schema']
                ref_table = fk_dict.get(fk_id)['ref_table']
                fk_part = fk_query.format(table_name, ref_table, fk_id,
                                          "{}.{}".format(ref_schema, ref_table),
                                          fk_id)
                fk_constraint_list.append(fk_part)
            fk_constraint = ', '.join(fk_constraint_list)
                
        query = query.format(schema, table_name, pk_id, 
                             pk_contraint.format(table_name, pk_id),
                             uq_contraint.format(table_name, ', '.join(uq_list)),
                             "{}".format(fk_constraint))
        qdb.execute(query)


###############################################################################
if __name__ == "__main__":
    create_tables()