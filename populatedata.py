# -*- coding: utf-8 -*-
"""
Created on Thu Sep  5 14:44:46 2019

@author: Data Scientist 1
"""

import os
import pandas as pd
import re
import sys

import querydb as qdb
import read_shp as shp


def site():
    query = "SELECT * FROM sites"
    mysql_df = qdb.read_df(query, dbc='mysql')
    pg_df = mysql_df.loc[:, ['site_id', 'site_code', 'purok', 'sitio',
                             'barangay', 'municipality', 'province', 'region',
                             'psgc', 'active', 'season']]
    pg_df.loc[:, 'psgc'] = pg_df.loc[:, 'psgc'].astype(pd.Int64Dtype())
    qdb.write_df(pg_df, 'site', schema='public')
    

def spatial_dict():
    
    dct = {'exposure': {'name': ['Basketball Court',
                                 'Gabion Wall',
                                 'House',
                                 'House (Abandoned)',
                                 'Retaining Wall'],
                        'pk': 'exp_id',
                        'uq': 'exp_name'
                       },
           'feature': {'name': ['Bulging',
                                'Canal',
                                'Channel',
                                'Channel/Stream',
                                'Crack',
                                'Crack (Inferred)',
                                'Damaged Road Section',
                                'Damaged Structure',
                                'Depression',
                                'Landslide Scarp',
                                'Ponding',
                                'Seepage NE',
                                'Seepage NW',
                                'Seepage SE',
                                'Seepage SW',
                                'Stream',
                                'Stream/Canal',
                                'Well'],
                       'pk': 'feat_id',
                       'uq': 'feat_name'
                      },
           'monitoring': {'name': ['Extensometer',
                                   'GIZ Sensor',
                                   'Mohon',
                                   'Piezometer',
                                   'Sensor',
                                   'Sensor Gateway',
                                   'Sensor (Damaged)',
                                   'Sensor (LiDAR)',
                                   'Surficial Marker'],
                          'pk': 'mon_id',
                          'uq': 'mon_name'
                         }
          }
                
    return dct
    

def spatial_ref_data(dct):
    
    for table_name in dct.keys():
        name_list = dct[table_name]['name']
        name_list = ', '.join(map(lambda x: "('{}')".format(x), name_list))
        query = """INSERT INTO spatial.{} ({}) VALUES {}
            ON CONFLICT ON CONSTRAINT {}_uq 
            DO UPDATE SET {} = EXCLUDED.{}
            """.format(table_name, dct[table_name]['uq'], name_list,
                       table_name, dct[table_name]['uq'], dct[table_name]['uq'])
        qdb.execute(query)
    

def ref_data():
    site()
    dct = spatial_dict()
    spatial_ref_data(dct)
    
    
def site_spatial_data(file_path):
    # site code
    site_code = re.findall(r"(?<=\\)[A-Za-z]*(?=\\Site Map)",
                           file_path)[0].lower()
    query = "SELECT site_id FROM public.site WHERE site_code = '{}'".format(site_code)
    site_id = qdb.read_df(query).site_id[0]
    # get filename of all shp files
    filename_list = []
    for dirpath, dirnames, filenames in os.walk(file_path):
        for filename in filenames:
            curpath = os.path.join(dirpath, filename)
            filename_list.append(curpath)
    filename_list = pd.Series(filename_list)
    filename_list = filename_list[(filename_list.str.contains('.shp'))]
    # get data from shp files
    shp_df = shp.shp_file_data(file_path, filename_list)
    shp_df.loc[:, 'site_id'] = site_id
#    # process per spatial category
    spatial_cat_dict = {"Exposure": {'id': 'exp_id',
                                     'name': 'exp_name'}, 
                        "Feature": {'id': 'feat_id',
                                    'name': 'feat_name'}, 
                        "Monitoring": {'id': 'mon_id',
                                       'name': 'mon_name'}, 
                        "Hazard_Zone": {'id': 'site_id'}}
    for spatial_cat in spatial_cat_dict.keys():
        print("\n\n#####", spatial_cat, "#####")
        columns = ['site_id', 'geometry', 'activated', 'deactivated']
        table_name = spatial_cat.lower()
        spatial_df = shp_df.loc[shp_df.spatial_cat == spatial_cat]
        if spatial_cat != 'Hazard_Zone':
            columns.append(spatial_cat_dict[spatial_cat]['id'])
            table_name = "site_{}".format(table_name)
            query = "SELECT * FROM spatial.{}".format(spatial_cat.lower())
            ref_id = qdb.read_df(query)
            spatial_df = spatial_df.rename(columns = {'name': spatial_cat_dict[spatial_cat]['name']})        
            spatial_df = pd.merge(spatial_df, ref_id, on=spatial_cat_dict[spatial_cat]['name'])
        spatial_df = spatial_df.sort_values(['deactivated', 'version', 'activated'], ascending=[False, False, False])
        for index in set(spatial_df[spatial_cat_dict[spatial_cat]['id']]):
            spatial_df_id = spatial_df.loc[spatial_df[spatial_cat_dict[spatial_cat]['id']] == index, :]
            if spatial_cat != 'Hazard_Zone':
                print("#", spatial_df_id[spatial_cat_dict[spatial_cat]['name']].values[0])
            if len(spatial_df_id) > 1:
                spatial_df_id = spatial_df_id.loc[spatial_df_id.index.isin(spatial_df_id.geometry.drop_duplicates(keep='last').index), :]
            if len(spatial_df_id.loc[~spatial_df_id.label_name.isnull(), :]) != 0:
                columns.append('label_name')
            spatial_df_id = spatial_df_id.loc[:, columns]
            if 'label_name' in columns:
                columns.remove('label_name')
            srid = spatial_df_id.crs['init'].split(':')[1]
            spatial_df_id.loc[:, 'geom'] = list(map(lambda x: "ST_GeomFromText('{}', {})".format(x, srid), spatial_df_id.geometry))
            for index in spatial_df_id.index:
                df = spatial_df_id.loc[spatial_df_id.index == index, ['site_id', 'geom', 'activated', 'deactivated']]
                qdb.write_df(df, table_name, schema='spatial')
            print("saved in database: srid =", srid)
            

    return shp_df
    

def main(file_path=""):
    if file_path == "":
        file_path = os.path.abspath(sys.argv[1])
    ref_data()
    return site_spatial_data(file_path)










###############################################################################
if __name__ == "__main__":
    
    main()