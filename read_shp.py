

# -*- coding: utf-8 -*-
"""
Created on Thu Sep  5 14:44:46 2019

@author: Data Scientist 1
"""

from datetime import datetime
import geopandas as gpd
import numpy as np
import os
import pandas as pd
import re


def shp_file_data(file_path, filename_list):
    
    active_path = file_path
    deactivated_path = "{}\\Deactivated".format(file_path)
    
    deactivated_filename = filename_list[filename_list.str.contains('Deactivated')]
    active_filename = filename_list[~filename_list.str.contains('Deactivated')]
    
    filename_dict = {}
    filename_dict['deactivated'] = {'path': 'Deactivated',
                                    'file_path': deactivated_path,
                                    'filename_list': deactivated_filename
                                   }
    filename_dict['active'] = {'path': 'Site Map',
                               'file_path': active_path,
                               'filename_list': active_filename
                              }
    
    shp_df = pd.DataFrame()

    for key in filename_dict.keys():

        filename_list = filename_dict[key]['filename_list']
        path = filename_dict[key]['path']

        for filename in filename_list:
            
            if 'Hazard_Zone' in filename:
                spatial_cat = 'Hazard_Zone'
                name = filename.replace("{}\\".format(filename_dict[key]['file_path'], spatial_cat), '')

            else:
                spatial_cat = re.findall(r"(?<={}\\)[A-Za-z]*(?=\\)".format(path),
                                         filename)[0]
                name = filename.replace("{}\\{}\\".format(filename_dict[key]['file_path'], spatial_cat), '')


            name = name.replace('.shp', '')

            version = re.findall(r"(?<=v)\d+", filename)
            if len(version) != 0:
                version = int(version[0])
                name = name.replace('v{}'.format(version), '')
            else:
                version = np.nan
    
            if key == 'deactivated':
                re_to = 'to\d+'
            else:
                re_to = ''
                deactivated = np.nan    
            from_to_str = re.findall(r"\b\d+{}(?=.shp)".format(re_to), filename)
            if len(from_to_str) != 0:
                name = name.replace(from_to_str[0], '')
                from_to = from_to_str[0].split('to')
                activated = datetime.strptime(from_to[0],
                                              '%Y%m%d').strftime('%Y-%m-%d')
                if key == 'deactivated':
                    deactivated = datetime.strptime(from_to[1],
                                                    '%Y%m%d').strftime('%Y-%m-%d')
            else:
                file_modified = datetime.fromtimestamp(os.path.getmtime(filename))
                ts = file_modified.strftime('%Y-%m-%d')
                activated = ts
                if key == 'deactivated':
                    deactivated = ts
                    
            name = name.strip()
            
            try:
                shp_file = gpd.read_file(filename)
            except:
                shp_file = gpd.read_file(filename, encoding="utf-8")
            columns = ['geometry']
            if 'NAME' in shp_file.columns:
                columns.append('NAME')
            shp_file = shp_file.loc[:, columns]
            shp_file.loc[:, 'spatial_cat'] = spatial_cat
            shp_file.loc[:, 'name'] = name
            shp_file.loc[:, 'version'] = version
            shp_file.loc[:, 'activated'] = activated
            shp_file.loc[:, 'deactivated'] = deactivated
            shp_df = shp_df.append(shp_file, ignore_index=True, sort=False)
    
    shp_df = shp_df.rename(columns={'NAME': 'label_name'})

    return shp_df