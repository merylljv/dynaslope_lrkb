

# -*- coding: utf-8 -*-
"""
Created on Thu Sep  5 14:44:46 2019

@author: Data Scientist 1
"""

########################## READ SHAPEFILE ##########################

#import geopandas as gpd
#
#
#filename = "C:\\Users\\Data Scientist 1\\Documents\\ArcGIS\\Default.gdb\\Export_Output.shp"
#hh_filename = "D:\\Meryll\\Research\\LRKB\\IMU\\Site Map\\Households.shp"
#
#hh = gpd.read_file(hh_filename).loc[:, ['NAME', 'geometry']].sort_values('NAME')
#hh_str = ', '.join(hh.apply(lambda row: "(2, 1, {}, ST_GeomFromText('{}'))".format(row.NAME, row.geometry), axis=1).values)
#
#
#
#
#
#query =  "INSERT INTO public.site_exposure "
#query += "(exp_id, site_id, label_name, geom) "
#query += "VALUES {};".format(hh_str)
#print(query.replace(", (", ",\n(").replace(") ", ")\n"))

########################## GET POINTS OF POLY LINE ##########################

from arcpy import da
from datetime import datetime
import arcpy
import geopandas as gpd
import numpy as np
import os
import pandas as pd
import re


def get_active_layer(mxd_filename):
    mxd = arcpy.mapping.MapDocument(mxd_filename)
    active_lyr = []
    for lyr in arcpy.mapping.ListLayers(mxd):
        if lyr.supports("DATASOURCE"):
            active_lyr += [str(lyr.dataSource)]
    active_lyr = pd.Series(active_lyr)
    active_lyr = sorted(active_lyr[(active_lyr.str.contains('.shp')) & (active_lyr.str.contains('Site Map'))])
    return (active_lyr)


def get_vertex(filename):
    """For each polygon geometry in a shapefile get the sequence number and
    and coordinates of each vertex and tie it to the OID of its corresponding
    polygon"""

    vtx_dict = {}
    s_fields = ['OID@', 'Shape@XY']
    pt_array = da.FeatureClassToNumPyArray(filename, s_fields, 
        explode_to_points=True)

    for oid, xy in pt_array:
        xy_tup = tuple(xy)
        if oid not in vtx_dict:
            vtx_dict[oid] = [xy_tup]
        # this clause ensures that the first/last point which is listed
        # twice only appears in the list once
        elif xy_tup not in vtx_dict[oid]:
            vtx_dict[oid].append(xy_tup)


    vtx_sheet = []
    for oid, vtx_list in vtx_dict.iteritems():
        for i, vtx in enumerate(vtx_list):
            vtx_sheet.append((oid, i, vtx[0], vtx[1]))
    
    return vtx_sheet


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
            shp_df = shp_df.append(shp_file, ignore_index=True)
    
    shp_df = shp_df.rename(columns={'NAME': 'label_name',
                                    'geometry': 'geom'})

    return shp_df