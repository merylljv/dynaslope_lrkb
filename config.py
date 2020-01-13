# -*- coding: utf-8 -*-
"""
Created on Tue Sep 10 14:16:11 2019

@author: Meryll
"""

import configparser
import os 
dir_path = os.path.dirname(os.path.realpath(__file__))
#------------------------------------------------------------------------------


def server_config():
    cfile = os.path.join(dir_path, "connections.cnf")
    cnf = configparser.ConfigParser(inline_comment_prefixes=';')
    cnf.read(cfile)
    
    config_dict = dict()
    for section in cnf.sections():
        options = dict()
        for opt in cnf.options(section):
    
            try:
                options[opt] = cnf.getboolean(section, opt)
                continue
            except ValueError:
                pass
    
            try:
                options[opt] = cnf.getint(section, opt)
                continue
            except ValueError:
                pass
    
            try:
                options[opt] = cnf.getfloat(section, opt)
                continue
            except ValueError:
                pass
    
            options[opt] = cnf.get(section, opt)
     
    
        config_dict[section.lower()]= options

    return config_dict