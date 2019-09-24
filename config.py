# -*- coding: utf-8 -*-
"""
Created on Tue Sep 10 14:16:11 2019

@author: Meryll
"""

import configparser
#------------------------------------------------------------------------------


def server_config():
    cfile = "connections.cnf"
    
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