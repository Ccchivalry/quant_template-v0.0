# -*- coding: utf-8 -*-
"""
Created on Thu Sep 12 12:28:17 2019

@author: wangp
"""
import numpy as np
import pandas as pd
import yaml
import os
import cx_Oracle as cx
from typing import Dict
from .. import settings
config = settings.config

# load ssql.yaml中的语句
_SQLS_YAML_CFG = os.path.join(settings.DIR_OF_MAIN_PROG, 'code', 'scripts', 'db', 'sqls.yaml')
with open(_SQLS_YAML_CFG) as cfg:
    sqls_config = yaml.safe_load(cfg)

class DataQuery(object):
    def __init__(self, log_info: Dict):
        """
        初始化，输入参数为字典，包含user，pwd，ip
        :param log_info:
        """
        self.con=cx.connect(log_info['user'], log_info['pwd'], log_info['ip'], encoding = "UTF-8", nencoding = "UTF-8")


    def sql_query(self,query):
        tb=pd.read_sql(query,self.con)
        return tb

    def close_query(self):
        # cursor.close()  #关闭游标
        self.con.close()

