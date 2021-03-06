'''
Description: 
Author: Wangp
Date: 2021-05-17 17:47:29
LastEditTime: 2021-05-17 18:04:13
LastEditors: Wangp
'''
# -*- coding: utf-8 -*-
'''
# @Desc    : 个性化代码模块
# @Author  : zhouyy
# @Time    : 2021/5/10
'''
import pandas as pd
import os
from .utils.log_utils import logger
from .settings import config, DIR_OF_MAIN_PROG
from .db.connecting_database import DataQuery, sqls_config


def test_read_data():
    """
    从data文件夹取数
    :return:
    """
    print("*"*30, '测试data读取', "*"*30)
    file_name = 'CreditTransitionMatrix.xlsx'
    df = pd.read_excel(os.path.join(DIR_OF_MAIN_PROG, 'data', file_name))
    print(df.head)

def test_database():
    """
    测试数据库
    :return:
    """
    print("*" * 30, '测试数据库模块', "*" * 30)
    dq = DataQuery(config['data_base']['JY'])
    ssql = sqls_config['ashare_calendar']['Sql'] # sqls.yaml中的sql语句
    df = dq.sql_query(ssql)
    print(df.head())

    # demo： 含参数的sql语句
    dq = DataQuery(config['data_base']['JY'])
    ssql = sqls_config['demo_set_sqlparam']['Sql'] # sqls.yaml中的sql语句
    ssql_param = ssql.format(param='83')
    df = dq.sql_query(ssql_param)
    print(df.head())

    pass

def test_log_utils():
    """
    测试日志工具
    :return:
    """
    print("*" * 30, '测试日志模块', "*" * 30)
    logger.debug("---测试开始----")
    logger.info("操作步骤")
    logger.warning("----测试结束----")
    logger.error("----测试错误----")


def run_my_function():
    test_read_data()
    test_database()
    test_log_utils()


# # 

if __name__ == '__main__':
<<<<<<< HEAD
    # gitlab 
    run_my_function()  
=======
    # github test
    run_my_function()
>>>>>>> 32c483f911239666ae30e7b5a2d5ba995c50d11f
