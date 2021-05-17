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
        # self.con = cx.connect('risk', 'risk', '10.198.60.77:1521/RISK', encoding="UTF-8", nencoding="UTF-8")
        # cursor = con.cursor()       #创建游标
        # cursor.execute("")
        # data = cursor.fetch()
        # self.preset_dicts()


    def sql_query(self,query):
        tb=pd.read_sql(query,self.con)
        return tb

    def close_query(self):
        # cursor.close()  #关闭游标
        self.con.close()

    # def __preset_query_1(self,t0):
    #     #日度存量
    #     q = "WITH tb_CB /*中债，table_chinabond*/ AS ( SELECT ENDDATE,INNERCODE,TRUEREMAINMATURITY AS 中债剩余期限, VALUEFULLPRICE AS 中债全价,VALUECLEANPRICE AS 中债净价,ACCRUINTEREST AS 中债应计利息,VPYIELD AS 中债收益率,VPADURATION AS 中债修正久期,VPCONVEXITY AS 中债修正凸性, YIELDCODE AS 中债行权类型代码,CREDIBILITYCODE AS 中债可信度代码 FROM JYDB.BOND_CBVALUATIONALL WHERE ENDDATE = date '" + t0 + "' )," \
    #         "tb_CS /*中证，table_chinasecurities*/ AS (SELECT INNERCODE,VALUEFULLPRICE AS 中证全价,VALUECLEANPRICE AS 中证净价,ACCRUINTEREST AS 中证应计利息,VPYIELD AS 中证收益率,VPADURATION AS 中证修正久期,VPCONVEXITY AS 中证修正凸性 FROM JYDB.BOND_CSIVALUATION WHERE ENDDATE=date'" + t0 + "')," \
    #         "tb_JY_BC /*聚源，table_JY_bondcode*/ AS (SELECT MAINCODE,INNERCODE,SECUCODE AS 债券代码,SECUABBR AS 债券简称,CHINAME AS 债券全称,SECUMARKET AS 发行场所代码,BONDTYPELEVEL1DESC AS 债券类型一级,BONDTYPELEVEL2DESC AS 债券类型二级, ISSUER AS 发行人名称 FROM JYDB.BOND_CODE)," \
    #         "tb_JY_BBI /*聚源，table_JY_bondbasicinfo*/ AS (SELECT MAINCODE,INNERCODE,BONDFULLNAME AS 债券全称,OPTIONTYPE AS 特殊条款,ISSUER AS 发行人名称,GUARANTOR AS 担保人名称,ISSUEPRICE AS 债券发行价格,COMPOUNDMETHOD AS 计息方式代码,INTPAYMENTMETHOD AS 付息方式代码,COUPONRATE AS 票面利率,PAYINTERESTEFFENCY AS 付息频率,VALUEDATE AS 起息日期,ENDDATE AS 到期日期,MATURITY AS 债券期限 FROM JYDB.BOND_BASICINFO)," \
    #         "tb_JY_BSD AS ( SELECT MAINCODE, MAX( INFOPUBLDATE ) AS INFOPUBLDATE, MAX(ENDDATE) AS ENDDATE FROM JYDB.BOND_SIZE WHERE INFOPUBLDATE <= date '" + t0 + "' AND ENDDATE <= date '" + t0 + "' GROUP BY MAINCODE )," \
    #         "tb_JY_BST AS ( SELECT MAINCODE, INFOPUBLDATE, ENDDATE,TOTALSIZE FROM JYDB.BOND_SIZE WHERE INFOPUBLDATE <= date '" + t0 + "' AND ENDDATE<= date '" + t0 + "' )," \
    #         "tb_JY_BS /*聚源债券规模，table_JY_bondsize*/ AS (SELECT tb_JY_BST.MAINCODE,tb_JY_BST.TOTALSIZE/100 AS 市场存量 FROM tb_JY_BST LEFT JOIN tb_JY_BSD ON tb_JY_BST.MAINCODE=tb_JY_BSD.MAINCODE WHERE tb_JY_BST.INFOPUBLDATE=tb_JY_BSD.INFOPUBLDATE AND tb_JY_BST.ENDDATE=tb_JY_BSD.ENDDATE)," \
    #         "tb_JY_BIC /*聚源，table_JY_bondinfoconsolidated*/ AS (SELECT tb_JY_BC.*,tb_JY_BBI.特殊条款,tb_JY_BBI.担保人名称,tb_JY_BBI.计息方式代码,tb_JY_BBI.付息方式代码,tb_JY_BBI.票面利率,tb_JY_BBI.付息频率,tb_JY_BBI.起息日期,tb_JY_BBI.到期日期,tb_JY_BBI.债券期限,tb_JY_BBI.债券发行价格,tb_JY_BS.市场存量 FROM tb_JY_BC LEFT JOIN tb_JY_BBI ON tb_JY_BBI.INNERCODE=tb_JY_BC.INNERCODE LEFT JOIN tb_JY_BS ON tb_JY_BS.MAINCODE=tb_JY_BC.MAINCODE)," \
    #         "tb_CM /*常量表：计息方式,compoundmethod*/ AS (SELECT DISTINCT * FROM CT_SYSTEMCONST WHERE LB='1213')," \
    #         "tb_IPM /*常量表：付息方式,interestpaymentmethod*/ AS (SELECT DISTINCT * FROM CT_SYSTEMCONST WHERE LB='1168')," \
    #         "tb_SM /*常量表：债券发行场所,securitymarket*/ AS (SELECT DISTINCT * FROM CT_SYSTEMCONST WHERE LB='201')," \
    #         "tb_BIC /*债券信息表，table_bondinfoconsolidated*/ AS ( SELECT tb_JY_BIC.INNERCODE AS 债券内码,tb_JY_BIC.MAINCODE AS 债券主码,tb_JY_BIC.债券代码,tb_JY_BIC.债券简称,tb_JY_BIC.债券全称,tb_JY_BIC.发行人名称,tb_JY_BIC.担保人名称,tb_JY_BIC.起息日期,tb_JY_BIC.到期日期,tb_JY_BIC.债券期限,tb_JY_BIC.票面利率,tb_CM.MS AS 计息方式,tb_IPM.MS AS 付息方式,tb_JY_BIC.付息频率,tb_JY_BIC.债券发行价格,tb_JY_BIC.特殊条款,tb_JY_BIC.债券类型一级,tb_JY_BIC.债券类型二级,tb_SM.MS AS 发行场所,tb_JY_BIC.市场存量,tb_CB.中债剩余期限,tb_CB.中债收益率,tb_CB.中债全价,tb_CB.中债净价,tb_CB.中债应计利息,tb_CB.中债修正久期,tb_CB.中债修正凸性,tb_CB.中债行权类型代码,tb_CB.中债可信度代码,tb_CS.中证收益率,tb_CS.中证全价,tb_CS.中证净价,tb_CS.中证应计利息,tb_CS.中证修正久期,tb_CS.中证修正凸性 " \
    #         "FROM tb_JY_BIC LEFT JOIN tb_CB ON tb_CB.INNERCODE = tb_JY_BIC.INNERCODE LEFT JOIN tb_CS ON tb_JY_BIC.INNERCODE=tb_CS.INNERCODE " \
    #         "LEFT JOIN tb_CM ON tb_CM.DM=tb_JY_BIC.计息方式代码 LEFT JOIN tb_IPM ON tb_IPM.DM=tb_JY_BIC.付息方式代码 LEFT JOIN tb_SM on tb_SM.DM=tb_JY_BIC.发行场所代码 WHERE tb_JY_BIC.到期日期>=date'" + t0 + "') " \
    #         "SELECT * FROM tb_BIC WHERE 债券代码 IS NOT NULL AND 发行场所 IS NOT NULL AND 中债可信度代码!=1"
    #     return q
    #
    # def __preset_query_2(self,startdate,enddate):
    #     #区间存量
    #     q="WITH tb_CB /*中债，table_chinabond*/ AS ( SELECT DISTINCT INNERCODE FROM JYDB.BOND_CBVALUATIONALL WHERE ENDDATE>=date'" + startdate + "' AND ENDDATE<=date'" + enddate + "' )," \
    #       "tb_JY_BC /*聚源，table_JY_bondcode*/ AS (SELECT MAINCODE,SECUCODE AS 债券代码,SECUABBR AS 债券简称,SECUMARKET AS 发行场所代码,BONDTYPELEVEL1DESC AS 债券类型一级,BONDTYPELEVEL2DESC AS 债券类型二级 FROM JYDB.BOND_CODE)," \
    #       "tb_JY_BBI /*聚源，table_JY_bondbasicinfo*/ AS (SELECT MAINCODE,INNERCODE,BONDFULLNAME AS 债券全称,OPTIONTYPE AS 特殊条款,ISSUER AS 发行人名称,GUARANTOR AS 担保人名称,PARVALUE AS 债券初始面值,COMPOUNDMETHOD AS 计息方式代码,INTPAYMENTMETHOD AS 付息方式代码,COUPONRATE AS 票面利率,PAYINTERESTEFFENCY AS 付息频率,VALUEDATE AS 起息日期,ENDDATE AS 到期日期,MATURITY AS 债券期限 FROM JYDB.BOND_BASICINFO)," \
    #       "tb_JY_BIC /*聚源，table_JY_bondinfoconsolidated*/ AS (SELECT tb_JY_BBI.*,tb_JY_BC.债券代码,tb_JY_BC.债券简称,tb_JY_BC.发行场所代码,tb_JY_BC.债券类型一级,tb_JY_BC.债券类型二级 FROM tb_JY_BBI LEFT JOIN tb_JY_BC ON tb_JY_BBI.MAINCODE=tb_JY_BC.MAINCODE)," \
    #       "tb_CM /*常量表：计息方式,compoundmethod*/ AS (SELECT DISTINCT * FROM CT_SYSTEMCONST WHERE LB='1213')," \
    #       "tb_IPM /*常量表：付息方式,interestpaymentmethod*/ AS (SELECT DISTINCT * FROM CT_SYSTEMCONST WHERE LB='1168')," \
    #       "tb_SM /*常量表：债券发行场所,securitymarket*/ AS (SELECT DISTINCT * FROM CT_SYSTEMCONST WHERE LB='201')," \
    #       "tb_BIC /*债券信息表，table_bondinfoconsolidated*/ AS " \
    #       "( SELECT tb_CB.INNERCODE AS 债券内码,tb_JY_BIC.债券代码,tb_JY_BIC.债券简称,tb_JY_BIC.债券全称,tb_JY_BIC.发行人名称,tb_JY_BIC.担保人名称,tb_JY_BIC.起息日期,tb_JY_BIC.到期日期,tb_JY_BIC.债券期限,tb_JY_BIC.票面利率,tb_CM.MS AS 计息方式,tb_IPM.MS AS 付息方式,tb_JY_BIC.付息频率,tb_JY_BIC.债券初始面值,tb_JY_BIC.特殊条款,tb_JY_BIC.债券类型一级,tb_JY_BIC.债券类型二级,tb_SM.MS AS 发行场所 " \
    #       "FROM tb_CB LEFT JOIN tb_JY_BIC ON tb_CB.INNERCODE = tb_JY_BIC.INNERCODE " \
    #       "LEFT JOIN tb_CM ON tb_CM.DM=tb_JY_BIC.计息方式代码 LEFT JOIN tb_IPM ON tb_IPM.DM=tb_JY_BIC.付息方式代码 LEFT JOIN tb_SM on tb_SM.DM=tb_JY_BIC.发行场所代码) " \
    #       "SELECT * FROM tb_BIC WHERE 债券代码 IS NOT NULL"
    #     return q
    #
    # def __preset_query_3(self):
    #     #至今历史存量
    #     q="WITH " \
    #       "tb_JY_BC /*聚源，table_JY_bondcode*/ AS (SELECT MAINCODE,INNERCODE,SECUCODE AS 债券代码,SECUABBR AS 债券简称,CHINAME AS 债券全称,SECUMARKET AS 发行场所代码,BONDTYPELEVEL1DESC AS 债券类型一级,BONDTYPELEVEL2DESC AS 债券类型二级, ISSUER AS 发行人名称 FROM JYDB.BOND_CODE), " \
    #       "tb_JY_BBI /*聚源，table_JY_bondbasicinfo*/ AS (SELECT MAINCODE,INNERCODE,BONDFULLNAME AS 债券全称,OPTIONTYPE AS 特殊条款,ISSUER AS 发行人名称,GUARANTOR AS 担保人名称,COMPOUNDMETHOD AS 计息方式代码,INTPAYMENTMETHOD AS 付息方式代码,COUPONRATE AS 票面利率,PAYINTERESTEFFENCY AS 付息频率,VALUEDATE AS 起息日期,ENDDATE AS 到期日期,MATURITY AS 债券期限,ISSUEPRICE AS 债券发行价格 FROM JYDB.BOND_BASICINFO)," \
    #       "tb_JY_BIC /*聚源，table_JY_bondinfoconsolidated*/ AS (SELECT tb_JY_BC.*,tb_JY_BBI.特殊条款,tb_JY_BBI.担保人名称,tb_JY_BBI.计息方式代码,tb_JY_BBI.付息方式代码,tb_JY_BBI.票面利率,tb_JY_BBI.付息频率,tb_JY_BBI.起息日期,tb_JY_BBI.到期日期,tb_JY_BBI.债券期限,tb_JY_BBI.债券发行价格 FROM tb_JY_BC LEFT JOIN tb_JY_BBI ON tb_JY_BBI.INNERCODE=tb_JY_BC.INNERCODE)," \
    #       "tb_CM /*常量表：计息方式,compoundmethod*/ AS (SELECT DISTINCT * FROM CT_SYSTEMCONST WHERE LB='1213')," \
    #       "tb_IPM /*常量表：付息方式,interestpaymentmethod*/ AS (SELECT DISTINCT * FROM CT_SYSTEMCONST WHERE LB='1168')," \
    #       "tb_SM /*常量表：债券发行场所,securitymarket*/ AS (SELECT DISTINCT * FROM CT_SYSTEMCONST WHERE LB='201')," \
    #       "tb_BIC /*债券信息表，table_bondinfoconsolidated*/ AS " \
    #       "( SELECT tb_JY_BIC.INNERCODE AS 债券内码,tb_JY_BIC.MAINCODE AS 债券主码,tb_JY_BIC.债券代码,tb_JY_BIC.债券简称,tb_JY_BIC.债券全称,tb_JY_BIC.发行人名称,tb_JY_BIC.担保人名称,tb_JY_BIC.起息日期,tb_JY_BIC.到期日期,tb_JY_BIC.债券期限,tb_JY_BIC.票面利率,tb_CM.MS AS 计息方式,tb_IPM.MS AS 付息方式,tb_JY_BIC.付息频率,tb_JY_BIC.债券发行价格,tb_JY_BIC.特殊条款,tb_JY_BIC.债券类型一级,tb_JY_BIC.债券类型二级,tb_SM.MS AS 发行场所 " \
    #       "FROM tb_JY_BIC LEFT JOIN tb_CM ON tb_CM.DM=tb_JY_BIC.计息方式代码 LEFT JOIN tb_IPM ON tb_IPM.DM=tb_JY_BIC.付息方式代码 LEFT JOIN tb_SM on tb_SM.DM=tb_JY_BIC.发行场所代码) " \
    #       "SELECT * FROM tb_BIC WHERE 债券代码 IS NOT NULL AND 发行场所 IS NOT NULL"
    #     return q
    #
    # def preset_dicts(self):
    #     self.option_type={1:'行权',
    #                       2:'不行权'}
    #     self.market_type={'银行间债券市场':'.IB',
    #                       '上海证券交易所':'.SH',
    #                       '深圳证券交易所':'.SZ',
    #                       '柜台交易市场':'.BC'}
    #
    # def dict_curves(self):
    #     q="SELECT DISTINCT MS AS 曲线名称,DM AS 曲线代码 " \
    #       "FROM CT_SYSTEMCONST WHERE LB='1448'"
    #     dict=self.sql_query(q)
    #     return dict
    #
    # def data_market_daily(self,t):
    #     q=self.__preset_query_1(t)
    #     tb=self.sql_query(q)
    #     tb['万德代码'] = tb['债券代码'] + tb['发行场所'].map(self.market_type)  # 补万德代码
    #     tb=tb.loc[tb['发行场所'].isin(['银行间债券市场','上海证券交易所','深圳证券交易所']),:]
    #     return tb
    #
    # def data_market_period(self,startdate,enddate):
    #     q=self.__preset_query_2(startdate,enddate)
    #     tb=self.sql_query(q)
    #     tb['万德代码'] = tb['债券代码'] + tb['发行场所'].map(self.market_type)  # 补万德代码
    #     tb=tb.loc[tb['发行场所'].isin(['银行间债券市场','上海证券交易所','深圳证券交易所']),:]
    #     return tb
    #
    # def data_market_historical(self):
    #     q = self.__preset_query_3()
    #     tb = self.sql_query(q)
    #     tb['万德代码'] = tb['债券代码'] + tb['发行场所'].map(self.market_type)  # 补万德代码
    #     tb=tb.loc[tb['发行场所'].isin(['银行间债券市场','上海证券交易所','深圳证券交易所']),:]
    #     return tb
    #
    # def curve_yield(self,curvecode,ttm,t0):       # 取出给定剩余期限的曲线数据
    #     q="SELECT YIELD FROM JYDB.BOND_CBYIELDCURVEALL " \
    #       "WHERE CURVECODE=" + curvecode + " AND YIELDTYPECODE=1 " \
    #       "AND ENDDATE=date'" + t0 + "' " \
    #       "AND STEPTYPECODE=1 " \
    #       "AND YEARSTOMATURITY=" + ttm
    #     curve_yield=self.sql_query(q)
    #     return curve_yield
    #
    # def curve_yield_all(self, curvecode, t0):     # 取出所有剩余期限的曲线数据
    #     q = '''SELECT ENDDATE,YEARSTOMATURITY,YIELD FROM JYDB.BOND_CBYIELDCURVEALL WHERE CURVECODE = %s AND YIELDTYPECODE = 1 AND ENDDATE = DATE '%s' AND STEPTYPECODE =1'''%(curvecode, t0)
    #     curve_yield = self.sql_query(q)
    #
    #     return curve_yield
    #
    # def curve_yield_interval(self, curve_list, startDate, endDate):
    #     if type(curve_list) != list:
    #         curve_list = [curve_list]
    #     curves = ','.join(str(x) for x in curve_list)
    #     q = '''SELECT ENDDATE,CURVECODE,YEARSTOMATURITY,YIELD FROM JYDB.BOND_CBYIELDCURVEALL WHERE CURVECODE in (%s) AND YIELDTYPECODE = 1 AND ENDDATE >= DATE '%s' AND ENDDATE <= DATE '%s' AND STEPTYPECODE =1'''%(curves, startDate, endDate)
    #     curve_yield = self.sql_query(q)
    #
    #     return curve_yield
    #
    # def curve_yield_interval_stock(self, curve_list, startDate, endDate):
    #     if type(curve_list) != list:
    #         curve_list = [curve_list]
    #     curves = ','.join('\'' + str(x.split('.')[0]) + '\'' for x in curve_list)   # 剔除市场后缀
    #     q = '''
    #         SELECT innercode, t2.secucode, t2.secumarket, t2.secuabbr, tradingday, prevcloseprice, closeprice, turnovervolume, turnovervalue
    #         FROM QT_INDEXQUOTE t1
    #         RIGHT JOIN ( SELECT innercode, secucode, secuabbr, secumarket FROM secumain WHERE secucode IN (%s) AND secumarket IS NOT NULL ) t2 USING ( innercode )
    #         WHERE tradingday >= date '%s'  AND tradingday <= date '%s'
    #         ORDER BY innercode, tradingday
    #     '''%(curves, startDate, endDate)
    #     curve_yield = self.sql_query(q)
    #
    #     return curve_yield
    #
    # def bond_yield_interval(self, sec_list, startDate, endDate):     # 取出个券的YTM和Duration数据（可信度代码为“推荐”），给定参数为债券wind代码、起始日、截止日
    #     if type(sec_list) != list:
    #         sec_list = [sec_list]
    #     secs = ','.join('\'' + str(x.split('.')[0]) + '\'' for x in sec_list)
    #
    #     q = '''SELECT	enddate, t2.secucode, vpyield, vpaduration, VPConvexity, CredibilityCode, CredibilityDesc, YieldCode
    #             FROM	bond_cbvaluationall t1	LEFT JOIN bond_code t2 ON t1.innercode = t2.innercode
    #             WHERE	t2.secucode IN (%s)	AND CredibilityCode = 1 AND ENDDATE >= DATE '%s' AND ENDDATE <= DATE '%s'
    #             ORDER BY	enddate'''%(secs, startDate, endDate)
    #     sec_data = self.sql_query(q)
    #
    #     return sec_data
    #
    # def bond_yield(self, sec_list, givenDate):     # 取出个券的YTM和Duration数据（可信度代码为“推荐”），给定参数为债券wind代码、给定日期; YieldCode（1行权2到期）
    #     if type(sec_list) != list:
    #         sec_list = [sec_list]
    #     sec_data = pd.DataFrame()
    #     loop_num = int(np.ceil(len(sec_list)/1000))
    #     for i in range(loop_num):
    #         secs_temp = sec_list[i*1000: min((i+1)*1000, len(sec_list))]
    #         secs_temp_str = ','.join('\'' + str(x.split('.')[0]) + '\'' for x in secs_temp)
    #         q = '''SELECT	enddate, t2.secucode, t2.secumarket,  vpyield, vpaduration, VPConvexity, CredibilityCode, CredibilityDesc, YieldCode
    #                 FROM	bond_cbvaluationall t1	LEFT JOIN bond_code t2 ON t1.innercode = t2.innercode
    #                 WHERE	t2.secucode IN (%s)	AND CredibilityCode = 1 AND ENDDATE = DATE '%s'
    #             '''%(secs_temp_str, givenDate)
    #         sec_data_temp = self.sql_query(q)
    #         sec_data = pd.concat([sec_data, sec_data_temp])
    #
    #     return sec_data
    #
    # def bond_repurchase(self, sec_list, option_type=201):                  # optype为201代表“回售权”
    #     if type(sec_list) != list:
    #         sec_list = [sec_list]
    #     sec_data = pd.DataFrame()
    #     loop_num = int(np.ceil(len(sec_list)/1000))
    #     for i in range(loop_num):
    #         secs_temp = sec_list[i*1000: min((i+1)*1000, len(sec_list))]
    #         secs_temp_str = ','.join('\'' + str(x.split('.')[0]) + '\'' for x in secs_temp)
    #         q = '''select t2.secucode, t2.secumarket, optype, ExpectedExerciseDate
    #                 from BOND_CALLPUTINFO t1 LEFT JOIN bond_code t2 ON t1.maincode = t2.innercode
    #                 WHERE t2.secucode IN (%s) and optype = %d
    #         '''%(secs_temp_str, option_type)
    #         sec_data_temp = self.sql_query(q)
    #         sec_data = pd.concat([sec_data, sec_data_temp])
    #
    #     return sec_data
    #
    # def stock_evalIndices(self, sec_list, givenDate):
    #     if type(sec_list) != list:
    #         sec_list = [sec_list]
    #     sec_data = pd.DataFrame()
    #     loop_num = int(np.ceil(len(sec_list)/1000))
    #     for i in range(loop_num):
    #         secs_temp = sec_list[i*1000: min((i+1)*1000, len(sec_list))]
    #         secs_temp_str = ','.join('\'' + str(x.split('.')[0]) + '\'' for x in secs_temp)
    #         q = '''select t1.tradingday, secucode, SecuMarket, ListedSector, totalMV, pe, pb
    #                 from LC_DINDICESFORVALUATION t1 left join secumain t2 on t1.innercode = t2.innercode
    #                 WHERE t2.secucode IN (%s) and t1.tradingday = DATE '%s'
    #             '''%(secs_temp_str, givenDate)
    #         sec_data_temp = self.sql_query(q)
    #         sec_data = pd.concat([sec_data, sec_data_temp])
    #
    #     return sec_data
    #
    # def stockMarket_mv(self, givenDate, in_type=[1,2]):  # 默认只取主板和中小板的股票数据
    #     type_list = ','.join(str(x) for x in in_type)
    #     q = '''select t1.tradingday, SecuCode, SecuMarket, ListedSector, totalMV, pe, pb
    #             from LC_DINDICESFORVALUATION t1 left join secumain t2 on t1.innercode = t2.innercode
    #             where t1.tradingday = date'%s' and ListedSector in (%s)
    #         '''%(givenDate, type_list)
    #     sec_data = self.sql_query(q)
    #
    #     return sec_data


# class HoldTrade_Query(object):
#     def __init__(self):
#         self.conn_oracle = cx.connect('risk/risk@10.198.60.77/RISK', encoding = "UTF-8", nencoding = "UTF-8")
#
#     def close_query(self):
#         self.conn_oracle.close()
#
#     def get_fundvaluation(self,fundname,begindate,enddate):
#         '''
#         获取估值表
#         fundtype:1为基金，2为专户
#         fundkind2: fundtype为2时：2为债基，3为股基，4位指数，5为货基。fundtype为1时：1为股，2为债，3为子公司
#         日期格式为str，例如“2020-01-01”
#          t.l_fundtype="+fundtype+" and t.l_fundkind2="+fundkind2+"
#         '''
#         fundname = '(' + ','.join('\'' + x + '\'' for x in fundname) + ')'
#         sql_oracle = " select * from TREP_VALUATION t where t.c_fullname in "+fundname+" and t.d_date >= date'"+begindate+"' and t.d_date <= date'"+enddate+"'"
#         trade_detail = pd.read_sql(sql_oracle,self.conn_oracle)
#         return trade_detail
#
#     def get_fundtrade(self,fundname,begindate,enddate):
#         #交易表，传入基金全称C_FULLNAME
#         fundname = '(' + ','.join('\'' + x + '\'' for x in fundname) + ')'
#         trading_mapping={'DATA_DATE':'D_DATE','FUND_NAME':'C_FUNDNAME_O32','SEC_CODE':'证券代码','SEC_NAME':'证券简称','ENTRUST_DIRECTION':'委托方向','REDEEM_RATE':'回购利率','DEAL_VOLUME':'成交数量','DEAL_NET_PRICE':'成交净价','DEAL_FULL_PRICE':'成交全价','BOND_NET_PRICE':'首次净价价格','BOND_FULL_PRICE':'首次全价价格','DEAL_BALANCE':'成交金额','SETTLE_SPEED':'清算速度','RIVAL_NAME':'交易对手','OPERATOR_NAME':'操作人','BUSI_CLASS':'业务分类','TRUST_ORG':'托管机构','REDEEM_DATE':'法定购回日','SETTLE_DATE':'购回交割日','SOURCE_NO':'市场类型'}
#         begindate=begindate.replace("-","")
#         enddate=enddate.replace("-","")
#         sql_oracle = " select t2.C_FULLNAME, t1.* from TREP_TRADEDETAIL t1 LEFT JOIN trep_fund_info t2 ON t1.fund_name = t2.C_FUNDNAME where t2.C_FULLNAME in "+fundname+" and t1.data_date>="+begindate+" and t1.data_date<="+enddate
#         trade_detail = pd.read_sql(sql_oracle,self.conn_oracle)
#         trade_detail=trade_detail.rename(columns=trading_mapping)#.drop_duplicates()
#         return trade_detail
#
#     def get_voucher(self,fundname,begindate,enddate):
#         #凭证表
#         fundname = '(' + ','.join('\'' + x + '\'' for x in fundname) + ')'
#         sql_oracle = " select * from TREP_VOUCHERS t where t.fullname in "+fundname+" and t.c_subcode like '6%' and t.d_vchdate >= date'"+begindate+"' and t.d_vchdate <= date'"+enddate+"'"
#         trade_detail = pd.read_sql(sql_oracle,self.conn_oracle)
#         return trade_detail
#
#     def get_yieldcurve(self):
#         #凭证表
#         sql_oracle = " select * from yield_curve"
#         trade_detail = pd.read_sql(sql_oracle,self.conn_oracle)
#         return trade_detail
#
#     def get_bondinfo(self):
#         #债券静态信息表
#
#         sql_oracle = " select t.windcode as 万德代码,t.ischengtou as 是否城投债, t.couponrate_wind as 票面利率万德,t.callbackdate as 赎回日期,"\
#         "t.PAYMENTMETHOD as 付息方式, t.FREQUENCY as 付息频率, t.BEGINDATE as 起息日期, t.ENDDATE as 到期日期, t.SPECIALLAWS_WIND as 特殊条款万德,"\
#         "t.BONDTYPE_2_WIND AS Wind债券二级分类,t.HISRATE AS 市场历史隐含评级中债,t.RATETYPE_WIND AS 利率类型万德,t.COUPONENDDATE_WIND AS 计息截止日万德,t.ISPIN AS 是否提前偿还 from BONDBASICINFO_JY_WIND t"
#
#         #sql_oracle = " select t.windcode as 万德代码,t.ischengtou as 是否城投债, t.couponrate_wind ,t.callbackdate as 赎回日期 from BONDBASICINFO_JY_WIND t"
#
#         trade_detail = pd.read_sql(sql_oracle,self.conn_oracle)
#         return trade_detail

# '''
# dq=Data_Query() #创建对象
# tb=dq.market_data('2019-10-18') #全市场数据
# tb['中债行权类型'] = tb['中债行权类型代码'].map(dq.option_type)  # 补中债收益率行权类型
# #曲线收益率对应剩余期限收益率
# #curvecode见dict_curves映射表，ttm为剩余期限，保留一位小数
# y=dq.curve_yield(curvecode='10',ttm='2.2',t0='2019-10-18')
# dq.close_query()
# '''