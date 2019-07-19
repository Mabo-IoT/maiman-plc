# -*- coding: utf-8 -*-
import time
import random
from logging import getLogger

from Doctopus.Doctopus_main import Check, Handler

log = getLogger('Doctopus.plugins')


class MyCheck(Check):
    def __init__(self, configuration):
        super(MyCheck, self).__init__(configuration=configuration)

    def user_check(self):
        """

        :param command: user defined parameter.
        :return: the data you requested.
        """
        data = {
            "1#_liquid_level": random.choice([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
            "1#_temperature": round(random.uniform(30, 70), 1),
            "1#_pressure": round(random.uniform(1, 5), 1),
            "1#_PH_value": round(random.uniform(3, 6.9), 1),
            "1#_conductivity": round(random.uniform(500, 1000), 1),
            "1#_medicinal_reserves": 100,
            "1#_instrument_life": 10000,
            "1#_dosing_pump_a_status": 1,
            "1#_dosing_pump_b_status": 1,
        }
        log.debug('%s', data)
        time.sleep(2)
        yield data


class MyHandler(Handler):
    def __init__(self, configuration):
        super(MyHandler, self).__init__(configuration=configuration)

    def user_handle(self, raw_data):
        """
        用户须输出一个dict，可以填写一下键值，也可以不填写
        timestamp， 从数据中处理得到的时间戳（整形?）
        tags, 根据数据得到的tag
        data_value 数据拼接形成的 list 或者 dict，如果为 list，则上层框架
         对 list 与 field_name_list 自动组合；如果为 dict，则不处理，认为该数据
         已经指定表名
        measurement 根据数据类型得到的 influxdb表名

        e.g:
        list:
        {'data_value':[list] , required
        'tags':[dict],        optional
        'table_name',[str]   optional
        'timestamp',int}      optional

        dict：
        {'data_value':{'fieldname': value} , required
        'tags':[dict],        optional
        'table_name',[str]   optional
        'timestamp',int}      optional

        :param raw_data:
        :return:
        """
        # exmple.
        # 数据经过处理之后生成 value_list
        log.debug('%s', raw_data)
        data_value_list = raw_data

        # tags = {'user_defined_tag': 'data_ralated_tag'}

        # user 可以在handle里自己按数据格式制定tags
        user_postprocessed = {'data_value': data_value_list,}
        yield user_postprocessed
