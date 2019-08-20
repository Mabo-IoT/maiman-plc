# -*- coding: utf-8 -*-
import random
import time
from logging import getLogger

import snap7
from snap7 import util
from snap7.snap7exceptions import Snap7Exception

from Doctopus.Doctopus_main import Check, Handler

log = getLogger('Doctopus.plugins')


class MyCheck(Check):
    def __init__(self, configuration):
        super(MyCheck, self).__init__(configuration=configuration)
        self.conf = conf = configuration
        self.check_conf = check_conf = conf['user_conf']['check']

        # connection info
        address = check_conf.get('address', '127.0.0.1')
        rack = check_conf.get('rack', 0)
        slot = check_conf.get('slot', 0)
        tcpport = check_conf.get('tcpport', 102)
        self.db_number = check_conf.get('db_number', 100)

        self.sep = check_conf.get('sep', '#')
        self.groups = check_conf.get('groups', 1)

        # Real
        self.real_points = check_conf['Real'].get('points', [0])
        self.real_names = check_conf['Real'].get('names', [])
        self.real_data_size = check_conf['Real'].get('data_size', 28)

        # Bool
        self.bool_points = check_conf['Bool'].get('points', [28])
        self.bool_names = check_conf['Bool'].get('names', [])
        self.bool_data_size = check_conf['Bool'].get('data_size', 1)

        # create client
        self.client = snap7.client.Client()
        try:
            self.client.connect(address, rack, slot, tcpport)
        except Snap7Exception as err:
            log.error(err)

    def get_real(self):
        """get Real type data
        :returns: real_list: list

        """
        real_list = []

        for point in self.real_points:
            try:
                data = self.client.db_read(db_number=self.db_number, start=point,
                                           size=self.real_data_size)
            except Exception as err:
                log.warn(err)
            else:
                for index in range(0, self.real_data_size-1, 4):
                    real_data = util.get_real(_bytearray=data, byte_index=index)
                    real_list.append(real_data)

        return real_list

    def get_bool(self):
        """get Bool type data
        :returns: bool_list: list

        """
        bool_list = []

        for point in self.bool_points:
            try:
                data = self.client.db_read(db_number=self.db_number, start=point,
                                           size=self.bool_data_size)
            except Exception as err:
                log.warn(err)
            else:
                for index in range(0, 2):
                    bool_data = util.get_bool(_bytearray=data, byte_index=0,
                                              bool_index=index)
                    bool_list.append(bool_data)

        return bool_list

    def process_data(self):
        """data processing
        :returns: data: dict

        """
        real_list = self.get_real()
        bool_list = self.get_bool()

        real_full_names = []
        bool_full_names = []
        for group in range(1, self.groups):
            for name in self.real_names:
                real_fullname = '{}{}{}'.format(group, self.sep, name)
                real_full_names.append(real_fullname)
            for name in self.bool_names:
                bool_fullname = '{}{}{}'.format(group, self.sep, name)
                bool_full_names.append(bool_fullname)

        real_dict = dict(zip(real_full_names, real_list))
        bool_dict = dict(zip(bool_full_names, bool_list))

        data = dict(real_dict, **bool_dict)

        return data

    def user_check(self):
        """

        :param command: user defined parameter.
        :return: the data you requested.
        """
        data = self.process_data()
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
