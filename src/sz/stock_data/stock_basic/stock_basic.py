import logging
import os
from datetime import date
from typing import List, Union

import colorama
import numpy as np
import pandas as pd
from pandas import Timestamp
from ratelimiter import RateLimiter
from sz.stock_data.toolbox.helper import need_update

from sz.stock_data.toolbox.data_provider import ts_pro_api


class StockBasic(object):

    def __init__(self, data_dir: str):
        self.data_dir: str = data_dir
        self.dataframe: Union[pd.DataFrame, None] = None

    def file_path(self) -> str:
        """
        返回保存交易日历的csv文件路径
        :return:
        """
        return os.path.join(self.data_dir, 'stock_basic', 'stock_basic.csv')

    def _setup_dir_(self):
        """
        初始化数据目录
        :return:
        """
        os.makedirs(os.path.dirname(self.file_path()), exist_ok = True)

    def update(self):
        self._setup_dir_()
        self.prepare()
        if self.should_update():
            df = self.ts_stock_basic()
            df.to_csv(
                path_or_buf = self.file_path(),
                index = False
            )

    def should_update(self) -> bool:
        """
        判断 stock_basic 数据是否需要更新.(更新频率: 每周更新)
        :return:
        """
        return need_update(self.file_path(), 7)

    def load(self) -> pd.DataFrame:
        if os.path.exists(self.file_path()):
            self.dataframe = pd.read_csv(
                filepath_or_buffer = self.file_path(),
                dtype = {'symbol': str},
                parse_dates = ['list_date', 'delist_date']
            )
            self.dataframe.set_index(keys = 'ts_code', drop = False, inplace = True)
            self.dataframe.sort_index(inplace = True)
        else:
            logging.warning(colorama.Fore.RED + 'StockBasic 本地数据文件不存在,请及时下载更新')
            self.dataframe = pd.DataFrame()

        return self.dataframe

    def prepare(self):
        if self.dataframe is None:
            self.load()

    def list_date_of(self, ts_code: str) -> date:
        """
        返回指定证券的上市日期
        :param ts_code:
        :return:
        """
        self.prepare()
        return self.dataframe.loc[ts_code].loc['list_date'].date()

    @staticmethod
    def ts_stock_basic() -> pd.DataFrame:
        df: pd.DataFrame = ts_pro_api().stock_basic(
            exchange = '',
            list_status = 'L',
            fields = 'ts_code,symbol,name,area,industry,fullname,market,exchange,list_status,list_date,delist_date,is_hs'
        )
        df['list_date'] = pd.to_datetime(df['list_date'], format = '%Y%m%d')
        df['delist_date'] = pd.to_datetime(df['delist_date'], format = '%Y%m%d')
        logging.info(colorama.Fore.YELLOW + '下载股票列表基础信息数据')
        return df
