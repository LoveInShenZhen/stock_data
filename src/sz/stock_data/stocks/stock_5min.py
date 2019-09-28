import logging
import os
from datetime import date, timedelta
from typing import Union, List

import baostock as bao
import colorama
import numpy as np
import pandas as pd

from sz.stock_data.stock_data import StockData
from sz.stock_data.toolbox.data_provider import ts_code
from sz.stock_data.toolbox.helper import need_update_by_trade_date


class Stock5min(object):
    base_date = date(year = 2011, month = 1, day = 1)

    def __init__(self, data_dir: str, stock_code: str):
        self.data_dir = data_dir
        self.stock_code = ts_code(stock_code)
        self.dataframe: Union[pd.DataFrame, None] = None

    def file_path(self) -> str:
        """
        返回保存数据的csv文件路径
        :return:
        """
        return os.path.join(self.data_dir, 'stocks', self.stock_code, '5min.csv')

    def _setup_dir_(self):
        """
        初始化数据目录
        :return:
        """
        os.makedirs(os.path.dirname(self.file_path()), exist_ok = True)

    def should_update(self) -> bool:
        """
        如果数据文件的最后修改日期, 早于最近的一个交易日, 则需要更新数据
        如果文件不存在, 直接返回 True
        :return:
        """
        if not os.path.exists(self.file_path()):
            return True

        self.prepare()

        return need_update_by_trade_date(self.dataframe, 'date')

    def load(self) -> pd.DataFrame:
        if os.path.exists(self.file_path()):
            self.dataframe = pd.read_csv(
                filepath_or_buffer = self.file_path(),
                parse_dates = ['time', 'date'],
                dtype = {
                    'open': np.float64,
                    'high': np.float64,
                    'low': np.float64,
                    'close': np.float64,
                    'volume': np.float64,
                    'amount': np.float64
                }
            )
            self.dataframe.set_index(keys = 'time', drop = False, inplace = True)
            self.dataframe.sort_index(inplace = True)
        else:
            logging.warning(colorama.Fore.RED + '%s 本地 5min 线数据文件不存在,请及时下载更新' % self.stock_code)
            self.dataframe = pd.DataFrame()

        return self.dataframe

    def prepare(self):
        if self.dataframe is None:
            self.load()
        return self

    def start_date(self) -> date:
        """
        计算本次更新的起始日期
        :return:
        """
        self.prepare()

        if self.dataframe.empty:
            return self.base_date
        else:
            return self.dataframe.iloc[-1].loc['date'].date() + timedelta(days = 1)

    def update(self):
        self._setup_dir_()
        self.prepare()

        if self.should_update():
            start_date: date = max(self.start_date(), StockData().stock_basic.list_date_of(self.stock_code))
            end_date: date = start_date
            last_trade_day = StockData().trade_calendar.latest_trade_day()
            df_list: List[pd.DataFrame] = [self.dataframe]
            step_days = timedelta(days = 50)

            while start_date <= last_trade_day:
                end_date = start_date + step_days
                end_date = min(end_date, last_trade_day)
                rs = bao.query_history_k_data_plus(
                    code = self.stock_code,
                    start_date = str(start_date),
                    end_date = str(end_date),
                    frequency = '5',
                    fields = 'date,time,code,open,high,low,close,volume,amount,adjustflag',
                    adjustflag = '3'
                )
                df_5min = rs.get_data()
                if not df_5min.empty:
                    df_5min['date'] = pd.to_datetime(df_5min['date'], format = '%Y-%m-%d')
                    df_5min['time'] = df_5min['time'].apply(lambda x: pd.to_datetime(x[:-3], format = '%Y%m%d%H%M%S'))
                    df_5min['code'] = df_5min['code'].apply(lambda x: ts_code(x))
                    df_5min['open'] = df_5min['open'].astype(np.float64)
                    df_5min['high'] = df_5min['high'].astype(np.float64)
                    df_5min['low'] = df_5min['low'].astype(np.float64)
                    df_5min['close'] = df_5min['close'].astype(np.float64)
                    df_5min['volume'] = df_5min['volume'].astype(np.float64)
                    df_5min['amount'] = df_5min['amount'].astype(np.float64)
                    df_5min.set_index(keys = 'time', drop = False, inplace = True)
                    logging.debug(colorama.Fore.YELLOW + '下载 %s 5min 线数据, 从 %s 到 %s 共 %s 条' %
                                  (self.stock_code, start_date, end_date, df_5min.shape[0]))

                    df_list.append(df_5min)

                start_date = end_date + timedelta(days = 1)

            self.dataframe = pd.concat(df_list).drop_duplicates()
            self.dataframe.sort_index(inplace = True)

            self.dataframe.to_csv(
                path_or_buf = self.file_path(),
                index = False
            )

            logging.info(
                colorama.Fore.YELLOW + '%s 5min 线数据更新到: %s path: %s' % (
                    self.stock_code, str(end_date), self.file_path()))
        else:
            logging.info(colorama.Fore.BLUE + '%s 5min 线数据无须更新' % self.stock_code)
