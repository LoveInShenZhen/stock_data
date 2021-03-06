import logging
import os
from datetime import date, timedelta
from typing import Union, List

import colorama
import numpy as np
import pandas as pd

from sz.stock_data.stock_data import StockData
from sz.stock_data.toolbox.data_provider import ts_code, ts_pro_api
from sz.stock_data.toolbox.datetime import ts_date
from sz.stock_data.toolbox.helper import need_update_by_trade_date
from sz.stock_data.toolbox.limiter import ts_rate_limiter


class AdjFactor(object):

    def __init__(self, data_dir: str, stock_code: str):
        self.data_dir = data_dir
        self.stock_code = ts_code(stock_code)
        self.dataframe: Union[pd.DataFrame, None] = None

    def file_path(self) -> str:
        """
        返回保存数据的csv文件路径
        :return:
        """
        return os.path.join(self.data_dir, 'stocks', self.stock_code, 'adj_factor.csv')

    def _setup_dir_(self):
        """
        初始化数据目录
        :return:
        """
        os.makedirs(os.path.dirname(self.file_path()), exist_ok = True)

    def should_update(self) -> bool:
        """
        如果文件不存在, 直接返回 True
        :return:
        """
        if not os.path.exists(self.file_path()):
            return True

        self.prepare()

        return need_update_by_trade_date(self.dataframe, 'trade_date')

    def load(self) -> pd.DataFrame:
        if os.path.exists(self.file_path()):
            self.dataframe = pd.read_csv(
                filepath_or_buffer = self.file_path(),
                parse_dates = ['trade_date'],
                dtype = {
                    'adj_factor': np.float64
                }
            )
            self.dataframe.set_index(keys = 'trade_date', drop = False, inplace = True)
            self.dataframe.sort_index(inplace = True)
        else:
            logging.warning(colorama.Fore.RED + '%s 本地复权因子数据文件不存在,请及时下载更新' % self.stock_code)
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
            return StockData().stock_basic.list_date_of(self.stock_code)
        else:
            return self.dataframe.iloc[-1].loc['trade_date'].date() + timedelta(days = 1)

    def update(self):
        self._setup_dir_()
        self.prepare()

        if self.should_update():
            start_date: date = max(self.start_date(), StockData().stock_basic.list_date_of(self.stock_code))
            end_date: date = start_date
            last_trade_day = StockData().trade_calendar.latest_trade_day()
            df_list: List[pd.DataFrame] = [self.dataframe]
            step_days = timedelta(days = 3000)

            while start_date <= last_trade_day:
                end_date = start_date + step_days
                end_date = min(end_date, last_trade_day)
                df = self.ts_adj_factor(start_date = start_date, end_date = end_date)
                df_list.append(df)
                start_date = end_date + timedelta(days = 1)

            self.dataframe = pd.concat(df_list).drop_duplicates()
            self.dataframe.sort_index(inplace = True)

            self.dataframe.to_csv(
                path_or_buf = self.file_path(),
                index = False
            )

            logging.info(
                colorama.Fore.YELLOW + '%s 复权因子数据更新到: %s path: %s' % (
                    self.stock_code, str(end_date), self.file_path()))
        else:
            logging.info(colorama.Fore.BLUE + '%s 复权因子数据无须更新' % self.stock_code)

    @ts_rate_limiter
    def ts_adj_factor(self, start_date: date, end_date: date) -> pd.DataFrame:
        df: pd.DataFrame = ts_pro_api().adj_factor(
            ts_code = self.stock_code,
            start_date = ts_date(start_date),
            end_date = ts_date(end_date)
        )
        df['trade_date'] = pd.to_datetime(df['trade_date'], format = '%Y%m%d')
        df.set_index(keys = 'trade_date', drop = False, inplace = True)
        df.sort_index(inplace = True)
        logging.info(colorama.Fore.YELLOW + '下载 %s 复权因子数据: %s - %s 共 %s 条' % (self.stock_code, start_date, end_date, df.shape[0]))
        return df
