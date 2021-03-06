import logging
import os
from datetime import date, timedelta
from typing import Union, List

import colorama
import numpy as np
import pandas as pd

from sz.stock_data.stock_data import StockData
from sz.stock_data.toolbox.data_provider import ts_code, ts_pro_api
from sz.stock_data.toolbox.datetime import ts_date, to_datetime64
from sz.stock_data.toolbox.helper import mtime_of_file
from sz.stock_data.toolbox.limiter import ts_rate_limiter


class Top10Holders(object):

    def __init__(self, data_dir: str, stock_code: str):
        self.data_dir = data_dir
        self.stock_code = ts_code(stock_code)
        self.dataframe: Union[pd.DataFrame, None] = None

    def file_path(self) -> str:
        """
        返回保存数据的csv文件路径
        :return:
        """
        return os.path.join(self.data_dir, 'stocks', self.stock_code, 'top10_holders.csv')

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

        mtime = mtime_of_file(self.file_path())
        if mtime < StockData().trade_calendar.latest_trade_day():
            return True
        else:
            return False

    def load(self) -> pd.DataFrame:
        if os.path.exists(self.file_path()):
            self.dataframe = pd.read_csv(
                filepath_or_buffer = self.file_path(),
                parse_dates = ['ann_date', 'end_date'],
                dtype = {
                    'hold_amount': np.float64,
                    'hold_ratio': np.float64
                }
            )
        else:
            logging.warning(colorama.Fore.RED + '%s 本地前十大股东数据文件不存在,请及时下载更新' % self.stock_code)
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
            return self.dataframe.iloc[-1].loc['end_date'].date() + timedelta(days = 1)

    @ts_rate_limiter
    def ts_top10_holders(self, start_date: date, end_date: date) -> pd.DataFrame:
        df: pd.DataFrame = ts_pro_api().top10_holders(
            ts_code = self.stock_code,
            start_date = ts_date(start_date),
            end_date = ts_date(end_date)
        )
        if not df.empty:
            df['ann_date'] = df['ann_date'].apply(lambda x: to_datetime64(x))
            df['end_date'] = df['end_date'].apply(lambda x: to_datetime64(x))
            df.sort_values(by = 'end_date', inplace = True)
            logging.info(colorama.Fore.YELLOW + '下载 %s 前十大股东数据: %s -- %s, 共 %s 条' % (self.stock_code, start_date, end_date, df.shape[0]))
        else:
            logging.info(colorama.Fore.YELLOW + '%s 前十大股东数据: %s -- %s 无数据' % (self.stock_code, start_date, end_date))
        return df

    def update(self):
        self._setup_dir_()
        self.prepare()

        if self.should_update():
            start_date: date = max(self.start_date(), StockData().stock_basic.list_date_of(self.stock_code))
            end_date: date = start_date
            last_trade_day = StockData().trade_calendar.latest_trade_day()
            df_list: List[pd.DataFrame] = [self.dataframe]
            step_days = timedelta(days = 3650)

            while start_date <= last_trade_day:
                end_date = start_date + step_days
                end_date = min(end_date, last_trade_day)
                df = self.ts_top10_holders(start_date = start_date, end_date = end_date)
                df_list.append(df)
                start_date = end_date + timedelta(days = 1)

            self.dataframe = pd.concat(df_list).drop_duplicates()
            self.dataframe.sort_values(by = 'end_date', inplace = True)

            self.dataframe.to_csv(
                path_or_buf = self.file_path(),
                index = False
            )

            logging.info(
                colorama.Fore.YELLOW + '%s 前十大股东数据更新到: %s path: %s' % (
                    self.stock_code, str(end_date), self.file_path()))
        else:
            logging.info(colorama.Fore.BLUE + '%s 前十大股东数据无须更新' % self.stock_code)
