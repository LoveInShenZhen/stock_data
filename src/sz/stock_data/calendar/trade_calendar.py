import logging
import os
from datetime import date
from typing import List, Union

import colorama
import numpy as np
import pandas as pd
from pandas import Timestamp

from sz.stock_data.toolbox.data_provider import ts_pro_api
from sz.stock_data.toolbox.limiter import ts_rate_limiter


class TradeCalendar(object):

    def __init__(self, data_dir: str):
        """
        沪深A股交易日历对象, 用于获取和更新本地数据, 提供相关查询接口
        :param data_dir:
        """
        self.data_dir = data_dir
        self.dataframe: Union[pd.DataFrame, None] = None

    def _setup_dir_(self):
        """
        初始化数据目录
        :return:
        """
        os.makedirs(os.path.dirname(self.file_path()), exist_ok = True)

    def update(self):
        self._setup_dir_()
        self.load()
        df_list: List[pd.DataFrame] = [self.dataframe]

        for year in range(2000, Timestamp.today().year + 1):
            check_date = '%s-01-01' % year
            df_tmp: pd.DataFrame = self.dataframe.loc[check_date:check_date]
            if df_tmp.empty:
                # 说明 year 对应年份的日历不在本地数据文件中
                df_list.append(self.ts_trade_cal(
                    start_date = '%s0101' % year,
                    end_date = '%s1231' % year
                ))

        if len(df_list) > 1:
            self.dataframe: pd.DataFrame = pd.concat(df_list).drop_duplicates(subset = 'cal_date').sort_index()
            self.dataframe.to_csv(
                path_or_buf = self.file_path(),
                index = False
            )

    def file_path(self) -> str:
        """
        返回保存交易日历的csv文件路径
        :return:
        """
        return os.path.join(self.data_dir, 'trade_calendar', 'trade_calendar.csv')

    def load(self) -> pd.DataFrame:
        """
        从数据文件加载交易日历
        :return:
        """
        if os.path.exists(self.file_path()):
            self.dataframe = pd.read_csv(
                filepath_or_buffer = self.file_path(),
                dtype = {'is_open': np.bool},
                parse_dates = ['cal_date', 'pretrade_date']
            )
            self.dataframe.set_index(keys = 'cal_date', drop = False, inplace = True)
            self.dataframe.sort_index(inplace = True)
        else:
            self.dataframe = pd.DataFrame(columns = ['cal_date', 'is_open', 'pretrade_date'])

        return self.dataframe

    def prepare(self):
        if self.dataframe is None:
            self.load()

    def latest_trade_day(self) -> date:
        """
        返回距离当天之前最近的一个交易日的日期. 如果当天是交易日,则返回当天
        :return:
        """
        self.prepare()
        today = date.today()
        row = self.dataframe.loc[today]
        if row.loc['is_open']:
            return today
        else:
            return row.loc['pretrade_date'].date()

    def next_n_trade_day(self, base_date: date, n: int, last_date: Union[None, date] = None) -> date:
        """
        返回 base_date 后第n个交易日的日期.
        :param last_date:
        :param base_date:
        :param n:
        :return:
        """
        self.prepare()
        df: pd.DataFrame = self.dataframe
        df = df[(df['cal_date'] >= str(base_date)) & (df['is_open'] == True)]
        rows_count = df.shape[0]
        row_index = min(rows_count - 1, n)
        day = df.iloc[row_index].loc['cal_date'].date()
        if last_date is None:
            return day
        else:
            latest_day = self.latest_trade_day()
            if day <= latest_day:
                return day
            else:
                return latest_day

    @staticmethod
    def end_date() -> str:
        today = date.today()
        return '%s1231' % today.year

    @staticmethod
    @ts_rate_limiter
    def ts_trade_cal(start_date: str, end_date: str) -> pd.DataFrame:
        df: pd.DataFrame = ts_pro_api().trade_cal(
            exchange = 'SSE',
            start_date = start_date,
            end_date = end_date,
            fields = ','.join(['cal_date', 'is_open', 'pretrade_date'])
        )
        df['cal_date'] = pd.to_datetime(df['cal_date'], format = '%Y%m%d')
        df['pretrade_date'] = pd.to_datetime(df['pretrade_date'], format = '%Y%m%d')
        df['is_open'] = df['is_open'].apply(lambda x: str(x) == '1')
        df.set_index(keys = 'cal_date', drop = False, inplace = True)
        df.sort_index(inplace = True)
        logging.info(colorama.Fore.YELLOW + '下载交易日历数据: %s -- %s' % (start_date, end_date))
        return df
