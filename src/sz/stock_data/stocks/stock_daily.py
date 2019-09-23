import logging
import os
from typing import Union, List

import baostock as bao
import colorama
import pandas as pd

from datetime import date, timedelta
from sz.stock_data.stock_data import StockData
from sz.stock_data.toolbox.data_provider import ts_code
from sz.stock_data.toolbox.helper import mtime_of_file


class StockDaily(object):
    """
    baostock 能获取2006-01-01至当前时间的数据
    """
    base_date = date(year = 2006, month = 1, day = 1)

    def __init__(self, data_dir: str, stock_code: str):
        self.data_dir = data_dir
        self.stock_code = ts_code(stock_code)
        self.dataframe: Union[pd.DataFrame, None] = None

    def file_path(self) -> str:
        """
        返回保存数据的csv文件路径
        :return:
        """
        return os.path.join(self.data_dir, 'stocks', self.stock_code, 'day.csv')

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
                parse_dates = ['date']
            )
            self.dataframe.set_index(keys = 'date', drop = False, inplace = True)
            self.dataframe.sort_index(inplace = True)
        else:
            logging.warning(colorama.Fore.RED + '%s 本地日线数据文件不存在,请及时下载更新' % self.stock_code)
            self.dataframe = pd.DataFrame()

        return self.dataframe

    def prepare(self):
        if self.dataframe is None:
            self.load()

    def start_date(self) -> date:
        """
        计算本次更新的起始日期
        :return:
        """
        if self.dataframe is None:
            self.load()

        if self.dataframe.empty:
            return self.base_date
        else:
            return self.dataframe.iloc[-1].loc['date'].date() + timedelta(days = 1)

    def update(self):
        self._setup_dir_()
        if self.dataframe is None:
            self.load()

        if self.should_update():
            start_date: date = max(self.start_date(), StockData().stock_basic.list_date_of(self.stock_code))
            end_date: date = start_date
            last_trade_day = StockData().trade_calendar.latest_trade_day()
            df_list: List[pd.DataFrame] = [self.dataframe]
            step_days = timedelta(days = 1000)

            while start_date <= last_trade_day:
                end_date = start_date + step_days
                end_date = min(end_date, last_trade_day)
                rs = bao.query_history_k_data_plus(
                    code = self.stock_code,
                    start_date = str(start_date),
                    end_date = str(end_date),
                    frequency = 'd',
                    fields = 'date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,psTTM,pcfNcfTTM,pbMRQ,isST',
                    adjustflag = '3'
                )
                df = rs.get_data()
                # logging.debug(colorama.Fore.GREEN + '\n' + df.to_string())
                df['date'] = pd.to_datetime(df['date'], format = '%Y-%m-%d')
                df['is_open'] = df['isST'].apply(lambda x: str(x) == '1')
                df['code'] = df['code'].apply(lambda x: ts_code(x))
                df.set_index(keys = 'date', drop = False, inplace = True)
                logging.debug(
                    colorama.Fore.YELLOW + '下载 %s 日线数据, 从 %s 到 %s' % (self.stock_code, str(start_date), str(end_date)))

                df_list.append(df)
                start_date = end_date + timedelta(days = 1)

            self.dataframe = pd.concat(df_list).drop_duplicates()
            self.dataframe.sort_index(inplace = True)

            self.dataframe.to_csv(
                path_or_buf = self.file_path(),
                index = False
            )
            logging.info(
                colorama.Fore.YELLOW + '%s 日线数据更新到: %s path: %s' % (self.stock_code, str(end_date), self.file_path()))
        else:
            logging.info(colorama.Fore.BLUE + '%s 日线数据无须更新' % self.stock_code)
