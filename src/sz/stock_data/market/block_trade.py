import logging
import os
from datetime import date, timedelta
from typing import Union, List

import colorama
import pandas as pd

from sz.stock_data.stock_data import StockData
from sz.stock_data.toolbox.data_provider import ts_pro_api
from sz.stock_data.toolbox.datetime import to_datetime64, ts_date
from sz.stock_data.toolbox.helper import mtime_of_file
from sz.stock_data.toolbox.limiter import ts_rate_limiter


class BlockTrade(object):
    """
    大宗交易
    ref: https://tushare.pro/document/2?doc_id=161
    """
    base_date = date(year = 2008, month = 1, day = 2)

    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.dataframe: Union[pd.DataFrame, None] = None

    def _setup_dir_(self):
        """
        初始化数据目录
        :return:
        """
        os.makedirs(os.path.dirname(self.file_path()), exist_ok = True)

    def file_path(self) -> str:
        """
        返回数据文件路径
        :return:
        """
        return os.path.join(self.data_dir, 'market', 'block_trade.csv')

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
        """
        从数据文件加载
        :return:
        """
        if os.path.exists(self.file_path()):
            self.dataframe = pd.read_csv(
                filepath_or_buffer = self.file_path(),
                parse_dates = ['trade_date']
            )
            self.dataframe.sort_values(by = 'trade_date', inplace = True)
        else:
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
            return self.dataframe.iloc[-1].loc['trade_date'].date() + timedelta(days = 1)

    @ts_rate_limiter
    def ts_block_trade(self, start_date: date, end_date: date) -> pd.DataFrame:
        df: pd.DataFrame = ts_pro_api().block_trade(
            start_date = ts_date(start_date),
            end_date = ts_date(end_date)
        )
        if not df.empty:
            df['trade_date'] = pd.to_datetime(df['trade_date'], format = '%Y%m%d')
            df.sort_values(by = 'trade_date', inplace = True)
            row_count = df.shape[0]
            if row_count == 1000:
                raise Exception('超出记录条数上限[1000], %s -- %s' % (start_date, end_date))
            logging.info(colorama.Fore.YELLOW + '下载 [大宗交易] 数据: %s -- %s %s条' % (start_date, end_date, df.shape[0]))
        else:
            logging.info(colorama.Fore.YELLOW + '[大宗交易] : %s -- %s 无数据' % (start_date, end_date))
        return df

    def update(self):
        self._setup_dir_()
        self.prepare()

        if self.should_update():
            start_date: date = self.start_date()
            end_date: date = start_date
            last_trade_day = StockData().trade_calendar.latest_trade_day()
            df_list: List[pd.DataFrame] = [self.dataframe]
            step_days = timedelta(days = 10)

            try:
                while start_date <= last_trade_day:
                    end_date = start_date + step_days
                    end_date = min(end_date, last_trade_day)
                    df = self.ts_block_trade(start_date = start_date, end_date = end_date)
                    df_list.append(df)
                    start_date = end_date + timedelta(days = 1)
            except Exception as ex:
                logging.warning('更新 [大宗交易] 发生异常中断: %s' % ex)
                raise ex
            finally:
                self.dataframe = pd.concat(df_list).drop_duplicates()
                self.dataframe.sort_values(by = 'trade_date', inplace = True)

                self.dataframe.to_csv(
                    path_or_buf = self.file_path(),
                    index = False
                )

                logging.info(
                    colorama.Fore.YELLOW + '[大宗交易] 数据更新到: %s path: %s' % (end_date, self.file_path()))
        else:
            logging.info(colorama.Fore.BLUE + '%s [大宗交易] 数据无须更新')
