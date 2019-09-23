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


class StockTopInst(object):
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
        return os.path.join(self.data_dir, 'market', 'top_inst.csv')

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

    @staticmethod
    @ts_rate_limiter
    def ts_top_inst(trade_date: date) -> pd.DataFrame:
        df: pd.DataFrame = ts_pro_api().top_inst(
            trade_date = ts_date(trade_date),
        )
        df['trade_date'] = df['trade_date'].apply(lambda x: to_datetime64(x))
        df.sort_values(by = 'trade_date', inplace = True)
        logging.info(colorama.Fore.YELLOW + '下载 [龙虎榜机构明细] 数据: %s, %s 条' % (trade_date, df.shape[0]))
        return df

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

    def update(self):
        self._setup_dir_()
        self.prepare()

        latest_trade_day = StockData().trade_calendar.latest_trade_day()
        trad_date_list = StockData().trade_calendar.trade_day_between(
            from_date = self.start_date(),
            to_date = StockData().trade_calendar.latest_trade_day()
        )

        df_list: List[pd.DataFrame] = [self.dataframe]

        try:
            for trade_date in trad_date_list:
                df = self.ts_top_inst(trade_date)
                if not df.empty:
                    df_list.append(df)
                    latest_trade_day = trade_date
                else:
                    logging.warning(colorama.Fore.RED + '没有 %s 的 [龙虎榜机构明细] 数据' % trade_date)
        except Exception as ex:
            logging.warning('更新 [龙虎榜机构明细] 发送异常中断: %s' % ex)
            raise ex
        finally:
            if len(df_list) > 1:
                self.dataframe = pd.concat(df_list).drop_duplicates()
                self.dataframe.sort_values(by = 'trade_date', inplace = True)

                self.dataframe.to_csv(
                    path_or_buf = self.file_path(),
                    index = False
                )

                logging.info(
                    colorama.Fore.YELLOW + '[龙虎榜机构明细] 数据更新到: %s path: %s' % (latest_trade_day, self.file_path()))
            else:
                logging.info(colorama.Fore.BLUE + '[龙虎榜机构明细] 数据无须更新')
