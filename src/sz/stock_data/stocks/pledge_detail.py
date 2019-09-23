import logging
import os
from datetime import date
from typing import Union

import colorama
import pandas as pd

from sz.stock_data.stock_data import StockData
from sz.stock_data.toolbox.data_provider import ts_code, ts_pro_api
from sz.stock_data.toolbox.datetime import to_datetime64
from sz.stock_data.toolbox.helper import mtime_of_file
from sz.stock_data.toolbox.limiter import ts_rate_limiter


class PledgeDetail(object):
    """
    股权质押明细
    https://tushare.pro/document/2?doc_id=111
    """

    def __init__(self, data_dir: str, stock_code: str):
        self.data_dir = data_dir
        self.stock_code = ts_code(stock_code)
        self.dataframe: Union[pd.DataFrame, None] = None

    def file_path(self) -> str:
        """
        返回保存数据的csv文件路径
        :return:
        """
        return os.path.join(self.data_dir, 'stocks', self.stock_code, 'pledge_detail.csv')

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
                parse_dates = ['end_date']
            )
        else:
            logging.warning(colorama.Fore.RED + '%s 本地 [股权质押明细] 数据文件不存在,请及时下载更新' % self.stock_code)
            self.dataframe = pd.DataFrame()

        return self.dataframe

    def prepare(self):
        if self.dataframe is None:
            self.load()

    @ts_rate_limiter
    def ts_pledge_detail(self) -> pd.DataFrame:
        df: pd.DataFrame = ts_pro_api().pledge_detail(
            ts_code = self.stock_code,
            fields = 'ts_code,ann_date,holder_name,pledge_amount,start_date,end_date,is_release,release_date,pledgor,holding_amount,pledged_amount,p_total_ratio,h_total_ratio,is_buyback'
        )
        if not df.empty:
            df['ann_date'] = df['ann_date'].apply(lambda x: to_datetime64(x))
            df['start_date'] = df['start_date'].apply(lambda x: to_datetime64(x))
            df['end_date'] = df['end_date'].apply(lambda x: to_datetime64(x))
            df['release_date'] = df['release_date'].apply(lambda x: to_datetime64(x))

            df.sort_values(by = 'end_date', inplace = True)
            logging.info(colorama.Fore.YELLOW + '下载 %s [股权质押明细] 数据' % self.stock_code)
        else:
            logging.info(colorama.Fore.YELLOW + '%s [股权质押明细] 无最新数据' % self.stock_code)
        return df

    def update(self):
        self._setup_dir_()
        self.prepare()

        if self.should_update():
            # 获取最新
            latest_df = self.ts_pledge_detail()
            # 合并最新, 并且去掉重复记录
            self.dataframe = pd.concat([self.dataframe, latest_df]).drop_duplicates()
            self.dataframe.to_csv(
                path_or_buf = self.file_path(),
                index = False
            )

            logging.info(
                colorama.Fore.YELLOW + '%s [股权质押明细] 数据更新到: %s' % (
                    self.stock_code, str(date.today())))
        else:
            logging.info(colorama.Fore.BLUE + '%s [股权质押明细] 数据无须更新' % self.stock_code)
