import logging
import os
from typing import Union

import colorama
import pandas as pd

from sz.stock_data.toolbox.data_provider import ts_pro_api
from sz.stock_data.toolbox.helper import need_update
from sz.stock_data.toolbox.limiter import ts_rate_limiter


class StockCompany(object):

    def __init__(self, data_dir: str):
        self.data_dir: str = data_dir
        self.dataframe: Union[pd.DataFrame, None] = None

    def file_path(self) -> str:
        """
        返回保存交易日历的csv文件路径
        :return:
        """
        return os.path.join(self.data_dir, 'stock_basic', 'stock_company.csv')

    def _setup_dir_(self):
        """
        初始化数据目录
        :return:
        """
        os.makedirs(os.path.dirname(self.file_path()), exist_ok = True)

    def update(self):
        self._setup_dir_()
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
                parse_dates = ['setup_date']
            )
            self.dataframe.set_index(keys = 'ts_code', drop = False, inplace = True)
            self.dataframe.sort_index(inplace = True)
        else:
            logging.warning(colorama.Fore.RED + 'StockCompany 本地数据文件不存在,请及时下载更新')
            self.dataframe = pd.DataFrame()

        return self.dataframe

    @staticmethod
    @ts_rate_limiter
    def ts_stock_basic() -> pd.DataFrame:
        df: pd.DataFrame = ts_pro_api().stock_company(
            exchange = '',
            fields = ','.join(['ts_code', 'exchange', 'chairman', 'manager', 'secretary', 'reg_capital', 'setup_date',
                               'province', 'city', 'introduction', 'website', 'email', 'office', 'employees',
                               'main_business', 'business_scope'])
        )
        df['setup_date'] = pd.to_datetime(df['setup_date'], format = '%Y%m%d')
        logging.info(colorama.Fore.YELLOW + '下载股票上市公司基本信息数据')
        return df
