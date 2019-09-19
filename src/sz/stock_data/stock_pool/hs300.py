import logging
import os
from typing import Union

import baostock as bao
import colorama
import pandas as pd

from sz.stock_data.toolbox.helper import need_update


class HS300(object):

    def __init__(self, data_dir: str):
        self.data_dir: str = data_dir
        self.dataframe: Union[pd.DataFrame, None] = None

    def file_path(self) -> str:
        """
        返回保存数据的csv文件路径
        :return:
        """
        return os.path.join(self.data_dir, 'stock_pool', 'hs300.csv')

    def _setup_dir_(self):
        """
        初始化数据目录
        :return:
        """
        os.makedirs(os.path.dirname(self.file_path()), exist_ok = True)

    def update(self):
        self._setup_dir_()
        if self.should_update():
            df = self.bao_hs300_stocks()
            df.to_csv(
                path_or_buf = self.file_path(),
                index = False
            )

    def should_update(self) -> bool:
        """
        判断数据是否需要更新.(更新频率: 每周更新)
        :return:
        """
        return need_update(self.file_path(), 7)

    def load(self) -> pd.DataFrame:
        if os.path.exists(self.file_path()):
            self.dataframe = pd.read_csv(
                filepath_or_buffer = self.file_path(),
                parse_dates = ['updateDate']
            )
            self.dataframe.set_index(keys = 'code', drop = False, inplace = True)
            self.dataframe.sort_index(inplace = True)
        else:
            logging.warning(colorama.Fore.RED + 'StockCompany 本地数据文件不存在,请及时下载更新')
            self.dataframe = pd.DataFrame()

        return self.dataframe

    @staticmethod
    def bao_hs300_stocks() -> pd.DataFrame:
        """
        获取沪深300成分股信息
        :return:
        """
        lg = bao.login()
        logging.debug(
            colorama.Fore.BLUE + 'baostock login => error_code: %s error_msg: %s' % (lg.error_code, lg.error_msg))
        df = bao.query_hs300_stocks().get_data()
        bao.logout()
        df.set_index(keys = 'code', drop = False, inplace = True)
        logging.info(colorama.Fore.YELLOW + '获取沪深300成分股信息')
        return df
