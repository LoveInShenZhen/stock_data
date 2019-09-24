import logging
import os
from typing import Union

import baostock as bao
import colorama
import pandas as pd

from sz.stock_data.toolbox.data_provider import ts_code
from sz.stock_data.toolbox.helper import need_update


class StockIndustry(object):
    """
    业分类信息，更新频率：每周一更新
    ref: http://baostock.com/baostock/index.php/%E8%A1%8C%E4%B8%9A%E5%88%86%E7%B1%BB
    """
    def __init__(self, data_dir: str):
        self.data_dir: str = data_dir
        self.dataframe: Union[pd.DataFrame, None] = None

    def file_path(self) -> str:
        """
        返回保存数据的csv文件路径
        :return:
        """
        return os.path.join(self.data_dir, 'market', 'stock_industry.csv')

    def _setup_dir_(self):
        """
        初始化数据目录
        :return:
        """
        os.makedirs(os.path.dirname(self.file_path()), exist_ok = True)

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
            logging.warning(colorama.Fore.RED + '[行业分类信息] 本地数据文件不存在,请及时下载更新')
            self.dataframe = pd.DataFrame()

        return self.dataframe

    def prepare(self):
        if self.dataframe is None:
            self.load()

    @staticmethod
    def bao_query_stock_industry() -> pd.DataFrame:
        """
        获取行业分类信息
        :return:
        """
        df = bao.query_stock_industry().get_data()
        df['code'] = df['code'].apply(lambda x: ts_code(x))
        df.set_index(keys = 'code', drop = False, inplace = True)
        return df

    def update(self):
        self._setup_dir_()

        if self.should_update():
            df = self.bao_query_stock_industry()
            df.to_csv(
                path_or_buf = self.file_path(),
                index = False
            )
            logging.info(colorama.Fore.YELLOW + '[行业分类信息] 数据更新到最新: %s' % df.iloc[-1].loc['updateDate'])
        else:
            logging.info(colorama.Fore.YELLOW + '[行业分类信息] 数据无须更新')
