import logging
import os
from typing import Union, List

import colorama
import pandas as pd

from sz.stock_data.toolbox.data_provider import ts_pro_api
from sz.stock_data.toolbox.helper import need_update
from sz.stock_data.toolbox.limiter import ts_rate_limiter


class StockConcept(object):

    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.dataframe: Union[pd.DataFrame, None] = None

    def file_path(self) -> str:
        """
        返回保存数据的csv文件路径
        :return:
        """
        return os.path.join(self.data_dir, 'market', 'concept_detail.csv')

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
                parse_dates = ['in_date', 'out_date']
            )
            self.dataframe.set_index(keys = 'id', drop = False, inplace = True)
            self.dataframe.sort_index(inplace = True)
        else:
            logging.warning(colorama.Fore.RED + '[概念股列表] 本地数据文件不存在,请及时下载更新')
            self.dataframe = pd.DataFrame()

        return self.dataframe

    def prepare(self):
        if self.dataframe is None:
            self.load()

    @ts_rate_limiter
    def ts_concept(self) -> pd.DataFrame:
        df: pd.DataFrame = ts_pro_api().concept(
            src = 'ts'
        )
        return df

    @ts_rate_limiter
    def ts_concept_detail(self, concept_id: str, concept_name: str) -> pd.DataFrame:
        df: pd.DataFrame = ts_pro_api().concept_detail(
            id = concept_id
        )
        logging.info(colorama.Fore.YELLOW + '下载 [概念股列表] - %s 共 %s 条' % (concept_name, df.shape[0]))
        return df

    def update(self):
        self._setup_dir_()
        self.prepare()

        if self.should_update():
            df_list: List[pd.DataFrame] = [self.dataframe]
            try:
                df_concept = self.ts_concept()
                for index in range(0, df_concept.shape[0]):
                    concept_id = df_concept.iloc[index].loc['code']
                    concept_name = df_concept.iloc[index].loc['name']
                    df = self.ts_concept_detail(concept_id, concept_name)
                    df_list.append(df)

            except Exception as ex:
                logging.warning('更新 [概念股列表] 发生异常中断: %s' % ex)
                raise ex

            finally:
                if len(df_list) > 1:
                    self.dataframe = pd.concat(df_list).drop_duplicates()
                    self.dataframe.set_index(keys = 'id', drop = False, inplace = True)
                    self.dataframe.sort_index(inplace = True)

                    self.dataframe.to_csv(
                        path_or_buf = self.file_path(),
                        index = False
                    )

                    logging.info(
                        colorama.Fore.YELLOW + '[概念股列表] 数据更新到最新: %s' % (self.file_path()))
                else:
                    logging.info(colorama.Fore.BLUE + '[概念股列表] 数据无须更新')
