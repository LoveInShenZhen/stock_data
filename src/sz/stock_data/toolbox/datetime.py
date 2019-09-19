import pandas as pd
import numpy as np


def yyyymmdd_date_parser(x):
    """
    在 pandas.read_csv(...) 的时候, 指定解析 yyyymmdd 格式的函数
    :param x:
    :return:
    """
    item = str(x)
    if item and item != 'nan':
        return pd.datetime.strptime(item, "%Y%m%d")
    else:
        return np.nan
