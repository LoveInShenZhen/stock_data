from typing import Union

import pandas as pd
import numpy as np
from datetime import datetime, date


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


def ts_date(day: date) -> str:
    return day.strftime('%Y%m%d')


def to_datetime64(x: Union[str, pd.Timestamp, date]) -> Union[np.datetime64, None]:
    if x is None:
        return None
    if x is date:
        return x
    if x is pd.Timestamp:
        return pd.to_datetime(datetime(x.year, x.month, x.day))

    if len(x) == 6:
        return pd.to_datetime(x, format = '%Y%m')
    elif len(x) == 8:
        return pd.to_datetime(x, format = '%Y%m%d')
    elif len(x) == 10:
        return pd.to_datetime(x, format = '%Y-%m-%d')
    elif len(x) == 10:
        return pd.to_datetime(x, format = '%Y-%m-%d %H:%M:%S')
    else:
        return None
