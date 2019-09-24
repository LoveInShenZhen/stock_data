import os
from datetime import date
import pandas as pd


def need_update(fpath: str, outdate_days: int) -> bool:
    """
    根据指定的数据文件的最后修改时间, 判断是否需要进行更新
    :param fpath: 数据文件路径
    :param outdate_days:
    :return: 需要更新时返回 True
    """
    if not os.path.exists(fpath):
        return True
    else:
        modify_date = date.fromtimestamp(os.stat(fpath).st_mtime)
        today = date.today()
        diff_days = (today - modify_date).days
        if diff_days > outdate_days:
            # 距离上次更新时间,超过指定天数
            return True
        else:
            return False


def need_update_by_trade_date(df: pd.DataFrame, column_name: str) -> bool:
    """
    根据DataFrame中指定的交易日期的最后记录的值, 如果在最近的交易日之前, 则说明需要更新
    :param df:
    :param column_name: 交易日期对应的字段名称
    :return:
    """
    if df.empty:
        # 如果DataFrame为空, 说明没有数据在本地, 需要更新
        return True
    else:
        from sz.stock_data.stock_data import StockData
        return df.iloc[-1].loc[column_name].date() < StockData().trade_calendar.latest_trade_day()


def mtime_of_file(fpath: str) -> date:
    """
    获取文件的最后修改日期
    :param fpath:
    :return:
    """
    return date.fromtimestamp(os.stat(fpath).st_mtime)
