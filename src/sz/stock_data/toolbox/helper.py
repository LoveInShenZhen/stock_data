import os
from datetime import date


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
