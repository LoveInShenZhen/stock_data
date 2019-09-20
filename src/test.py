#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging

import colorama

from sz.stock_data.stock_data import StockData

colorama.init(autoreset = True)

logging.basicConfig(
    level = logging.DEBUG,
    format = "[%(asctime)-15s] [%(filename)s:%(lineno)d] [%(threadName)s] [%(levelname)s] %(message)s"
)


def test():
    db = StockData().zz500
    db.update()
    db.load()
    logging.info(colorama.Fore.GREEN + '更新完毕')
    logging.info('\n' + colorama.Fore.LIGHTYELLOW_EX + db.dataframe.dtypes.to_string())


if __name__ == '__main__':
    StockData().setup(data_dir = '/Volumes/USBDATA/stock_data')
    test()
