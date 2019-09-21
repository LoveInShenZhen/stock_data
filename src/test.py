#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging

import colorama

from sz.stock_data.stock_data import StockData
from sz.stock_data.stocks.stock_daily import StockDaily
from sz.stock_data.toolbox.data_provider import bao_login, bao_logout

colorama.init(autoreset = True)

logging.basicConfig(
    level = logging.DEBUG,
    format = "[%(asctime)-15s] [%(filename)s:%(lineno)d] [%(threadName)s] [%(levelname)s] %(message)s"
)


def test():
    # StockData().zz500.update()
    # StockData().hs300.update()

    StockDaily(data_dir = StockData().data_dir, stock_code = '300059.SZ').update()

    logging.info(colorama.Fore.YELLOW + '更新完毕')


if __name__ == '__main__':
    bao_login()
    StockData().setup(data_dir = '/Volumes/USBDATA/stock_data')
    test()
    bao_logout()
