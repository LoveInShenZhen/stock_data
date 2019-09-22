#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging

import colorama

from sz.stock_data.stock_data import StockData
from sz.stock_data.stocks.adj_factor import AdjFactor
from sz.stock_data.stocks.money_flow import MoneyFlow
from sz.stock_data.stocks.pledge_stat import PledgeStat
from sz.stock_data.stocks.stk_holder_number import StkHolderNumber
from sz.stock_data.stocks.stk_holder_trade import StkHolderTrade
from sz.stock_data.stocks.stock_5min import Stock5min
from sz.stock_data.stocks.stock_daily import StockDaily
from sz.stock_data.stocks.top10_floatholders import Top10FloatHolders
from sz.stock_data.stocks.top10_holders import Top10Holders
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
    Stock5min(data_dir = StockData().data_dir, stock_code = '300059.SZ').update()
    AdjFactor(data_dir = StockData().data_dir, stock_code = '300059.SZ').update()
    MoneyFlow(data_dir = StockData().data_dir, stock_code = '300059.SZ').update()
    Top10Holders(data_dir = StockData().data_dir, stock_code = '300059.SZ').update()
    Top10FloatHolders(data_dir = StockData().data_dir, stock_code = '300059.SZ').update()
    StkHolderNumber(data_dir = StockData().data_dir, stock_code = '300059.SZ').update()
    StkHolderTrade(data_dir = StockData().data_dir, stock_code = '300059.SZ').update()
    PledgeStat(data_dir = StockData().data_dir, stock_code = '300059.SZ').update()

    logging.info(colorama.Fore.YELLOW + '更新完毕')


if __name__ == '__main__':
    bao_login()
    StockData().setup(data_dir = '/Volumes/USBDATA/stock_data')
    test()
    bao_logout()
