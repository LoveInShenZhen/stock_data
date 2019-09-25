#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
from datetime import date

import colorama

from sz.stock_data.index.index_basic import IndexBasic
from sz.stock_data.index.index_daily import IndexDaily
from sz.stock_data.market.block_trade import BlockTrade
from sz.stock_data.market.concept import StockConcept
from sz.stock_data.market.margin import StockMargin
from sz.stock_data.market.margin_detail import StockMarginDetail
from sz.stock_data.market.stock_industry import StockIndustry
from sz.stock_data.market.top_inst import StockTopInst
from sz.stock_data.market.top_list import StockTopList
from sz.stock_data.stock_data import StockData
from sz.stock_data.stocks.adj_factor import AdjFactor
from sz.stock_data.stocks.money_flow import MoneyFlow
from sz.stock_data.stocks.pledge_detail import PledgeDetail
from sz.stock_data.stocks.pledge_stat import PledgeStat
from sz.stock_data.stocks.stk_holder_number import StkHolderNumber
from sz.stock_data.stocks.stk_holder_trade import StkHolderTrade
from sz.stock_data.stocks.stock_5min import Stock5min
from sz.stock_data.stocks.stock_daily import StockDaily
from sz.stock_data.stocks.suspend import Suspend
from sz.stock_data.stocks.top10_floatholders import Top10FloatHolders
from sz.stock_data.stocks.top10_holders import Top10Holders
from sz.stock_data.toolbox.data_provider import bao_login, bao_logout

colorama.init(autoreset = True)

logging.basicConfig(
    level = logging.DEBUG,
    # format = "[%(asctime)-15s] [%(filename)s:%(lineno)d] [%(threadName)s] [%(levelname)s] %(message)s"
    format = "[%(asctime)-15s] [%(threadName)s] [%(levelname)s] %(message)s"
)


def test():
    StockData().stock_basic.update()
    StockData().stock_company.update()
    StockData().zz500.update()
    StockData().hs300.update()

    StockTopList(data_dir = StockData().data_dir).update()
    StockTopInst(data_dir = StockData().data_dir).update()
    BlockTrade(data_dir = StockData().data_dir).update()
    StockConcept(data_dir = StockData().data_dir).update()
    StockMargin(data_dir = StockData().data_dir).update()
    StockMarginDetail(data_dir = StockData().data_dir).update()
    StockIndustry(data_dir = StockData().data_dir).update()
    IndexBasic(data_dir = StockData().data_dir).update()

    for index_code in StockData().index_basic.default_index_pool():
        IndexDaily(data_dir = StockData().data_dir, index_code = index_code).update()

    for stock_code in StockData().hs300.stock_codes():
        update_for_stock(stock_code)

    for stock_code in StockData().zz500.stock_codes():
        update_for_stock(stock_code)

    logging.info(colorama.Fore.YELLOW + '更新完毕')


def update_for_stock(stock_code: str):
    StockDaily(data_dir = StockData().data_dir, stock_code = stock_code).update()
    Stock5min(data_dir = StockData().data_dir, stock_code = stock_code).update()
    AdjFactor(data_dir = StockData().data_dir, stock_code = stock_code).update()
    MoneyFlow(data_dir = StockData().data_dir, stock_code = stock_code).update()
    Top10Holders(data_dir = StockData().data_dir, stock_code = stock_code).update()
    Top10FloatHolders(data_dir = StockData().data_dir, stock_code = stock_code).update()
    StkHolderNumber(data_dir = StockData().data_dir, stock_code = stock_code).update()
    StkHolderTrade(data_dir = StockData().data_dir, stock_code = stock_code).update()
    PledgeStat(data_dir = StockData().data_dir, stock_code = stock_code).update()
    PledgeDetail(data_dir = StockData().data_dir, stock_code = stock_code).update()
    Suspend(data_dir = StockData().data_dir, stock_code = stock_code).update()


if __name__ == '__main__':
    bao_login()
    StockData().setup(data_dir = '/Volumes/USBDATA/stock_data')
    test()
    bao_logout()
