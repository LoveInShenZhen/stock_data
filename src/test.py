#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from sz.stock_data.calendar.trade_calendar import TradeCalendar
import colorama
import logging

colorama.init(autoreset = True)
logging.basicConfig(
    level = logging.DEBUG,
    format = "[%(asctime)-15s] [%(filename)s:%(lineno)d] [%(threadName)s] [%(levelname)s] %(message)s"
)

data_dir = '/Volumes/USBDATA/stock_data'


def test():
    cal = TradeCalendar(data_dir = data_dir)
    cal.update()
    logging.info(colorama.Fore.GREEN + '更新完毕')


if __name__ == '__main__':
    test()
