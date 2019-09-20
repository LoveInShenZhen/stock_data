import logging
import os
from typing import Union

import baostock as bao
import colorama
import pandas as pd


class StockDaily(object):

    def __init__(self, data_dir: str, stock_code: str):
        self.data_dir = data_dir
