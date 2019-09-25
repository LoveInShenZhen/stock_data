from typing import Union

from sz.stock_data.calendar.trade_calendar import TradeCalendar
from sz.stock_data.index.index_basic import IndexBasic
from sz.stock_data.stock_basic.stock_basic import StockBasic
from sz.stock_data.stock_basic.stock_company import StockCompany
from sz.stock_data.stock_pool.hs300 import HS300
from sz.stock_data.stock_pool.zz500 import ZZ500
from sz.stock_data.toolbox.singleton import SingletonMeta


class StockData(object, metaclass = SingletonMeta):

    def __init__(self):
        self._data_dir = ''
        self._trade_calendar: Union[None, TradeCalendar] = None
        self._stock_basic: Union[None, StockBasic] = None
        self._stock_company: Union[None, StockCompany] = None
        self._hs300: Union[None, HS300] = None
        self._zz500: Union[None, ZZ500] = None
        self._index_basic: Union[None, IndexBasic] = None

    def setup(self, data_dir: str):
        self._data_dir = data_dir
        return self

    @property
    def data_dir(self) -> str:
        """
        本地数据目录
        :return:
        """
        if self._data_dir is None:
            raise Exception('请先调用StockData().setup(data_dir="指定路径")')

        return self._data_dir

    @property
    def trade_calendar(self) -> TradeCalendar:
        """
        交易日历数据
        :return:
        """
        if self._trade_calendar is None:
            self._trade_calendar = TradeCalendar(self._data_dir)
            self._trade_calendar.load()

        return self._trade_calendar

    @property
    def stock_basic(self) -> StockBasic:
        """
        获取基础信息数据，包括股票代码、名称、上市日期、退市日期等
        :return:
        """
        if self._stock_basic is None:
            self._stock_basic = StockBasic(self._data_dir)
            self._stock_basic.load()

        return self._stock_basic

    @property
    def stock_company(self) -> StockCompany:
        """
        获取上市公司基础信息
        :return:
        """
        if self._stock_company is None:
            self._stock_company = StockCompany(self.data_dir)
            self._stock_company.load()

        return self._stock_company

    @property
    def hs300(self) -> HS300:
        """
        沪深300成分股数据
        :return:
        """
        if self._hs300 is None:
            self._hs300 = HS300(self._data_dir)
            self._hs300.load()

        return self._hs300

    @property
    def zz500(self) -> ZZ500:
        """
        中证500成分股数据
        :return:
        """
        if self._zz500 is None:
            self._zz500 = ZZ500(self._data_dir)
            self._zz500.load()

        return self._zz500

    @property
    def index_basic(self) -> IndexBasic:
        """
        指数基础信息
        :return:
        """
        if self._index_basic is None:
            self._index_basic = IndexBasic(self.data_dir)
            self._index_basic.load()

        return self._index_basic