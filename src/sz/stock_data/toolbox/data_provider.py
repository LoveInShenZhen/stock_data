import tushare as ts
from tushare.pro.client import DataApi


def ts_pro_api() -> DataApi:
    return ts.pro_api(ts_token())


def ts_token() -> str:
    return 'f96b1eeee9c8fddd357f2299cdedc1c88b2bb2a30ae1f772cf810dea'
