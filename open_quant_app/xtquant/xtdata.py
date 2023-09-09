#coding:utf-8

import os, sys
import time
import traceback
import json
import datetime as dt

from . import xtbson as bson
from . import xtutil


__all__ = [
    'subscribe_quote'
    , 'subscribe_whole_quote'
    , 'unsubscribe_quote'
    , 'run'
    , 'get_market_data'
    , 'get_local_data'
    , 'get_full_tick'
    , 'get_divid_factors'
    , 'get_l2_quote'
    , 'get_l2_order'
    , 'get_l2_transaction'
    , 'download_history_data'
    , 'get_financial_data'
    , 'download_financial_data'
    , 'get_instrument_detail'
    , 'get_instrument_type'
    , 'get_trading_dates'
    , 'get_sector_list'
    , 'get_stock_list_in_sector'
    , 'download_sector_data'
    , 'add_sector'
    , 'remove_sector'
    , 'get_index_weight'
    , 'download_index_weight'
    , 'get_holidays'
    , 'get_trading_calendar'
    , 'get_trade_times'
    #, 'get_industry'
    #, 'get_etf_info'
    #, 'get_main_contract'
    #, 'download_history_contracts'
    , 'download_cb_data'
    , 'get_cb_info'
    , 'create_sector_folder'
    , 'create_sector'
    , 'remove_stock_from_sector'
    , 'reset_sector'
    , 'get_period_list'
]

def try_except(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception:
            exc_type, exc_instance, exc_traceback = sys.exc_info()
            formatted_traceback = ''.join(traceback.format_tb(exc_traceback))
            message = '\n{0} raise {1}:{2}'.format(
                formatted_traceback,
                exc_type.__name__,
                exc_instance
            )
            # raise exc_type(message)
            # print(message)
            return None

    return wrapper


### config

debug_mode = 0

default_data_dir = '../userdata_mini/datadir'
data_dir = default_data_dir


### connection

__client = None
__client_last_spec = ('', None)


def connect(ip = '', port = None, remember_if_success = True):
    global __client
    global data_dir

    if __client:
        if __client.is_connected():
            return __client

        __client.shutdown()
        __client = None
        data_dir = default_data_dir

    from . import xtconn

    if not ip:
        ip = 'localhost'

    if port:
        server_list = [f'{ip}:{port}']
        __client = xtconn.connect_any(server_list)
    else:
        server_list = xtconn.scan_available_server()

        default_addr = 'localhost:58610'
        if not default_addr in server_list:
            server_list.append(default_addr)

        __client = xtconn.connect_any(server_list)

    if not __client or not __client.is_connected():
        raise Exception("无法连接行情服务！")

    if remember_if_success:
        global __client_last_spec
        __client_last_spec = (ip, port)

    data_dir = __client.get_data_dir()
    if data_dir == "":
        data_dir = os.path.join(__client.get_app_dir(), default_data_dir)

    data_dir = os.path.abspath(data_dir)
    return __client


def reconnect(ip = '', port = None, remember_if_success = True):
    global __client
    global data_dir

    if __client:
        __client.shutdown()
        __client = None
        data_dir = default_data_dir

    return connect(ip, port, remember_if_success)


def get_client():
    global __client

    if not __client or not __client.is_connected():
        global __client_last_spec

        ip, port = __client_last_spec
        __client = connect(ip, port, False)

    return __client



### utils

def create_array(shape, dtype_tuple, capsule, size):
    import numpy as np
    import ctypes

    ctypes.pythonapi.PyCapsule_GetPointer.restype = ctypes.POINTER(ctypes.c_char)
    ctypes.pythonapi.PyCapsule_GetPointer.argtypes = [ctypes.py_object, ctypes.c_char_p]
    buff = ctypes.pythonapi.PyCapsule_GetPointer(capsule, None)
    base_type = size * buff._type_

    for dim in shape[::-1]:
        base_type = dim * base_type
    p_arr_type = ctypes.POINTER(base_type)
    obj = ctypes.cast(buff, p_arr_type).contents
    obj._base = capsule
    return np.ndarray(shape = shape, dtype = np.dtype(dtype_tuple), buffer = obj)

from .IPythonApiClient import register_create_nparray
register_create_nparray(create_array)


def __bsoncall_common(interface, func, param):
    return bson.BSON.decode(interface(func, bson.BSON.encode(param)))


### function

def get_industry(industry_name):
    '''
    获取行业成份股，支持申万行业和证监会行业
    :param industry_name: (str)行业名称
    :return: list
    '''
    client = get_client()
    return client.get_industry(industry_name)


def get_stock_list_in_sector(sector_name):
    '''
    获取板块成份股，支持客户端左侧板块列表中任意的板块，包括自定义板块
    :param sector_name: (str)板块名称
    :return: list
    '''
    client = get_client()
    return client.get_stock_list_in_sector(sector_name, 0)


def get_index_weight(index_code):
    '''
    获取某只股票在某指数中的绝对权重
    :param index_code: (str)指数名称
    :return: dict
    '''
    client = get_client()
    return client.get_weight_in_index(index_code)


def get_financial_data(stock_list, table_list=[], start_time='', end_time='', report_type='report_time'):
    '''
     获取财务数据
    :param stock_list: (list)合约代码列表
    :param table_list: (list)报表名称列表
    :param start_time: (str)起始时间
    :param end_time: (str)结束时间
    :param report_type: (str) 时段筛选方式 'announce_time' / 'report_time'
    :return:
        field: list[str]
        date: list[int]
        stock: list[str]
        value: list[list[float]]
    '''
    client = get_client()
    all_table = {
        'Balance' : 'ASHAREBALANCESHEET'
        , 'Income' : 'ASHAREINCOME'
        , 'CashFlow' : 'ASHARECASHFLOW'
        , 'Capital' : 'CAPITALSTRUCTURE'
        , 'HolderNum' : 'SHAREHOLDER'
        , 'Top10Holder' : 'TOP10HOLDER'
        , 'Top10FlowHolder' : 'TOP10FLOWHOLDER'
        , 'PershareIndex' : 'PERSHAREINDEX'
    }

    if not table_list:
        table_list = list(all_table.keys())

    all_table_upper = {table.upper() : all_table[table] for table in all_table}
    req_list = []
    names = {}
    for table in table_list:
        req_table = all_table_upper.get(table.upper(), table)
        req_list.append(req_table)
        names[req_table] = table

    data = {}
    sl_len = 20
    stock_list2 = [stock_list[i : i + sl_len] for i in range(0, len(stock_list), sl_len)]
    for sl in stock_list2:
        data2 = client.get_financial_data(sl, req_list, start_time, end_time, report_type)
        for s in data2:
            data[s] = data2[s]

    import time
    import math
    def conv_date(data, key, key2):
        if key in data:
            tmp_data = data[key]
            if math.isnan(tmp_data):
                if key2 not in data or math.isnan(data[key2]):
                    data[key] = ''
                else:
                    tmp_data = data[key2]
            data[key] = time.strftime('%Y%m%d', time.localtime(tmp_data / 1000))
        return

    result = {}
    import pandas as pd
    for stock in data:
        stock_data = data[stock]
        result[stock] = {}
        for table in stock_data:
            table_data = stock_data[table]
            for row_data in table_data:
                conv_date(row_data, 'm_anntime', 'm_timetag')
                conv_date(row_data, 'm_timetag', '')
                conv_date(row_data, 'declareDate', '')
                conv_date(row_data, 'endDate', '')
            result[stock][names[table]] = pd.DataFrame(table_data)
    return result


def get_market_data_ori(
    field_list = [], stock_list = [], period = '1d'
    , start_time = '', end_time = '', count = -1
    , dividend_type = 'none', fill_data = True, enable_read_from_server = True
):
    client = get_client()
    enable_read_from_local = period in {'1m', '5m', '15m', '30m', '1h', '1d', 'tick'}
    global debug_mode
    return client.get_market_data3(field_list, stock_list, period, start_time, end_time, count, dividend_type, fill_data, 'v2', enable_read_from_local, enable_read_from_server, debug_mode)


def get_market_data(
    field_list = [], stock_list = [], period = '1d'
    , start_time = '', end_time = '', count = -1
    , dividend_type = 'none', fill_data = True
):
    '''
    获取历史行情数据
    :param field_list: 行情数据字段列表，[]为全部字段
        K线可选字段：
            "time"                #时间戳
            "open"                #开盘价
            "high"                #最高价
            "low"                 #最低价
            "close"               #收盘价
            "volume"              #成交量
            "amount"              #成交额
            "settle"              #今结算
            "openInterest"        #持仓量
        分笔可选字段：
            "time"                #时间戳
            "lastPrice"           #最新价
            "open"                #开盘价
            "high"                #最高价
            "low"                 #最低价
            "lastClose"           #前收盘价
            "amount"              #成交总额
            "volume"              #成交总量
            "pvolume"             #原始成交总量
            "stockStatus"         #证券状态
            "openInt"             #持仓量
            "lastSettlementPrice" #前结算
            "askPrice1", "askPrice2", "askPrice3", "askPrice4", "askPrice5" #卖一价~卖五价
            "bidPrice1", "bidPrice2", "bidPrice3", "bidPrice4", "bidPrice5" #买一价~买五价
            "askVol1", "askVol2", "askVol3", "askVol4", "askVol5"           #卖一量~卖五量
            "bidVol1", "bidVol2", "bidVol3", "bidVol4", "bidVol5"           #买一量~买五量
    :param stock_list: 股票代码 "000001.SZ"
    :param period: 周期 分笔"tick" 分钟线"1m"/"5m"/"15m" 日线"1d"
        Level2行情快照"l2quote" Level2行情快照补充"l2quoteaux" Level2逐笔委托"l2order" Level2逐笔成交"l2transaction" Level2大单统计"l2transactioncount" Level2委买委卖队列"l2orderqueue"
        Level1逐笔成交统计一分钟“transactioncount1m” Level1逐笔成交统计日线“transactioncount1d”
        期货仓单“warehousereceipt” 期货席位“futureholderrank” 互动问答“interactiveqa”
    :param start_time: 起始时间 "20200101" "20200101093000"
    :param end_time: 结束时间 "20201231" "20201231150000"
    :param count: 数量 -1全部/n: 从结束时间向前数n个
    :param dividend_type: 除权类型"none" "front" "back" "front_ratio" "back_ratio"
    :param fill_data: 对齐时间戳时是否填充数据，仅对K线有效，分笔周期不对齐时间戳
        为True时，以缺失数据的前一条数据填充
            open、high、low、close 为前一条数据的close
            amount、volume为0
            settle、openInterest 和前一条数据相同
        为False时，缺失数据所有字段填NaN
    :return: 数据集，分笔数据和K线数据格式不同
        period为'tick'时：{stock1 : value1, stock2 : value2, ...}
            stock1, stock2, ... : 合约代码
            value1, value2, ... : np.ndarray 数据列表，按time增序排列
        period为其他K线周期时：{field1 : value1, field2 : value2, ...}
            field1, field2, ... : 数据字段
            value1, value2, ... : pd.DataFrame 字段对应的数据，各字段维度相同，index为stock_list，columns为time_list
    '''
    if period in {'1m', '5m', '15m', '30m', '1h', '1d'}:
        import pandas as pd
        index, data = get_market_data_ori(field_list, stock_list, period, start_time, end_time, count, dividend_type, fill_data)

        result = {}
        for field in data:
            result[field] = pd.DataFrame(data[field], index = index[0], columns = index[1])
        return result

    if period in {'warehousereceipt', 'futureholderrank', 'interactiveqa'}:
        data = get_bson_data(field_list, stock_list, period, start_time, end_time, count)
        return data

    return get_market_data_ori(field_list, stock_list, period, start_time, end_time, count, dividend_type, fill_data)


def get_market_data_ex_ori(
    field_list = [], stock_list = [], period = '1d'
    , start_time = '', end_time = '', count = -1
    , dividend_type = 'none', fill_data = True, enable_read_from_server = True
):
    client = get_client()
    enable_read_from_local = period in {'1m', '5m', '15m', '30m', '1h', '1d', 'tick'}
    global debug_mode
    return client.get_market_data3(field_list, stock_list, period, start_time, end_time, count, dividend_type, fill_data, 'v3', enable_read_from_local, enable_read_from_server, debug_mode)

def convertTimetag(time, is_upper_bound):
    try:
        time1970 = dt.datetime(1970, 1, 1)

        if len(time) == 8:
            time_obj = dt.datetime.strptime(time, '%Y%m%d')
        elif len(time) == 14:
            time_obj = dt.datetime.strptime(time, '%Y%m%d%H%M%S')
        else:
            raise Exception()

        result = int((time_obj - time1970).total_seconds() * 1000) - 28800000

        if len(time) == 8 and is_upper_bound:
            result = result + 86400000 - 1

        return result

    except Exception as e:
        return None

    return None

def get_bson_data(
    field_list, stock_list, period
    , start_time, end_time, count
):
    client = get_client()
    result = {}

    if start_time == '':
        start_time_num = -0x24BCAEE0800
    else:
        start_time_num = convertTimetag(start_time, False)
    if end_time == '':
        end_time_num = 0x1F3FFFFF830
    else:
        end_time_num = convertTimetag(end_time, True)

    if start_time_num is None or end_time_num is None:
        return result

    period_dict = {
        'warehousereceipt': {'time': 'G', 'warehouse': '1', 'receipt': '2'},
        'futureholderrank': {'time': 'G', 'tradingVolumeRank': '1', 'buyPositionRank': '2', 'sellPositionRank': '3'},
        'interactiveqa': {'time': 'G', 'id': '0', 'questionTime': '1', 'question': '2', 'answerTime': '3', 'answer': '4'}
    }
    field_list = field_list if field_list else list(period_dict[period].keys())

    if period in period_dict:
        field_dict = period_dict[period]
    else:
        return result

    data = {
        'stocklist': stock_list,
        'period': period
    }

    result_bson = client.commonControl('getdatafilepath', bson.BSON.encode(data))
    path_result = bson.BSON.decode(result_bson)
    data_path_dict = path_result.get('result', {})

    import pandas as pd

    for stockcode in data_path_dict:
        try:
            bson_data = open(data_path_dict[stockcode], 'rb').read()
            data_list = xtutil.read_from_bson_buffer(bson_data)

            if len(data_list) == 0:
                raise Exception()

            time_index = []
            cut_data_list = []
            for bson_doc in data_list:
                data_time = bson_doc.get('G', None)
                if data_time == None:
                    raise Exception()
                if start_time_num <= data_time and data_time <= end_time_num:
                    time_index.append(data_time)
                    cut_data_list.append(bson_doc)

            data_list = cut_data_list
        except Exception as e:
            result[stockcode] = pd.DataFrame()
            continue

        if count > 0:
            cut_time_index = []
            cut_data_list = []
            index = max(len(data_list) - count, 0)
            for i in range(index, len(data_list)):
                cut_time_index.append(time_index[i])
                cut_data_list.append(data_list[i])
            time_index = cut_time_index
            data_list = cut_data_list

        stockcode_info = {}
        for key in field_list:
            name = field_dict.get(key, None)
            if name is None:
                continue

            value = []
            for bson_doc in data_list:
                field_value = bson_doc.get(name, None)
                value.append(field_value)

            stockcode_info[key] = value

        datetime_index = [timetag_to_datetime(t, '%Y%m%d') for t in time_index]
        stockcode_info_df = pd.DataFrame(stockcode_info, index = datetime_index)
        result[stockcode] = stockcode_info_df

    return result

def get_market_data_ex(
    field_list = [], stock_list = [], period = '1d'
    , start_time = '', end_time = '', count = -1
    , dividend_type = 'none', fill_data = True
):
    if period in {'1m', '5m', '15m', '30m', '1h', '1d'}:
        return _get_market_data_ex_ori_221207(field_list, stock_list, period, start_time, end_time, count, dividend_type, fill_data)

    if period in {'warehousereceipt', 'futureholderrank', 'interactiveqa'}:
        data = get_bson_data(field_list, stock_list, period, start_time, end_time, count)
        return data

    import pandas as pd
    result = {}

    ifield = 'time'
    query_field_list = field_list if (not field_list) or (ifield in field_list) else [ifield] + field_list
    ori_data = get_market_data_ex_ori(query_field_list, stock_list, period, start_time, end_time, count, dividend_type, fill_data)

    if not ori_data:
        return result

    fl = field_list
    stime_fmt = '%Y%m%d' if period == '1d' else '%Y%m%d%H%M%S'
    if fl:
        fl2 = fl if ifield in fl else [ifield] + fl
        for s in ori_data:
            sdata = pd.DataFrame(ori_data[s], columns = fl2)
            sdata2 = sdata[fl]
            sdata2.index = [timetag_to_datetime(t, stime_fmt) for t in sdata[ifield]]
            result[s] = sdata2
    else:
        for s in ori_data:
            sdata = pd.DataFrame(ori_data[s])
            sdata.index = [timetag_to_datetime(t, stime_fmt) for t in sdata[ifield]]
            result[s] = sdata

    return result


def _get_market_data_ex_ori_221207(
    field_list = [], stock_list = [], period = '1d'
    , start_time = '', end_time = '', count = -1
    , dividend_type = 'none', fill_data = True, enable_read_from_server = True
):
    import numpy as np
    import pandas as pd
    client = get_client()
    enable_read_from_local = period in {'1m', '5m', '15m', '30m', '1h', '1d', 'tick'}
    global debug_mode
    ret = client.get_market_data3(field_list, stock_list, period, start_time, end_time, count, dividend_type, fill_data, 'v4', enable_read_from_local,
                                  enable_read_from_server, debug_mode)
    result = {}
    for stock, index, npdatas in ret:
        data = {field: np.frombuffer(b, fi) for field, fi, b in npdatas}
        result[stock] = pd.DataFrame(data=data, index=index)
    return result

def _get_market_data_ex_221207(
    field_list = [], stock_list = [], period = '1d'
    , start_time = '', end_time = '', count = -1
    , dividend_type = 'none', fill_data = True, enable_read_from_server = True
):
    ifield = 'time'
    query_field_list = field_list if (not field_list) or (ifield in field_list) else [ifield] + field_list

    if period in {'1m', '5m', '15m', '30m', '1h', '1d'}:
        ori_data = _get_market_data_ex_ori_221207(query_field_list, stock_list, period, start_time, end_time, count, dividend_type, fill_data, enable_read_from_server)
    else:
        ori_data = get_market_data_ex_ori(query_field_list, stock_list, period, start_time, end_time, count, dividend_type, fill_data, enable_read_from_server)

    import pandas as pd
    result = {}

    fl = field_list

    if fl:
        fl2 = fl if ifield in fl else [ifield] + fl
        for s in ori_data:
            sdata = pd.DataFrame(ori_data[s], columns = fl2)
            sdata2 = sdata[fl]
            sdata2.index = pd.to_datetime((sdata[ifield] + 28800000) * 1000000)
            result[s] = sdata2
    else:
        for s in ori_data:
            sdata = pd.DataFrame(ori_data[s])
            sdata.index = pd.to_datetime((sdata[ifield] + 28800000) * 1000000)
            result[s] = sdata

    return result


get_market_data3 = _get_market_data_ex_221207

def get_data_dir():
    client = get_client()
    global data_dir
    return data_dir

def get_local_data(field_list=[], stock_list=[], period='1d', start_time='', end_time='', count=-1,
                              dividend_type='none', fill_data=True, data_dir=None):
    if data_dir == None:
        data_dir = get_data_dir()

    if period in {'1m', '5m', '15m', '30m', '1h', '1d'}:
        return _get_market_data_ex_ori_221207(field_list, stock_list, period, start_time, end_time, count,
                                              dividend_type, fill_data, False)

    import pandas as pd
    result = {}

    ifield = 'time'
    query_field_list = field_list if (not field_list) or (ifield in field_list) else [ifield] + field_list
    ori_data = get_market_data_ex_ori(query_field_list, stock_list, period, start_time, end_time, count, dividend_type,
                                      fill_data, False)

    if not ori_data:
        return result

    fl = field_list
    stime_fmt = '%Y%m%d' if period == '1d' else '%Y%m%d%H%M%S'
    if fl:
        fl2 = fl if ifield in fl else [ifield] + fl
        for s in ori_data:
            sdata = pd.DataFrame(ori_data[s], columns = fl2)
            sdata2 = sdata[fl]
            sdata2.index = [timetag_to_datetime(t, stime_fmt) for t in sdata[ifield]]
            result[s] = sdata2
    else:
        for s in ori_data:
            sdata = pd.DataFrame(ori_data[s])
            sdata.index = [timetag_to_datetime(t, stime_fmt) for t in sdata[ifield]]
            result[s] = sdata

    return result


def get_l2_quote(field_list=[], stock_code='', start_time='', end_time='', count=-1):
    '''
    level2实时行情
    '''
    global debug_mode
    client = get_client()
    datas = client.get_market_data3(field_list, [stock_code], 'l2quote', start_time, end_time, count, 'none', False, '', False, True, debug_mode)
    if datas:
        return datas[stock_code]
    return None


def get_l2_order(field_list=[], stock_code='', start_time='', end_time='', count=-1):
    '''
    level2逐笔委托
    '''
    global debug_mode
    client = get_client()
    datas = client.get_market_data3(field_list, [stock_code], 'l2order', start_time, end_time, count, 'none', False, '', False, True, debug_mode)
    if datas:
        return datas[stock_code]
    return None


def get_l2_transaction(field_list=[], stock_code='', start_time='', end_time='', count=-1):
    '''
    level2逐笔成交
    '''
    global debug_mode
    client = get_client()
    datas = client.get_market_data3(field_list, [stock_code], 'l2transaction', start_time, end_time, count, 'none', False, '', False, True, debug_mode)
    if datas:
        return datas[stock_code]
    return None


def get_divid_factors(stock_code, start_time='', end_time=''):
    '''
    获取除权除息日及对应的权息
    :param stock_code: (str)股票代码
    :param date: (str)日期
    :return: pd.DataFrame 数据集
    '''
    client = get_client()
    datas = client.get_divid_factors(stock_code, start_time, end_time)
    import pandas as pd
    datas = pd.DataFrame(datas).T
    return datas


@try_except
def getDividFactors(stock_code, date):
    client = get_client()
    resData = client.get_divid_factors(stock_code, date)
    res = {resData[i]: [resData[i + 1][j] for j in
                        range(0, len(resData[i + 1]), 1)] for i in range(0, len(resData), 2)}
    if isinstance(res, dict):
        for k, v in res.items():
            if isinstance(v, list) and len(v) > 5:
                v[5] = int(v[5])
    return res


def get_main_contract(code_market):
    '''
    获取当前期货主力合约
    :param code_market: (str)股票代码
    :return: str
    '''
    client = get_client()
    return client.get_main_contract(code_market)

def datetime_to_timetag(datetime, format = "%Y%m%d%H%M%S"):
    if len(datetime) == 8:
        format = "%Y%m%d"
    timetag = time.mktime(time.strptime(datetime, format))
    return timetag * 1000

def timetag_to_datetime(timetag, format):
    '''
    将毫秒时间转换成日期时间
    :param timetag: (int)时间戳毫秒数
    :param format: (str)时间格式
    :return: str
    '''
    return timetagToDateTime(timetag, format)


@try_except
def timetagToDateTime(timetag, format):
    import time
    timetag = timetag / 1000
    time_local = time.localtime(timetag)
    return time.strftime(format, time_local)


def get_trading_dates(market, start_time='', end_time='', count=-1):
    '''
    根据市场获取交易日列表
    : param market: 市场代码 e.g. 'SH','SZ','IF','DF','SF','ZF'等
    : param start_time: 起始时间 '20200101'
    : param end_time: 结束时间 '20201231'
    : param count: 数据个数，-1为全部数据
    :return list(long) 毫秒数的时间戳列表
    '''
    client = get_client()
    datas = client.get_trading_dates_by_market(market, start_time, end_time, count)
    return datas


def get_full_tick(code_list):
    '''
    获取盘口tick数据
    :param code_list: (list)stock.market组成的股票代码列表
    :return: dict
    {'stock.market': {dict}}
    '''
    client = get_client()
    resp_json = client.get_full_tick(code_list)
    return json.loads(resp_json)


def subscribe_callback_wrapper(callback):
    import traceback
    def subscribe_callback(datas):
        try:
            if type(datas) == bytes:
                datas = bson.BSON.decode(datas)
            if callback:
                callback(datas)
        except:
            print('subscribe callback error:', callback)
            traceback.print_exc()
    return subscribe_callback


def subscribe_quote(stock_code, period='1d', start_time='', end_time='', count=0, callback=None):
    '''
    订阅股票行情数据
    :param stock_code: 股票代码 e.g. "000001.SZ"
    :param start_time: 开始时间，格式YYYYMMDD/YYYYMMDDhhmmss/YYYYMMDDhhmmss.milli，e.g."20200427" "20200427093000" "20200427093000.000"
        若取某日全量历史数据，时间需要具体到秒，e.g."20200427093000"
    :param end_time: 结束时间 同“开始时间”
    :param count: 数量 -1全部/n: 从结束时间向前数n个
    :param period: 周期 分笔"tick" 分钟线"1m"/"5m" 日线"1d"
    :param callback:
        订阅回调函数onSubscribe(datas)
        :param datas: {stock : [data1, data2, ...]} 数据字典
    :return: int 订阅序号
    '''
    if callback:
        callback = subscribe_callback_wrapper(callback)

    meta = {'stockCode': stock_code, 'period': period}
    region = {'startTime': start_time, 'endTime': end_time, 'count': count}

    client = get_client()
    return client.subscribe_quote(bson.BSON.encode(meta), bson.BSON.encode(region), callback)


def subscribe_l2thousand(stock_code, gear_num = 0, callback = None):
    '''
    订阅千档盘口
    '''
    if callback:
        callback = subscribe_callback_wrapper(callback)

    meta = {'stockCode': stock_code, 'period': 'l2thousand'}
    region = {'thousandGearNum': gear_num, 'thousandDetailGear': 0, 'thousandDetailNum': 0}

    client = get_client()
    return client.subscribe_quote(bson.BSON.encode(meta), bson.BSON.encode(region), callback)


def subscribe_whole_quote(code_list, callback=None):
    '''
    订阅全推数据
    :param code_list: 市场代码列表 ["SH", "SZ"]
    :param callback:
        订阅回调函数onSubscribe(datas)
        :param datas: {stock1 : data1, stock2 : data2, ...} 数据字典
    :return: int 订阅序号
    '''
    if callback:
        callback = subscribe_callback_wrapper(callback)

    client = get_client()
    return client.subscribe_whole_quote(code_list, callback)


def unsubscribe_quote(seq):
    '''
    :param seq: 订阅接口subscribe_quote返回的订阅号
    :return:
    '''
    client = get_client()
    return client.unsubscribe_quote(seq)


def run():
    '''阻塞线程接收行情回调'''
    import time
    client = get_client()
    while True:
        time.sleep(3)
        if not client.is_connected():
            raise Exception('行情服务连接断开')
            break
    return

def create_sector_folder(parent_node,folder_name,overwrite = True):
    '''
    创建板块目录节点
    :parent_node str: 父节点,''为'我的' （默认目录）
    :sector_name str: 要创建的板块目录名称
    :overwrite bool:是否覆盖 True为跳过，False为在folder_name后增加数字编号，编号为从1开始自增的第一个不重复的值
    '''
    client = get_client()
    data = {}
    data['parent'] = parent_node
    data['foldername'] = folder_name
    data['overwrite'] = overwrite
    result_bson = client.commonControl('createsectorfolder', bson.BSON.encode(data))
    result = bson.BSON.decode(result_bson)
    return result.get('result')

def create_sector(parent_node,sector_name,overwrite = True):
    '''
    创建板块
    :parent_node str: 父节点,''为'我的' （默认目录）
    :sector_name str: 要创建的板块名
    :overwrite bool:是否覆盖 True为跳过，False为在sector_name后增加数字编号，编号为从1开始自增的第一个不重复的值
    '''
    client = get_client()
    data = {}
    data['parent'] = parent_node
    data['sectorname'] = sector_name
    data['overwrite'] = overwrite
    result_bson = client.commonControl('createsector', bson.BSON.encode(data))
    result = bson.BSON.decode(result_bson)
    return result.get('result')

def get_sector_list():
    '''
    获取板块列表
    :return: (list[str])
    '''
    client = get_client()
    return client.get_sector_list()


def add_sector(sector_name, stock_list):
    '''
    增加自定义板块
    :param sector_name: 板块名称 e.g. "我的自选"
    :param stock_list: (list)stock.market组成的股票代码列表
    '''
    client = get_client()
    data = {}
    data['sectorname'] = sector_name
    data['stocklist'] = stock_list
    result_bson = client.commonControl('addsector', bson.BSON.encode(data))
    result = bson.BSON.decode(result_bson)
    return result.get('result')

def remove_stock_from_sector(sector_name, stock_list):
    '''
    移除板块成分股
    :param sector_name: 板块名称 e.g. "我的自选"
    :stock_list: (list)stock.market组成的股票代码列表
    '''
    client = get_client()
    data = {}
    data['sectorname'] = sector_name
    data['stocklist'] = stock_list
    result_bson = client.commonControl('removestockfromsector', bson.BSON.encode(data))
    result = bson.BSON.decode(result_bson)
    return result.get('result')

def remove_sector(sector_name):
    '''
    删除自定义板块
    :param sector_name: 板块名称 e.g. "我的自选"
    '''
    client = get_client()
    data = {}
    data['sectorname'] = sector_name
    result_bson = client.commonControl('removesector', bson.BSON.encode(data))
    result = bson.BSON.decode(result_bson)
    return result.get('result')

def reset_sector(sector_name, stock_list):
    '''
    重置板块
    :param sector_name: 板块名称 e.g. "我的自选"
    :stock_list: (list)stock.market组成的股票代码列表
    '''
    client = get_client()
    data = {}
    data['sectorname'] = sector_name
    data['stocklist'] = stock_list
    result_bson = client.commonControl('resetsector', bson.BSON.encode(data))
    result = bson.BSON.decode(result_bson)
    return result.get('result')

def get_instrument_detail(stock_code, iscomplete = False):
    '''
    获取合约信息
    :param stock_code: 股票代码 e.g. "600000.SH"
    :return: dict
        ExchangeID(str):合约市场代码
        , InstrumentID(str):合约代码
        , InstrumentName(str):合约名称
        , ProductID(str):合约的品种ID(期货)
        , ProductName(str):合约的品种名称(期货)
        , ExchangeCode(str):交易所代码
        , UniCode(str):统一规则代码
        , CreateDate(int):上市日期(期货)
        , OpenDate(int):IPO日期(股票)
        , ExpireDate(int):退市日或者到期日
        , PreClose(double):前收盘价格
        , SettlementPrice(double):前结算价格
        , UpStopPrice(double):当日涨停价
        , DownStopPrice(double):当日跌停价
        , FloatVolume(double):流通股本
        , TotalVolume(double):总股本
        , LongMarginRatio(double):多头保证金率
        , ShortMarginRatio(double):空头保证金率
        , PriceTick(double):最小变价单位
        , VolumeMultiple(int):合约乘数(对期货以外的品种，默认是1)
        , MainContract(int):主力合约标记
        , LastVolume(int):昨日持仓量
        , InstrumentStatus(int):合约停牌状态
        , IsTrading(bool):合约是否可交易
        , IsRecent(bool):是否是近月合约,
    '''
    client = get_client()
    inst = client.get_instrument_detail(stock_code)
    if not inst:
        return None

    inst = xtutil.read_from_bson_buffer(inst)

    if len(inst) == 1:
        inst = inst[0]
    else:
        return None

    if iscomplete:
        def convNum2Str(field):
            if field in inst and isinstance(inst[field], int):
                inst[field] = str(inst[field])

        convNum2Str('CreateDate')
        convNum2Str('OpenDate')
        convNum2Str('ExpireDate')

        if 'ExtendInfo' in inst:
            def convNum2Str(field):
                if field in inst['ExtendInfo'] and isinstance(inst['ExtendInfo'][field], int):
                    inst['ExtendInfo'][field] = str(inst['ExtendInfo'][field])

            convNum2Str('EndDelivDate')
        return inst

    field_list = [
            'ExchangeID'
            , 'InstrumentID'
            , 'InstrumentName'
            , 'ProductID'
            , 'ProductName'
            , 'ExchangeCode'
            , 'UniCode'
            , 'CreateDate'
            , 'OpenDate'
            , 'ExpireDate'
            , 'PreClose'
            , 'SettlementPrice'
            , 'UpStopPrice'
            , 'DownStopPrice'
            , 'FloatVolume'
            , 'TotalVolume'
            , 'LongMarginRatio'
            , 'ShortMarginRatio'
            , 'PriceTick'
            , 'VolumeMultiple'
            , 'MainContract'
            , 'LastVolume'
            , 'InstrumentStatus'
            , 'IsTrading'
            , 'IsRecent'
        ]
    ret = {}
    for field in field_list:
        ret[field] = inst.get(field)

    exfield_list = [
            'ProductTradeQuota'
            , 'ContractTradeQuota'
            , 'ProductOpenInterestQuota'
            , 'ContractOpenInterestQuota'
        ]
    inst_ex = inst.get('ExtendInfo', {})
    for field in exfield_list:
        ret[field] = inst_ex.get(field)

    def convNum2Str(field):
        if field in ret and isinstance(ret[field], int):
            ret[field] = str(ret[field])
    convNum2Str('CreateDate')
    convNum2Str('OpenDate')
    return ret


def download_index_weight():
    '''
    下载指数权重数据
    '''
    client = get_client()
    client.down_index_weight()


def download_history_contracts():
    '''
    下载过期合约数据
    '''
    client = get_client()
    client.down_history_contracts()


class TimeListBuilder:
    def __init__(self):
        # param
        self.period = 3600000
        self.open_list = None  # [['093000', '113000'], ['130000', '150000']]

        # build up
        self.cur_date = 0
        self.date_offset = 3600000 * 8
        self.day_time_list = []
        self.cur_index = 0

    def init(self):
        if not self.open_list: return False
        if self.period <= 0: return False

        for scope in self.open_list:
            hour, minute, second = self.parse_time(scope[0])
            start = (((hour * 60) + minute) * 60 + second) * 1000
            hour, minute, second = self.parse_time(scope[1])
            end = (((hour * 60) + minute) * 60 + second) * 1000
            t = start + self.period
            while t <= end:
                self.day_time_list.append(t)
                t += self.period
        self.cur_index = 0

        if not self.day_time_list: return False
        return True

    def parse_time(self, ft):
        ft = int(ft)
        second = ft % 100
        ft = int((ft - second) / 100)
        minute = ft % 100
        ft = int((ft - minute) / 100)
        hour = ft % 100
        return hour, minute, second

    def get(self):
        if self.day_time_list:
            return self.cur_date + self.day_time_list[self.cur_index]
        else:
            return self.cur_date

    def next(self):
        self.cur_index += 1
        if self.cur_index >= len(self.day_time_list):
            self.cur_date += 86400000
            self.cur_index = 0

    def locate(self, t):
        day_time = t % 86400000
        self.cur_date = t - day_time - self.date_offset
        self.cur_index = 0
        for i in range(len(self.day_time_list)):
            te = self.day_time_list[i]
            if t < te:
                self.cur_index = i
                break


class MergeData:
    def __init__(self):
        # param
        self.period = 3600000
        self.open_list = None  # [['093000', '113000'], ['130000', '150000']]
        self.merge_func = None

        # build up
        self.timer = None

        # result
        self.time_list = []
        self.data_list = []

    def init(self):
        self.timer = TimeListBuilder()
        self.timer.open_list = self.open_list
        self.timer.period = self.period
        self.timer.init()

    def push(self, t, data):
        if self.time_list:
            te = self.time_list[-1]
            if t <= te:
                self.data_list[-1] = self.merge_func(self.data_list[-1], data)
            else:
                self.timer.next()
                te = self.timer.get()
                self.time_list.append(te)
                self.data_list.append(data)
        else:
            self.timer.locate(t)

            te = self.timer.get()
            self.time_list.append(te)
            self.data_list.append(data)


def merge_data_sum(data1, data2):
    return data1 + data2


def merge_data_max(data1, data2):
    return max(data1, data2)


def merge_data_min(data1, data2):
    return min(data1, data2)


def merge_data_first(data1, data2):
    return data1


def merge_data_last(data1, data2):
    return data2


def merge_data(time_list, data_list, period, open_list, field):
    merge_func = {}
    merge_func['open'] = merge_data_first
    merge_func['high'] = merge_data_max
    merge_func['low'] = merge_data_min
    merge_func['close'] = merge_data_last
    merge_func['volume'] = merge_data_sum
    merge_func['amount'] = merge_data_sum

    md = MergeData()
    md.period = period
    md.open_list = open_list
    md.merge_func = merge_func[field.lower()]
    md.init()

    for i in range(len(time_list)):
        md.push(time_list[i], data_list[i])

    return md.time_list, md.data_list


def download_history_data(stock_code, period, start_time='', end_time=''):
    '''
    :param stock_code: 股票代码 e.g. "000001.SZ"
    :param period: 周期 分笔"tick" 分钟线"1m"/"5m" 日线"1d"
        Level1逐笔成交统计一分钟“transactioncount1m” Level1逐笔成交统计日线“transactioncount1d”
        期货仓单“warehousereceipt” 期货席位“futureholderrank” 互动问答“interactiveqa”
    :param start_time: 开始时间，格式YYYYMMDD/YYYYMMDDhhmmss/YYYYMMDDhhmmss.milli，e.g."20200427" "20200427093000" "20200427093000.000"
        若取某日全量历史数据，时间需要具体到秒，e.g."20200427093000"
    :param end_time: 结束时间 同上，若是未来某时刻会被视作当前时间
    :return: bool 是否成功
    '''
    client = get_client()
    client.supply_history_data(stock_code, period, start_time, end_time)


supply_history_data = download_history_data


def download_history_data2(stock_list, period, start_time='', end_time='', callback=None):
    '''
    :param stock_code: 股票代码 e.g. "000001.SZ"
    :param period: 周期 分笔"tick" 分钟线"1m"/"5m" 日线"1d"
    :param start_time: 开始时间，格式YYYYMMDD/YYYYMMDDhhmmss/YYYYMMDDhhmmss.milli，e.g."20200427" "20200427093000" "20200427093000.000"
        若取某日全量历史数据，时间需要具体到秒，e.g."20200427093000"
    :param end_time: 结束时间 同上，若是未来某时刻会被视作当前时间
    :return: bool 是否成功
    '''
    client = get_client()

    status = [False, 0, 1, '']
    def on_progress(data):
        try:
            finished = data['finished']
            total = data['total']
            done = (finished >= total)
            status[0] = done
            status[1] = finished
            status[2] = total

            try:
                callback(data)
            except:
                pass

            return done
        except:
            status[0] = True
            status[3] = 'exception'
            return True

    client.supply_history_data2(stock_list, period, start_time, end_time, on_progress)

    import time
    try:
        # pass
        while not status[0] and client.is_connected():
            time.sleep(0.1)
    except:
        if status[1] < status[2]:
            client.stop_supply_history_data2()
        traceback.print_exc()
    if not client.is_connected():
        raise Exception('行情服务连接断开')
    if status[3]:
        raise Exception('下载数据失败：' + status[3])
    return


def download_financial_data(stock_list, table_list=[], start_time='', end_time=''):
    '''
    :param stock_list: 股票代码列表
    :param table_list: 财务数据表名列表，[]为全部表
        可选范围：['Balance','Income','CashFlow','Capital','Top10FlowHolder','Top10Holder','HolderNum','PershareIndex', 'PerShare']
    :param start_time: 开始时间，格式YYYYMMDD，e.g."20200427"
    :param end_time: 结束时间 同上，若是未来某时刻会被视作当前时间
    '''
    client = get_client()
    if not table_list:
        table_list = ['Balance','Income','CashFlow','Capital','Top10FlowHolder','Top10Holder','HolderNum','PershareIndex', 'PerShare']

    for stock_code in stock_list:
        for table in table_list:
            client.supply_history_data(stock_code, table, start_time, end_time)


def download_financial_data2(stock_list, table_list=[], start_time='', end_time='', callback=None):
    '''
    :param stock_list: 股票代码列表
    :param table_list: 财务数据表名列表，[]为全部表
        可选范围：['Balance','Income','CashFlow','Capital','Top10FlowHolder','Top10Holder','HolderNum','PershareIndex', 'PerShare']
    :param start_time: 开始时间，格式YYYYMMDD，e.g."20200427"
    :param end_time: 结束时间 同上，若是未来某时刻会被视作当前时间
    '''
    client = get_client()
    if not table_list:
        table_list = ['Balance','Income','CashFlow','Capital','Top10FlowHolder','Top10Holder','HolderNum','PershareIndex', 'PerShare']

    data = {}
    data['total'] = len(table_list) * len(stock_list)
    finish = 0
    for stock_code in stock_list:
        for table in table_list:
            client.supply_history_data(stock_code, table, start_time, end_time)

            finish = finish + 1
            try:
                data['finished'] = finish
                callback(data)
            except:
                pass

            if not client.is_connected():
                raise Exception('行情服务连接断开')
                break


def get_instrument_type(stock_code, variety_list = None):
    '''
    判断证券类型
    :param stock_code: 股票代码 e.g. "600000.SH"
    :return: dict{str : bool} {类型名：是否属于该类型}
    '''
    client = get_client()
    v_dct = client.get_stock_type(stock_code)#默认处理得到全部品种的信息
    if not v_dct:
        return {}
    v_dct1 = {}
    if variety_list == None or len(variety_list) == 0:#返回该stock_code所有的品种的T/None(False)
        v_dct1={k: v for k, v in v_dct.items() if v}
        return v_dct1

    for v in variety_list:
        if v in v_dct:
            v_dct1[v] = v_dct[v]
    return v_dct1

get_stock_type = get_instrument_type


def download_sector_data():
    '''
    下载行业板块数据
    '''
    client = get_client()
    client.down_all_sector_data()

def get_holidays():
    '''
    获取节假日列表
    :return: 8位int型日期
    '''
    client = get_client()
    return [str(d) for d in client.get_holidays()]


def get_market_last_trade_date(market):
    client = get_client()
    return client.get_market_last_trade_date(market)

def get_trading_calendar(market, start_time = '', end_time = '', tradetimes = False):
    '''
    获取指定市场交易日历
    :param market: str 市场
    :param start_time: str 起始时间 '20200101'
    :param end_time: str 结束时间 '20201231'
    :param tradetimes: bool 是否包含日内交易时段
    :return:
    '''
    holidays_list = get_holidays()   # 19900101格式的数字
    import datetime
    now = datetime.datetime.combine(datetime.date.today(), datetime.time())
    last = datetime.datetime(now.year + 1, 1, 1)

    client = get_client()
    trading_list = [timetag_to_datetime(x, "%Y%m%d") for x in client.get_trading_dates_by_market(market, start_time, end_time, -1)]

    if start_time == '' and trading_list:
        start_time = trading_list[0]
    start = datetime.datetime.strptime(start_time, "%Y%m%d")

    if end_time == '':
        end_time = now.strftime("%Y%m%d")
    end = min(datetime.datetime.strptime(end_time, "%Y%m%d"), last)

       # 时间戳毫秒
    if not trading_list:
        return []

    if not tradetimes:
        ret_list = trading_list
        while now < end:
            now += datetime.timedelta(days=1)
            if datetime.datetime.isoweekday(now) not in [6, 7]:
                ft = now.strftime("%Y%m%d")
                if ft not in holidays_list:
                    ret_list.append(ft)
        return ret_list
    else:
        ret_map = {}
        trading_times = get_trade_times(market)
        new_trading_times_prev = []  #21-24
        new_trading_times_mid = []  #0-3
        new_trading_times_next = [] #9-15

        for tt in trading_times:
            t0 = tt[0]
            t1 = tt[1]
            t2 = tt[2]
            try:
                if t1 <= 0:
                    new_trading_times_prev.append([t0 + 86400, t1 + 86400, t2])
                elif 0 <= t0 and t1 <= 10800:
                    new_trading_times_mid.append(tt)
                elif t0 <= 0 and t1 <= 10800:
                    new_trading_times_prev.append([t0 + 86400, 86400, t2])
                    new_trading_times_mid.append([0, t1, t2])
                else:
                    new_trading_times_next.append(tt)
            except:
                pass

        end = end + datetime.timedelta(days=1)
        prev_open_flag = False
        while start < end:
            weekday = datetime.datetime.isoweekday(start)
            ft = start.strftime("%Y%m%d")
            if weekday not in [6, 7]:
                if ft not in holidays_list:
                    ret_map[ft] = []
                    if prev_open_flag:
                        ret_map[ft].extend(new_trading_times_mid)  # 早盘
                    ret_map[ft].extend(new_trading_times_next)
                    if weekday != 5:
                        if (start + datetime.timedelta(days=1)).strftime("%Y%m%d") not in holidays_list:
                            ret_map[ft].extend(new_trading_times_prev)
                    else:
                        if (start + datetime.timedelta(days=3)).strftime("%Y%m%d") not in holidays_list:
                            ret_map[ft].extend(new_trading_times_prev)
                    prev_open_flag = True
                else:
                    prev_open_flag = False
            start += datetime.timedelta(days=1)
        return ret_map

def get_trade_times(stockcode):
    '''
    返回指定市场或者指定股票的交易时段
    :param stockcode:  市场或者代码.市场  例如 'SH' 或者 '600000.SH'
    :return: 返回交易时段列表，第一位是开始时间，第二位结束时间，第三位交易类型   （2 - 开盘竞价， 3 - 连续交易， 8 - 收盘竞价， 9 - 盘后定价）
    '''
    stockcode_split = stockcode.split('.')
    if len(stockcode_split) == 2:
        ins_dl = get_instrument_detail(stockcode)
        product = ins_dl['ProductID']
        stock = stockcode_split[0]
        market = stockcode_split[1]
        default = 0
    else:
        market = stockcode
        product = ""
        stock = ""
        default = 1

    trader_time = {}
    try:
        with open(os.path.join(data_dir, '../open_quant_trade', 'config', 'tradetimeconfig2.json'), 'r') as f:
            trader_time = json.loads(f.read())
    except:
        pass

    ret = []
    import re
    for tdm in trader_time:
        if tdm['default'] == default and tdm['market'] == market:
            if tdm['product'] == [] and tdm['type'] == "":
                ret = tdm['tradetime'] #默认为product为空的 默认值
            if tdm['type'] != "" and re.match(tdm['type'], stock):
                ret = tdm['tradetime']
                break
            if product != "" and product in tdm['product']:
                ret = tdm['tradetime']
                break

    import datetime
    def convert(t):
        if t == "240000" or t == "-240000":
            return 0
        if t[0] == '-':
            parc = datetime.datetime.strptime(t, "-%H%M%S")
            t = datetime.timedelta(hours=-parc.hour, minutes=-parc.minute)
        else:
            parc = datetime.datetime.strptime(t, "%H%M%S")
            t = datetime.timedelta(hours=parc.hour, minutes=parc.minute)
        return int(t.total_seconds())
    ret = [[convert(timepair[0]), convert(timepair[1]), int(timepair[2])] for timepair in ret]
    return ret

def is_stock_type(stock, tag):
    client = get_client()
    return client.is_stock_type(stock, tag)

def download_cb_data():
    client = get_client()
    return client.down_cb_data()
    
def get_cb_info(stockcode):
    client = get_client()
    return client.get_cb_info(stockcode)
    
def get_option_detail_data(optioncode):
    client = get_client()
    inst = client.get_instrument_detail(optioncode)
    if not inst:
        return None
    
    ret = {}
    market = inst.get('ExchangeID')
    if market == 'SHO' or market == "SZO" or (
            (market == "CFFEX" or market == "IF") and inst.get('InstrumentID').find('-') >= 0):
        field_list = [
            'ExchangeID'
            , 'InstrumentID'
            , 'ProductID'
            , 'OpenDate'
            , 'CreateDate'
            , 'ExpireDate'
            , 'PreClose'
            , 'SettlementPrice'
            , 'UpStopPrice'
            , 'DownStopPrice'
            , 'LongMarginRatio'
            , 'ShortMarginRatio'
            , 'PriceTick'
            , 'VolumeMultiple'
            , 'MaxMarketOrderVolume'
            , 'MinMarketOrderVolume'
            , 'MaxLimitOrderVolume'
            , 'MinLimitOrderVolume'
        ]
        ret = {}
        for field in field_list:
            ret[field] = inst.get(field)

        exfield_list = [
            'OptUnit'
            , 'MarginUnit'
            , 'OptUndlCode'
            , 'OptUndlMarket'
            , 'OptExercisePrice'
            , 'NeeqExeType'
            , 'OptUndlRiskFreeRate'
            , 'OptUndlHistoryRate'
            , 'EndDelivDate'
        ]
        inst_ex = inst.get('ExtendInfo', {})
        for field in exfield_list:
            ret[field] = inst_ex.get(field)

        def convNum2Str(field):
            if field in ret and isinstance(ret[field], int):
                ret[field] = str(ret[field])

        convNum2Str('ExpireDate')
        convNum2Str('CreateDate')
        convNum2Str('OpenDate')
        convNum2Str('EndDelivDate')

        ret["optType"] = ""

        instrumentName = inst.get("InstrumentName")
        if instrumentName.find('C') > 0 or instrumentName.find('购') > 0:
            ret["optType"] = "CALL"
        elif instrumentName.find('P') > 0 or instrumentName.find('沽') > 0:
            ret["optType"] = "PUT"
    return ret


def get_option_undl_data(undl_code_ref):
    def get_option_undl(opt_code):
        inst = get_option_detail_data(opt_code)
        if inst and 'OptUndlCode' in inst and 'OptUndlMarket' in inst:
            return inst['OptUndlCode'] + '.' + inst['OptUndlMarket']
        return ''

    if undl_code_ref:
        opt_list = []
        if undl_code_ref.endswith('.SH'):
            if undl_code_ref == "000016.SH" or undl_code_ref == "000300.SH" or undl_code_ref == "000852.SH" or undl_code_ref == "000905.SH":
                opt_list = get_stock_list_in_sector('中金所')
            else:
                opt_list = get_stock_list_in_sector('上证期权')
        if undl_code_ref.endswith('.SZ'):
            opt_list = get_stock_list_in_sector('深证期权')
        data = []
        for opt_code in opt_list:
            undl_code = get_option_undl(opt_code)
            if undl_code == undl_code_ref:
                data.append(opt_code)
        return data
    else:
        opt_list = []
        opt_list += get_stock_list_in_sector('上证期权')
        opt_list += get_stock_list_in_sector('深证期权')
        opt_list += get_stock_list_in_sector('中金所')
        result = {}
        for opt_code in opt_list:
            undl_code = get_option_undl(opt_code)
            if undl_code:
                if undl_code in result:
                    result[undl_code].append(opt_code)
                else:
                    result[undl_code] = [opt_code]
        return result


def get_option_list(undl_code, dedate, opttype = "", isavailavle = False):
    result = []

    marketcodeList = undl_code.split('.')
    if (len(marketcodeList) != 2):
        return []
    undlCode = marketcodeList[0]
    undlMarket = marketcodeList[1]
    market = ""
    if (undlMarket == "SH"):
        if undlCode == "000016" or undlCode == "000300" or undlCode == "000852" or undlCode == "000905":
            market = 'IF'
        else:
            market = "SHO"
    elif (undlMarket == "SZ"):
        market = "SZO"
    if (opttype.upper() == "C"):
        opttype = "CALL"
    elif (opttype.upper() == "P"):
        opttype = "PUT"
    optList = []
    if market == 'SHO':
        optList += get_stock_list_in_sector('上证期权')
        optList += get_stock_list_in_sector('过期上证期权')
    elif market == 'SZO':
        optList += get_stock_list_in_sector('深证期权')
        optList += get_stock_list_in_sector('过期深证期权')
    elif market == 'IF':
        optList += get_stock_list_in_sector('中金所')
        optList += get_stock_list_in_sector('过期中金所')
    for opt in optList:
        if (opt.find(market) < 0):
            continue
        inst = get_option_detail_data(opt)
        if not inst:
            continue
        if (opttype.upper() != "" and opttype.upper() != inst["optType"]):
            continue
        if ((len(dedate) == 6 and inst['ExpireDate'].find(dedate) < 0)):
            continue
        if (len(dedate) == 8):  # option is trade,guosen demand
            createDate = inst['CreateDate']
            openDate = inst['OpenDate']
            if (createDate > '0'):
                openDate = min(openDate, createDate)
            if (openDate < '20150101' or openDate > dedate):
                continue
            endDate = inst['ExpireDate']
            if (isavailavle and endDate < dedate):
                continue
        if inst['OptUndlCode'].find(undlCode) >= 0:
            result.append(opt)
    return result


def get_ipo_info(start_time = '', end_time = ''):
    client = get_client()
    data = client.get_ipo_info(start_time, end_time)
    pylist = [
        'securityCode'          #证券代码
        , 'codeName'            #代码简称
        , 'market'              #所属市场
        , 'actIssueQty'         #发行总量  单位：股
        , 'onlineIssueQty'      #网上发行量  单位：股
        , 'onlineSubCode'       #申购代码
        , 'onlineSubMaxQty'     #申购上限  单位：股
        , 'publishPrice'        #发行价格
        , 'startDate'           #申购开始日期
        , 'onlineSubMinQty'     #最小申购数，单位：股
        , 'isProfit'            #是否已盈利 0：上市时尚未盈利 1：上市时已盈利
        , 'industryPe'          #行业市盈率
        , 'beforePE'            #发行前市盈率
        , 'afterPE'             #发行后市盈率
        , 'listedDate'          #上市日期
        , 'declareDate'         #中签号公布日期
        , 'paymentDate'         #中签缴款日
        , 'lwr'                 #中签率
    ]
    result = []
    for datadict in data:
        resdict = {}
        for field in pylist:
            resdict[field] = datadict.get(field)
        result.append(resdict)
    return result


def get_markets():
    return [
        'SH', 'SZ', 'BJ', 'HK', 'HGT', 'SGT',
        'IF', 'SF', 'DF', 'ZF', 'GF', 'INE',
        'SHO', 'SZO',
        'BKZS', 'WP'
    ]

def get_his_st_data(stock_code):
    fileName = os.path.join(get_data_dir(), '../open_quant_trade', 'data', 'SH_XXXXXX_2011_86400000.csv')

    try:
        with open(fileName, "r") as f:
            datas = f.readlines()
    except:
        return {}

    status = []
    for data in datas:
        cols = data.split(',')
        if len(cols) >= 4 and cols[0] == stock_code:
            status.append((cols[2], cols[3]))

    if not status:
        return {}

    result = {}
    i = 0
    while i < len(status):
        start = status[i][0]
        flag = status[i][1]

        i += 1

        end = '20380119'
        if i < len(status):
            end = status[i][0]

        realStatus = ''
        if (flag == '1'):
            realStatus = 'ST'
        elif (flag == '2'):
            realStatus = '*ST'
        elif (flag == '3'):
            realStatus = 'PT'
        else:
            continue

        if realStatus not in result:
            result[realStatus] = []
        result[realStatus].append([start, end])

    return result


def set_instrument_detail(market, stock, stock_name, abbreviation, preclose):
    data = [{"ExchangeID": market, "InstrumentID": stock, "InstrumentName": stock_name, "abbreviation": abbreviation, "PreClose": preclose}]
    client = get_client()
    client.custom_data_control("set_instrument_detail", bson.BSON.encode({"data": data}))

def set_instrument_data2(instruments = []):
    for inst in instruments:
        if type(inst) != dict:
            return "error"
    client = get_client()
    client.custom_data_control("set_instrument_detail", bson.BSON.encode({"data": instruments}))

def create_formula(formula_name, stock_code, period, start_time = '', end_time = '', count = -1, dividend_type = 'none', extend_param = {}, callback = None):
    cl = get_client()

    result = bson.BSON.decode(cl.commonControl('createrequestid', bson.BSON.encode({})))
    request_id = result['result']

    data = {
        'formulaname': formula_name, 'stockcode': stock_code, 'period': period
        , 'starttime': start_time, 'endtime': end_time, 'count': count
        , 'dividendtype': dividend_type, 'extendparam': extend_param
        , 'create': True
    }

    if callback:
        callback = subscribe_callback_wrapper(callback)

    cl.subscribeFormula(request_id, bson.BSON.encode(data), callback)
    return request_id

def subscribe_formula(request_id, callback = None):
    cl = get_client()

    if callback:
        callback = subscribe_callback_wrapper(callback)

    cl.subscribeFormula(request_id, bson.BSON.encode({}), callback)
    return

def unsubscribe_formula(request_id):
    cl = get_client()
    cl.subscribeFormula(request_id)
    return

def call_formula(
    formula_name, stock_code, period
    , start_time = '', end_time = '', count = -1
    , dividend_type = 'none', extend_param = {}
):
    cl = get_client()

    result = bson.BSON.decode(cl.commonControl('createrequestid', bson.BSON.encode({})))
    request_id = result['result']

    data = {
        'formulaname': formula_name, 'stockcode': stock_code, 'period': period
        , 'starttime': start_time, 'endtime': end_time, 'count': count
        , 'dividendtype': dividend_type, 'extendparam': extend_param
        , 'create': True
    }

    data = cl.subscribeFormulaSync(request_id, bson.BSON.encode(data))
    return bson.BSON.decode(data)

gmd = get_market_data
gmd2 = get_market_data_ex
gmd3 = get_market_data3
gld = get_local_data
t2d = timetag_to_datetime
gsl = get_stock_list_in_sector


def reset_market_trading_day_list(market, datas):
    cl = get_client()

    result = __bsoncall_common(
        cl.custom_data_control, 'createmarketchange'
        , {
            'market': market
        }
    )
    cid = result['cid']

    result = __bsoncall_common(
        cl.custom_data_control, 'addtradingdaytochange'
        , {
            'cid': cid
            , 'datas': datas
            , 'coverall': True
        }
    )

    result = __bsoncall_common(
        cl.custom_data_control, 'finishmarketchange'
        , {
            'cid': cid
            #, 'abort': False
            , 'notifyupdate': True
        }
    )
    return


def reset_market_stock_list(market, datas):
    cl = get_client()

    result = __bsoncall_common(
        cl.custom_data_control, 'createmarketchange'
        , {
            'market': market
        }
    )
    cid = result['cid']

    result = __bsoncall_common(
        cl.custom_data_control, 'addstocktochange'
        , {
            'cid': cid
            , 'datas': datas
            , 'coverall': True
        }
    )

    result = __bsoncall_common(
        cl.custom_data_control, 'finishmarketchange'
        , {
            'cid': cid
            #, 'abort': False
            , 'notifyupdate': True
        }
    )
    return


def push_custom_data(meta, datas, coverall = False):
    cl = get_client()
    result = __bsoncall_common(
        cl.custom_data_control, 'pushcustomdata'
        , {
            "meta": meta
            , 'datas': datas
            , 'coverall': coverall
        }
    )
    return

def get_period_list():
    client = get_client()
    result = bson.BSON.decode(client.commonControl('getperiodlist', bson.BSON.encode({})))
    request_id = result['result']
    return request_id

