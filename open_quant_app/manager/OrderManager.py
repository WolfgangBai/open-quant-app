import enum
from typing import Dict, List
from datetime import datetime, timedelta

import open_quant_app.xtquant.xtdata as xt_data
from open_quant_app.trade.Trader import Trader

from loguru import logger


class Order:
    def __init__(self, order_id: int, order_timestamp: datetime):
        self.order_id: int = order_id
        self.order_timestamp = order_timestamp


class OrderTuple:
    def __init__(self, high_stock_id: str, low_stock_id: str, high_order: Order, low_order: Order):
        self.high_stock_id: str = high_stock_id
        self.low_stock_id: str = low_stock_id
        self.order_tuple: Dict[str, Order] = {
            low_stock_id: low_order,
            high_stock_id: high_order
        }
        self.status: OrderStatus = OrderStatus.INIT
        self.order_timestamp: datetime = low_order.order_timestamp  # low / high timestamp is the same

    def get_high_order(self) -> Order:
        return self.order_tuple[self.high_stock_id]

    def get_low_order(self) -> Order:
        return self.order_tuple[self.low_stock_id]


class OrderStatus(enum.Enum):
    INIT = 1
    SELL_UNFINISHED = 2
    BUY_UNFINISHED = 3
    FINISHED = 4


class OrderManager:
    def __init__(self, trader: Trader, stock_ids: List[str], delay: float = 1, sliding_point: float = 0.0005):
        self.orders: List[OrderTuple] = []
        self.trader = trader
        self.stock_ids = stock_ids
        self.low_stock = self.stock_ids[0]
        self.high_stock = self.stock_ids[1]
        self.delay = delay
        self.sliding_point: float = sliding_point

    def insert(self, order_tuple: OrderTuple):
        self.orders.append(order_tuple)

    def size(self) -> int:
        return len(self.orders)

    def empty(self) -> bool:
        return self.size() == 0

    def clear_finished(self):
        self.orders = list(filter(lambda order_tuple: order_tuple.status != OrderStatus.FINISHED, self.orders))

    def get_latest_price(self) -> dict:
        # get data
        timestamp = datetime.now()
        end_timestamp = (timestamp + timedelta(minutes=1)).strftime("%Y%m%d%H%M%S")
        prev_timestamp = (timestamp + timedelta(minutes=-1)).strftime("%Y%m%d%H%M%S")
        data = xt_data.get_market_data(field_list=['askPrice', 'bidPrice'],
                                       stock_list=self.stock_ids, period='tick', count=1,
                                       start_time=prev_timestamp, end_time=end_timestamp,
                                       dividend_type='front', fill_data=True)
        # subtract price
        low_bid_price = data[self.low_stock]['bidPrice'][-1][0] if len(data[self.low_stock]['bidPrice']) != 0 else -1
        high_bid_price = data[self.high_stock]['bidPrice'][-1][0] if len(data[self.high_stock]['bidPrice']) != 0 else -1
        low_ask_price = data[self.low_stock]['askPrice'][-1][0] if len(data[self.low_stock]['askPrice']) != 0 else -1
        high_ask_price = data[self.high_stock]['askPrice'][-1][0] if len(data[self.high_stock]['askPrice']) != 0 else -1

        return {
            "low_bid_price": low_bid_price,
            "high_bid_price": high_bid_price,
            "low_ask_price": low_ask_price,
            "high_ask_price": high_ask_price
        }

    def handle_once(self, order_tuple: OrderTuple) -> OrderTuple:
        # if delta t < delay, skip the check
        if (order_tuple.order_timestamp + timedelta(seconds=self.delay)) >= datetime.now() \
                or order_tuple.status == OrderStatus.FINISHED:
            logger.info(f"waiting for order at {order_tuple.order_timestamp}")
            return order_tuple
        # handle history order
        # get order data
        high_order = order_tuple.get_high_order()
        low_order = order_tuple.get_low_order()

        high_order_info = self.trader.query_order_by_id(high_order.order_id)
        low_order_info = self.trader.query_order_by_id(low_order.order_id)

        if high_order_info is None or low_order_info is None:
            order_tuple.status = OrderStatus.FINISHED
            return order_tuple

        high_traded_vol, high_order_vol = high_order_info.traded_volume, high_order_info.order_volume
        low_traded_vol, low_order_vol = low_order_info.traded_volume, low_order_info.order_volume

        low_order_type = low_order_info.order_type
        high_order_type = high_order_info.order_type

        min_traded_vol = min(high_traded_vol, low_traded_vol)
        max_traded_vol = max(high_traded_vol, low_traded_vol)
        diff_traded_vol = max_traded_vol - min_traded_vol

        logger.warning(f"handling unfinished order...")
        logger.warning(f"low_traded_vol = {low_traded_vol}, high_traded_vol = {high_traded_vol}")
        logger.warning(f"low_order_vol = {low_order_vol}, high_order_vol = {high_order_vol}")
        logger.warning(f"canceling orders: low_id = {low_order.order_id}, high_id = {high_order.order_id} ...\n")

        # cancel prev order
        self.trader.cancel_order_stock(high_order.order_id)
        self.trader.cancel_order_stock(low_order.order_id)

        # status
        order_tuple.status = OrderStatus.FINISHED

        return order_tuple

        # if low_traded_vol == 0 and high_traded_vol == 0:
        #     pass
        # elif low_traded_vol == low_order_vol and high_traded_vol == high_order_vol:
        #     pass
        # elif low_traded_vol < high_traded_vol:
        #     price_data = self.get_latest_price()
        #     if low_order_type == xtconstant.STOCK_BUY:
        #         self.trader.order_stock(self.low_stock, low_order_type, diff_traded_vol,
        #                                 price_data['low_ask_price'] + 0.001)
        #     elif low_order_type == xtconstant.STOCK_SELL:
        #         self.trader.order_stock(self.low_stock, low_order_type, diff_traded_vol,
        #                                 price_data['low_bid_price'] - 0.001)
        # elif low_traded_vol > high_traded_vol:
        #     price_data = self.get_latest_price()
        #     if high_order_type == xtconstant.STOCK_BUY:
        #         self.trader.order_stock(self.high_stock, high_order_type, diff_traded_vol,
        #                                 price_data['high_ask_price'] + 0.001)
        #     elif high_order_type == xtconstant.STOCK_SELL:
        #         self.trader.order_stock(self.high_stock, high_order_type, diff_traded_vol,
        #                                 price_data['high_bid_price'] - 0.001)
        # elif low_traded_vol == high_traded_vol:
        #     pass
        #
        # return order_tuple

    def handle(self):
        logger.success(f"handle begin. {self.size()} order tuples left")
        for i in range(self.size()):
            self.orders[i] = self.handle_once(self.orders[i])
        self.clear_finished()

        logger.success(f"handle done. {self.size()} order tuples left")
