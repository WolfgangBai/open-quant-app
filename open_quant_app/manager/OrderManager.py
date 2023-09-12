import enum
from typing import Dict, List
from datetime import datetime, timedelta

from open_quant_app.trade.Trader import Trader

from loguru import logger


class Order:
    def __init__(self, order_id: int, order_timestamp: datetime):
        self.order_id: int = order_id
        self.order_timestamp = order_timestamp


class OrderTuple:
    def __init__(self, stock_ids: [str], orders: [Order]):
        self.stock_ids: [str] = stock_ids
        self.orders: [Order] = orders
        self.status: OrderStatus = OrderStatus.INIT


class OrderStatus(enum.Enum):
    INIT = 1
    SELL_UNFINISHED = 2
    BUY_UNFINISHED = 3
    FINISHED = 4


class OrderManager:
    def __init__(self, trader: Trader, stock_ids: List[str], delay: float = 1, sliding_point: float = 0.0005):
        self.orders: List[OrderTuple] = []
        self.trader: Trader = trader
        self.stock_ids: [str] = stock_ids
        self.delay: float = delay
        self.sliding_point: float = sliding_point

    def insert(self, order_tuple: OrderTuple):
        self.orders.append(order_tuple)

    def size(self) -> int:
        return len(self.orders)

    def empty(self) -> bool:
        return self.size() == 0

    def clear_finished(self):
        self.orders = list(filter(lambda order_tuple: order_tuple.status != OrderStatus.FINISHED, self.orders))

    def handle_once(self, order_tuple: OrderTuple) -> OrderTuple:
        return order_tuple

    def handle(self):
        logger.success(f"handle begin. {self.size()} order tuples left")
        for i in range(self.size()):
            self.orders[i] = self.handle_once(self.orders[i])
        self.clear_finished()

        logger.success(f"handle done. {self.size()} order tuples left")
