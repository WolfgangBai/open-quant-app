from typing import List

from loguru import logger
from xtquant.xttype import XtPosition


class PositionManager:
    def __init__(self, config: dict):
        self.position_avg_mode = config['assets']['position_avg_mode']
        self.num = len(config['stock']['stock_ids'])
        self.positions = []
        if self.position_avg_mode:
            for i in range(self.num):
                self.positions.append(1.0 / self.num)
        else:
            self.positions = config['assets']['positions']
            if len(self.positions) != self.num:
                logger.error(f"unequal position setting: {self.num} stock tuples, but {len(self.positions)} "
                             f"position limits")

    def is_position_limit(self, position_data: List[XtPosition], strategy_id: int, volume: int, price: float,
                          total_assets: float) -> bool:
        stock_assets = 0
        for position in position_data:
            if position is None:
                return False
            stock_assets += position.open_price * (position.can_use_volume + position.frozen_volume)
        if stock_assets + price * volume >= self.positions[strategy_id] * total_assets:
            logger.warning(f"potential stock asset = {stock_assets + price * volume}, position limit "
                           f"= {self.positions[strategy_id]}, cannot buy more !")
            return True
        else:
            return False
