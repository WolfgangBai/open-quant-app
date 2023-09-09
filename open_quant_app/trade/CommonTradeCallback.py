from open_quant_app.xtquant.xttrader import XtQuantTraderCallback
from open_quant_app.xtquant.xttype import XtOrder, XtAsset, XtOrderError, XtOrderResponse, XtPosition, XtCancelError, \
    XtAccountStatus, XtTrade

from loguru import logger


class CommonXtQuantTraderCallback(XtQuantTraderCallback):
    def on_disconnected(self):
        logger.error("Warning: connection lost!")

    def on_stock_order(self, order: XtOrder):
        logger.info("on order callback")
        logger.info(order.stock_code, order.order_status, order.order_sysid)

    def on_stock_asset(self, asset: XtAsset):
        logger.info("on asset callback")
        logger.info(asset.account_id, asset.cash, asset.total_asset)

    def on_stock_trade(self, trade: XtTrade):
        logger.info("on trade callback")
        logger.info(trade.account_id, trade.stock_code, trade.order_id)

    def on_stock_position(self, position: XtPosition):
        logger.info("on position callback")
        logger.info(position.stock_code, position.volume)

    def on_order_error(self, order_error: XtOrderError):
        logger.info("on order_error callback")
        logger.info(order_error.order_id, order_error.error_id, order_error.error_msg)

    def on_cancel_error(self, cancel_error: XtCancelError):
        logger.info("on cancel_error callback")
        logger.info(cancel_error.order_id, cancel_error.error_id, cancel_error.error_msg)

    def on_order_stock_async_response(self, response: XtOrderResponse):
        logger.info("on_order_stock_async_response")
        logger.info(response.account_id, response.order_id, response.seq)

    def on_account_status(self, status: XtAccountStatus):
        logger.info("on_account_status")
        logger.info(status.account_id, status.account_type, status.status)
