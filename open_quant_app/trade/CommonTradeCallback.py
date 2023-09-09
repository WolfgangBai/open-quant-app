from open_quant_app.xtquant.xttrader import XtQuantTraderCallback
from open_quant_app.xtquant.xttype import XtOrder, XtAsset, XtOrderError, XtOrderResponse, XtPosition, XtCancelError, XtAccountStatus, \
    XtTrade


class CommonXtQuantTraderCallback(XtQuantTraderCallback):
    def on_disconnected(self):
        print("Warning: connection lost!")

    def on_stock_order(self, order: XtOrder):
        print("on order callback")
        print(order.stock_code, order.order_status, order.order_sysid)

    def on_stock_asset(self, asset: XtAsset):
        """
        资金变动推送  注意，该回调函数目前不生效
        :param asset: XtAsset对象
        :return:
        """
        print("on asset callback")
        print(asset.account_id, asset.cash, asset.total_asset)

    def on_stock_trade(self, trade: XtTrade):
        """
        成交变动推送
        :param trade: XtTrade对象
        :return:
        """
        print("on trade callback")
        print(trade.account_id, trade.stock_code, trade.order_id)

    def on_stock_position(self, position: XtPosition):
        """
        持仓变动推送  注意，该回调函数目前不生效
        :param position: XtPosition对象
        :return:
        """
        print("on position callback")
        print(position.stock_code, position.volume)

    def on_order_error(self, order_error: XtOrderError):
        """
        委托失败推送
        :param order_error:XtOrderError 对象
        :return:
        """
        print("on order_error callback")
        print(order_error.order_id, order_error.error_id, order_error.error_msg)

    def on_cancel_error(self, cancel_error: XtCancelError):
        """
        撤单失败推送
        :param cancel_error: XtCancelError 对象
        :return:
        """
        print("on cancel_error callback")
        print(cancel_error.order_id, cancel_error.error_id, cancel_error.error_msg)

    def on_order_stock_async_response(self, response: XtOrderResponse):
        """
        异步下单回报推送
        :param response: XtOrderResponse 对象
        :return:
        """
        print("on_order_stock_async_response")
        print(response.account_id, response.order_id, response.seq)

    def on_account_status(self, status: XtAccountStatus):
        """
        :param response: XtAccountStatus 对象
        :return:
        """
        print("on_account_status")
        print(status.account_id, status.account_type, status.status)
