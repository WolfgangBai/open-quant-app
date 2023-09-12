from loguru import logger
from datetime import datetime, timedelta

from threading import Thread
import time

import xtquant.xtdata as xt_data


class DownloadUtils:
    @staticmethod
    def on_download_progress(data):
        logger.info(f"downloading... total = {data['total']}, finished = {data['finished']}")

    @staticmethod
    def download_history_data(config: dict, period_list: [str], back_days: int = 15, start_time: str = "",
                              end_time: str = ""):
        # parse
        stock_ids = []
        for i in range(len(config['stock']['stock_ids'])):
            for j in range(len(config['stock']['stock_ids'][i])):
                stock_ids.append(config['stock']['stock_ids'][i][j])
        # use an extra thread to download
        download_thread = None
        for period in period_list:
            if start_time == "" or end_time == "":
                end_timestamp = datetime.now()
                start_timestamp = end_timestamp - timedelta(days=back_days)
                download_thread = Thread(target=xt_data.download_history_data2,
                                         args=(stock_ids, period, start_timestamp.strftime("%Y%m%d")
                                               , end_timestamp.strftime("%Y%m%d"), DownloadUtils.on_download_progress))
            else:
                download_thread = Thread(target=xt_data.download_history_data2,
                                         args=(stock_ids, period, start_time
                                               , end_time, DownloadUtils.on_download_progress))
        download_thread.start()
        time.sleep(5)
