import datetime


class TimeUtils:
    @staticmethod
    def get_morning_start() -> datetime.datetime:
        return datetime.datetime.now().replace(hour=9, minute=30, second=0)

    @staticmethod
    def get_morning_end() -> datetime.datetime:
        return datetime.datetime.now().replace(hour=11, minute=30, second=0, microsecond=0)

    @staticmethod
    def get_afternoon_start() -> datetime.datetime:
        return datetime.datetime.now().replace(hour=13, minute=0, second=0)

    @staticmethod
    def get_afternoon_end() -> datetime.datetime:
        return datetime.datetime.now().replace(hour=15, minute=0, second=0)

    @staticmethod
    def get_trade_time_seg() -> [datetime.datetime]:
        morning_start = datetime.datetime.now().replace(hour=9, minute=30, second=0, microsecond=0)
        morning_end = morning_start + datetime.timedelta(hours=2)

        afternoon_start = datetime.datetime.now().replace(hour=13, minute=0, second=0, microsecond=0)
        afternoon_end = afternoon_start + datetime.timedelta(hours=2)

        trade_times = [morning_start, morning_end, afternoon_start, afternoon_end]
        return trade_times

    @staticmethod
    def judge_trade_time(timestamp: datetime.datetime) -> bool:
        now = datetime.datetime.now()
        year, month, day = now.year, now.month, now.day
        morning_start = datetime.datetime(year, month, day, 9, 30, 0)
        morning_end = datetime.datetime(year, month, day, 11, 30, 0)
        afternoon_start = datetime.datetime(year, month, day, 13, 0, 0)
        afternoon_end = datetime.datetime(year, month, day, 15, 0, 0)
        return (morning_start <= timestamp <= morning_end) or (
                afternoon_start <= timestamp <= afternoon_end)

    @staticmethod
    def next_trade_timestamp(timestamp: datetime.datetime, period=1) -> datetime.datetime:
        year, month, day = timestamp.year, timestamp.month, timestamp.day
        morning_start = datetime.datetime(year, month, day, 9, 30, 0)
        morning_end = datetime.datetime(year, month, day, 11, 30, 0)
        afternoon_start = datetime.datetime(year, month, day, 13, 0, 0)
        afternoon_end = datetime.datetime(year, month, day, 15, 0, 0)
        if TimeUtils.judge_trade_time(timestamp):
            return timestamp + datetime.timedelta(seconds=period)
        else:
            if timestamp > morning_end and timestamp < afternoon_start:
                return afternoon_start
            elif timestamp > afternoon_end:
                return morning_start + datetime.timedelta(days=1)
            else:
                return timestamp + datetime.timedelta(seconds=period)
