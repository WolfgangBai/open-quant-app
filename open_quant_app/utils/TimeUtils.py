from datetime import datetime, timedelta


class TimeUtils:
    @staticmethod
    def get_morning_start(timestamp: datetime = datetime.now()) -> datetime:
        return timestamp.replace(hour=9, minute=30, second=0)

    @staticmethod
    def get_morning_end(timestamp: datetime = datetime.now()) -> datetime:
        return timestamp.replace(hour=11, minute=30, second=0, microsecond=0)

    @staticmethod
    def get_afternoon_start(timestamp: datetime = datetime.now()) -> datetime:
        return timestamp.replace(hour=13, minute=0, second=0)

    @staticmethod
    def get_afternoon_end(timestamp: datetime = datetime.now()) -> datetime:
        return timestamp.replace(hour=15, minute=0, second=0)

    @staticmethod
    def get_trade_time_seg() -> [datetime]:
        morning_start = datetime.now().replace(hour=9, minute=30, second=0, microsecond=0)
        morning_end = morning_start + timedelta(hours=2)

        afternoon_start = datetime.now().replace(hour=13, minute=0, second=0, microsecond=0)
        afternoon_end = afternoon_start + timedelta(hours=2)

        trade_times = [morning_start, morning_end, afternoon_start, afternoon_end]
        return trade_times

    @staticmethod
    def judge_trade_time(timestamp: datetime) -> bool:
        return (TimeUtils.get_morning_start(timestamp) <= timestamp <= TimeUtils.get_morning_end(timestamp)) or (
                TimeUtils.get_afternoon_start(timestamp) <= timestamp <= TimeUtils.get_afternoon_end(timestamp))

    @staticmethod
    def next_trade_timestamp(timestamp: datetime, period=1) -> datetime:
        if TimeUtils.judge_trade_time(timestamp):
            return timestamp + timedelta(seconds=period)
        else:
            if TimeUtils.get_morning_end(timestamp) < timestamp < TimeUtils.get_afternoon_end(timestamp):
                return TimeUtils.get_afternoon_start(timestamp)
            elif timestamp > TimeUtils.get_afternoon_end(timestamp):
                return TimeUtils.get_morning_start(timestamp) + timedelta(days=1)
            else:
                return timestamp + timedelta(seconds=period)
