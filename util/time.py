from datetime import datetime, timedelta

import pytz

DEFAULT_TZ = pytz.timezone('hongkong')


def now_time() -> datetime:
    return datetime.now(DEFAULT_TZ)


def convert_interval_to_timedelta(time_interval: str) -> timedelta:
    if time_interval.endswith('m') or time_interval.endswith('T'):
        return timedelta(minutes=int(time_interval[:-1]))

    if time_interval.endswith('H') or time_interval.endswith('h'):
        return timedelta(hours=int(time_interval[:-1]))

    raise ValueError('time_interval %s format error', time_interval)