import asyncio
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from dateutil import parser as date_parser

DEFAULT_TZ = ZoneInfo("Asia/Shanghai")


def now_time() -> datetime:
    return datetime.now(DEFAULT_TZ)


def convert_interval_to_timedelta(time_interval: str) -> timedelta:
    if time_interval.endswith("m") or time_interval.endswith("T"):
        return timedelta(minutes=int(time_interval[:-1]))

    if time_interval.endswith("H") or time_interval.endswith("h"):
        return timedelta(hours=int(time_interval[:-1]))

    if time_interval.endswith("D") or time_interval.endswith("d"):
        return timedelta(days=int(time_interval[:-1]))
        
    raise ValueError("time_interval %s format error", time_interval)


def convert_date(dt) -> date:
    if isinstance(dt, str):
        return date_parser.parse(dt).date()
    return dt


async def async_sleep_until_run_time(run_time):
    sleep_seconds = (run_time - now_time()).total_seconds()
    await asyncio.sleep(max(0, sleep_seconds - 1))
    while now_time() < run_time:
        await asyncio.sleep(0.001)


def next_run_time(time_interval):
    """
    Calculate the next run time based on the time_interval.
    Currently, only supports minutes and hours.
    :param time_interval: The running cycle, e.g., 15m, 1h
    :return: The next run time
    Example:
    15m  Current time: 12:50:51  Returns: 13:00:00
    15m  Current time: 12:39:51  Returns: 12:45:00
    """
    ti = convert_interval_to_timedelta(time_interval)

    now = now_time()
    this_midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    min_step = timedelta(minutes=1)

    target_time = now.replace(second=0, microsecond=0)

    while True:
        target_time = target_time + min_step
        delta = target_time - this_midnight
        if delta.seconds % ti.seconds == 0:
            break

    return target_time
