import asyncio
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from dateutil import parser as date_parser

DEFAULT_TZ = ZoneInfo("Asia/Hong_Kong")


def now_time(tz: ZoneInfo = DEFAULT_TZ) -> datetime:
    return datetime.now(tz)


def convert_interval_to_timedelta(time_interval: str) -> timedelta:
    """
    Convert a time interval string to a Python timedelta object.
    
    This function parses time interval strings commonly used in Binance API and
    trading contexts, converting them into equivalent timedelta objects for
    datetime calculations.
    
    Args:
        time_interval (str): The time interval string to convert. Supported formats:
            - Minutes: "1m", "5m", "15m", "30m", "60m", "1T", "5T", etc.
            - Hours: "1h", "2h", "4h", "6h", "8h", "12h", "1H", "2H", etc.
            - Days: "1d", "3d", "7d", "30d", "1D", "3D", etc.
            
    Returns:
        timedelta: A timedelta object representing the equivalent duration.
        
    Raises:
        ValueError: If the time_interval format is not supported or cannot be parsed.
        
    Examples:
        >>> convert_interval_to_timedelta("5m")
        datetime.timedelta(seconds=300)
        >>> convert_interval_to_timedelta("2h")
        datetime.timedelta(seconds=7200)
        >>> convert_interval_to_timedelta("1d")
        datetime.timedelta(days=1)
    """
    if time_interval.endswith("m") or time_interval.endswith("T"):
        return timedelta(minutes=int(time_interval[:-1]))

    if time_interval.endswith("H") or time_interval.endswith("h"):
        return timedelta(hours=int(time_interval[:-1]))

    if time_interval.endswith("D") or time_interval.endswith("d"):
        return timedelta(days=int(time_interval[:-1]))

    raise ValueError("time_interval %s format error", time_interval)


def convert_date(dt: str | date) -> date:
    if isinstance(dt, str):
        return date_parser.parse(dt).date()
    return dt


async def async_sleep_until_run_time(run_time: datetime):
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
