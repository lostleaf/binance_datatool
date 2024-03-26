import asyncio
from datetime import datetime, timedelta
from decimal import Decimal

import aiohttp
import pytz

DEFAULT_TZ = pytz.timezone('hongkong')


def remove_exponent(d: Decimal):
    return d.quantize(Decimal(1)) if d == d.to_integral() else d.normalize()


def create_aiohttp_session(timeout_sec):
    timeout = aiohttp.ClientTimeout(total=timeout_sec)
    session = aiohttp.ClientSession(timeout=timeout)
    return session


async def async_retry_getter(func, max_times=5, **kwargs):
    sleep_seconds = 1
    while True:
        try:
            return await func(**kwargs)
        except Exception as e:
            if max_times == 0:
                raise e
            else:
                import traceback

                traceback.print_exc()
            await asyncio.sleep(sleep_seconds)
            max_times -= 1
            sleep_seconds *= 2


def now_time():
    return datetime.now(DEFAULT_TZ)


def convert_interval_to_timedelta(time_interval):
    if time_interval.endswith('m') or time_interval.endswith('T'):
        return timedelta(minutes=int(time_interval[:-1]))

    if time_interval.endswith('H') or time_interval.endswith('h'):
        return timedelta(hours=int(time_interval[:-1]))

    raise ValueError('time_interval %s format error', time_interval)
