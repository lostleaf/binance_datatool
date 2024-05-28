import asyncio
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


async def async_sleep_until_run_time(run_time):
    sleep_seconds = (run_time - now_time()).total_seconds()
    await asyncio.sleep(max(0, sleep_seconds - 1))
    while now_time() < run_time:  # 在靠近目标时间时
        await asyncio.sleep(0.001)


def next_run_time(time_interval):
    """
    =====辅助功能函数, 下次运行时间
    根据time_interval，计算下次运行的时间，下一个整点时刻。
    目前只支持分钟和小时。
    :param time_interval: 运行的周期，15m，1h
    :param ahead_seconds: 预留的目标时间和当前时间的间隙
    :return: 下次运行的时间
    案例：
    15m  当前时间为：12:50:51  返回时间为：13:00:00
    15m  当前时间为：12:39:51  返回时间为：12:45:00
    """
    ti = convert_interval_to_timedelta(time_interval)

    now = now_time()
    # now = datetime(2019, 5, 9, 23, 50, 30)  # 修改now，可用于测试
    this_midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    min_step = timedelta(minutes=1)

    target_time = now.replace(second=0, microsecond=0)

    while True:
        target_time = target_time + min_step
        delta = target_time - this_midnight
        if delta.seconds % ti.seconds == 0:
            # 当符合运行周期，并且目标时间有足够大的余地，默认为60s
            break

    return target_time