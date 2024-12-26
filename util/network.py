import asyncio

import aiohttp

from .log_kit import logger


async def async_retry_getter(func, _max_times=5, _sleep_seconds=1, **kwargs):

    while True:
        try:
            return await func(**kwargs)
        except Exception as e:
            if _max_times == 0:
                logger.exception('Error occurred, 0 times retry left')
                raise e

            await asyncio.sleep(_sleep_seconds)
            _max_times -= 1
            _sleep_seconds *= 2


def create_aiohttp_session(timeout_sec):
    timeout = aiohttp.ClientTimeout(total=timeout_sec)
    session = aiohttp.ClientSession(timeout=timeout)
    return session
