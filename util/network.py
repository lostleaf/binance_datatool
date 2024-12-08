import asyncio

import aiohttp

from .log_kit import logger


async def async_retry_getter(func, max_times=5, **kwargs):
    sleep_seconds = 1
    while True:
        try:
            return await func(**kwargs)
        except Exception as e:
            if max_times == 0:
                raise e
            else:
                logger.warning('Error occurred, %s, %d times retry left', repr(e), max_times)

            await asyncio.sleep(sleep_seconds)
            max_times -= 1
            sleep_seconds *= 2


def create_aiohttp_session(timeout_sec):
    timeout = aiohttp.ClientTimeout(total=timeout_sec)
    session = aiohttp.ClientSession(timeout=timeout)
    return session
