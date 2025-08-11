import asyncio

import aiohttp

from bdt_common.exceptions import BinanceAPIException

from bdt_common.log_kit import logger

BINANCE_CODES = {-1122, -1121}


async def async_retry_getter(func, _max_times=5, _sleep_seconds=1, **kwargs):

    while True:
        try:
            return await func(**kwargs)
        except BinanceAPIException as e:
            if e.code in BINANCE_CODES:
                logger.warning(e)
                return None

            raise e
        except Exception as e:
            if _max_times == 0:
                logger.exception("Error occurred, 0 times retry left")
                raise e

            await asyncio.sleep(_sleep_seconds)
            _max_times -= 1
            _sleep_seconds *= 2


def create_aiohttp_session(timeout_sec):
    timeout = aiohttp.ClientTimeout(total=timeout_sec)
    session = aiohttp.ClientSession(timeout=timeout)
    return session
