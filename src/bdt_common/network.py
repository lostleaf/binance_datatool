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


def create_aiohttp_session(timeout_sec: int | float) -> aiohttp.ClientSession:
    """Create an aiohttp ClientSession with specified timeout.

    Factory function to create a configured aiohttp ClientSession with a total timeout.
    The session should be used as an async context manager to ensure proper cleanup.

    Args:
        timeout_sec: Total timeout in seconds for HTTP requests.

    Returns:
        aiohttp.ClientSession: Configured HTTP client session.

    Examples:
        >>> async with create_aiohttp_session(30) as session:
        ...     async with session.get('https://api.example.com') as response:
        ...         data = await response.json()
        
        >>> # Or for use in other async functions
        >>> session = create_aiohttp_session(10)
        >>> # Remember to close the session when done
        >>> await session.close()
    """
    timeout = aiohttp.ClientTimeout(total=timeout_sec)
    session = aiohttp.ClientSession(timeout=timeout)
    return session
