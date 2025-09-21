import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

import aiohttp

from bdt_common.exceptions import BinanceAPIException
from bdt_common.log_kit import logger

GetterRetType = TypeVar("T")

BINANCE_CODES: set[int] = {-1122, -1121}


async def async_retry_getter(
    func: Callable[..., Awaitable[GetterRetType]],
    _max_times: int = 5,
    _sleep_seconds: float = 1,
    **kwargs: Any,
) -> GetterRetType | None:
    """Call an async function with retries using exponential backoff.

    This helper retries the given async callable when generic exceptions occur, doubling the sleep between attempts.
    If a BinanceAPIException is raised with a code in ``BINANCE_CODES``, the error is considered non-retryable and
    ``None`` is returned; otherwise the exception is re-raised.

    Args:
        func: Async callable to execute. It will be awaited as ``func(**kwargs)``.
        _max_times: Maximum number of retries on generic exceptions. Zero means raise immediately on next failure.
        _sleep_seconds: Initial backoff sleep in seconds; it doubles after each failed attempt.
        **kwargs: Keyword arguments passed through to ``func``.

    Returns:
        The awaited result of ``func`` on success, or ``None`` when a non-retryable BinanceAPIException code is hit.

    Raises:
        BinanceAPIException: Re-raised when the error code is not in ``BINANCE_CODES``.
        Exception: The last exception after exhausting retries.
    """
    # Loop until success or until retries are exhausted.
    while True:
        try:
            # Attempt the async call with provided kwargs.
            return await func(**kwargs)
        except BinanceAPIException as e:
            # Non-retryable Binance error codes: log and return None to signal "handled but no data".
            if e.code in BINANCE_CODES:
                logger.warning(e)
                return None
            # Other Binance errors are propagated to the caller.
            raise
        except Exception:
            # If no retries left, log with stacktrace and propagate the last exception.
            if _max_times == 0:
                logger.exception("Error occurred, 0 times retry left")
                raise
            # Exponential backoff then decrement retries and continue.
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
