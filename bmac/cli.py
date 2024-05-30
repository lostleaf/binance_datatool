import asyncio

from . import bmac


class Bmac:
    """
    Binance Marketdata Async Client
    """

    def start(self, base_dir):
        asyncio.run(bmac.main(base_dir))
