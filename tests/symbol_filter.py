import asyncio
import os
import random
from typing import Optional

from bdt_common.constants import HTTP_TIMEOUT_SEC
from bdt_common.enums import ContractType, DataFrequency, TradeType
from bdt_common.log_kit import logger, divider
from bdt_common.network import create_aiohttp_session
from bdt_common.symbol_filter import SpotFilter, UmFuturesFilter, CmFuturesFilter
from bhds.aws.client import AwsClient
from bhds.aws.path_builder import AwsKlinePathBuilder


async def get_symbols(trade_type: TradeType, time_interval: str, http_proxy: Optional[str]) -> list[str]:
    async with create_aiohttp_session(HTTP_TIMEOUT_SEC) as session:
        path_builder = AwsKlinePathBuilder(
            trade_type=trade_type,
            data_freq=DataFrequency.daily,
            time_interval=time_interval,
        )
        client = AwsClient(
            path_builder=path_builder,
            session=session,
            http_proxy=http_proxy,
        )
        return await client.list_symbols()


async def test_spot_filters(http_proxy: Optional[str]):
    divider("Spot Filters")
    symbols = await get_symbols(TradeType.spot, "1h", http_proxy)
    logger.info(f"Symbols count: {len(symbols)}")
    if not symbols:
        logger.warning("No spot symbols available; skipping spot tests")
        return

    # USDT only, exclude stable pairs and leverage tokens
    f1 = SpotFilter(quote="USDT", stable_pairs=False, leverage_tokens=False)
    r1 = f1.filter(symbols)
    logger.info(f"USDT (no-stable, no-lev) count: {len(r1)}")
    random.shuffle(r1)
    logger.debug(f"Sample symbols: {r1[:5]}")

    # USDT only, include stable pairs, exclude leverage tokens
    f2 = SpotFilter(quote="USDT", stable_pairs=True, leverage_tokens=False)
    r2 = f2.filter(symbols)
    logger.info(f"USDT (with-stable, no-lev) count: {len(r2)}")
    random.shuffle(r2)
    logger.debug(f"Sample symbols: {r2[:5]}")

    logger.debug(f"USDT stable pairs: {set(r2) - set(r1)}") 

    # BTC quote, include all
    f3 = SpotFilter(quote="BTC", stable_pairs=True, leverage_tokens=True)
    r3 = f3.filter(symbols)
    logger.info(f"BTC quote (all) count: {len(r3)}")
    random.shuffle(r3)
    logger.debug(f"Sample symbols: {r3[:5]}")

    # No quote filter, include all
    f4 = SpotFilter(quote=None, stable_pairs=True, leverage_tokens=True)
    r4 = f4.filter(symbols)
    logger.info(f"All quotes (all) count: {len(r4)}")
    random.shuffle(r4)
    logger.debug(f"Sample symbols: {r4[:5]}")

    f5 = SpotFilter(quote='USDT', stable_pairs=False, leverage_tokens=True)
    r5 = f5.filter(symbols)
    logger.info(f"USDT (no-stable, lev) count: {len(r5)}")
    random.shuffle(r5)
    logger.debug(f"Sample symbols: {r5[:5]}")
    logger.debug(f"USDT leverage tokens: {set(r5) - set(r1)}")


async def test_um_futures_filters(http_proxy: Optional[str]):
    divider("UM Futures Filters")
    symbols = await get_symbols(TradeType.um_futures, "1h", http_proxy)
    logger.info(f"Symbols count: {len(symbols)}")
    if not symbols:
        logger.warning("No UM futures symbols available; skipping UM tests")
        return

    # USDT perpetual, exclude stable pairs
    f1 = UmFuturesFilter(quote="USDT", contract_type=ContractType.perpetual, stable_pairs=False)
    r1 = f1.filter(symbols)
    logger.info(f"USDT perpetual (no-stable) count: {len(r1)}")
    random.shuffle(r1)
    logger.debug(f"Sample symbols: {r1[:5]}")

    # USDT delivery, exclude stable pairs
    f2 = UmFuturesFilter(quote="USDT", contract_type=ContractType.delivery, stable_pairs=False)
    r2 = f2.filter(symbols)
    logger.info(f"USDT delivery (no-stable) count: {len(r2)}")
    random.shuffle(r2)
    logger.debug(f"Sample symbols: {r2[:5]}")

    # Any contract type, include stable pairs
    f3 = UmFuturesFilter(quote="USDT", contract_type=ContractType.perpetual, stable_pairs=True)
    r3 = f3.filter(symbols)
    logger.info(f"USDT perpetual (with-stable) count: {len(r3)}")
    random.shuffle(r3)
    logger.debug(f"Sample symbols: {r3[:5]}")
    logger.debug(f"USDT perpetual stable pairs: {set(r3) - set(r1)}")

    # No filters
    f4 = UmFuturesFilter(quote=None, contract_type=None, stable_pairs=True)
    r4 = f4.filter(symbols)
    logger.info(f"All UM contracts (with-stable) count: {len(r4)}")
    random.shuffle(r4)
    logger.debug(f"Sample symbols: {r4[:5]}")


async def test_cm_futures_filters(http_proxy: Optional[str]):
    divider("CM Futures Filters")
    symbols = await get_symbols(TradeType.cm_futures, "1h", http_proxy)
    logger.info(f"Symbols count: {len(symbols)}")
    if not symbols:
        logger.warning("No CM futures symbols available; skipping CM tests")
        return

    # Perpetual
    f1 = CmFuturesFilter(contract_type=ContractType.perpetual)
    r1 = f1.filter(symbols)
    logger.info(f"CM perpetual count: {len(r1)}")
    random.shuffle(r1)
    logger.debug(f"Sample symbols: {r1[:5]}")

    # Delivery
    f2 = CmFuturesFilter(contract_type=ContractType.delivery)
    r2 = f2.filter(symbols)
    logger.info(f"CM delivery count: {len(r2)}")
    random.shuffle(r2)
    logger.debug(f"Sample symbols: {r2[:5]}")


async def main():
    http_proxy = os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
    
    try:
        await test_spot_filters(http_proxy)
        await test_um_futures_filters(http_proxy)
        await test_cm_futures_filters(http_proxy)
        
        divider("All tests completed")
        logger.ok("Symbol filter testing completed successfully")
    except Exception as e:
        logger.exception(f"Error during symbol filter testing: {e}")


if __name__ == "__main__":
    asyncio.run(main())