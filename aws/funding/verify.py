from itertools import chain
from typing import List

import config
from aws.checksum import get_unverified_aws_data_files, verify_multi_process
from aws.client_async import AwsFundingRateClient
from aws.funding.util import local_list_funding_symbols
from config import DataFrequency, TradeType
from util.log_kit import divider, logger


def verify_funding_rates(trade_type: TradeType, symbols: List[str]):
    logger.info(f"Start verify funding rates checksums")
    logger.debug(f"trade_type={trade_type.value}, num_symbols={len(symbols)}, " f"{symbols[0]} -- {symbols[-1]}")

    local_funding_dir = AwsFundingRateClient.LOCAL_DIR / AwsFundingRateClient.get_base_dir(
        trade_type=trade_type, data_freq=DataFrequency.monthly
    )
    unverified_files = sorted(
        chain.from_iterable(get_unverified_aws_data_files(local_funding_dir / symbol) for symbol in symbols)
    )

    if not unverified_files:
        logger.ok("All files verified")
        return

    logger.debug(f"num_unverified={len(unverified_files)}, n_jobs={config.N_JOBS}")
    logger.debug(f"first={unverified_files[0]}")
    logger.debug(f"last={unverified_files[-1]}")

    num_success, num_fail = verify_multi_process(unverified_files)

    msg = f"{num_success} successfully verified"
    if num_fail > 0:
        msg += f", deleted {num_fail} corrupted files"
    logger.ok(msg)


def verify_type_all_funding_rates(trade_type: TradeType):
    divider(f"BHDS verify {trade_type.value} Funding Rates")
    symbols = local_list_funding_symbols(trade_type)
    verify_funding_rates(trade_type, symbols)
