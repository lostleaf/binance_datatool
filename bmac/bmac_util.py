import traceback

import aiohttp

from bmac.handler import BmacHandler
from fetcher import BinanceFetcher
from msg_sender.dingding import DingDingSender
from util import create_aiohttp_session


async def report_error(handler: BmacHandler, e: Exception):
    # 出错则通过钉钉报错
    handler.logger.error(f'An error occurred {str(e)}')
    traceback.print_exc()
    if handler.dingding is not None and 'error' in handler.dingding:
        dingding_err = handler.dingding['error']
        try:
            error_stack_str = traceback.format_exc()
            async with create_aiohttp_session(handler.http_timeout_sec) as session:
                msg_sender = DingDingSender(session, dingding_err['secret'], dingding_err['access_token'])
                msg = f'An error occurred {str(e)}\n' + error_stack_str
                await msg_sender.send_message(msg)
        except:
            pass


def bmac_init_conns(handler: BmacHandler,
                    session: aiohttp.ClientSession) -> tuple[BinanceFetcher, dict[str, DingDingSender]]:
    senders = dict()
    if handler.dingding is not None:
        for channel_name, dcfg in handler.dingding.items():
            access_token = dcfg['access_token']
            secret = dcfg['secret']
            senders[channel_name] = DingDingSender(session, secret, access_token)
    fetcher = BinanceFetcher(handler.api_trade_type, session)
    return fetcher, senders
