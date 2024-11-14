import traceback
from typing import Union

import aiohttp

from bmac.handler import BmacHandler
from fetcher import BinanceFetcher
from msg_sender.dingding import DingDingSender
from msg_sender.dingding import WechatWorkSender
from util import create_aiohttp_session


async def report_error(handler: BmacHandler, e: Exception):
    # 出错则通过钉钉报错
    handler.logger.error(f'An error occurred {str(e)}')
    traceback.print_exc()
    if handler.msg_sender is not None and 'error' in handler.msg_sender:
        error_cfg = handler.msg_sender['error']
        try:
            error_stack_str = traceback.format_exc()
            async with create_aiohttp_session(handler.http_timeout_sec) as session:
                if error_cfg['type'] == 'dingding':
                    msg_sender = DingDingSender(session, error_cfg['secret'], error_cfg['access_token'])
                elif error_cfg['type'] == 'wechat_work':
                    msg_sender = WechatWorkSender(session, error_cfg['webhook_url'])
                msg = f'An error occurred {str(e)}\n' + error_stack_str
                await msg_sender.send_message(msg)
        except:
            pass


def bmac_init_conns(handler: BmacHandler, session: aiohttp.ClientSession) -> tuple[BinanceFetcher, dict[str, Union[DingDingSender, WechatWorkSender]]]:
    senders = dict()
    if handler.msg_sender is not None:
        sender_type = handler.msg_sender.get('type', 'dingding')
        for channel_name, cfg in handler.msg_sender.items():
            if channel_name == 'type':
                continue
                
            if sender_type == 'dingding':
                access_token = cfg['access_token']
                secret = cfg['secret']
                senders[channel_name] = DingDingSender(session, secret, access_token)
            elif sender_type == 'wechat_work':
                webhook_url = cfg['webhook_url']
                senders[channel_name] = WechatWorkSender(session, webhook_url)
                
    fetcher = BinanceFetcher(handler.api_trade_type, session)
    return fetcher, senders