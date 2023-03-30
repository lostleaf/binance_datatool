import base64
import hashlib
import hmac
import json
import logging
import time
from urllib.parse import quote_plus

import aiohttp

from util import async_retry_getter


def retry_getter(func, retry_times=5, sleep_seconds=1, default=None, raise_err=True):
    for i in range(retry_times):
        try:
            return func()
        except Exception as e:
            logging.warn(f'An error occurred {str(e)}')
            if i == retry_times - 1 and raise_err:
                raise e
            time.sleep(sleep_seconds)
            sleep_seconds *= 2
    return


class DingDingSender:

    def __init__(self, api_info, aiohttp_session):
        self.api_info = api_info
        self.session: aiohttp.ClientSession = aiohttp_session

    def generate_post_url(self, channel_info):
        secret = channel_info['secret']
        access_token = channel_info['access_token']
        timestamp = str(round(time.time() * 1000))
        secret_enc = secret.encode('utf-8')
        string_to_sign = '{}\n{}'.format(timestamp, secret)
        string_to_sign_enc = string_to_sign.encode('utf-8')
        hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
        sign = quote_plus(base64.b64encode(hmac_code))
        url = f'https://oapi.dingtalk.com/robot/send?access_token={access_token}&timestamp={timestamp}&sign={sign}'
        return url

    async def send_message(self, msg, channel):
        post_url = self.generate_post_url(self.api_info[channel])
        headers = {"Content-Type": "application/json", "Charset": "UTF-8"}
        req_json_str = json.dumps({"msgtype": "text", "text": {"content": msg}})
        await async_retry_getter(lambda: self.session.post(url=post_url, data=req_json_str, headers=headers))


if __name__ == '__main__':
    API = json.load(open('../dingding.json', 'r'))
    D = DingDingSender(API)
    D.send_message('test from python', 'trade')
    D.send_message('test from python', 'error')