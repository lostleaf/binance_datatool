import base64
import hashlib
import hmac
import json
import time
from urllib.parse import quote_plus

import aiohttp

from util import async_retry_getter


class DingDingSender:

    def __init__(self, aiohttp_session, secret, access_token):
        self.secret = secret
        self.access_token = access_token
        self.session: aiohttp.ClientSession = aiohttp_session

    def generate_post_url(self):
        secret = self.secret
        access_token = self.access_token
        timestamp = str(round(time.time() * 1000))
        secret_enc = secret.encode('utf-8')
        string_to_sign = '{}\n{}'.format(timestamp, secret)
        string_to_sign_enc = string_to_sign.encode('utf-8')
        hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
        sign = quote_plus(base64.b64encode(hmac_code))
        url = f'https://oapi.dingtalk.com/robot/send?access_token={access_token}&timestamp={timestamp}&sign={sign}'
        return url

    async def send_message(self, msg):
        post_url = self.generate_post_url()
        headers = {"Content-Type": "application/json", "Charset": "UTF-8"}
        req_json_str = json.dumps({"msgtype": "text", "text": {"content": msg}})
        await async_retry_getter(lambda: self.session.post(url=post_url, data=req_json_str, headers=headers))
