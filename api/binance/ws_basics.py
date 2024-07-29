import asyncio
import gzip
import json
from enum import Enum
from random import random
from socket import gaierror
from typing import Optional

import websockets as ws
from websockets.exceptions import ConnectionClosedError

from api.binance.exceptions import BinanceWebsocketUnableToConnect
from util import get_loop, get_logger


class WSListenerState(Enum):
    INITIALISING = 'Initialising'
    STREAMING = 'Streaming'
    RECONNECTING = 'Reconnecting'
    EXITING = 'Exiting'


class ReconnectingWebsocket:
    MAX_RECONNECTS = 5
    MAX_RECONNECT_SECONDS = 60
    MIN_RECONNECT_WAIT = 0.1
    TIMEOUT = 60
    NO_MESSAGE_RECONNECT_TIMEOUT = 60
    MAX_QUEUE_SIZE = 100

    def __init__(self,
                 url: str,
                 path: Optional[str] = None,
                 prefix: str = 'ws/',
                 is_binary: bool = False,
                 exit_coro=None):
        self._loop = get_loop()
        self._log = get_logger()
        self._path = path
        self._url = url
        self._exit_coro = exit_coro
        self._prefix = prefix
        self._reconnects = 0
        self._is_binary = is_binary
        self._conn = None
        self._socket = None
        self.ws: Optional[ws.WebSocketClientProtocol] = None  # type: ignore
        self.ws_state = WSListenerState.INITIALISING
        self._queue = asyncio.Queue()
        self._handle_read_loop = None

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._exit_coro:
            await self._exit_coro(self._path)
        self.ws_state = WSListenerState.EXITING
        if self.ws:
            self.ws.fail_connection()
        if self._conn and hasattr(self._conn, 'protocol'):
            await self._conn.__aexit__(exc_type, exc_val, exc_tb)
        self.ws = None
        if not self._handle_read_loop:
            self._log.error("CANCEL read_loop")
            await self._kill_read_loop()

    async def connect(self):
        await self._before_connect()
        assert self._path
        ws_url = self._url + self._prefix + self._path
        self._conn = ws.connect(ws_url, close_timeout=0.1)  # type: ignore
        try:
            self.ws = await self._conn.__aenter__()
        except:  # noqa
            await self._reconnect()
            return
        self.ws_state = WSListenerState.STREAMING
        self._reconnects = 0
        await self._after_connect()
        # To manage the "cannot call recv while another coroutine is already waiting for the next message"
        if not self._handle_read_loop:
            self._handle_read_loop = self._loop.call_soon_threadsafe(asyncio.create_task, self._read_loop())

    async def _kill_read_loop(self):
        self.ws_state = WSListenerState.EXITING
        while self._handle_read_loop:
            await asyncio.sleep(0.1)

    async def _before_connect(self):
        pass

    async def _after_connect(self):
        pass

    def _handle_message(self, evt):
        if self._is_binary:
            try:
                evt = gzip.decompress(evt)
            except (ValueError, OSError):
                return None
        try:
            return json.loads(evt)
        except ValueError:
            self._log.debug(f'error parsing evt json:{evt}')
            return None

    async def _read_loop(self):
        try:
            while True:
                try:
                    while self.ws_state == WSListenerState.RECONNECTING:
                        await self._run_reconnect()

                    if self.ws_state == WSListenerState.EXITING:
                        self._log.debug(f"_read_loop {self._path} break for {self.ws_state}")
                        break
                    elif self.ws.state == ws.protocol.State.CLOSING:  # type: ignore
                        await asyncio.sleep(0.1)
                        continue
                    elif self.ws.state == ws.protocol.State.CLOSED:  # type: ignore
                        await self._reconnect()
                    elif self.ws_state == WSListenerState.STREAMING:
                        assert self.ws
                        res = await asyncio.wait_for(self.ws.recv(), timeout=self.TIMEOUT)
                        res = self._handle_message(res)
                        if res:
                            if self._queue.qsize() < self.MAX_QUEUE_SIZE:
                                await self._queue.put(res)
                            else:
                                self._log.debug(f"Queue overflow {self.MAX_QUEUE_SIZE}. Message not filled")
                                await self._queue.put({'e': 'error', 'm': 'Queue overflow. Message not filled'})
                                raise BinanceWebsocketUnableToConnect
                except asyncio.TimeoutError:
                    self._log.debug(f"no message in {self.TIMEOUT} seconds")
                    # _no_message_received_reconnect
                except asyncio.CancelledError as e:
                    self._log.debug(f"cancelled error {e}")
                    break
                except asyncio.IncompleteReadError as e:
                    self._log.debug(f"incomplete read error ({e})")
                except ConnectionClosedError as e:
                    self._log.debug(f"connection close error ({e})")
                except gaierror as e:
                    self._log.debug(f"DNS Error ({e})")
                except BinanceWebsocketUnableToConnect as e:
                    self._log.debug(f"BinanceWebsocketUnableToConnect ({e})")
                    break
                except Exception as e:
                    self._log.debug(f"Unknown exception ({e})")
                    continue
        finally:
            self._handle_read_loop = None  # Signal the coro is stopped
            self._reconnects = 0

    async def _run_reconnect(self):
        await self.before_reconnect()
        if self._reconnects < self.MAX_RECONNECTS:
            reconnect_wait = self._get_reconnect_wait(self._reconnects)
            self._log.debug(f"websocket reconnecting. {self.MAX_RECONNECTS - self._reconnects} reconnects left - "
                            f"waiting {reconnect_wait}")
            await asyncio.sleep(reconnect_wait)
            await self.connect()
        else:
            self._log.error(f'Max reconnections {self.MAX_RECONNECTS} reached:')
            # Signal the error
            await self._queue.put({'e': 'error', 'm': 'Max reconnect retries reached'})
            raise BinanceWebsocketUnableToConnect

    async def recv(self):
        return await asyncio.wait_for(self._queue.get(), timeout=self.TIMEOUT)

    async def _wait_for_reconnect(self):
        while self.ws_state != WSListenerState.STREAMING and self.ws_state != WSListenerState.EXITING:
            await asyncio.sleep(0.1)

    def _get_reconnect_wait(self, attempts: int) -> int:
        expo = 2**attempts
        return round(random() * min(self.MAX_RECONNECT_SECONDS, expo - 1) + 1)

    async def before_reconnect(self):
        if self.ws and self._conn:
            await self._conn.__aexit__(None, None, None)
            self.ws = None
        self._reconnects += 1

    def _no_message_received_reconnect(self):
        self._log.debug('No message received, reconnecting')
        self.ws_state = WSListenerState.RECONNECTING

    async def _reconnect(self):
        self.ws_state = WSListenerState.RECONNECTING
