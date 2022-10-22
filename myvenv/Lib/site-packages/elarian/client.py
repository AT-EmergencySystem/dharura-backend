
import json
import ssl
import asyncio
import atexit
from abc import ABCMeta
from rsocket import RSocket, Payload
from .utils.generated.app_socket_pb2 import AppConnectionMetadata
from .utils.request_handler import _RequestHandler


class Client(metaclass=ABCMeta):
    """Client class that handles connections to the Elarian backend

        :param org_id: The organization id
        :param api_key: The generated API key from the dashboard
        :param app_id: The app id generated from the dashboard
    """
    _default_options = {
        "resumable": False,
        "lifetime": 60000,
        "keep_alive": 1000,
        "allow_notifications": True,
        "reconnect_timeout": 60000
    }

    _org_id = None
    _app_id = None
    _api_key = None
    _is_simulator = False
    _options = _default_options

    _socket = None
    _connection = None
    _is_connected = False
    _is_reconnect = False
    _loop = None
    _request_handler = _RequestHandler(None)
    _host = "tcp.elarian.com"
    _port = 443

    def __init__(self, org_id: str, api_key: str, app_id: str, events: list, options: dict = None):
        self._app_id = app_id
        self._api_key = api_key
        self._org_id = org_id
        self._expected_events = events + ['pending', 'error', 'connecting', 'connected', 'closed']
        if options is not None:
            self._options.update(options)

    async def connect(self, host='tcp.elarian.com', port=443):
        """Used to connect to Elarian."""
        self._request_handler.handle("pending")

        self._host = host
        self._port = port

        setup = AppConnectionMetadata()
        setup.org_id = self._org_id
        setup.app_id = self._app_id
        setup.api_key.value = self._api_key
        setup.simplex_mode = not self._options['allow_notifications']
        setup.simulator_mode = self._is_simulator

        metadata = json.dumps({
            "agent": "python"
        })

        self._loop = asyncio.get_event_loop()
        self._connection = await asyncio.open_connection(
            port=self._port,
            host=self._host,
            loop=self._loop,
            server_hostname=self._host,
            ssl=ssl.create_default_context(),
        )
        self._socket = RSocket(
            reader=self._connection[0],
            writer=self._connection[1],
            server=False,
            loop=self._loop,
            data_encoding=b'application/octet-stream',
            metadata_encoding=b'application/octet-stream',
            setup_payload=Payload(data=setup.SerializeToString(), metadata=bytearray(metadata, 'utf-8')),
            keep_alive_milliseconds=self._options['keep_alive'],
            max_lifetime_milliseconds=self._options['lifetime'],
            handler_factory=self._make_request_handler,
            error_handler=self._error_handler
        )

        self._is_connected = True
        if self._is_reconnect == False:
            self._loop.create_task(self.__keep_running())
            atexit.register(self.__clean_up)

        self._request_handler.handle("connected")

        return self

    async def disconnect(self):
        """Used to disconnect from Elarian."""
        self._is_connected = False
        self._connection[1].close()
        await self._socket.close()
        self._request_handler.handle("closed")

    def is_connected(self):
        """Checks whether the connection has been successfully established. 

           :returns: True or False
        """
        return self._is_connected

    def set_on_connection_pending(self, handler):
        """Sets handler for when the connection is pending

           :param handler: Dedicated handler function
        """
        return self._on('pending', handler)

    def set_on_connection_error(self, handler):
        """Sets handler for when there is a connection error

            :param handler: Dedicated handler function
        """
        return self._on('error', handler)

    def set_on_connection_closed(self, handler):
        """Sets handler for when the connection is closed

            :param handler: Dedicated handler function
        """
        return self._on('closed', handler)

    def set_on_connecting(self, handler):
        """Sets handler for when the connection is being opened

            :param handler: Dedicated handler function
        """
        return self._on('connecting', handler)

    def set_on_connected(self, handler):
        """Sets handler for when the connection has been successfully established

            :param handler: Dedicated handler function
        """
        return self._on('connected', handler)

    def _on(self, event, handler):
        if event in self._expected_events:
            self._request_handler.register_handler(event, handler)
        else:
            raise RuntimeError("Unexpected event {0}. Must be one of {1}".format(event, self._expected_events))
        return self

    def _make_request_handler(self, socket):
        registered = self._request_handler.get_handlers()
        self._request_handler = _RequestHandler(socket)
        self._request_handler._is_simulator = self._is_simulator
        self._request_handler._client = self
        for event in registered.keys():
            self._request_handler.register_handler(event, registered[event])
        self._request_handler.handle("connecting")
        return self._request_handler

    def _error_handler(self, error: Payload):
        self._request_handler.handle("error", RuntimeError(error.data.decode()))
        self._loop.create_task(self.disconnect())

    def __clean_up(self):
        if self.is_connected():
            if self._loop.is_running():
                self._loop.create_task(self.disconnect())
            else:
                asyncio.run(self.disconnect())

    async def __keep_running(self):
        while self.is_connected():
            await asyncio.sleep(2)
            if self.is_connected and (self._connection[1].transport is None or self._connection[1].transport.is_closing()):
                self._is_reconnect = True
                timeout = self._options['reconnect_timeout'] / 1000
                print(f'Connection was closed, will attempt to reconnect in {timeout}s...')
                await asyncio.sleep(timeout)
                await self.connect()
            else:
                break

    async def _send_command(self, data):
        if not self.is_connected():
            raise RuntimeError("Client is not connected")
        return await self._socket.request_response(self._make_payload(data))

    @staticmethod
    def _make_payload(data):
        return Payload(data=data.SerializeToString(), metadata=bytes())
