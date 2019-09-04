from pyfastocloud.client_handler import IClientHandler, ClientStatus
from pyfastocloud.client import Client, make_utc_timestamp, Socket


class Commands:
    CLIENT_PING_COMMAND = 'client_ping'
    ACTIVATE_COMMAND = 'client_active'
    GET_SERVER_INFO_COMMAND = 'get_server_info'
    GET_CHANNELS = 'get_channels'
    GET_RUNTIME_CHANNEL_INFO = 'get_runtime_channel_info'

    SERVER_PING_COMMAND = 'server_ping'
    SERVER_GET_CLIENT_INFO_COMMAND = 'get_client_info'
    SERVER_SEND_MESSAGE_COMMAND = 'send_message'


class Fields:
    TIMESTAMP = 'timestamp'


class SubscriberClient(Client):
    def __init__(self, sock: Socket, addr, handler: IClientHandler):
        super(SubscriberClient, self).__init__(sock, ClientStatus.CONNECTED, handler)
        self._addr = addr

    def address(self):
        return self._addr

    def activate_success(self, command_id: str):
        self._send_response_ok(command_id)
        self._set_state(ClientStatus.ACTIVE)

    def activate_fail(self, command_id: str, error: str):
        self._send_response_fail(command_id, error)

    def check_activate_fail(self, command_id: str, error: str):
        self.activate_fail(command_id, error)

    def get_channels_success(self, command_id: str, params):
        self._send_response(command_id, params)

    @Client.is_active_decorator
    def get_server_info_success(self, command_id: str, bandwidth_host: str):
        command_args = {'bandwidth_host': bandwidth_host}
        self._send_response(command_id, command_args)

    @Client.is_active_decorator
    def get_runtime_channel_info_success(self, command_id: str, sid: str, watchers: int):
        command_args = {'id': sid, 'watchers': watchers}
        self._send_response(command_id, command_args)

    @Client.is_active_decorator
    def ping(self, command_id: int):
        self._send_request(command_id, Commands.SERVER_PING_COMMAND, {Fields.TIMESTAMP: make_utc_timestamp()})

    @Client.is_active_decorator
    def get_client_info(self, command_id: int):
        command_args = {}
        self._send_request(command_id, Commands.SERVER_GET_CLIENT_INFO_COMMAND, command_args)

    @Client.is_active_decorator
    def send_message(self, command_id: int, message: str, ttl: int):
        command_args = {'message': message, 'show_time': ttl}
        self._send_request(command_id, Commands.SERVER_SEND_MESSAGE_COMMAND, command_args)

    def process_commands(self, data: bytes):
        if not data:
            return

        req, resp = self._decode_response_or_request(data)
        if req:
            if req.method == Commands.CLIENT_PING_COMMAND:
                self.__pong(req.id)

            if self._handler:
                self._handler.process_request(self, req)
        elif resp:
            saved_req = self._request_queue.pop(resp.id, None)
            if self._handler:
                self._handler.process_response(self, saved_req, resp)

    # private
    @Client.is_active_decorator
    def __pong(self, command_id: str):
        ts = make_utc_timestamp()
        self._send_response(command_id, {Fields.TIMESTAMP: ts})
