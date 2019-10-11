from pyfastocloud.client_handler import IClientHandler, ClientStatus
from pyfastocloud.client import Client, make_utc_timestamp_msec


class Commands:
    CLIENT_ACTIVATE_DEVICE = 'client_activate_device'
    CLIENT_LOGIN = 'client_login'
    CLIENT_PING = 'client_ping'
    CLIENT_GET_SERVER_INFO = 'get_server_info'
    CLIENT_GET_CHANNELS = 'get_channels'
    CLIENT_GET_RUNTIME_CHANNEL_INFO = 'get_runtime_channel_info'

    SERVER_PING = 'server_ping'
    SERVER_GET_CLIENT_INFO = 'get_client_info'
    SERVER_SEND_MESSAGE = 'send_message'


class Fields:
    TIMESTAMP = 'timestamp'


class SubscriberClient(Client):
    def __init__(self, sock, addr, handler: IClientHandler, socket_mod):
        super(SubscriberClient, self).__init__(sock, ClientStatus.CONNECTED, handler, socket_mod)
        self._addr = addr

    def address(self):
        return self._addr

    # responses
    def activate_device_success(self, command_id: str, devices: list) -> bool:
        command_args = {'devices': devices}
        result = self._send_response(command_id, command_args)
        if not result:
            return False

        return True

    def activate_device_fail(self, command_id: str, error: str) -> bool:
        return self._send_response_fail(command_id, error)

    def login_success(self, command_id: str) -> bool:
        result = self._send_response_ok(command_id)
        if not result:
            return False

        self._set_state(ClientStatus.ACTIVE)
        return True

    def login_fail(self, command_id: str, error: str) -> bool:
        return self._send_response_fail(command_id, error)

    def check_login_fail(self, command_id: str, error: str) -> bool:
        return self.login_fail(command_id, error)

    def get_channels_success(self, command_id: str, channels: list, vods: list) -> bool:
        command_args = {'channels': channels, 'vods': vods}
        return self._send_response(command_id, command_args)

    @Client.is_active_decorator
    def get_server_info_success(self, command_id: str, bandwidth_host: str) -> bool:
        command_args = {'bandwidth_host': bandwidth_host}
        return self._send_response(command_id, command_args)

    @Client.is_active_decorator
    def get_runtime_channel_info_success(self, command_id: str, sid: str, watchers: int) -> bool:
        command_args = {'id': sid, 'watchers': watchers}
        return self._send_response(command_id, command_args)

    @Client.is_active_decorator
    def pong(self, command_id: str) -> bool:
        ts = make_utc_timestamp_msec()
        return self._send_response(command_id, {Fields.TIMESTAMP: ts})

    # requests
    @Client.is_active_decorator
    def ping(self, command_id: int) -> bool:
        return self._send_request(command_id, Commands.SERVER_PING, {Fields.TIMESTAMP: make_utc_timestamp_msec()})

    @Client.is_active_decorator
    def get_client_info(self, command_id: int) -> bool:
        command_args = {}
        return self._send_request(command_id, Commands.SERVER_GET_CLIENT_INFO, command_args)

    @Client.is_active_decorator
    def send_message(self, command_id: int, message: str, message_type: int, ttl: int) -> bool:
        command_args = {'message': message, 'type': message_type, 'show_time': ttl}
        return self._send_request(command_id, Commands.SERVER_SEND_MESSAGE, command_args)

    def process_commands(self, data: bytes):
        if not data:
            return

        req, resp = self._decode_response_or_request(data)
        if req:
            if self._handler:
                self._handler.process_request(self, req)
        elif resp:
            saved_req = self._request_queue.pop(resp.id, None)
            if self._handler:
                self._handler.process_response(self, saved_req, resp)
