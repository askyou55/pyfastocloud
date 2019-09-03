from pyfastocloud.client_constants import ClientStatus
from pyfastocloud.client_handler import IClientHandler
from pyfastocloud.client import Client, make_utc_timestamp, SocketError, create_tcp_socket


class Commands:
    # stream
    START_STREAM_COMMAND = 'start_stream'
    STOP_STREAM_COMMAND = 'stop_stream'
    RESTART_STREAM_COMMAND = 'restart_stream'
    GET_LOG_STREAM_COMMAND = 'get_log_stream'
    GET_PIPELINE_STREAM_COMMAND = 'get_pipeline_stream'
    CHANGED_STREAM_COMMAND = 'changed_source_stream'
    STATISTIC_STREAM_COMMAND = 'statistic_stream'
    QUIT_STATUS_STREAM_COMMAND = 'quit_status_stream'

    # service
    ACTIVATE_COMMAND = 'activate_request'
    PREPARE_SERVICE_COMMAND = 'prepare_service'
    SYNC_SERVICE_COMMAND = 'sync_service'
    STOP_SERVICE_COMMAND = 'stop_service'
    SERVICE_PING_COMMAND = 'ping_service'
    STATISTIC_SERVICE_COMMAND = 'statistic_service'
    CLIENT_PING_COMMAND = 'ping_client'  # ping from service
    GET_LOG_SERVICE_COMMAND = 'get_log_service'


class Fields:
    TIMESTAMP = 'timestamp'
    FEEDBACK_DIRECTORY = 'feedback_directory'
    TIMESHIFTS_DIRECTORY = 'timeshifts_directory'
    HLS_DIRECTORY = 'hls_directory'
    PLAYLISTS_DIRECTORY = 'playlists_directory'
    DVB_DIRECTORY = 'dvb_directory'
    CAPTURE_CARD_DIRECTORY = 'capture_card_directory'
    VODS_IN_DIRECTORY = 'vods_in_directory'
    VODS_DIRECTORY = 'vods_directory'
    CODS_DIRECTORY = 'cods_directory'
    STREAMS = 'streams'
    STREAM_ID = 'id'
    LICENSE_KEY = 'license_key'
    PATH = 'path'
    CONFIG = 'config'
    DELAY = 'delay'


class FastoCloudClient(Client):
    def __init__(self, host: str, port: int, handler: IClientHandler):
        super(FastoCloudClient, self).__init__(None, ClientStatus.INIT, handler)
        self.host = host
        self.port = port

    def connect(self) -> bool:
        if self.is_connected():
            return True

        try:
            sock = create_tcp_socket()
            sock.connect((self.host, self.port))
        except SocketError as exc:
            return False

        self._socket = sock
        self._set_state(ClientStatus.CONNECTED)
        return True

    def activate(self, command_id: int, license_key: str):
        command_args = {Fields.LICENSE_KEY: license_key}
        self._send_request(command_id, Commands.ACTIVATE_COMMAND, command_args)

    @Client.is_active_decorator
    def ping(self, command_id: int):
        command_args = {Fields.TIMESTAMP: make_utc_timestamp()}
        self._send_request(command_id, Commands.SERVICE_PING_COMMAND, command_args)

    @Client.is_active_decorator
    def prepare_service(self, command_id: int, feedback_directory: str, timeshifts_directory: str, hls_directory: str,
                        playlists_directory: str, dvb_directory: str, capture_card_directory: str,
                        vods_in_directory: str, vods_directory: str, cods_directory: str):
        command_args = {
            Fields.FEEDBACK_DIRECTORY: feedback_directory,
            Fields.TIMESHIFTS_DIRECTORY: timeshifts_directory,
            Fields.HLS_DIRECTORY: hls_directory,
            Fields.PLAYLISTS_DIRECTORY: playlists_directory,
            Fields.DVB_DIRECTORY: dvb_directory,
            Fields.CAPTURE_CARD_DIRECTORY: capture_card_directory,
            Fields.VODS_IN_DIRECTORY: vods_in_directory,
            Fields.VODS_DIRECTORY: vods_directory,
            Fields.CODS_DIRECTORY: cods_directory
        }
        self._send_request(command_id, Commands.PREPARE_SERVICE_COMMAND, command_args)

    @Client.is_active_decorator
    def sync_service(self, command_id: int, streams: list):
        command_args = {Fields.STREAMS: streams}
        self._send_request(command_id, Commands.SYNC_SERVICE_COMMAND, command_args)

    @Client.is_active_decorator
    def stop_service(self, command_id: int, delay: int):
        command_args = {Fields.DELAY: delay}
        self._send_request(command_id, Commands.STOP_SERVICE_COMMAND, command_args)

    @Client.is_active_decorator
    def get_log_service(self, command_id: int, path: str):
        command_args = {Fields.PATH: path}
        self._send_request(command_id, Commands.GET_LOG_SERVICE_COMMAND, command_args)

    @Client.is_active_decorator
    def start_stream(self, command_id: int, config: dict):
        command_args = {Fields.CONFIG: config}
        self._send_request(command_id, Commands.START_STREAM_COMMAND, command_args)

    @Client.is_active_decorator
    def stop_stream(self, command_id: int, stream_id: str):
        command_args = {Fields.STREAM_ID: stream_id}
        self._send_request(command_id, Commands.STOP_STREAM_COMMAND, command_args)

    @Client.is_active_decorator
    def restart_stream(self, command_id: int, stream_id: str):
        command_args = {Fields.STREAM_ID: stream_id}
        self._send_request(command_id, Commands.RESTART_STREAM_COMMAND, command_args)

    @Client.is_active_decorator
    def get_log_stream(self, command_id: int, stream_id: str, feedback_directory: str, path: str):
        command_args = {Fields.STREAM_ID: stream_id, Fields.FEEDBACK_DIRECTORY: feedback_directory, Fields.PATH: path}
        self._send_request(command_id, Commands.GET_LOG_STREAM_COMMAND, command_args)

    @Client.is_active_decorator
    def get_pipeline_stream(self, command_id: int, stream_id: str, feedback_directory: str, path: str):
        command_args = {Fields.STREAM_ID: stream_id, Fields.FEEDBACK_DIRECTORY: feedback_directory, Fields.PATH: path}
        self._send_request(command_id, Commands.GET_PIPELINE_STREAM_COMMAND, command_args)

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
            if saved_req and saved_req.method == Commands.ACTIVATE_COMMAND and resp.is_message():
                self._set_state(ClientStatus.ACTIVE)
            elif saved_req and saved_req.method == Commands.STOP_SERVICE_COMMAND and resp.is_message():
                self._reset()

            if self._handler:
                self._handler.process_response(self, saved_req, resp)

    # private
    @Client.is_active_decorator
    def __pong(self, command_id: str):
        ts = make_utc_timestamp()
        self._send_response(command_id, {Fields.TIMESTAMP: ts})
