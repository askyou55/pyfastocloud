import socket
import struct
import json

from datetime import datetime
from pyfastocloud.client_constants import Commands, ClientStatus
from pyfastocloud.client_handler import IClientHandler
from pyfastocloud.json_rpc import Request, Response, parse_response_or_request
from pyfastocloud.compressor_zlib import CompressorZlib


def make_utc_timestamp() -> int:
    return int(datetime.now().timestamp() * 1000)


def generate_seq_id(command_id):  # uint64_t
    if command_id is None:
        return None

    converted_bytes = command_id.to_bytes(8, byteorder='big')
    return converted_bytes.hex()


def generate_json_rpc_response_message(result, command_id: str) -> Response:
    return Response(command_id, result)


def generate_json_rpc_response_error(message: str, code: int, command_id: str) -> Response:
    return Response(command_id, None, {'code': code, 'message': message})


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


class FastoCloudClient:
    MAX_PACKET_SIZE = 4294967295

    def __init__(self, host: str, port: int, handler: IClientHandler):
        self.host = host
        self.port = port
        self._handler = handler
        self._socket = None
        self._request_queue = dict()
        self._state = ClientStatus.INIT
        self._gzip_compress = CompressorZlib(True)

    def is_active(self):
        return self._state == ClientStatus.ACTIVE

    def is_active_decorator(func):
        def closure(self, *args, **kwargs):
            if not self.is_active():
                return
            return func(self, *args, *kwargs)

        return closure

    def status(self) -> ClientStatus:
        return self._state

    def connect(self) -> bool:
        if self.is_connected():
            return True

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self.host, self.port))
        except socket.error as exc:
            return False

        self._socket = sock
        self._set_state(ClientStatus.CONNECTED)
        return True

    def is_connected(self):
        return self._state != ClientStatus.INIT

    def disconnect(self):
        if not self.is_connected():
            return

        self._reset()

    def socket(self):
        return self._socket

    def activate(self, command_id: int, license_key: str):
        command_args = {Fields.LICENSE_KEY: license_key}
        self._send_request(command_id, Commands.ACTIVATE_COMMAND, command_args)

    @is_active_decorator
    def ping_service(self, command_id: int):
        self._send_request(command_id, Commands.SERVICE_PING_COMMAND, {Fields.TIMESTAMP: make_utc_timestamp()})

    @is_active_decorator
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

    @is_active_decorator
    def sync_service(self, command_id: int, streams: list):
        command_args = {Fields.STREAMS: streams}
        self._send_request(command_id, Commands.SYNC_SERVICE_COMMAND, command_args)

    @is_active_decorator
    def stop_service(self, command_id: int, delay: int):
        command_args = {Fields.DELAY: delay}
        self._send_request(command_id, Commands.STOP_SERVICE_COMMAND, command_args)

    @is_active_decorator
    def get_log_service(self, command_id: int, path: str):
        command_args = {Fields.PATH: path}
        self._send_request(command_id, Commands.GET_LOG_SERVICE_COMMAND, command_args)

    @is_active_decorator
    def start_stream(self, command_id: int, config: dict):
        command_args = {Fields.CONFIG: config}
        self._send_request(command_id, Commands.START_STREAM_COMMAND, command_args)

    @is_active_decorator
    def stop_stream(self, command_id: int, stream_id: str):
        command_args = {Fields.STREAM_ID: stream_id}
        self._send_request(command_id, Commands.STOP_STREAM_COMMAND, command_args)

    @is_active_decorator
    def restart_stream(self, command_id: int, stream_id: str):
        command_args = {Fields.STREAM_ID: stream_id}
        self._send_request(command_id, Commands.RESTART_STREAM_COMMAND, command_args)

    @is_active_decorator
    def get_log_stream(self, command_id: int, stream_id: str, feedback_directory: str, path: str):
        command_args = {Fields.STREAM_ID: stream_id, Fields.FEEDBACK_DIRECTORY: feedback_directory, Fields.PATH: path}
        self._send_request(command_id, Commands.GET_LOG_STREAM_COMMAND, command_args)

    @is_active_decorator
    def get_pipeline_stream(self, command_id: int, stream_id: str, feedback_directory: str, path: str):
        command_args = {Fields.STREAM_ID: stream_id, Fields.FEEDBACK_DIRECTORY: feedback_directory, Fields.PATH: path}
        self._send_request(command_id, Commands.GET_PIPELINE_STREAM_COMMAND, command_args)

    def read_command(self):
        if not self.is_connected():
            return None

        data_size_bytes = self._recv(4)
        if not data_size_bytes:
            return None

        data_size = struct.unpack('>I', data_size_bytes)[0]
        if data_size > FastoCloudClient.MAX_PACKET_SIZE:
            return None

        return self._recv(data_size)

    def process_commands(self, data: bytes):
        if not data:
            return

        req, resp = self._decode_response_or_request(data)
        if req:
            if req.method == Commands.CLIENT_PING_COMMAND:
                self._pong(req.id)

            if self._handler:
                self._handler.process_request(req)
        elif resp:
            saved_req = self._request_queue.pop(resp.id, None)
            if saved_req and saved_req.method == Commands.ACTIVATE_COMMAND and resp.is_message():
                self._set_state(ClientStatus.ACTIVE)
            elif saved_req and saved_req.method == Commands.STOP_SERVICE_COMMAND and resp.is_message():
                self._reset()

            if self._handler:
                self._handler.process_response(saved_req, resp)

    # private
    def _reset(self):
        self._socket.close()
        self._socket = None
        self._set_state(ClientStatus.INIT)

    def _set_state(self, status: ClientStatus):
        self._state = status
        if self._handler:
            self._handler.on_client_state_changed(status)

    @is_active_decorator
    def _pong(self, command_id: str):
        ts = make_utc_timestamp()
        self._send_response(command_id, {Fields.TIMESTAMP: ts})

    def _send_request(self, command_id, method: str, params):
        if not self.is_connected():
            return

        cid = generate_seq_id(command_id)
        req = Request(cid, method, params)

        data = json.dumps(req.to_dict())
        data_to_send_bytes = self._generate_data_to_send(data)
        if not req.is_notification():
            self._request_queue[cid] = req
        self._socket.send(data_to_send_bytes)

    def _generate_data_to_send(self, data: str) -> bytes:
        compressed = self._gzip_compress.compress(data.encode())
        compressed_len = len(compressed)
        data_len = socket.ntohl(compressed_len)
        array = struct.pack("I", data_len)
        return array + compressed

    def _send_notification(self, method: str, params):
        return self._send_request(None, method, params)

    def _send_response(self, command_id, params):
        resp = generate_json_rpc_response_message(params, command_id)
        data = json.dumps(resp.to_dict())
        data_to_send_bytes = self._generate_data_to_send(data)
        self._socket.send(data_to_send_bytes)

    def _recv(self, n: int):
        # Helper function to recv n bytes or return None if EOF is hit
        data = b''
        while len(data) < n:
            packet = self._socket.recv(n - len(data))
            if not packet:
                return None
            data += packet
        return data

    def _decode_response_or_request(self, data: bytes) -> (Request, Response):
        decoded_data = self._gzip_compress.decompress(data)
        return parse_response_or_request(decoded_data.decode())
