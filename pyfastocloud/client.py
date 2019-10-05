import socket
import struct
import json

from abc import ABC, abstractmethod

from datetime import datetime
from pyfastocloud.client_constants import ClientStatus
from pyfastocloud.client_handler import IClientHandler
from pyfastocloud.json_rpc import Request, Response, parse_response_or_request, JSONRPC_OK_RESULT, JsonRPCErrorCode
from pyfastocloud.compressor_zlib import CompressorZlib


def make_utc_timestamp_seconds() -> int:
    return int(datetime.now().timestamp())


def make_utc_timestamp_msec() -> int:
    return make_utc_timestamp_seconds() * 1000


def generate_seq_id(command_id):  # uint64_t
    if command_id is None:
        return None

    converted_bytes = command_id.to_bytes(8, byteorder='big')
    return converted_bytes.hex()


def generate_json_rpc_response_message(result, command_id: str) -> Response:
    return Response(command_id, result)


def generate_json_rpc_response_error(message: str, code: int, command_id: str) -> Response:
    return Response(command_id, None, {'code': code, 'message': message})


class Client(ABC):
    MAX_PACKET_SIZE = 64 * 1024 * 1024

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

    def is_connected(self):
        return self._state != ClientStatus.INIT

    def disconnect(self):
        if not self.is_connected():
            return

        self._reset()

    def socket(self):
        return self._socket

    def read_command(self):
        if not self.is_connected():
            return None

        try:
            data_size_bytes = self._recv(4)
        except socket.error:
            return None

        if not data_size_bytes:
            return None

        data_size = struct.unpack('>I', data_size_bytes)[0]
        if data_size > Client.MAX_PACKET_SIZE:
            return None

        try:
            data = self._recv(data_size)
        except socket.error:
            return None
        return data

    @abstractmethod
    def process_commands(self, data: bytes):
        pass

    def create_tcp_socket(self):
        return self._socket_mod.create_tcp_socket()

    def create_tcp_connection(self, host: str, port: int):
        try:
            sock = self.create_tcp_socket()
            sock.connect((host, port))
        except socket.error as exc:
            return None

        return sock

    # protected
    def __init__(self, sock, state: ClientStatus, handler: IClientHandler, socket_mod):
        self._handler = handler
        self._socket = sock
        self._request_queue = dict()
        self._state = state
        self._gzip_compress = CompressorZlib(True)
        self._socket_mod = socket_mod

    def _reset(self):
        self._socket.close()
        self._socket = None
        self._set_state(ClientStatus.INIT)

    def _set_state(self, status: ClientStatus):
        self._state = status
        if self._handler:
            self._handler.on_client_state_changed(self, status)

    def _send_request(self, command_id, method: str, params) -> bool:
        if not self.is_connected():
            return False

        cid = generate_seq_id(command_id)
        req = Request(cid, method, params)

        data = json.dumps(req.to_dict())
        data_to_send_bytes = self._generate_data_to_send(data)
        if not req.is_notification():
            self._request_queue[cid] = req
        try:
            self._socket.send(data_to_send_bytes)
        except socket.error:
            return False
        return True

    def _generate_data_to_send(self, data: str) -> bytes:
        compressed = self._gzip_compress.compress(data.encode())
        compressed_len = len(compressed)
        array = struct.pack(">I", compressed_len)
        return array + compressed

    def _send_notification(self, method: str, params) -> bool:
        return self._send_request(None, method, params)

    def _send_response(self, command_id: str, params) -> bool:
        resp = generate_json_rpc_response_message(params, command_id)
        data = json.dumps(resp.to_dict())
        data_to_send_bytes = self._generate_data_to_send(data)
        try:
            self._socket.send(data_to_send_bytes)
        except socket.error:
            return False
        return True

    def _send_response_ok(self, command_id: str) -> bool:
        return self._send_response(command_id, JSONRPC_OK_RESULT)

    def _send_response_fail(self, command_id: str, error: str) -> bool:
        resp = generate_json_rpc_response_error(error, JsonRPCErrorCode.JSON_RPC_SERVER_ERROR, command_id)
        data = json.dumps(resp.to_dict())
        data_to_send_bytes = self._generate_data_to_send(data)
        try:
            self._socket.send(data_to_send_bytes)
        except socket.error:
            return False
        return True

    def _recv(self, n: int):
        # Helper function to recv n bytes or return None if EOF is hit
        data = b''
        while len(data) < n:
            try:
                packet = self._socket.recv(n - len(data))
            except socket.error:
                return None
            if not packet:
                return None
            data += packet
        return data

    def _decode_response_or_request(self, data: bytes) -> (Request, Response):
        decoded_data = self._gzip_compress.decompress(data)
        return parse_response_or_request(decoded_data.decode())
