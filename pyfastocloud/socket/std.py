import socket
import select
import time

Sleep = time.sleep
Select = select.select
Socket = socket.socket


def create_tcp_socket():
    return Socket(socket.AF_INET, socket.SOCK_STREAM)
