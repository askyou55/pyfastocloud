import socket
import gevent
from gevent import select

Sleep = gevent.sleep
Select = select.select
Socket = gevent.socket.socket


def create_tcp_socket():
    return Socket(socket.AF_INET, socket.SOCK_STREAM)
