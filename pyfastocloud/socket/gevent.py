import gevent
from gevent import select
from gevent import socket

Sleep = gevent.sleep
Select = select.select
Socket = socket.socket


def create_tcp_socket():
    import socket
    return Socket(socket.AF_INET, socket.SOCK_STREAM)
