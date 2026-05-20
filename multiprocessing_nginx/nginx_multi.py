import itertools
import selectors
import socket
from multiprocessing import Process

from multiprocessing_nginx.domain import HttpRequest
from multiprocessing_nginx.domain.ports import UpstreamStrategy


class RoundRobin(UpstreamStrategy):
    list_backend_servers = None

    def select_backend(self, upstream):
        if not self.list_backend_servers:
            self.list_backend_servers = itertools.cycle(upstream.list_backend_servers)
        return next(self.list_backend_servers)



class BackendServer:
    def __init__(self, host, port):
        self.host = host
        self.port = port




def handle_client(client_socket, backend_servers):
    request_data = read_http_request_header(client_socket)
    http_request = HttpRequest.parser(request_data)
    http_request_send = http_request.aggregate_data()
    backend_server = backend_servers.get(9001)
    backend_server.sendall(http_request_send)
    response = backend_server.recv(1024)
    client_socket.sendall(response)
    client_socket.close()


def read_http_request_header(server_socket):
    request_data = b""
    while True:
        data = server_socket.recv(1024)
        if not data:
            break
        request_data += data
        if b"\r\n\r\n" in data:
            break
    return request_data



def server_echo():

    backend_server_9001 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    backend_server_9001.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    backend_server_9001.connect(('localhost', 9001))

    backend_server_9002 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    backend_server_9002.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    backend_server_9002.connect(('localhost', 9002))

    backend_servers = {
        9001: backend_server_9001,
        9002: backend_server_9002
    }
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    server_socket.bind(('localhost', 8090))
    server_socket.listen()
    server_socket.setblocking(False)
    selector = selectors.DefaultSelector()
    selector.register(server_socket, selectors.EVENT_READ)
    while True:
        events = selector.select(timeout=5)
        for event, _ in events:
            event_socket = event.fileobj
            if event_socket == server_socket:
                connection, address = server_socket.accept()
                selector.register(connection, selectors.EVENT_READ)
            else:
                try:
                    data = event_socket.recv(1024)
                    if not data:
                        selector.unregister(connection)
                        event_socket.close()
                        connection.close()
                        break
                    http_request = HttpRequest.parser(data)
                    http_request_send = http_request.aggregate_data()
                    backend_server = backend_servers.get(9001)
                    backend_server.sendall(http_request_send)
                    response = backend_server.recv(1024)
                    event_socket.send(response)
                except (ConnectionResetError, BrokenPipeError):
                    try:
                        selector.unregister(event_socket)
                    except Exception as e:
                        print(e)
                        pass
                    connection.close()
                    event_socket.close()



if __name__ == '__main__':
    Process_1 = Process(name='Process_1', target=server_echo)
    Process_2 = Process(name='Process_2', target=server_echo)
    Process_1.start()
    Process_2.start()
