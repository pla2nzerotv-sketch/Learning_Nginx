import socket
import threading
from multiprocessing import Process

from multiprocessing_nginx.adapters.config import FileConfigProvider
from multiprocessing_nginx.adapters.upstream_strategy import RoundRobin
from multiprocessing_nginx.infastructure.logging import Logger
from multiprocessing_nginx.application.handler_request import HandleHttpRequest
from multiprocessing_nginx.adapters.http_client import SocketHttpClient
from multiprocessing_nginx.adapters.http_server import HttpServer


def handler():
    config_provider = FileConfigProvider('../config.yml')
    logger = Logger(config_provider)
    max_conns_per_backend = config_provider.config['limits']['max_conns_per_upstream']
    backend_semaphores = {
        upstream['port']: threading.Semaphore(max_conns_per_backend) for upstream in config_provider.config['upstreams']
    }
    http_server_semaphore = threading.Semaphore(config_provider.config['limits']['max_client_conns'])
    upstream_strategy = RoundRobin()
    route = config_provider.get_rotes(upstream_strategy)
    socket_http_client = SocketHttpClient(config_provider, backend_semaphores, logger)
    handle_http_request = HandleHttpRequest(route, config_provider, socket_http_client, upstream_strategy)
    thread1 = threading.Thread(target=start_http_server, args=(http_server_semaphore, handle_http_request))
    thread2 = threading.Thread(target=start_http_server, args=(http_server_semaphore, handle_http_request))
    thread1.start()
    thread2.start()


def start_http_server(http_server_semaphore, handle_http_request):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    server_socket.bind(('localhost', 8090))
    server_socket.listen()
    server_socket.setblocking(False)
    http_server = HttpServer(handle_http_request, http_server_semaphore)
    http_server.handler(server_socket=server_socket)


if __name__ == '__main__':
    Process_1 = Process(name='Process_1', target=handler)
    Process_2 = Process(name='Process_2', target=handler)
    Process_1.start()
    Process_2.start()
