import socket
import threading
from collections import defaultdict
from logging import Logger
from queue import Queue
from multiprocessing_nginx.utils import read_http_header, handler_body
from multiprocessing_nginx.domain.entities import HttpRequest, BackendServer, HttpResponse
from multiprocessing_nginx.domain.ports import HttpClient, ConfigProvider



class SocketHttpClient(HttpClient):

    def __init__(self, config_provider: ConfigProvider, backend_semaphores: dict[int, threading.Semaphore],
                 logger: Logger) -> None:
        self.config_provider = config_provider
        self.backend_semaphores = backend_semaphores
        self.logger = logger
        self.upstream_connection_poll = defaultdict(Queue)

    def send(self, request: HttpRequest, backend: BackendServer) -> HttpResponse:
        with self.backend_semaphores.get(backend.port):
            backend_server = self._get_backend_connection(backend)
            backend_server.send(request.aggregate_data())
            header_response, body = read_http_header(backend_server)
            http_response = HttpResponse.parser(header_response)
            if content_length := http_response.header.get('content-length:'):
                http_response.body = handler_body(content_length, backend_server, self.config_provider, body)
            return http_response

    def _get_backend_connection(self, backend: BackendServer):
        pool = self.upstream_connection_poll[(backend.host, backend.port)]
        if not pool.empty():
            return pool.get()
        backend_socket = self._connect(backend)
        pool.put(backend_socket)
        return pool.get()

    def _connect(self, backend: BackendServer):
        backend_server = socket.socket()
        backend_server.connect((backend.host, backend.port))
        return backend_server
