import selectors
import threading

from multiprocessing_nginx.application.handler_request import HandleHttpRequest
from multiprocessing_nginx.domain.entities import HttpRequest
from multiprocessing_nginx.utils import read_http_header, handler_body


class HttpServer:
    def __init__(self, handler_http_request: HandleHttpRequest, http_server_semaphore: threading.Semaphore):
        self.handler_http_request = handler_http_request
        self.http_server_semaphore = http_server_semaphore

    def handler(self, server_socket):
        selector = selectors.DefaultSelector()
        selector.register(server_socket, selectors.EVENT_READ)
        while True:
            events = selector.select(timeout=self.handler_http_request.config_provider.config['timeouts']['total_ms'])
            for event, _ in events:
                event_socket = event.fileobj
                if event_socket == server_socket:
                    self.http_server_semaphore.acquire()
                    connection, address = server_socket.accept()
                    selector.register(connection, selectors.EVENT_READ)
                    continue
                task_proxy_reader_header, body = read_http_header(event_socket)
                if not task_proxy_reader_header:
                    event_socket.close()
                    selector.unregister(event_socket)
                    continue
                http_request = HttpRequest.parser(task_proxy_reader_header)
                if content_length := http_request.header.get('Content-Length:'):
                    http_request.body = handler_body(content_length, event_socket, self.handler_http_request.config_provider, body)
                response = self.handler_http_request.handler(http_request)
                event_socket.send(response.aggregate_data())
                self.http_server_semaphore.release()
