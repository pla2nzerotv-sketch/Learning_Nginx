import asyncio

from application.handler_request import HandleHttpRequest
from domain.entities import HttpRequest
from utils import handler_body


class HttpServer:
    def __init__(self, handler_http_request: HandleHttpRequest, http_server_semaphore: asyncio.Semaphore):
        self.handler_http_request = handler_http_request
        self.http_server_semaphore = http_server_semaphore

    async def handler(self, proxy_reader, proxy_writer):
        async with self.http_server_semaphore:
            task_proxy_reader_header = await self.handler_reader(proxy_reader)
            http_request = HttpRequest.parser(task_proxy_reader_header)
            if content_length := http_request.header.get('Content-Length:'):
                http_request.body = await handler_body(content_length, proxy_reader, self.handler_http_request.config_provider)
            response = await self.handler_http_request.handler(http_request)
            proxy_writer.write(response.aggregate_data())
            await asyncio.wait_for(proxy_writer.drain(),
                                   self.handler_http_request.config_provider.config['timeouts']['write_ms'])
            proxy_writer.close()
            await proxy_writer.wait_closed()

    async def handler_reader(self, reader):
        try:
            return await asyncio.wait_for(reader.readuntil(b'\r\n\r\n'),
                                          self.handler_http_request.config_provider.config['timeouts']['read_ms'])
        except asyncio.TimeoutError:
            raise Exception("Таймаут ожидания данных, закрытие соединения")
        except asyncio.IncompleteReadError:
            raise Exception("Клиент закрыл соединение раньше, чем были получены заголовки")
