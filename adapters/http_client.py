import asyncio
from asyncio import StreamReader, StreamWriter
from collections import defaultdict

from domain.entities import HttpResponse, HttpRequest, BackendServer
from domain.ports import HttpClient, ConfigProvider
from infastructure.logging import Logger
from utils import handler_body


class SocketHttpClient(HttpClient):

    def __init__(self, config_provider: ConfigProvider, backend_semaphores: dict[int, asyncio.Semaphore], logger: Logger) -> None:
        self.config_provider = config_provider
        self.backend_semaphores = backend_semaphores
        self.logger = logger
        self.upstream_connection_poll = defaultdict(asyncio.Queue)

    async def send(self, request: HttpRequest, backend: BackendServer) -> HttpResponse:
        async with self.backend_semaphores.get(backend.port):
            try:
                reader, writer = await self._get_backend_connection(backend)
            except asyncio.TimeoutError:
                raise Exception("Таймаут подключения к Бекенд серверу, закрытие соединения")
            writer.write(request.aggregate_data())
            try:
                await asyncio.wait_for(writer.drain(), self.config_provider.config['timeouts']['write_ms'])
            except asyncio.TimeoutError:
                writer.close()
                await writer.wait_closed()
                raise Exception("Таймаут записи данных, закрытие соединения")
            try:
                response_data = await asyncio.wait_for(
                    reader.readuntil(b"\r\n\r\n"), self.config_provider.config['timeouts']['read_ms']
                )
            except asyncio.TimeoutError:
                writer.close()
                await writer.wait_closed()
                raise Exception("Таймаут ожидания данных, закрытие соединения")
            http_response = HttpResponse.parser(response_data)
            if content_length := http_response.header.get('content-length:'):
                http_response.body = await handler_body(content_length, reader, self.config_provider)
            return http_response

    async def _connect(self, backend: BackendServer) -> tuple[StreamReader, StreamWriter]:
        return await asyncio.wait_for(
            asyncio.open_connection(backend.host, backend.port), self.config_provider.config['timeouts']['connect_ms']
        )

    async def _get_backend_connection(self, backend: BackendServer) -> tuple[StreamReader, StreamWriter]:
        pool = self.upstream_connection_poll[(backend.host, backend.port)]
        if not pool.empty():
            reader, writer = await pool.get()
            if not reader.at_eof():
                return reader, writer
            writer.close()
            await writer.wait_closed()
            return await self._connect(backend)
        return await self._connect(backend)
