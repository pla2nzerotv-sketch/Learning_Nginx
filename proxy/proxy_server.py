import asyncio

import uvloop

from config import Config
from proxy.logger_class import Logger
from proxy.parser import HeaderParser
from proxy.upstream import HandlerUpstream


class ProxyServer:
    def __init__(self, config: Config):
        self.config = config
        self.logger = Logger(self.config).logger
        self.upstream = HandlerUpstream(config)
        self.parser = HeaderParser()
        self.semaphore_proxy = asyncio.Semaphore(config.Limit.MAX_CLIENT_CONNS)
        self.upstream_semaphores = {
            config.Upstream.PORT_1: asyncio.Semaphore(config.Limit.MAX_CONNS_PER_UPSTREAM),
            config.Upstream.PORT_2: asyncio.Semaphore(config.Limit.MAX_CONNS_PER_UPSTREAM)
        }

    def _header_to_lower(self, header):
        return {k.lower(): v for k, v in header['headers'].items()}

    async def handler(self, reader, writer):
        task_proxy_reader_header = await self.handler_reader(reader)
        header = self.parser.start_parser(task_proxy_reader_header)
        writer.write(self.parser.aggregate_data(header))
        try:
            await asyncio.wait_for(writer.drain(), self.config.Timeout.WRITE_MS)
        except asyncio.TimeoutError:
            raise Exception("Таймаут записи данных, закрытие соединения")
        header = self._header_to_lower(header)
        if header.get('content-length:'):
            await self.handler_body(reader, writer, header)

    async def handler_reader(self, reader):
        try:
            return await asyncio.wait_for(reader.readuntil(b'\r\n\r\n'), self.config.Timeout.READ_MS)
        except asyncio.TimeoutError:
            raise Exception("Таймаут ожидания данных, закрытие соединения")
        except asyncio.IncompleteReadError:
            raise Exception("Клиент закрыл соединение раньше, чем были получены заголовки")

    async def handler_body(self, reader, writer, header):
        current_content_length = 0
        need_body_length = int(header.get('content-length:'))
        while need_body_length != current_content_length:
            try:
                body = await asyncio.wait_for(reader.read(1024), self.config.Timeout.READ_MS)
            except asyncio.TimeoutError:
                raise Exception("Таймаут ожидания данных, закрытие соединения")
            if not body:
                break
            current_content_length += len(body)
            await self._write(writer, body)

    async def _write(self, writer, body):
        writer.write(body)
        try:
            await asyncio.wait_for(writer.drain(), self.config.Timeout.WRITE_MS)
        except asyncio.TimeoutError:
            raise Exception("Таймаут записи данных, закрытие соединения")

    async def proxy(self, proxy_reader, proxy_writer):
        self.logger.info(f"Попытка захвата семафора ПРОКСИ. Текущий лимит: {self.semaphore_proxy._value}")
        async with self.semaphore_proxy:
            self.logger.info(
                f"Семафор ПРОКСИ захвачен, обработка соединения. Текущий лимит: {self.semaphore_proxy._value}"
            )
            host, port = self.upstream.get_data_connect()
            semaphore_upstream = self.upstream_semaphores.get(port)
            async with semaphore_upstream:
                self.logger.info(f"Попытка захвата семафора Апстрима. Текущий лимит: {semaphore_upstream._value}")
                upstream_reader, upstream_writer = None, None
                try:
                    upstream_reader, upstream_writer = await self.upstream.get_upstream_connection(host, port)
                    self.logger.info(
                        f"Семафор Апстрима захвачен, обработка соединения. Текущий лимит: {semaphore_upstream._value}"
                    )
                except asyncio.TimeoutError:
                    self.logger.info("Таймаут подключения к ЭХО, закрытие соединения")
                    return
                except ConnectionRefusedError:
                    self.logger.info("Не удалось установить соединение с ЭХО")
                    return
                else:
                    task_1 = asyncio.create_task(self.handler(proxy_reader, upstream_writer))
                    task_2 = asyncio.create_task(self.handler(upstream_reader, proxy_writer))
                    try:
                        await asyncio.wait_for(asyncio.gather(task_1, task_2), self.config.Timeout.TOTAL_MS)
                    except asyncio.TimeoutError:
                        self.logger.info("Таймаут общей обработки, закрытие соединения")
                        task_1.cancel()
                        task_2.cancel()
                        await asyncio.gather(task_1, task_2, return_exceptions=True)
                finally:
                    self.logger.info(
                        f"Семафор Апстрима отпущен, обработка соединения. Текущий лимит: {semaphore_upstream._value}"
                    )
                    proxy_writer.close()
                    await proxy_writer.wait_closed()
                    if upstream_writer is not None:
                        await self.upstream.return_upstream_connection(host, port, upstream_reader, upstream_writer)

    async def start_proxy(self):
        server = await asyncio.start_server(self.proxy, self.config.PROXY.HOST, self.config.PROXY.PORT)
        async with server:
            await server.serve_forever()
