import asyncio

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
        lower_headers = {k.lower(): v for k, v in header['headers'].items()}
        lower_headers['body'] = header.get('body', '')
        return lower_headers

    def _get_body_length(self, header):
        return int(header.get('content-length:', '0')) - len(header.get('body', ''))

    async def handler(self, reader, writer):
        task_proxy_reader_header = await self.handler_reader(reader)
        header = self.parser.start_parser(task_proxy_reader_header)
        writer.write(self.parser.aggregate_data(header))
        await writer.drain()
        header = self._header_to_lower(header)
        if header.get('content-length:'):
            await self.handler_body(reader, writer, header)

    async def handler_reader(self, reader):
        buffer = b''
        while b'\r\n\r\n' not in buffer:
            try:
                header = await asyncio.wait_for(reader.read(1024), self.config.Timeout.READ_MS)
            except asyncio.TimeoutError:
                self.logger.info("Таймаут ожидания данных, закрытие соединения")
                break
            if not header:
                break
            buffer += header
        return buffer

    async def handler_body(self, reader, writer, header):
        current_content_length = 0
        need_body_length = self._get_body_length(header)
        if need_body_length == 0:
            await self._write(writer, b'', header)
        while need_body_length != current_content_length:
            try:
                body = await asyncio.wait_for(reader.read(1024), self.config.Timeout.READ_MS)
            except asyncio.TimeoutError:
                self.logger.info("Таймаут ожидания данных, закрытие соединения")
                break
            if not body:
                break
            current_content_length += len(body)
            await self._write(writer, body, header)

    async def _write(self, writer, body, header):
        writer.write(header.pop('body', '').encode() + body)
        await writer.drain()

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
                try:
                    upstream_reader, upstream_writer = await self.upstream.get_upstream_connection(host, port)
                    self.logger.info(
                        f"Семафор Апстрима захвачен, обработка соединения. Текущий лимит: {semaphore_upstream._value}"
                    )
                except asyncio.TimeoutError:
                    self.logger.info("Таймаут подключения к ЭХО, закрытие соединения")
                    proxy_writer.close()
                    await proxy_writer.wait_closed()
                    return
                except ConnectionRefusedError:
                    self.logger.info("Не удалось установить соединение с ЭХО")
                    proxy_writer.close()
                    await proxy_writer.wait_closed()
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
                    await self.upstream.return_upstream_connection(host, port, upstream_reader, upstream_writer)

    async def start_proxy(self):
        server = await asyncio.start_server(self.proxy, self.config.PROXY.HOST, self.config.PROXY.PORT)
        async with server:
            await server.serve_forever()
