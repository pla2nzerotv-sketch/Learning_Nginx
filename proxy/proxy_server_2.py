import asyncio
import itertools
import logging
from datetime import datetime

from config import Config


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
        buffer = []
        while b'\r\n\r\n' not in b''.join(buffer):
            try:
                header = await asyncio.wait_for(reader.read(1024), self.config.Timeout.READ_MS)
            except asyncio.TimeoutError:
                self.logger.info("Таймаут ожидания данных, закрытие соединения")
                break
            if not header:
                break
            buffer.append(header)
        return b''.join(buffer)

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
                    upstream_reader, upstream_writer = await asyncio.wait_for(
                        asyncio.open_connection(host, port), self.config.Timeout.CONNECT_MS
                    )
                    self.logger.info(
                        f"Семафор Апстрима захвачен, обработка соединения. Текущий лимит: {semaphore_upstream._value}"
                    )
                except asyncio.TimeoutError:
                    logging.info("Таймаут подключения к ЭХО, закрытие соединения")
                    proxy_writer.close()
                    proxy_writer.wait_closed()
                    return
                except ConnectionRefusedError:
                    logging.info("Не удалось установить соединение с ЭХО")
                    proxy_writer.close()
                    proxy_writer.wait_closed()
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
                self.logger.info(
                    f"Семафор Апстрима отпущен, обработка соединения. Текущий лимит: {semaphore_upstream._value}"
                )
    async def start_proxy(self):
        server = await asyncio.start_server(self.proxy, self.config.PROXY.HOST, self.config.PROXY.PORT)
        async with server:
            await server.serve_forever()


class Logger:
    def __init__(self, config: Config):
        logging.basicConfig(
            level=config.LOGGING.LEVEL,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler()]
        )
        self.logger = logging.getLogger("ProxyServer")


class HandlerUpstream:

    def __init__(self, config: Config):
        self.echo_server_iter = iter(itertools.cycle([
            (config.Upstream.HOST, getattr(config.Upstream, attr_name))
            for attr_name in dir(config.Upstream)
            if attr_name.startswith("PORT_")
        ]))

    def get_data_connect(self):
        return next(self.echo_server_iter)


class HeaderParser:

    def start_parser(self, header):
        decoded_header = header.decode()
        headers_part, body = decoded_header.split('\r\n\r\n', 1)
        header = headers_part.split('\r\n')
        start_line = header[0]
        if start_line.startswith('HTTP'):
            data = self._parser_for_response(start_line)
        else:
            data = self._parser_for_request(start_line)
        headers = {}
        for i in header[1:]:
            value = i.split(':', 1)
            headers[value[0] + ':'] = value[1].strip()
        return {
            'start_line': data,
            'headers': headers,
            'body': body,
        }

    def _parser_for_response(self, start_line):
        protocol, status, status_message = start_line.split()
        return {
            'protocol': protocol,
            'status': status,
            'status_message': status_message,
        }

    def _parser_for_request(self, start_line):
        method, url_path, protocol = start_line.split()
        return {
            'method': method,
            'url_path': url_path,
            'protocol': protocol,
        }

    def aggregate_data(self, data: dict) -> bytes:
        result = ''

        for key, value in data.items():
            if key == 'body':
                continue
            for inner_key, inner_value in value.items():
                if inner_key in ['method', 'url_path', 'protocol', 'status', 'status_message']:
                    if result == '':
                        result += f"{inner_value}"
                    else:
                        result += f" {inner_value}"
                else:
                    result += f"\r\n{inner_key} {inner_value}"

        return (result + '\r\n\r\n').encode()
