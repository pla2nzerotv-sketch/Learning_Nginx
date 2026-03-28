import asyncio
import itertools
import logging

from config import Config

ECHO_SERVERS = [('127.0.0.1', 9020), ('127.0.0.1', 9021)]


class Logger:
    def __init__(self, config: Config):
        logging.basicConfig(
            level=config.LOGGING.LEVEL,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler()]
        )
        self.logger = logging.getLogger("ProxyServer")


class HandlerUpstream:
    echo_server_iter = iter(itertools.cycle(ECHO_SERVERS))

    @classmethod
    def get_data_connect(cls):
        return next(cls.echo_server_iter)


class HeaderParser:

    def start_parser(self, header):
        header = header.split('\r\n')
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
            for inner_key, inner_value in value.items():
                if inner_key in ['method', 'url_path', 'protocol', 'status', 'status_message']:
                    if result == '':
                        result += f"{inner_value}"
                    else:
                        result += f" {inner_value}"
                else:
                    result += f"\r\n{inner_key} {inner_value}"

        return (result + '\r\n\r\n').encode()


class ProxyServer:
    def __init__(self, config: Config):
        self.config = config
        self.semaphore_proxy = asyncio.Semaphore(self.config.Limit.MAX_CLIENT_CONNS)
        self.semaphore_upstream = asyncio.Semaphore(self.config.Limit.MAX_CONNS_PER_UPSTREAM)
        self.upstream = HandlerUpstream()
        self.logger = Logger(self.config).logger
        self.parser = HeaderParser()

    async def handler(self, writer, reader):
        while True:
            try:
                data = await asyncio.wait_for(reader.read(1024), self.config.Timeout.READ_MS)
                if not data:
                    break
                split_data = data.decode().split('\r\n\r\n')
                if len(split_data) == 1:
                    body = split_data[0]
                else:
                    header, body = split_data
                    parser_data = self.parser.start_parser(header)
                    aggregate_data = self.parser.aggregate_data(parser_data)
                    writer.write(aggregate_data)
            except asyncio.TimeoutError:
                self.logger.info("Таймаут ожидания данных, закрытие соединения")
                break
            else:
                writer.write(body.encode())
                try:
                    await asyncio.wait_for(writer.drain(), self.config.Timeout.WRITE_MS)
                except asyncio.TimeoutError:
                    self.logger.info("Таймаут записи данных, закрытие соединения")
                    break
        writer.close()
        await writer.wait_closed()

    async def proxy(self, proxy_reader, proxy_writer):
        self.logger.info(f"Попытка захвата семафора ПРОКСИ. Текущий лимит: {self.semaphore_proxy._value}")
        async with self.semaphore_proxy:
            self.logger.info(
                f"Семафор ПРОКСИ захвачен, обработка соединения. Текущий лимит: {self.semaphore_proxy._value}"
            )
            host, port = self.upstream.get_data_connect()
            self.logger.info(f"PORT: {port}")
            try:
                self.logger.info(f"Попытка захвата семафора Апстрима. Текущий лимит: {self.semaphore_upstream._value}")
                async with self.semaphore_upstream:
                    echo_server_reader, echo_server_writer = await asyncio.wait_for(
                        asyncio.open_connection(host, port), self.config.Timeout.CONNECT_MS
                    )
                    self.logger.info(
                        f"Семафор Апстрима захвачен, обработка соединения. Текущий лимит: {self.semaphore_upstream._value}"
                    )
            except asyncio.TimeoutError:
                logging.info("Таймаут подключения к ЭХО, закрытие соединения")
                proxy_writer.close()
                return
            except ConnectionRefusedError:
                logging.info("Не удалось установить соединение с ЭХО")
                proxy_writer.close()
                return
            else:
                task_1 = asyncio.create_task(self.handler(echo_server_writer, proxy_reader))
                task_2 = asyncio.create_task(self.handler(proxy_writer, echo_server_reader))
                try:
                    await asyncio.wait_for(asyncio.gather(task_1, task_2), self.config.Timeout.TOTAL_MS)
                except asyncio.TimeoutError:
                    self.logger.info("Таймаут общей обработки, закрытие соединения")
                    task_1.cancel()
                    task_2.cancel()
                    await asyncio.gather(task_1, task_2)

    async def start_proxy(self):
        server = await asyncio.start_server(self.proxy, self.config.PROXY.HOST, self.config.PROXY.PORT)
        async with server:
            await server.serve_forever()
