class HttpRequest:
    def __init__(self, method, url, protocol, header, body):
        self.method = method
        self.url = url
        self.protocol = protocol
        self.header = header
        self.body = body

    @classmethod
    def parser(cls, data):
        decoded_header = data.decode().strip()
        header = decoded_header.split('\r\n')
        start_line = header[0]
        method, url_path, protocol = start_line.split()
        return cls(
            method=method, url=url_path, protocol=protocol,
            header={i.split(':', 1)[0] + ':': i.split(':', 1)[1].strip() for i in header[1:]}, body=b''
        )

    def aggregate_data(self) -> bytes:
        start_line = f'{self.method} {self.url} {self.protocol}'
        result = [start_line]
        for key, value in self.header.items():
            result.append(f'\r\n{key} {value}')
        return ''.join(result).encode() + b'\r\n\r\n' + self.body


class HttpResponse:
    def __init__(self, protocol, status, message_status, header, body):
        self.protocol = protocol
        self.status = status
        self.message_status = message_status
        self.header = header
        self.body = body

    @classmethod
    def parser(cls, data):
        decoded_header = data.decode().strip()
        header = decoded_header.split('\r\n')
        start_line = header[0]
        protocol, status, message_status = start_line.split()
        return cls(
            protocol=protocol, message_status=message_status, status=status,
            header={i.split(':', 1)[0] + ':': i.split(':', 1)[1].strip() for i in header[1:]},
            body=b''
        )

    def aggregate_data(self) -> bytes:
        start_line = f'{self.protocol} {self.status} {self.message_status}'
        result = [start_line]
        for key, value in self.header.items():
            result.append(f'\r\n{key} {value}')
        return ''.join(result).encode() + b'\r\n\r\n' + self.body


class Route:
    def __init__(self, path_pattern, method, upstream):
        self.path_pattern = path_pattern
        self.method = method
        self.upstream = upstream


class Upstream:
    def __init__(self, list_backend_servers, upstream_strategy):
        self.list_backend_servers = list_backend_servers
        self.upstream_strategy = upstream_strategy


class BackendServer:
    def __init__(self, host, port):
        self.host = host
        self.port = port
