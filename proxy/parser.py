class HttpRequest:
    def __init__(self, method, url, header, body):
        self.method = method
        self.url = url
        self.header = header
        self.body = body


class HttpResponse:
    def __init__(self, status, header, body):
        self.status = status
        self.header = header
        self.body = body


class HeaderParser:

    def start_parser(self, header):
        decoded_header = header.decode().strip()
        header = decoded_header.split('\r\n')
        start_line = header[0]
        if start_line.startswith('HTTP'):
            data = self._parser_for_response(start_line)
        else:
            data = self._parser_for_request(start_line)
        return {
            'start_line': data,
            'headers': {i.split(':', 1)[0] + ':': i.split(':', 1)[1].strip() for i in header[1:]},
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
        result = []

        for key, value in data.items():
            for inner_key, inner_value in value.items():
                if inner_key in ['method', 'url_path', 'protocol', 'status', 'status_message']:
                    result.append(inner_value if not result else f" {inner_value}")
                else:
                    result.append(f"\r\n{inner_key} {inner_value}")
        return ''.join(result).encode() + b'\r\n\r\n'
