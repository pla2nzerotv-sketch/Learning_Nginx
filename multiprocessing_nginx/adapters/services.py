from multiprocessing_nginx.domain.entities import HttpRequest


class HeaderParser:

    def start_parser(self, header):
        decoded_header = header.decode().strip()
        header = decoded_header.split('\r\n')
        start_line = header[0]
        return self._parser_for_request(start_line, header)

    def _parser_for_request(self, start_line, header):
        method, url_path, protocol = start_line.split()
        return HttpRequest(
            method=method, url=url_path, protocol=protocol,
            header={i.split(':', 1)[0] + ':': i.split(':', 1)[1].strip() for i in header[1:]}, body=None
        )

    def aggregate_data(self, data: dict) -> bytes:
        result = []

        for key, value in data.items():
            for inner_key, inner_value in value.items():
                if inner_key in ['method', 'url_path', 'protocol', 'status', 'status_message']:
                    result.append(inner_value if not result else f" {inner_value}")
                else:
                    result.append(f"\r\n{inner_key} {inner_value}")
        return ''.join(result).encode() + b'\r\n\r\n'
