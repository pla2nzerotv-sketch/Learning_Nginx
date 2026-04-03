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
