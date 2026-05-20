def handler_body(content_length: str, socket, config_provider, body):
    current_content_length = 0 or len(body)
    full_body = [body]
    while int(content_length) != current_content_length:
        body = socket.recv(1024)
        if not body:
            break
        current_content_length += len(body)
        full_body.append(body)
    return b''.join(full_body)


def read_http_header(event_socket):
    request_data = b""
    while True:
        data = event_socket.recv(1024)
        if not data:
            break
        request_data += data
        if b"\r\n\r\n" in data:
            break
    if b"\r\n\r\n" in request_data:
        headers, body = request_data.split(b"\r\n\r\n", 1)
        return headers, body
    else:
        return request_data, b""