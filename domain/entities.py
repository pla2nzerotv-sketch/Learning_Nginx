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


class Route:
    pass


class Upstream:
    pass


class BackendServer:
    pass
