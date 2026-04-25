from domain.entities import HttpRequest, Route
from domain.ports import ConfigProvider, HttpClient, UpstreamStrategy


class HandleHttpRequest:
    def __init__(self, route: Route, config_provider: ConfigProvider, http_client: HttpClient, upstream_strategy: UpstreamStrategy):
        self.config_provider = config_provider
        self.http_client = http_client
        self.upstream_strategy = upstream_strategy
        self.route = route

    async def handler(self, request: HttpRequest):
        backend_server = self.upstream_strategy.select_backend(self.route.upstream)
        return await self.http_client.send(request, backend_server)
