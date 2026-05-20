import asyncio
from abc import ABC, abstractmethod

from multiprocessing_nginx.domain.entities import Route, HttpRequest, BackendServer, HttpResponse, Upstream


class ConfigProvider(ABC):
    @abstractmethod
    def get_rotes(self, upstream_strategy) -> Route:
        raise NotImplementedError


class HttpClient(ABC):
    @abstractmethod
    def __init__(self, config_provider: ConfigProvider, backend_semaphores: dict[int, asyncio.Semaphore]) -> None:
        raise NotImplementedError

    @abstractmethod
    async def send(self, request: HttpRequest, backend: BackendServer) -> HttpResponse:
        raise NotImplementedError


class UpstreamStrategy(ABC):
    @abstractmethod
    def select_backend(self, upstream: Upstream) -> BackendServer:
        raise NotImplementedError
