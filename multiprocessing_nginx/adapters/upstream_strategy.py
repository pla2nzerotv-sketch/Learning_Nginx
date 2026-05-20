import itertools

from multiprocessing_nginx.domain.entities import BackendServer, Upstream
from multiprocessing_nginx.domain.ports import UpstreamStrategy


class RoundRobin(UpstreamStrategy):
    list_backend_servers = None

    def select_backend(self, upstream: Upstream) -> BackendServer:
        if not self.list_backend_servers:
            self.list_backend_servers = itertools.cycle(upstream.list_backend_servers)
        return next(self.list_backend_servers)
