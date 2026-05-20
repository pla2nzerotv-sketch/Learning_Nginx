import yaml

from multiprocessing_nginx.domain.entities import Route, Upstream, BackendServer
from multiprocessing_nginx.domain.ports import ConfigProvider


class FileConfigProvider(ConfigProvider):
    def __init__(self, path):
        with open(path, 'r') as f:
            self.config = yaml.safe_load(f)

    def get_rotes(self, upstream_strategy):
        backend_servers = [
            BackendServer(host=upstream['host'], port=upstream['port']) for upstream in
            self.config['upstreams']
        ]
        upstream = Upstream(upstream_strategy=upstream_strategy, list_backend_servers=backend_servers)
        return Route(path_pattern='/', method=['GET'], upstream=upstream)

