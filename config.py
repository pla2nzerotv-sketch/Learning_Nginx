import yaml


class Config:
    class PROXY:
        HOST = None
        PORT = None

    class Upstream:
        HOST = None
        PORT_1 = None
        PORT_2 = None

    class Timeout:
        CONNECT_MS = None
        READ_MS = None
        WRITE_MS = None
        TOTAL_MS = None

    class Limit:
        MAX_CLIENT_CONNS = None
        MAX_CONNS_PER_UPSTREAM = None

    class LOGGING:
        LEVEL = None

    @staticmethod
    def file_read():
        with open('../config.yml', 'r') as f:
            return yaml.safe_load(f)

    @classmethod
    def setup(cls, data: dict):
        cls.PROXY.HOST = data['listen'].split(':')[0]
        cls.PROXY.PORT = data['listen'].split(':')[1]
        cls.Upstream.HOST = data['upstreams'][0].get('host')
        cls.Upstream.PORT_1 = data['upstreams'][0].get('port')
        cls.Upstream.PORT_2 = data['upstreams'][1].get('port')
        cls.Timeout.CONNECT_MS = data['timeouts'].get('connect_ms')
        cls.Timeout.READ_MS = data['timeouts'].get('read_ms')
        cls.Timeout.WRITE_MS = data['timeouts'].get('write_ms')
        cls.Timeout.TOTAL_MS = data['timeouts'].get('total_ms')
        cls.Limit.MAX_CLIENT_CONNS = data['limits'].get('max_client_conns')
        cls.Limit.MAX_CONNS_PER_UPSTREAM = data['limits'].get('max_conns_per_upstream')
        cls.LOGGING.LEVEL = data['logging'].get('level')