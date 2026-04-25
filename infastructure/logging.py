import logging
from domain.ports import ConfigProvider


class Logger:
    def __init__(self, config_provider: ConfigProvider):
        logging.basicConfig(
            level=config_provider.config['logging']['level'],
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler()]
        )
        self.logger = logging.getLogger("ProxyServer")
