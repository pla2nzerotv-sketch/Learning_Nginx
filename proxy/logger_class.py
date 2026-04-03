import logging

from config import Config


class Logger:
    def __init__(self, config: Config):
        logging.basicConfig(
            level=config.LOGGING.LEVEL,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler()]
        )
        self.logger = logging.getLogger("ProxyServer")
