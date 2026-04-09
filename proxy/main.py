import asyncio
import logging

import uvloop

from config import Config
from proxy.proxy_server import ProxyServer
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())



if __name__ == '__main__':
    config = Config()
    config_data = config.file_read()
    config.setup(config_data)
    proxy_server = ProxyServer(config)
    asyncio.run(proxy_server.start_proxy())
