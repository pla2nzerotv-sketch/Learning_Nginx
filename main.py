import asyncio

from adapters.config import FileConfigProvider
from adapters.http_client import SocketHttpClient
from adapters.http_server import HttpServer
from adapters.upstream_strategy import RoundRobin
from application.handler_request import HandleHttpRequest
from infastructure.logging import Logger


async def main():
    config_provider = FileConfigProvider('config.yml')
    logger = Logger(config_provider)
    max_conns_per_backend = config_provider.config['limits']['max_conns_per_upstream']
    backend_semaphores = {
        upstream['port']: asyncio.Semaphore(max_conns_per_backend) for upstream in config_provider.config['upstreams']
    }
    http_server_semaphore = asyncio.Semaphore(config_provider.config['limits']['max_client_conns'])
    upstream_strategy = RoundRobin()
    route = config_provider.get_rotes(upstream_strategy)
    socket_http_client = SocketHttpClient(config_provider, backend_semaphores, logger)
    handle_http_request = HandleHttpRequest(route, config_provider, socket_http_client, upstream_strategy)
    http_server = HttpServer(handle_http_request, http_server_semaphore)
    server = await asyncio.start_server(http_server.handler, config_provider.config.get('listen').split(":")[0], config_provider.config.get('listen').split(":")[1])
    async with server:
        await server.serve_forever()

if __name__ == '__main__':
    asyncio.run(main())