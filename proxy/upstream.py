import asyncio
import itertools
from collections import defaultdict

from config import Config


class HandlerUpstream:

    def __init__(self, config: Config):
        self.config = config
        self.echo_server_iter = iter(itertools.cycle([
            (config.Upstream.HOST, getattr(config.Upstream, attr_name))
            for attr_name in dir(config.Upstream)
            if attr_name.startswith("PORT_")
        ]))
        self.upstream_connection_poll = defaultdict(asyncio.Queue)

    def get_data_connect(self):
        return next(self.echo_server_iter)

    async def get_upstream_connection(self, host, port):
        pool = self.upstream_connection_poll[(host, port)]
        if not pool.empty():
            return await pool.get()
        return await self._connect(host, port)

    async def return_upstream_connection(self, host, port, reader, writer):
        pool = self.upstream_connection_poll[(host, port)]
        await pool.put((reader, writer))

    async def _connect(self, host, port):
        return await asyncio.wait_for(
            asyncio.open_connection(host, port), self.config.Timeout.CONNECT_MS
        )
