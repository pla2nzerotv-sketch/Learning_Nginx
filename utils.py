import asyncio


async def handler_body(content_length: str, reader, config_provider):
    current_content_length = 0
    full_body = []
    while int(content_length) != current_content_length:
        try:
            body = await asyncio.wait_for(reader.read(1024), config_provider.config['timeouts']['write_ms'])
        except asyncio.TimeoutError:
            raise Exception("Таймаут ожидания данных, закрытие соединения")
        if not body:
            break
        current_content_length += len(body)
        full_body.append(body)
    return b''.join(full_body)