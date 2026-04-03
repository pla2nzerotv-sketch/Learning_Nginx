import asyncio
import multiprocessing
from time import sleep

import uvicorn
from fastapi import FastAPI, Request

app = FastAPI()


@app.get("/")
async def read_root(request: Request):
    return {"message": "Hello, World!"}

@app.post("/echo")
async def read_root(request: Request):
    body = await request.body()
    return {"message": body}



def start_server(port):
    uvicorn.run(app, host="localhost", port=port)


if __name__ == '__main__':
    p1 = multiprocessing.Process(target=start_server, args=(9001,))
    p2 = multiprocessing.Process(target=start_server, args=(9002,))
    p1.start()
    p2.start()
    p1.join()
    p2.join()
