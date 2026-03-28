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
    print(body)
    return {"message": body}



def start_server(port):
    uvicorn.run(app, host="localhost", port=port)


if __name__ == '__main__':
    p1 = multiprocessing.Process(target=start_server, args=(9020,))
    p2 = multiprocessing.Process(target=start_server, args=(9021,))
    p1.start()
    p2.start()
    p1.join()
    p2.join()
