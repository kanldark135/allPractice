import asyncio
from asyncio import Lock


class FilteredMessagesQueue:
    def __init__(self):
        self.queue = asyncio.Queue()
        self.lock = Lock()

    async def put(self, item):
        await self.queue.put(item)

    async def get(self):
        return await self.queue.get()

    async def clear(self):
        while not self.queue.empty():
            await self.queue.get()

    def is_empty(self):
        return self.queue.empty()
