import aiohttp
import asyncio

visited = set()

# async def async_fetch(session, url):
#     if url in visited:
#         return None
#     visited.add(url)
#     async with session.get(url) as response:
#         return await response.text()

# async def async_fetch_all(urls):
#     async with aiohttp.ClientSession() as session:
#         return await asyncio.gather([fetch_async(session, url) for url in urls])