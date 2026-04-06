import asyncio
import unittest


class TestAsyncExample(unittest.TestCase):
    def test_basic(self):
        async def say_hello():
            print("hello")
            await asyncio.sleep(1)  # 模拟IO等待；有点类似bthread.sleep，而不能直接time.sleep(2)，后者会阻塞整个线程
            print("world")

        asyncio.run(say_hello())

    def test_concurrent(self):
        async def task(name):
            print(f"{name} start")
            await asyncio.sleep(2)
            print(f"{name} end")

        async def main():
            await asyncio.gather(
                task("A"),
                task("B"),
                task("C"),
            )

        asyncio.run(main())


    def test_http(self):
        """
        并发请求 API
        :return:
        """
        import asyncio
        import aiohttp

        async def fetch(url):
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    return await resp.text()

        async def main():
            urls = [
                "https://example.com",
                "https://example.org",
                "https://example.net",
            ]

            tasks = [fetch(url) for url in urls]
            results = await asyncio.gather(*tasks)

            for r in results:
                print(len(r))

        asyncio.run(main())
