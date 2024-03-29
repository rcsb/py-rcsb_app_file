##
# File:    testAsync.py
# Author:  James Smith
# Date:    Feb-2024
# Version: 0.001
#

import asyncio
import unittest
import subprocess
import os
import time
import aiohttp
import requests
import logging
from rcsb.app.file.ConfigProvider import ConfigProvider

logging.basicConfig(level=logging.INFO)


class TestAsyncServer(unittest.IsolatedAsyncioTestCase):
    """
    verify that server handles requests asynchronously
    """

    @classmethod
    def setUpClass(cls) -> None:
        provider = ConfigProvider()
        host = provider.get("SERVER_HOST_AND_PORT")
        host = host.replace("http://", "")
        subprocess.Popen(
            [
                "gunicorn",
                "--bind",
                host,
                "-k",
                "uvicorn.workers.UvicornWorker",
                "rcsb.app.file.main:app",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )
        time.sleep(5)

    @classmethod
    def tearDownClass(cls) -> None:
        os.system(
            "pid=$(ps -e | grep gunicorn | head -n1 | awk '{print $1;}';);echo 'the following pids ran'; echo $pid; kill $pid;"
        )

    async def asyncSetUp(self) -> None:
        self.base_url = ConfigProvider().get("SERVER_HOST_AND_PORT")
        self.url = os.path.join(self.base_url, "asyncTest")

    async def asyncTearDown(self) -> None:
        pass

    async def fetch(self, session, url, index, waittime):
        result = None
        async with session.post(
            url, data={"index": index, "waittime": waittime}
        ) as response:
            result = await response.json()
        logging.info(str(result))
        return result

    async def testAsync(self):
        """
        though aiohttp is on the client side, it cannot not retrieve async results unless the server also handles them asynchronously
        """
        logging.info("running async test")
        indices = range(1, 11)
        waittimes = range(10, 0, -1)
        expected = max(waittimes)  # 10 s
        null_model = sum(waittimes)  # 55 s
        url = self.url
        results = []
        start = time.time()
        async with aiohttp.ClientSession() as session:
            tasks = [
                self.fetch(session, url, index, waittime)
                for index, waittime in zip(indices, waittimes)
            ]
            for coro in asyncio.as_completed(tasks):
                result = await coro
                results.append(result)
        observed_time = time.time() - start
        observed_indices = [d["index"] for d in results]
        expected_indices = list(indices)
        if results:
            # verify not same duration as synchronous
            self.assertFalse(
                observed_time >= null_model,
                "error - expected %d s observed %.2f s" % (expected, observed_time),
            )
            # verify expected wait time, allow some latency from expected
            self.assertTrue(
                observed_time < expected * 1.5,
                "error - expected %d s observed %.2f s" % (expected, observed_time),
            )
            # verify much faster than synchronous
            self.assertTrue(
                observed_time < null_model / 4,
                "error - async time not significantly less than synchronous",
            )
            # verify results are out of order
            self.assertFalse(
                observed_indices == expected_indices,
                "error - did not get async results - observed %s"
                % str(observed_indices),
            )
        else:
            self.fail()

    def testSynchro(self):
        """
        control test to compare with async test even though synchronous order is enforced from the client side
        """
        logging.info("running synchronous test")
        indices = range(1, 11)
        waittimes = range(10, 0, -1)
        expected = sum(waittimes)  # 55 s
        url = self.url
        results = []
        start = time.time()
        for index, waittime in zip(indices, waittimes):
            response = requests.post(url, data={"index": index, "waittime": waittime})
            if response.status_code < 400:
                result = response.json()
                logging.info(result)
                results.append(result)
            else:
                logging.info("error - status code %d", response.status_code)
                self.fail()
        observed_time = time.time() - start
        observed_indices = [d["index"] for d in results]
        expected_indices = list(indices)
        self.assertFalse(
            observed_time < expected,
            "error - expected %d s observed %.2f s" % (expected, observed_time),
        )
        self.assertTrue(
            observed_indices == expected_indices,
            "error - expected %s observed %s" % (indices, observed_indices),
        )


if __name__ == "__main__":
    unittest.main()
