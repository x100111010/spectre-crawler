import logging
import os
import grpc
import time
import random
import ipaddress
import asyncio
import json
import aiohttp
import platform
import argparse

import p2p_pb2
import messages_pb2
import messages_pb2_grpc


async def message_stream(queue):
    message = await queue.get()
    while message is not None:
        logging.debug("Sending %s", message)
        yield message
        queue.task_done()
        message = await queue.get()
    queue.task_done()


class P2PNode(object):
    USER_AGENT = "/crawler:0.0.1/"

    def __init__(
        self,
        address="localhost:18111",
        network="spectre-mainnet",
        api_key=None,
        start_address=None,
    ):
        self.network = network
        self.address = address
        self.api_key = api_key
        self.start_address = start_address

    async def ipinfo(self, session):
        addr, _ = self.address.rsplit(":", 1)
        addr = addr.replace("ipv6:", "").strip("[]")

        # skip starting address specified with --addr
        if self.start_address and addr == self.start_address.split(":")[0]:
            logging.info(f"Skipping geolocation for start address {addr}")
            return ""

        retries = 2
        while retries > 0:
            try:
                logging.debug(
                    f"Requesting geolocation for {addr} (retries left: {retries})"
                )
                api_url = f"https://api.ipgeolocation.io/ipgeo?apiKey={self.api_key}&ip={addr}&fields=country_name,city,latitude,longitude"

                async with session.get(api_url) as response:
                    if response.status != 200:
                        logging.warning(
                            f"Geolocation request for {addr} failed with status {response.status}"
                        )
                        retries -= 1
                        await asyncio.sleep(2)  # wait
                        continue

                    resp = await response.json()
                    logging.debug(f"Geolocation response for {addr}: {resp}")

                    # check if required data (latitude and longitude) is available
                    if "latitude" in resp and "longitude" in resp:
                        latitude = resp["latitude"]
                        longitude = resp["longitude"]
                        loc = f"{latitude},{longitude}"
                        logging.info(f"Geolocation for {addr} found: {loc}")
                        return loc
                    else:
                        logging.warning(
                            f"Geolocation response is missing location for {addr}: {resp}"
                        )
                        return ""

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logging.warning(
                    f"Error reading geolocation data for {addr} (retries left: {retries}): {e}"
                )
                retries -= 1
                await asyncio.sleep(2)

        # skip this IP and return an empty location
        logging.warning(
            f"Failed to retrieve geolocation for {addr} after multiple attempts, skipping."
        )
        return ""

    async def __aenter__(self):
        self.ID = bytes.fromhex(hex(int(random.random() * 10000))[2:].zfill(32))
        self.channel = grpc.aio.insecure_channel(self.address)

        self.peer_version = 2
        self.peer_id = None

        await asyncio.wait_for(self.channel.channel_ready(), 5)
        self.stub = messages_pb2_grpc.P2PStub(self.channel)

        self.send_queue = asyncio.queues.Queue()

        self.stream = self.stub.MessageStream(message_stream(self.send_queue))
        self.stream.address = self.address
        await self.handshake()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.send_queue.put(None)
        if exc_type is not None and issubclass(exc_type, asyncio.CancelledError):
            self.stream.cancel()
        else:
            await self.send_queue.join()
        await self.channel.close(2)

    async def handshake(self):
        logging.debug("Starting handshake")
        async for item in self.stream:
            logging.debug("Getting %s", item)
            payload = item.WhichOneof("payload")
            if payload == "version":
                self.peer_id = item.version.id
                self.peer_version = item.version.protocolVersion
                self.peer_spectred = item.version.userAgent
                await self.send_queue.put(
                    messages_pb2.SpectredMessage(
                        version=p2p_pb2.VersionMessage(
                            protocolVersion=self.peer_version,
                            timestamp=int(time.time()),
                            id=self.ID,
                            userAgent=self.USER_AGENT,
                            network=self.network,
                        )
                    )
                )
            elif payload == "verack":
                await self.send_queue.put(
                    messages_pb2.SpectredMessage(verack=p2p_pb2.VerackMessage())
                )
                if self.peer_version < 4:
                    logging.debug("Handshake done")
                    return
            elif payload == "ready":
                await self.send_queue.put(
                    messages_pb2.SpectredMessage(ready=p2p_pb2.ReadyMessage())
                )
                logging.debug("Handshake done")
                return
            else:
                logging.debug("During handshake, got unexpected %s", payload)

    async def get_addresses(self):
        logging.debug("Starting get_addresses")
        await self.send_queue.put(
            messages_pb2.SpectredMessage(
                requestAddresses=p2p_pb2.RequestAddressesMessage()
            )
        )
        async for item in self.stream:
            logging.debug("Getting %s", item)
            payload = item.WhichOneof("payload")
            if payload == "addresses":
                return item.addresses.addressList
            elif payload == "requestAddresses":
                await self.send_queue.put(
                    messages_pb2.SpectredMessage(
                        addresses=p2p_pb2.AddressesMessage(addressList=[])
                    )
                )


async def get_addresses(
    address, network, semaphore: asyncio.Semaphore, api_key=None, start_address=None
):
    try:
        addresses = set()
        prev_size = -1
        patience = 10
        peer_id = ""
        peer_spectred = ""
        loc = ""
        try:
            async with aiohttp.ClientSession() as session:
                async with P2PNode(
                    address, network, api_key=api_key, start_address=start_address
                ) as node:
                    peer_id = node.peer_id.hex()
                    peer_spectred = node.peer_spectred
                    prev = time.time()
                    while len(addresses) > prev_size or patience > 0:
                        if time.time() - prev > 5:
                            logging.info("getting more addresses")
                            prev = time.time()
                        if len(addresses) <= prev_size:
                            patience -= 1
                        else:
                            patience = 10
                        prev_size = len(addresses)
                        item = await node.get_addresses()
                        if item is not None:
                            addresses.update(
                                ((x.timestamp, x.ip, x.port) for x in item)
                            )
                    loc = await node.ipinfo(session)

        except asyncio.exceptions.TimeoutError:
            logging.debug("Node %s timed out", address)
            return address, peer_id, peer_spectred, addresses, "timeout", loc
        except Exception as e:
            logging.exception("Error in task")
            return address, peer_id, peer_spectred, addresses, e, loc

        return address, peer_id, peer_spectred, addresses, "", loc
    except asyncio.CancelledError:
        logging.debug("Task was canceled")


async def main(addresses, network, output, api_key=None, start_address=None):
    # ulimit differently based on the operating system
    if platform.system() == "Windows":
        ulimit = 100  # limit for Windows
    else:
        import resource

        ulimit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
        ulimit = max(ulimit - 20, 1)

    logging.info(f"Running {ulimit} tasks concurrently")
    semaphore = asyncio.Semaphore(ulimit)

    res = {}
    bad_ipstrs = []
    seen = set()
    pending = [
        asyncio.create_task(
            get_addresses(
                f"{address}:{port}",
                network,
                semaphore,
                api_key=api_key,
                start_address=start_address,
            )
        )
        for address, port in addresses
    ]
    start_time = time.time()
    timeout_time = start_time + 60 * 25  # 25 minutes

    try:
        while len(pending) > 0 and time.time() < timeout_time:
            logging.info(f"Currently pending: {len(pending)}")
            done, pending = await asyncio.wait(
                pending, return_when=asyncio.FIRST_COMPLETED
            )

            for task in done:
                try:
                    result = task.result()
                    if result is None:
                        continue
                    address, peer_id, peer_spectred, addresses, error, loc = result
                except asyncio.TimeoutError:
                    logging.warning("Task timed out and was cancelled")
                    continue

                res[address] = {
                    "neighbors": [],
                    "id": peer_id,
                    "spectred": peer_spectred,
                    "error": error,
                    "loc": loc,
                }
                if error is not None:
                    res[address]["error"] = repr(error)
                for ts, ipstr, port in addresses:
                    if ipstr.hex() not in bad_ipstrs:
                        try:
                            ip = ipaddress.ip_address(ipstr)
                            if not ip.is_private and not ip.is_loopback:
                                if isinstance(ip, ipaddress.IPv6Address):
                                    new_address = f"ipv6:[{ip}]:{port}"
                                else:
                                    new_address = f"{ip}:{port}"
                                res[address]["neighbors"].append(new_address)
                                if new_address not in seen:
                                    seen.add(new_address)
                                    pending.add(
                                        asyncio.create_task(
                                            asyncio.wait_for(
                                                get_addresses(
                                                    new_address,
                                                    network,
                                                    semaphore,
                                                    api_key=api_key,
                                                    start_address=start_address,
                                                ),
                                                timeout=120,
                                            )
                                        )
                                    )
                            else:
                                logging.debug(f"Got private address {ip}")
                        except Exception:
                            logging.exception("Bad ip")
                            bad_ipstrs.append(ipstr.hex())
        logging.info("Done")
    finally:
        for task in pending:
            task.cancel()

        logging.info("Writing results...")
        clean_res = {}
        for i in res:
            if res[i]["neighbors"] != []:
                clean_res[i] = res[i]
                del clean_res[i]["neighbors"]

        async with semaphore:
            if len(clean_res) >= 10:
                json.dump(
                    {"nodes": clean_res, "updated_at": int(time.time())},
                    open(output, "w"),
                    allow_nan=False,
                    indent=2,
                    sort_keys=True,
                    ensure_ascii=True,
                )

        while len(pending) > 0:
            logging.warning(
                f"Shutting down after cancelling {len(pending)} tasks. Please wait..."
            )
            done, pending = await asyncio.wait(
                pending, return_when=asyncio.FIRST_COMPLETED
            )
        logging.warning("All tasks seem to be down. Finalizing shut down...")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Crawler to list all known p2p nodes and their information. Used to create a map of the p2p nodes"
    )
    parser.add_argument(
        "-v", "--verbose", help="Verbosity level", action="count", default=1
    )
    parser.add_argument(
        "--addr",
        help="Start ip:port for crawling",
        default="n-mainnet.spectre.ws:18111",
    )
    parser.add_argument("--output", help="output json path", default="data/nodes.json")
    parser.add_argument(
        "--network",
        help="Which network to connect to",
        choices=["spectre-mainnet", "spectre-testnet", "spectre-devnet"],
        default="spectre-mainnet",
    )
    parser.add_argument("--api_key", help="API key for ipgeolocation.io")

    args = parser.parse_args()

    if not (
        os.access(args.output, os.W_OK)
        or (
            not os.path.exists(args.output)
            and os.access(os.path.dirname(args.output), os.W_OK)
        )
    ):
        parser.error(
            f"Cannot write to {args.output} (check directory exists and you have permissions)"
        )

    logging.basicConfig(
        level=[logging.WARN, logging.INFO, logging.DEBUG][min(args.verbose, 2)]
    )
    hostpair = args.addr.split(":") if ":" in args.addr else (args.addr, "18111")

    asyncio.run(
        main(
            [hostpair],
            args.network,
            args.output,
            api_key=args.api_key,
            start_address=args.addr,
        )
    )
