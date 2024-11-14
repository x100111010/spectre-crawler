import asyncio
import json
import os
import logging
import aiohttp
import re

from fastapi import FastAPI
from spectre_crawler import main
from dotenv import load_dotenv
from cache import AsyncLRU
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi.middleware.cors import CORSMiddleware

load_dotenv(override=True)
app = FastAPI()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

seed_node = os.getenv("SEED_NODE", False)
verbose = os.getenv("VERBOSE", 0)
api_key = os.getenv("IPGEOLOCATION_API_KEY", 0)

logging.basicConfig(
    level=[logging.WARN, logging.INFO, logging.DEBUG][min(int(verbose), 2)]
)

NODE_OUTPUT_FILE = "data/nodes.json"


def extract_ip_address(input_string):
    pattern = r"(?:ipv6:\[([:0-9a-fA-F]+)\]|(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}))"
    match = re.search(pattern, input_string)

    if match:
        ipv6_address = match.group(1)
        ipv4_address = match.group(2)

        if ipv6_address:
            return ipv6_address
        elif ipv4_address:
            return ipv4_address
        else:
            return None
    else:
        return None


@AsyncLRU(maxsize=8192)
async def get_ip_info(ip):
    """Fetch IP geolocation using ipgeolocation.io."""
    url = f"https://api.ipgeolocation.io/ipgeo?apiKey={api_key}&ip={ip}&fields=country_name,city,latitude,longitude"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                res = await response.json()
                latitude = res.get("latitude")
                longitude = res.get("longitude")

                # loc as "latitude,longitude"
                if latitude and longitude:
                    return f"{latitude},{longitude}"
            else:
                logging.warning(
                    f"Geolocation request for {ip} failed with status {response.status}"
                )
            return None


@app.get("/")
async def read_root():
    """Return the contents of nodes.json without updating any geolocation data."""
    if not os.path.exists(NODE_OUTPUT_FILE):
        logging.error("nodes.json file does not exist. Run the crawler first.")
        return {"error": "nodes.json file does not exist. Run the crawler first."}

    try:
        with open(NODE_OUTPUT_FILE, "r") as f:
            data = json.loads(f.read())
    except json.JSONDecodeError:
        logging.error("nodes.json contains invalid JSON data.")
        return {"error": "nodes.json contains invalid JSON data."}

    return data


@app.on_event("startup")
def init_data():
    """Initialize scheduled crawler job on server startup and trigger the first run."""
    scheduler = BackgroundScheduler()
    scheduler.add_job(update_nodes, "interval", hours=12)
    scheduler.start()


async def update_nodes_async() -> None:
    """Run the crawler asynchronously."""
    logging.info("Starting crawler job")
    hostpair = seed_node.split(":") if ":" in seed_node else (seed_node, "18111")
    await main(
        [hostpair],
        "spectre-mainnet",
        NODE_OUTPUT_FILE,
        api_key=api_key,
        start_address=seed_node,
    )


def update_nodes() -> None:
    """Wrap the async update_nodes call in asyncio with timeout."""
    max_runtime = 15 * 60
    try:
        asyncio.run(asyncio.wait_for(update_nodes_async(), timeout=max_runtime))
    except asyncio.TimeoutError:
        logging.warning(f"Job exceeded max runtime of {max_runtime} seconds")
