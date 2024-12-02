import asyncio
import json
import os
import logging

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from spectre_crawler import main
from dotenv import load_dotenv
from apscheduler.schedulers.asyncio import AsyncIOScheduler
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

scheduler = AsyncIOScheduler()


@app.get("/nodes")
async def read_nodes():
    """Return the contents of nodes.json without updating any geolocation data."""
    if not os.path.exists(NODE_OUTPUT_FILE):
        logging.error("nodes.json file does not exist. Run the crawler first.")
        return JSONResponse(
            content={"error": "nodes.json file does not exist. Run the crawler first."},
            status_code=404,
        )

    try:
        with open(NODE_OUTPUT_FILE, "r") as f:
            data = json.loads(f.read())
    except json.JSONDecodeError:
        logging.error("nodes.json contains invalid JSON data.")
        return JSONResponse(
            content={"error": "nodes.json contains invalid JSON data."}, status_code=400
        )

    return data


@app.on_event("startup")
async def init_data():
    """Initialize scheduled crawler job on server startup."""
    scheduler.add_job(update_nodes, "interval", hours=24)
    scheduler.start()
    logging.info("Scheduler started")


async def update_nodes():
    """Run the crawler asynchronously."""
    logging.info("Starting crawler job")
    hostpair = seed_node.split(":") if ":" in seed_node else (seed_node, "18111")
    try:
        await asyncio.wait_for(
            main(
                [hostpair],
                "spectre-mainnet",
                NODE_OUTPUT_FILE,
                api_key=api_key,
                start_address=seed_node,
            ),
            timeout=30 * 60,  # 30min
        )
    except asyncio.TimeoutError:
        logging.warning("Crawler job exceeded the maximum runtime of 15 minutes.")
    except Exception as e:
        logging.error(f"An error occurred during the crawler job: {e}")


@app.on_event("shutdown")
async def shutdown_scheduler():
    """Shutdown the scheduler gracefully."""
    logging.info("Shutting down scheduler.")
    scheduler.shutdown(wait=True)
