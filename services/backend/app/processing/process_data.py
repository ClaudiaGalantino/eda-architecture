from concurrent.futures import ThreadPoolExecutor
from app.db_utils import *
from app.processing.wearable_producer import send_data
import app.garmin_client as garmin_module
import asyncio
import threading
import logging

logger = logging.getLogger(__name__)

# Global variables for the event loop and executor
loop = asyncio.new_event_loop()
# ThreadPoolExecutor to run blocking tasks without blocking the event loop
executor = ThreadPoolExecutor(max_workers=5)

def start_loop():
    """
    Starts the asyncio event loop in a separate thread.
    """
    # Set the event loop for the current thread and run forever
    asyncio.set_event_loop(loop)
    loop.run_forever()

# Start the event loop in a background thread when the application starts
threading.Thread(target=start_loop, daemon=True).start()

def _synch_fetch(token, secret, url):
    """
    Synchronously fetch data from Garmin API using the provided token, secret, and URL.

    Args:
        token (str): The authentication token.
        secret (str): The authentication secret.
        url (str): The API endpoint URL.

    Returns:
        dict or None: The fetched data or None if an error occurs.
    """

    if garmin_module.garmin_client is None:
        logger.error("Garmin client not initialized")
        return None
    
    try: 
        return garmin_module.garmin_client.call_protected_resources(
            token,
            secret,
            url, 
            method = 'GET',
            )
    except Exception as e:
        logger.error(f"Error fetching data from Garmin API: {e}")
        return None


async def fetch_data_from_garmin(token, secret, url):
    """
    Fetch data from Garmin API asynchronously using the provided token, secret, and URL.

    Args:
        token (str): The authentication token.
        secret (str): The authentication secret.
        url (str): The API endpoint URL.

    Returns:
        dict or None: The fetched data or None if an error occurs.
    """
    resp = await loop.run_in_executor(executor, _synch_fetch, token, secret, url)
    return resp


async def process_ping(summary_name, callback_url, garmin_id):
    logger.info(f"The summary is {summary_name} and the url is {callback_url}")

    token, secret = get_token(garmin_id)
    if not token or not secret:
        logger.error(f"No token found for Garmin ID {garmin_id}")
        return
    
    resp = await fetch_data_from_garmin(token, secret, callback_url)

    if resp is None:
        logger.error(f"Failed to fetch data for Garmin ID {garmin_id}")
        return
    
    if resp.status_code == 200:
        try:
            payload = resp.json()
        except Exception:
            payload = resp.text
        logger.info(f"Fetched data for Garmin ID {garmin_id}; scheduling publish to Kafka")
        
        # Publish to Kafka in executor
        try:
            await loop.run_in_executor(executor, send_data, summary_name, payload)
        except Exception as e:
            logger.error(f"Error sending data to Kafka: {e}")
    else:
        logger.warning(f"Fetch failed: status={resp.status_code} body={resp.text}")

    print(f"The summary is {summary_name} and the url is {callback_url}")
