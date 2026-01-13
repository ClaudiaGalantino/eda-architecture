from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from zoneinfo import ZoneInfo
from app.db_utils import *
from app.processing.wearable_producer import send_data
import app.garmin_client as garmin_module
import asyncio, requests, threading, io, base64, json

CET = ZoneInfo("Europe/Rome")
def log(prefix, message):
    """
    Simple logger function.
    """
    print(f"[{datetime.now(CET).strftime('%Y-%m-%d %H:%M:%S')}][{prefix}] {message}")

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
        log("PROCESS_DATA", "Garmin client not initialized")
        return None
    
    try: 
        return garmin_module.garmin_client.call_protected_resources(
            token,
            secret,
            url, 
            method = 'GET',
            )
    except Exception as e:
        log("PROCESS_DATA", f"Error fetching data from Garmin API: {e}")
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


async def process_ping(summary_type, callback_url, garmin_id):
    log('PROCESS_DATA', f"Processing ping for Garmin ID {garmin_id}, summary type {summary_type}")

    token, secret = get_token(garmin_id)
    if not token or not secret:
        log("PROCESS_DATA", f"No token found for Garmin ID {garmin_id}")
        return
    
    resp = await fetch_data_from_garmin(token, secret, callback_url)

    if resp is None:
        log("PROCESS_DATA", f"Failed to fetch data for Garmin ID {garmin_id}")
        return
    
    if resp.status_code == 200:
        if summary_type == 'activityFiles':
            log("PROCESS_DATA", f"Sending FIT file to fit-processor for {garmin_id}")
            fit_file_bin = io.BytesIO(resp.content)
            # Call Java fit-processor container
            files = {'file': ('activity.fit', fit_file_bin, 'application/octet-stream')}
            params = {'deviceIdentifier': garmin_id}
            try:
                # Eseguiamo la POST verso il container fit-processor
                # L'URL usa il nome del servizio definito nel docker-compose
                java_resp = requests.post(
                    "http://fit-processor:8080/process", 
                    params=params,
                    files=files
                )                
                if java_resp.status_code == 200:
                    processed_chunks = []
                    for line in java_resp.iter_lines():
                        if line:
                            processed_chunks.append(json.loads(line.decode('utf-8')))
                    payload = processed_chunks
                    log("PROCESS_DATA", f"FIT file processed successfully: {len(payload)} chunks received")
                else:
                    log("PROCESS_DATA", f"Java processor error: {java_resp.status_code}")
                    payload = base64.b64encode(resp.content).decode('utf-8')
                    
            except Exception as e:
                log("PROCESS_DATA", f"Failed to connect to fit-processor: {e}")
                payload = base64.b64encode(resp.content).decode('utf-8')
            try:
                payload = resp.json()
            except Exception:
                payload = resp.text
        else:
            try:
                payload = resp.json()
            except Exception as e:
                log("PROCESS_DATA", f"Error parsing JSON response: {e}")
                payload = resp.text
        log("PROCESS_DATA", f"Fetched data for Garmin ID {garmin_id}; scheduling publish to Kafka")
        
        current_time = datetime.now(CET).strftime("%Y-%m-%d %H:%M:%S")

        # Publish to Kafka in executor
        kafka_payload = {
            "summary_type": summary_type,
            "garmin_id": garmin_id,
            "timestamp": current_time,
            "data": payload
        }
        try:
            await loop.run_in_executor(executor, send_data, kafka_payload)
        except Exception as e:
            log("PROCESS_DATA", f"Error sending data to Kafka: {e}")
    else:
        log("PROCESS_DATA", f"Fetch failed: status={resp.status_code} body={resp.text}")

    print(f"The summary is {summary_type} and the url is {callback_url}")