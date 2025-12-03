from flask import Blueprint, request, jsonify
from ..db_utils import *
import app.processing.process_data as process_data_module
import asyncio

garmin_bp = Blueprint('garmin', __name__)
 
@garmin_bp.route('/webhook', methods=['POST'])
def garmin_webhook():
    payload = request.get_json()
    if not payload:
        return jsonify({"error": "no_payload"}), 400

    # --- Deregistration ---
    if "deregistrations" in payload:
        for dereg in payload["deregistrations"]:
            garmin_id = dereg["userId"]
            delete_user(garmin_id)
        return jsonify({"status": "deregistration_processed"}), 200

    # --- PING notifications ---
    for summary_type, summary_list in payload.items():
        if isinstance(summary_list, list):
            for ping_data in summary_list:
                garmin_id = ping_data.get("userId")
                callback_url = ping_data.get("callbackURL")
                # At minimum, you can log the ping:
                logger.info(f"Received ping {summary_type} for Garmin user {garmin_id}")
                # Asynchronously fetch data if callbackURL exists:
                if callback_url:
                    # Use of run_coroutine_threadsafe to schedule the async task
                    asyncio.run_coroutine_threadsafe(
                        process_data_module.process_ping(summary_type, callback_url, garmin_id), 
                        process_data_module.loop
                    )
                return jsonify({"status": f"{summary_type}_ping_received"}), 200

    return jsonify({"error": "unknown_payload_structure"}), 400