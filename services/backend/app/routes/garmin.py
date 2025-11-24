# to create webhook for garmin API
from flask import Blueprint, request, jsonify
from ..services.wearable_producer import send_data

garmin_bp = Blueprint('garmin', __name__)

@garmin_bp.route('/webhook', methods=['POST'])
def garmin_webhook():
    """
    Endpoint to receive Garmin webhook data.
    Returns:
        JSON response indicating success or failure.
    """
    # Process the incoming data from Garmin API
    data = request.get_json()
    if not data:
        return jsonify({"error": "Invalid data"}), 400
    
    # Produce data to Kafka
    send_data(data)
    return jsonify({"status": "success"}), 200