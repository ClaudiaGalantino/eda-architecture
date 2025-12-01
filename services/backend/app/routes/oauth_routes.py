import uuid
from venv import logger
from flask import Blueprint, request, redirect, url_for, render_template_string, session
from app.db_utils import *
import app.garmin_client as garmin_module

# Blueprint for OAuth routes
oauth_bp = Blueprint('oauth', __name__)

# In-memory store for the Request Token (temporary)
temp_request_tokens = {}

# ===========================
# UTILITY FOR USER SESSION
# ===========================

def get_current_user_id():
    """
    Retrieve or generate an internal user ID from the Flask session.
    This value is used as the internal user identifier.
    """
    if 'user_id' not in session:
        session['user_id'] = f"session_user_{uuid.uuid4().hex[:10]}"
        logger.info(f"New user session created: {session['user_id']}")
    return session['user_id']

# =======================
# OAUTH ROUTES
# =======================

@oauth_bp.route('/')
def index():
    # Use the get_current_user_id() function for the internal user ID
    current_user_id = get_current_user_id() 
    row = get_token(current_user_id)
    html_template = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Garmin API Demo</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            .container {{ max-width: 600px; margin: 0 auto; padding: 20px; border: 1px solid #0078D4; border-radius: 8px; }}
            h1 {{ color: #0078D4; }}
            .status-connected {{ color: green; font-weight: bold; }}
            .status-disconnected {{ color: red; font-weight: bold; }}
            a {{ color: #0078D4; text-decoration: none; }}
            a:hover {{ text-decoration: underline; }}
            .user-id {{ font-size: small; color: #555; margin-top: 10px; }}
            .warning {{ color: orange; font-weight: bold; }}
            .mapping-status {{ margin-top: 15px; padding: 10px; border-radius: 4px; background-color: #f0f8ff; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Garmin API Demo</h1>
            <p class="warning">WARNING: This user ID is fictitious and is tracked only via your browser session.</p>
            {}
        </div>
    </body>
    </html>
    """
    
    if row:
        mapping_content = f"""
            <div class="mapping-status">
                <p>Push Mapping Status (Webhook): <span class="status-connected">Registered</span></p>
                <p>Your backend is ready to receive Push data for the internal user <strong>{current_user_id}</strong>.</p>
            </div>
        """
        content = f"""
            <p>Garmin Status: <span class="status-connected">Connected</span></p>
            <p class="user-id">Current User ID (from session): <strong>{current_user_id}</strong></p>
            {mapping_content}
            <p><a href='{url_for("oauth.login")}'>3. Re-authenticate with Garmin (Update existing token)</a></p>
        """
    else:    
        content = f"""
            <p>Garmin Status: <span class="status-disconnected">Disconnected</span></p>
            <p class="user-id">Current User ID (from session): <strong>{current_user_id}</strong></p>
            <p>No Garmin token found for this user. Start the OAuth flow to enable both Pull and Push.</p>
            <p><a href='{url_for("oauth.login")}'>Login with Garmin</a></p>
        """
    return render_template_string(html_template.format(content))


@oauth_bp.route('/login')
def login():
    try:
        r_token, r_secret, auth_url = garmin_module.garmin_client.get_request_token_and_url()
        temp_request_tokens[r_token] = r_secret
        return redirect(auth_url)
    except Exception as e:
        return f"Error initiating login: {e}"

@oauth_bp.route('/oauth/callback')
def callback():
    oauth_token = request.args.get('oauth_token')
    oauth_verifier = request.args.get('oauth_verifier')
    
    current_user_id = get_current_user_id()
    
    if not oauth_token or not oauth_verifier:
        return "Missing OAuth parameters (token or verifier).", 400
        
    request_secret = temp_request_tokens.pop(oauth_token, None)
    if not request_secret:
        return "Session expired or invalid token. Please start the login process again.", 400
        
    try:
        acc_token, acc_secret = garmin_module.garmin_client.get_access_token(oauth_token, request_secret, oauth_verifier)
        
        garmin_subscriber_id = garmin_module.garmin_client.fetch_garmin_user_id(acc_token, acc_secret)

        if not garmin_subscriber_id:
            logger.error(f"Failed to retrieve Garmin User ID for internal user {current_user_id}. Cannot complete mapping.")
            return "Authorization success, but failed to retrieve persistent Garmin User ID.", 500
        
        logger.info(f"Garmin Subscriber ID (Webhook Key) obtained: {garmin_subscriber_id}")

        save_token(current_user_id, acc_token, acc_secret)
        
        save_garmin_mapping(garmin_subscriber_id, current_user_id)
        
        return redirect(url_for('oauth.index'))
    except Exception as e:
        logger.error(f"Error during authorization or ID retrieval: {e}", exc_info=True) 
        return f"Error during authorization or ID retrieval. Check console for details. {e}", 500
