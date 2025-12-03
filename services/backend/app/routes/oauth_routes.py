from flask import Blueprint, jsonify, request, session, redirect, url_for, render_template_string
from venv import logger
from app.db_utils import *
import app.garmin_client as garmin_module
import uuid

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
    """
    Main dashboard showing Garmin connection status and options.
    """
    current_user_id = get_current_user_id() 
    row = get_token_internal(current_user_id)
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
            <h1>Garmin API </h1>
            {}
        </div>
    </body>
    </html>
    """
    
    if row:
        mapping_content = f"""
            <div class="mapping-status">
            <form method="post" action="{url_for('oauth.bind_email')}" style="margin-top:10px;">
                <label for="email">Enter your email to link with this account:</label><br/>
                <input type="hidden" name="internal_user_id" value="{current_user_id}"/>
                <input type="email" id="email" name="email" required style="padding:6px;margin-top:6px;width:100%;box-sizing:border-box;"/>
                <button type="submit" style="margin-top:8px;background-color:#0078D4;color:white;border:none;padding:8px 12px;border-radius:4px;cursor:pointer;">
                Submit Email
                </button>
            </form>
            </div>
        """
        content = f"""
            <p>Garmin Status: <span class="status-connected">Connected</span></p>
            {mapping_content}            
        """
    else:    
        content = f"""
            <p>Garmin Status: <span class="status-disconnected">Disconnected</span></p>
            <p>No Garmin token found. Start the OAuth flow to enable both Pull and Push.</p>
            <p><a href='{url_for("oauth.login")}'>Login with Garmin</a></p>
        """
    return render_template_string(html_template.format(content))


@oauth_bp.route('/login')
def login():
    """
    Initiate the OAuth login process with Garmin.
    Redirects the user to Garmin's authorization URL.
    """
    try:
        r_token, r_secret, auth_url = garmin_module.garmin_client.get_request_token_and_url()
        temp_request_tokens[r_token] = r_secret
        return redirect(auth_url)
    except Exception as e:
        return f"Error initiating login: {e}"


@oauth_bp.route('/oauth/callback')
def callback():
    """
    Handle the OAuth callback from Garmin.
    Exchanges the Request Token for an Access Token and retrieves the Garmin User ID.
    """
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
        return redirect(url_for('oauth.index'))
    
    except Exception as e:
        logger.error(f"Error during authorization or ID retrieval: {e}", exc_info=True) 
        return f"Error during authorization or ID retrieval. Check console for details. {e}", 500


@oauth_bp.route('/bind_email', methods=['POST'])
def bind_email():
    """
    Bind an email address to the Garmin subscriber ID for the current user.
    Expects 'email' in the POST form data.
    """
    try:
        current_user_id = get_current_user_id()
        token, secret = get_token_internal(current_user_id)
        if not token or not secret:
            return "No Garmin token found for the current user. Please authenticate first.", 400
        garmin_subscriber_id = garmin_module.garmin_client.fetch_garmin_user_id(token, secret)
        email = request.form.get('email')
        if not email:
            return "Email is required.", 400
        save_garmin_mapping(garmin_subscriber_id, email, current_user_id)

        redirect_url = url_for('oauth.done')
        return redirect(redirect_url)
    
    except Exception as e:
        logger.error(f"Error during authorization or ID retrieval: {e}", exc_info=True) 
        return f"Error during authorization or ID retrieval. Check console for details. {e}", 500


@oauth_bp.route('/done')
def done():
    """
    Simple confirmation page after successful Garmin linking.
    """
    email = get_email(get_current_user_id())
    html = f"""<!DOCTYPE html>
    <html>
    <head>
        <title>All done!</title>
        <meta charset="utf-8"/>
    </head>
    <body style="font-family: Arial, sans-serif; margin: 40px;">
        <div style="max-width:600px;margin:0 auto;padding:20px;border:1px solid #0078D4;border-radius:8px;">
            <h1 style="color:#0078D4;">All done!</h1>
            <p>Your Garmin account has been linked to your email <strong>{email}</strong>.</p>
            <p><a href='{url_for("oauth.login")}'>Re-authenticate with Garmin (Update existing token)</a></p>
        </div>
    </body>
    </html>"""
    return render_template_string(html)