# __init__.py
import os
import logging
from flask import Flask
from dotenv import load_dotenv

load_dotenv() 

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Import the necessary components
from .routes.oauth_routes import oauth_bp 
from .routes.garmin_routes import garmin_bp
from .db_utils import init_db

# Initialize the database
init_db()

def create_app():
    """
   Function to initialise the app
    """
    app = Flask(__name__)

    # Flask configuration

    app.secret_key = os.getenv("FLASK_SECRET_KEY") 
    
    # Blueprint registration
    app.register_blueprint(oauth_bp, url_prefix="/")
    app.register_blueprint(garmin_bp, url_prefix="/garmin")

    return app