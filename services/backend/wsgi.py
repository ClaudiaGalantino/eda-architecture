import os
import logging
from flask import Flask
from app.routes.oauth_routes import oauth_bp
from dotenv import load_dotenv
from app.db_utils import init_db

load_dotenv() 

app = Flask(__name__)

# Logging configuration
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Configuration and Initialization

consumer_key = os.getenv('CONSUMER_KEY')
consumer_secret = os.getenv('CONSUMER_SECRET')
callback_url = os.getenv('CALLBACK_URL')
db_name = os.getenv('DB_NAME')
port = os.getenv('PORT')

app.secret_key = os.getenv("FLASK_SECRET_KEY")

# Registrazione dei Blueprint
app.register_blueprint(oauth_bp, url_prefix="/")
#app.register_blueprint(garmin_bp, url_prefix="/garmin") #todo

if __name__ == '__main__':
    # Delete old DB
    if os.path.exists(db_name):
         os.remove(db_name)
         print("Old database removed.")
    init_db()
    print("Database initialized (Tokens only).")
    print("Starting Flask server on http://localhost:5000")
    app.run(debug=True, port=port, host='0.0.0.0')