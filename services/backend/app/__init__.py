from flask import Flask
from .routes.garmin import garmin_bp
from .services.wearable_producer import close_producer, start_producer

def create_app():
    """
    Factory to create and configure the Flask application."""
    app = Flask(__name__)
    app.register_blueprint(garmin_bp, url_prefix='/garmin') # TBD: adjust prefix

    # start producer for first request
    try:
        start_producer()
    except Exception as e:
        print(f"Error starting producer: {e}")

    # close producer when closing app
    @app.teardown_appcontext
    def teardown_producer(exception=None):
        """
        This function is called automatically by Flask when the application
        context is torn down; no explicit invocation is necessary.
        """
        close_producer()

    return app