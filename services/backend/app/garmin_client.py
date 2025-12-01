import os
from venv import logger
import logging
from requests_oauthlib import OAuth1Session

garmin_client = None 
class OAuthAPI:
    """
    A class to handle Garmin OAuth 1.0a authentication.
    """
    def __init__(self, consumer_key, consumer_secret, callback_url=None):
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.callback_url = callback_url
        
        # Garmin API Endpoints
        self.request_token_url = os.getenv('REQUEST_TOKEN_URL')
        self.access_token_url = os.getenv('ACCESS_TOKEN_URL')
        self.auth_url_base = os.getenv('AUTHORIZATION_BASE_URL')
        self.user_id_url = os.getenv('USER_ID_URL')

    def get_request_token_and_url(self):
        """Step 1: Get the OAuth 1.0a request token and the user authorization URL."""
        oauth = OAuth1Session(
            self.consumer_key,
            client_secret=self.consumer_secret, 
            callback_uri=self.callback_url
        )
        fetch_response = oauth.fetch_request_token(self.request_token_url)
        request_token = fetch_response.get('oauth_token')
        request_token_secret = fetch_response.get('oauth_token_secret')
        
        auth_url = f"{self.auth_url_base}?oauth_token={request_token}"
        return request_token, request_token_secret, auth_url

    def get_access_token(self, request_token, request_token_secret, verifier):
        """Step 2: Exchange the approved request token for a permanent access token."""
        oauth = OAuth1Session(
            self.consumer_key,
            client_secret=self.consumer_secret,
            resource_owner_key=request_token,
            resource_owner_secret=request_token_secret,
            verifier=verifier
        )
        tokens = oauth.fetch_access_token(self.access_token_url)
        return tokens.get('oauth_token'), tokens.get('oauth_token_secret')

    def fetch_garmin_user_id(self, access_token, access_token_secret):
        """
        Retrieve the unique, persistent Garmin API user ID using the
        provided access token and access token secret.
        """
        oauth = OAuth1Session(
            self.consumer_key,
            client_secret=self.consumer_secret,
            resource_owner_key=access_token,
            resource_owner_secret=access_token_secret
        )
        
        logger.info(f"Fetching Garmin API User ID from: {self.user_id_url}")
        try:
            response = oauth.get(self.user_id_url) 
            
            if response.status_code == 200:
                data = response.json()
                user_id = data.get('userId')
                logger.info(f"Fetched Garmin API User ID: {user_id}")
                return user_id
            else:
                logger.error(f"Failed to fetch user ID. Status: {response.status_code}, Body: {response.text}")
                return None
        except Exception as e:
            logger.error(f"Exception during user ID fetch: {e}")
            return None

def initialize_garmin_client(callback_url):
    """
    Reconfigure the global GarminAPI client using the provided dynamic callback_url.
    """
    global garmin_client
    consumer_key = os.getenv('CONSUMER_KEY')
    consumer_secret = os.getenv('CONSUMER_SECRET')
    
    garmin_client = OAuthAPI(consumer_key, consumer_secret, callback_url)
    logging.info("GarminAPI client re-initialized with dynamic CALLBACK_URL.")