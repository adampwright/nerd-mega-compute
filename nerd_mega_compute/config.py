import os
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

# Configuration for the cloud service
API_KEY = os.getenv("API_KEY")
NERD_COMPUTE_ENDPOINT = "https://lbmoem9mdg.execute-api.us-west-1.amazonaws.com/prod/nerd-mega-compute"
DEBUG_MODE = False  # Default to False for production use

def set_nerd_compute_api_key(key):
    """Set the API key for Nerd Mega Compute."""
    global API_KEY
    API_KEY = key

def set_debug_mode(debug=True):
    """Enable or disable debug mode."""
    global DEBUG_MODE
    DEBUG_MODE = debug