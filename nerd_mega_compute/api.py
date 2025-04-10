import os
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

# Global variable to store API key
_NERD_COMPUTE_API_KEY = os.getenv("NERD_COMPUTE_API_KEY", None)

def set_nerd_compute_api_key(api_key):
    """
    Set the API key for Nerd Mega Compute.

    Args:
        api_key (str): Your Nerd Compute API key
    """
    global _NERD_COMPUTE_API_KEY
    _NERD_COMPUTE_API_KEY = api_key
    os.environ["NERD_COMPUTE_API_KEY"] = api_key
    return api_key

def get_nerd_compute_api_key():
    """
    Get the current API key for Nerd Mega Compute.

    Returns:
        str: The current API key or None if not set
    """
    return _NERD_COMPUTE_API_KEY