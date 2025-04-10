import os
import json
import requests
from .config import DEBUG_MODE, NERD_COMPUTE_ENDPOINT

def enable_debug_mode():
    """Enable debug mode for more verbose output."""
    from .config import set_debug_mode
    set_debug_mode(True)
    print("Debug mode enabled.")

def debug_print(msg):
    """Print debug messages only if DEBUG_MODE is True"""
    from .config import DEBUG_MODE
    if DEBUG_MODE:
        print(f"🔍 DEBUG: {msg}")

def check_job_manually(job_id):
    """Manual check of job status for debugging"""
    from .config import API_KEY
    try:
        headers = {"x-api-key": API_KEY}
        response = requests.get(
            NERD_COMPUTE_ENDPOINT,
            headers=headers,
            params={"jobId": job_id},
            timeout=10
        )
        print("\n==== MANUAL JOB STATUS CHECK ====")
        print(f"Status code: {response.status_code}")
        print(f"Response: {response.text[:500]}")

        # Try to parse as JSON
        try:
            data = response.json()
            if "result" in data:
                print(f"Result found! Length: {len(data['result'])}")
            else:
                print(f"No result field found. Keys: {list(data.keys())}")
        except:
            print("Response is not valid JSON")

        print("================================\n")
    except Exception as e:
        print(f"Error in manual check: {e}")