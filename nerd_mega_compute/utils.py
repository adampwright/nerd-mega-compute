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

def extract_result_between_markers(result_string, begin_marker="RESULT_MARKER_BEGIN", end_marker="RESULT_MARKER_END"):
    """Extract the result between begin and end markers."""
    try:
        if begin_marker in result_string and end_marker in result_string:
            start = result_string.find(begin_marker) + len(begin_marker) + 1  # +1 for newline
            end = result_string.find(end_marker)
            if start > 0 and end > start:
                return result_string[start:end].strip()
        return None
    except Exception as e:
        debug_print(f"Error extracting result between markers: {e}")
        return None

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

                # Try to extract result between markers
                extracted = extract_result_between_markers(data['result'])
                if extracted:
                    print(f"Extracted result: {extracted}")
                else:
                    print("Could not extract result between markers")
            else:
                print(f"No result field found. Keys: {list(data.keys())}")

            # Check if 'body' might contain the result (handling nested JSON)
            if "body" in data and isinstance(data["body"], str):
                try:
                    body_data = json.loads(data["body"])
                    if "result" in body_data:
                        print(f"Result found in body! Length: {len(body_data['result'])}")
                        extracted = extract_result_between_markers(body_data['result'])
                        if extracted:
                            print(f"Extracted result from body: {extracted}")
                except:
                    print("Body is not valid JSON")
        except:
            print("Response is not valid JSON")

        print("================================\n")
    except Exception as e:
        print(f"Error in manual check: {e}")