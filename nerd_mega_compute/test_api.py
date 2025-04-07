import requests
from .config import API_KEY, NERD_COMPUTE_ENDPOINT

def test_api_connection():
    """Simple function to test if the API is responding"""
    print(f"Testing connection to {NERD_COMPUTE_ENDPOINT}")
    print(f"Using API key: {API_KEY[:5]}...{API_KEY[-5:]} (length: {len(API_KEY)})")

    headers = {"x-api-key": API_KEY}
    try:
        # Add a dummy jobId parameter to satisfy the API's requirements
        response = requests.get(
            NERD_COMPUTE_ENDPOINT,
            headers=headers,
            params={"jobId": "test-health-check"},
            timeout=10
        )
        print(f"Status code: {response.status_code}")
        print(f"Response: {response.text[:500]}")

        return response.status_code == 200
    except Exception as e:
        print(f"Error testing API: {e}")
        return False

if __name__ == "__main__":
    success = test_api_connection()
    print(f"API test {'succeeded' if success else 'failed'}")