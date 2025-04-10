import time
import json
import requests
from requests.exceptions import RequestException, Timeout
from halo import Halo
from .config import API_KEY, NERD_COMPUTE_ENDPOINT, DEFAULT_MAX_WAIT
from .utils import debug_print, extract_result_between_markers

def check_job_status(job_id, max_wait_time=DEFAULT_MAX_WAIT):
    """
    Check the status of a submitted job.

    Parameters:
    job_id (str): The ID of the job to check
    max_wait_time (int): Maximum time to wait in seconds before timing out

    Returns:
    The result of the job if successful

    Raises:
    Exception: If the job fails or times out
    """
    headers = {"x-api-key": API_KEY}
    start_time = time.time()
    timeout_per_request = 10  # seconds

    spinner = Halo(text="Job: Script running...", spinner="dots")
    spinner.start()

    try:
        while time.time() - start_time < max_wait_time:
            elapsed = int(time.time() - start_time)
            spinner.text = f"Job: Script running... ({elapsed}s)"

            try:
                response = requests.get(
                    NERD_COMPUTE_ENDPOINT,
                    headers=headers,
                    params={"jobId": job_id},
                    timeout=timeout_per_request
                )

                debug_print(f"Status code: {response.status_code}")

                if response.status_code == 200:
                    try:
                        # First try to parse the response directly
                        data = response.json()

                        # Check if the result is in the response
                        if "result" in data:
                            result = data["result"]
                            parsed_result = extract_result_between_markers(result)
                            if parsed_result:
                                spinner.succeed("Job completed successfully!")
                                return parsed_result

                        # Check if we have a nested structure with body field (like AWS API Gateway)
                        if "body" in data and isinstance(data["body"], str):
                            try:
                                body_data = json.loads(data["body"])
                                if "result" in body_data:
                                    result = body_data["result"]
                                    parsed_result = extract_result_between_markers(result)
                                    if parsed_result:
                                        spinner.succeed("Job completed successfully!")
                                        return parsed_result
                            except json.JSONDecodeError:
                                debug_print("Body is not valid JSON")

                        # If we get here with a 200 response but no valid result, give it one more chance
                        debug_print(f"Got 200 response but couldn't extract result: {response.text[:200]}")

                    except json.JSONDecodeError:
                        debug_print(f"Invalid JSON response: {response.text[:200]}")

                elif response.status_code == 202:
                    debug_print("Job still processing...")

                else:
                    error_msg = f"Error checking job status: {response.status_code} - {response.text}"
                    debug_print(error_msg)
                    # Don't fail immediately on error, give it a few more tries

            except (RequestException, Timeout) as e:
                debug_print(f"Request error: {str(e)}")
                # Don't fail immediately on error, give it a few more tries

            # Wait before checking again
            time.sleep(2)

        # If we're here, we've exceeded max_wait_time
        spinner.fail(f"Job timed out after {max_wait_time} seconds")
        raise TimeoutError(f"Job did not complete within {max_wait_time} seconds")

    except Exception as e:
        spinner.fail(f"Error: {str(e)}")
        raise

    finally:
        if spinner.spinner_id:
            spinner.stop()

def grab_noise(num_cores=1):
    """
    Run a demo noise grab function.

    Parameters:
    num_cores (int): Number of CPU cores to use

    Returns:
    A random number from the cloud service
    """
    headers = {"x-api-key": API_KEY}
    payload = {
        "script": "grab_noise",
        "cores": num_cores
    }

    print(f"🚀 Running grab_noise on cloud server with {num_cores} cores...")

    try:
        response = requests.post(
            NERD_COMPUTE_ENDPOINT,
            headers=headers,
            json=payload,
            timeout=10
        )

        if response.status_code == 202:
            data = response.json()
            job_id = data.get("jobId")
            if not job_id:
                raise ValueError("No job ID in response")

            debug_print(f"Job submitted with ID: {job_id}")
            result = check_job_status(job_id)
            return result

        else:
            raise Exception(f"Error submitting job: {response.status_code} - {response.text}")

    except Exception as e:
        print(f"⚠️ Error: {str(e)}")
        raise