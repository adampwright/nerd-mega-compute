import functools
import inspect
import pickle
import base64
import zlib
import uuid
import time
import json
import requests
import traceback
import sys
from .config import API_KEY, NERD_COMPUTE_ENDPOINT, DEBUG_MODE
from .spinner import Spinner
from .utils import debug_print, check_job_manually

def cloud_compute(cores=8, timeout=1800):
    """
    A special function decorator that sends your computation to a powerful cloud server.

    Args:
        cores (int): Number of CPU cores to request (default: 8)
        timeout (int): Maximum time to wait for results in seconds (default: 1800)

    Returns:
        A decorated function that will run in the cloud instead of locally
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Check if API_KEY is set before proceeding
            if not API_KEY:
                raise ValueError(
                    "API_KEY is not set. Please set it using:\n"
                    "1. Create a .env file with API_KEY=your_key_here\n"
                    "2. Or call set_nerd_compute_api_key('your_key_here')"
                )

            print(f"🚀 Running {func.__name__} on cloud server with {cores} cores...")

            # Step 1: Get the actual code of your function
            source = inspect.getsource(func)

            # Remove the decorator line (first line with @cloud_compute)
            source_lines = source.splitlines()
            if any(line.strip().startswith('@cloud_compute') for line in source_lines):
                cleaned_lines = [line for line in source_lines if not line.strip().startswith('@cloud_compute')]
                source = '\n'.join(cleaned_lines)

            debug_print(f"Extracted function source code:\n{source[:200]}...")

            # Step 2: Package up all the data your function needs
            spinner = Spinner("Packaging function and data for cloud execution...")
            spinner.start()

            # Serialize arguments
            serialized_args = []
            for arg in args:
                try:
                    pickled = pickle.dumps(arg)
                    compressed = zlib.compress(pickled)
                    encoded = base64.b64encode(compressed).decode('utf-8')
                    serialized_args.append({'type': 'data', 'value': encoded})
                except Exception as e:
                    spinner.stop()
                    print(f"⚠️ Warning: Could not package argument {arg}: {e}")
                    serialized_args.append({'type': 'string', 'value': str(arg)})
                    spinner.start()

            # Serialize keyword arguments
            serialized_kwargs = {}
            for key, value in kwargs.items():
                try:
                    pickled = pickle.dumps(value)
                    compressed = zlib.compress(pickled)
                    encoded = base64.b64encode(compressed).decode('utf-8')
                    serialized_kwargs[key] = {'type': 'data', 'value': encoded}
                except Exception as e:
                    spinner.stop()
                    print(f"⚠️ Warning: Could not package keyword argument {key}: {e}")
                    serialized_kwargs[key] = {'type': 'string', 'value': str(value)}
                    spinner.start()


            # Add debugging code to make sure the results can be found
            cloud_code = """
import pickle
import base64
import zlib
import json
import time
import os
import traceback

# This function unpacks the data we sent
def deserialize_arg(arg_data):
    if arg_data['type'] == 'data':
        return pickle.loads(zlib.decompress(base64.b64decode(arg_data['value'])))
    else:
        return arg_data['value']

# Debug function to get environment variables
def debug_env():
    env_vars = {}
    for key in ['JOB_ID', 'AWS_BATCH_JOB_ID', 'BUCKET_NAME']:
        env_vars[key] = os.environ.get(key, 'NOT_SET')
    return env_vars

print(f"Cloud environment: {json.dumps(debug_env())}")

# Your original function is copied below (without the decorator)
""" + source + """

# Unpack all the arguments
args = []
for arg_data in """ + str(serialized_args) + """:
    args.append(deserialize_arg(arg_data))

# Unpack all the keyword arguments
kwargs = {}
for key, arg_data in """ + str(serialized_kwargs) + """.items():
    kwargs[key] = deserialize_arg(arg_data)

try:
    # Actually run your function with your data
    print(f"Starting cloud execution of """ + func.__name__ + """...")
    result = """ + func.__name__ + """(*args, **kwargs)
    print(f"Function execution completed successfully")

    # Package up the results to send back
    try:
        print("Packaging results to send back...")
        result_pickled = pickle.dumps(result)
        result_compressed = zlib.compress(result_pickled)
        result_encoded = base64.b64encode(result_compressed).decode('utf-8')
        print(f"Results packaged (size: {len(result_encoded)} characters)")

        # Write the result multiple ways for redundancy
        print("RESULT_MARKER_BEGIN")
        print(f'{{"result_size": {len(result_encoded)}, "result": "{result_encoded}"}}')
        print("RESULT_MARKER_END")

        # Also write to a file that will be uploaded to S3
        with open('/tmp/result.json', 'w') as f:
            f.write(f'{{"result_size": {len(result_encoded)}, "result": "{result_encoded}"}}')
        print("Saved result to /tmp/result.json")

        # Force flush stdout to make sure our results are captured
        import sys
        sys.stdout.flush()

        # Give the system time to capture our output
        time.sleep(1)
    except Exception as e:
        print(f"Error packaging results: {e}")
        print(traceback.format_exc())
        raise
except Exception as e:
    print(f"EXECUTION ERROR: {e}")
    print(traceback.format_exc())
"""

            # Generate a random ID for this job so we can track it
            job_id = str(uuid.uuid4())
            debug_print(f"Job ID: {job_id}")

            spinner.update_message(f"Sending {func.__name__} to cloud server...")

            # Step 3: Send our package to the cloud service
            headers = {
                "Content-Type": "application/json",
                "x-api-key": API_KEY
            }

            try:
                debug_print(f"Sending to API with job ID: {job_id}")

                response = requests.post(
                    NERD_COMPUTE_ENDPOINT,
                    json={"code": cloud_code, "cores": cores, "jobId": job_id},
                    headers=headers,
                    timeout=30
                )

                # Debug print the raw response for troubleshooting
                debug_print(f"POST response status: {response.status_code}")
                debug_print(f"POST response body: {response.text}")

                # Check if the request was successful
                if response.status_code != 200:
                    spinner.stop()
                    print(f"❌ Failed to send job: {response.status_code}")
                    if DEBUG_MODE:
                        print(f"Response: {response.text}")
                        check_job_manually(job_id)
                    return None

                data = response.json()
                job_id = data.get("jobId", job_id)
                batch_job_id = data.get("batchJobId")

                if batch_job_id:
                    debug_print(f"AWS Batch job ID: {batch_job_id}")

            except Exception as e:
                spinner.stop()
                print(f"❌ Error sending job to cloud: {e}")
                if DEBUG_MODE:
                    traceback.print_exc()
                return None

            # Step 4: Wait for the job to complete
            spinner.update_message(f"Running {func.__name__} in the cloud...")
            start_time = time.time()
            result = None
            check_count = 0

            while True:
                # Check if we've exceeded the timeout
                elapsed = time.time() - start_time
                check_count += 1

                if elapsed > timeout:
                    spinner.stop()
                    print(f"❌ Function timed out after {timeout} seconds")
                    return None

                try:
                    # Check job status
                    result_response = requests.get(
                        NERD_COMPUTE_ENDPOINT,
                        headers=headers,
                        params={"jobId": job_id, "debug": "true"},
                        timeout=10
                    )

                    # Debug info every 10 checks
                    if check_count % 10 == 0 or DEBUG_MODE:
                        debug_print(f"GET response status: {result_response.status_code}")
                        try:
                            debug_print(f"GET response text: {result_response.text[:200]}...")
                        except Exception:
                            debug_print("Could not display response text")

# Replace the response handling section with improved error detection:
                    # If we get a non-200 status code
                    if result_response.status_code != 200:
                        if result_response.status_code == 202:
                            # Status 202 means "Accepted" - the job is still processing
                            try:
                                status_data = result_response.json()
                                status_message = status_data.get('status', 'Unknown status')
                                # Truncate long status messages
                                if len(status_message) > 50:
                                    status_message = status_message[:47] + "..."
                                spinner.update_message(f"Job: {status_message} ({int(elapsed)}s)")
                            except Exception:
                                spinner.update_message(f"Job processing... ({int(elapsed)}s)")

                            # Check for timeout to prevent hanging
                            if elapsed > timeout:
                                spinner.stop()
                                print(f"\n❌ Job timed out after {int(elapsed)} seconds")
                                return None

                            time.sleep(2)
                            continue
                        elif result_response.status_code == 500:
                            # Job failed - extract error message and return
                            try:
                                error_data = result_response.json()
                                return process_error_response(error_data, spinner, elapsed)
                            except Exception as e:
                                debug_print(f"Error parsing failure response: {e}")
                                spinner.stop()
                                print(f"\n❌ Request failed with status {result_response.status_code}")
                                return None

                        # Unexpected status code
                        if check_count >= 30:  # After ~60 seconds of trying
                            spinner.stop()
                            print(f"\n❌ Job failed with unexpected status code: {result_response.status_code}")
                            try:
                                print(f"Response: {result_response.text[:500]}")
                            except:
                                pass
                            return None

                        if check_count % 10 == 0:
                            debug_print(f"Unexpected status code: {result_response.status_code}")
                        time.sleep(2)
                        continue

                    # Try to parse the JSON response
                    try:
                        result_data = result_response.json()

                        # Check for error field first
                        if "error" in result_data or "status" in result_data and result_data.get("status") == "FAILED":
                            return process_error_response(result_data, spinner, elapsed)

                        # If the response contains result data
                        if "result" in result_data:
                            output_text = result_data["result"]

                            # Try direct JSON parsing first
                            try:
                                # Is the result itself valid JSON?
                                direct_json = json.loads(output_text)
                                spinner.update_message(f"Cloud computation completed in {int(elapsed)}s")
                                spinner.stop()
                                return direct_json
                            except json.JSONDecodeError:
                                # Not direct JSON, continue with marker processing
                                debug_print("Output is not direct JSON, looking for markers")

                            # Look for result markers
                            if "RESULT_MARKER_BEGIN" in output_text and "RESULT_MARKER_END" in output_text:
                                try:
                                    start_marker = output_text.index("RESULT_MARKER_BEGIN") + len("RESULT_MARKER_BEGIN")
                                    end_marker = output_text.index("RESULT_MARKER_END")
                                    result_json_str = output_text[start_marker:end_marker].strip()

                                    # Parse the result JSON between markers
                                    result_json = json.loads(result_json_str)

                                    # Check for error information in the result
                                    if "error" in result_json:
                                        return process_error_response(result_json, spinner, elapsed)

                                    # Success - return the parsed JSON directly
                                    spinner.update_message(f"Cloud computation completed in {int(elapsed)}s")
                                    spinner.stop()
                                    return result_json
                                except Exception as e:
                                    debug_print(f"Error processing markers: {e}")

                            # Fallback - search for JSON in the output line by line
                            for line in output_text.split('\n'):
                                if line.strip().startswith("{") and line.strip().endswith("}"):
                                    try:
                                        line_json = json.loads(line)
                                        if isinstance(line_json, dict) and not "error" in line_json:
                                            spinner.update_message(f"Cloud computation completed in {int(elapsed)}s")
                                            spinner.stop()
                                            return line_json
                                    except:
                                        pass

                    except Exception as e:
                        debug_print(f"Error processing response: {e}")
                        if DEBUG_MODE:
                            traceback.print_exc()

                    # Safety timeout check
                    if elapsed > timeout - 30:
                        spinner.stop()
                        print(f"\n❌ Job timed out after {int(elapsed)} seconds")
                        return None

                except Exception as e:
                    # Add this exception handler for the outer try block
                    debug_print(f"Error in job status check: {e}")
                    if DEBUG_MODE:
                        traceback.print_exc()

                # Wait before checking again
                time.sleep(2)

        return wrapper
    return decorator

def process_error_response(response_data, spinner, elapsed_time):
    """Process an error response and display meaningful messages to the user."""
    spinner.stop()

    error_msg = "Unknown error occurred"
    details = ""

    # Try to extract error message from response
    try:
        if isinstance(response_data, dict):
            error_msg = response_data.get('error', error_msg)
            details = response_data.get('details', '')

            # Handle case where error is in 'body' (API Gateway format)
            if 'body' in response_data and isinstance(response_data['body'], str):
                try:
                    body_data = json.loads(response_data['body'])
                    if isinstance(body_data, dict):
                        error_msg = body_data.get('error', error_msg)
                        details = body_data.get('details', details)
                except:
                    pass
    except:
        pass

    print(f"\n❌ Cloud job failed after {int(elapsed_time)}s: {error_msg}")
    if details:
        print(f"Error details:\n{details}")

    return None  # Return None to indicate failure