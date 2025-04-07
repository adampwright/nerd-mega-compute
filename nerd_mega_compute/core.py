import requests
import time
import json
import inspect
import pickle
import base64
import zlib
import functools
import uuid
import sys
import itertools
import threading
import traceback
import os
from dotenv import load_dotenv

# Configuration for the cloud service
# Load environment variables from a .env file
load_dotenv()

# Get the API key from the environment
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

# Spinner for better UX
class Spinner:
    def __init__(self, message=""):
        self.spinner = itertools.cycle(['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'])
        self.message = message
        self.running = False
        self.thread = None

    def update_message(self, message):
        self.message = message

    def spin(self):
        while self.running:
            sys.stdout.write(f"\r{next(self.spinner)} {self.message} ")
            sys.stdout.flush()
            time.sleep(0.1)
            sys.stdout.write("\b" * (len(self.message) + 3))

    def start(self):
        self.running = True
        self.thread = threading.Thread(target=self.spin)
        self.thread.daemon = True
        self.thread.start()

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join()
        sys.stdout.write(f"\r✅ {self.message}\n")
        sys.stdout.flush()

def debug_print(msg):
    """Print debug messages only if DEBUG_MODE is True"""
    if DEBUG_MODE:
        print(f"🔍 DEBUG: {msg}")

def check_job_manually(job_id):
    """Manual check of job status for debugging"""
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

def cloud_compute(cores=8, timeout=1800):
    """
    A special function decorator that sends your computation to a powerful cloud server.

    How to use this:
    1. Add @cloud_compute(cores=32) before any function you want to run in the cloud
    2. Call the function normally in your code
    3. The function will automatically run on the cloud server instead of your computer
    4. Results will be returned to your local script when the computation is done

    Parameters:
        cores: How many CPU cores to use (more cores = faster, but costs more)
        timeout: Maximum time in seconds to wait for the result

    Example:
        @cloud_compute(cores=32)
        def my_intensive_calculation(data):
            # This runs on a powerful cloud server
            return results
    """
    def decorator(func):
        # This is where the magic happens - when your function is called,
        # it gets redirected to run on the cloud server instead
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Check if API_KEY is set before proceeding
            if not API_KEY:
                raise ValueError(
                    "API_KEY is not set. Please set it using:\n"
                    "1. Create a .env file with API_KEY=your_key_here\n"
                    "2. Or call nerd_megacompute.set_nerd_compute_api_key('your_key_here')"
                )

            print(f"🚀 Running {func.__name__} on cloud server with {cores} cores...")

            # Step 1: Get the actual code of your function
            source = inspect.getsource(func)

            # Remove the decorator line (first line with @cloud_compute)
            source_lines = source.splitlines()
            if any(line.strip().startswith('@cloud_compute') for line in source_lines):
                # Find and remove the decorator line
                cleaned_lines = [line for line in source_lines
                                if not line.strip().startswith('@cloud_compute')]
                source = '\n'.join(cleaned_lines)

            debug_print(f"Extracted function source code:\n{source[:200]}...")

            # Step 2: Package up all the data your function needs
            spinner = Spinner("Packaging function and data for cloud execution...")
            spinner.start()

            # (This part automatically handles sending numpy arrays, lists, etc.)
            serialized_args = []
            for arg in args:
                try:
                    # Convert your data to a format that can be sent over the internet
                    pickled = pickle.dumps(arg)
                    compressed = zlib.compress(pickled)
                    encoded = base64.b64encode(compressed).decode('utf-8')
                    serialized_args.append({
                        'type': 'data',
                        'value': encoded
                    })
                except Exception as e:
                    spinner.stop()
                    print(f"⚠️ Warning: Could not package argument {arg}: {e}")
                    # If we can't serialize, convert to string
                    serialized_args.append({
                        'type': 'string',
                        'value': str(arg)
                    })
                    spinner.start()

            # Do the same for keyword arguments (like name=value)
            serialized_kwargs = {}
            for key, value in kwargs.items():
                try:
                    pickled = pickle.dumps(value)
                    compressed = zlib.compress(pickled)
                    encoded = base64.b64encode(compressed).decode('utf-8')
                    serialized_kwargs[key] = {
                        'type': 'data',
                        'value': encoded
                    }
                except Exception as e:
                    spinner.stop()
                    print(f"⚠️ Warning: Could not package keyword argument {key}: {e}")
                    serialized_kwargs[key] = {
                        'type': 'string',
                        'value': str(value)
                    }
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

            # Step 4: Send the job to the cloud server
            job_id = str(uuid.uuid4())
            headers = {
                "Content-Type": "application/json",
                "x-api-key": API_KEY
            }

            spinner.update_message(f"Sending {func.__name__} to cloud server...")

            try:
                # Send the request to the cloud server
                response = requests.post(
                    NERD_COMPUTE_ENDPOINT,
                    headers=headers,
                    json={"code": cloud_code, "cores": cores, "jobId": job_id},
                    timeout=30
                )

                # Debug print the raw response for troubleshooting
                debug_print(f"POST response status: {response.status_code}")
                debug_print(f"POST response body: {response.text}")

                # Stop the spinner and print the response status
                spinner.stop()

                response.raise_for_status()
                data = response.json()
                job_id = data.get("jobId", job_id)
                batch_job_id = data.get("batchJobId")

                if batch_job_id:
                    debug_print(f"AWS Batch job ID: {batch_job_id}")

            except Exception as e:
                spinner.stop()
                print(f"❌ Error submitting job: {str(e)}")
                traceback.print_exc()
                raise

            # Step 5: Wait for the results with a spinner
            start_time = time.time()
            spinner = Spinner(f"Running {func.__name__} in the cloud (Job ID: {job_id})...")
            spinner.start()

            last_progress_update = ""
            check_count = 0
            allowed_retries = 3
            retries = 0
            result = None  # Initialize result variable

            try:
                while True:
                    elapsed = time.time() - start_time
                    check_count += 1

                    # If it's taking too long, give up
                    if elapsed > timeout:
                        spinner.stop()
                        print(f"⏱️ Operation timed out after {timeout} seconds")
                        # Do a final manual check before giving up
                        check_job_manually(job_id)
                        raise TimeoutError(f"Cloud computation timed out after {timeout} seconds")

                    try:
                        # Check if the results are ready
                        result_response = requests.get(
                            NERD_COMPUTE_ENDPOINT,
                            headers=headers,
                            params={"jobId": job_id, "debug": "true"},
                            timeout=10
                        )

                        # Debug info every 10 checks
                        if check_count % 10 == 0:
                            debug_print(f"GET response status: {result_response.status_code}")
                            debug_print(f"GET response headers: {result_response.headers}")
                            try:
                                debug_print(f"GET response text: {result_response.text[:200]}...")
                            except:
                                debug_print("Could not display response text")

                        # If we get a non-200 status code
                        if result_response.status_code != 200:
                            # Status 202 means "Accepted" - the job is still starting
                            if result_response.status_code == 202:
                                try:
                                    status_data = result_response.json()
                                    status_message = status_data.get('status', 'Unknown status')
                                    spinner.update_message(f"Job status: {status_message} (elapsed: {int(elapsed)}s)")
                                except:
                                    spinner.update_message(f"Job is starting up... (elapsed: {int(elapsed)}s)")
                                time.sleep(5)
                                continue
                            # Other error codes
                            else:
                                retries += 1
                                if retries <= allowed_retries:
                                    spinner.update_message(f"Retrying... ({retries}/{allowed_retries}) Status: {result_response.status_code}")
                                    time.sleep(5)
                                    continue
                                else:
                                    spinner.stop()
                                    print(f"⚠️ API returned error status: {result_response.status_code}")
                                    print(f"Response: {result_response.text}")
                                    raise Exception(f"API error: {result_response.status_code}")

                        # Reset retries on success
                        retries = 0

                        # Try to parse the JSON response
                        try:
                            result_data = result_response.json()
                        except json.JSONDecodeError:
                            spinner.update_message(f"Warning: Invalid JSON response (elapsed: {int(elapsed)}s)")
                            time.sleep(5)
                            continue

                        # Update the spinner with progress info
                        if "result" in result_data:
                            output_text = result_data["result"]

                            # Check for error messages in the output
                            error_indicators = ["traceback", "error:", "exception:", "modulenotfounderror"]
                            for error_text in error_indicators:
                                if error_text in output_text.lower():
                                    # Stop the spinner
                                    spinner.stop()

                                    # Print the error message
                                    print("\n❌ Error detected in cloud execution:")
                                    print("-" * 60)
                                    print(output_text)
                                    print("-" * 60)

                                    # More specific error handling for common issues
                                    if "modulenotfounderror: no module named 'numpy'" in output_text.lower():
                                        print("\n💡 Solution: The cloud environment is missing required Python packages.")
                                        print("   Please contact the system administrator to install numpy in the cloud environment.")

                                    # Raise an exception to exit the function cleanly
                                    raise Exception(f"Cloud execution of {func.__name__} failed with errors")

                            # Look for progress messages
                            progress_lines = [line for line in output_text.split('\n')
                                            if 'Progress:' in line or 'Completed' in line]

                            if progress_lines:
                                latest_progress = progress_lines[-1].strip()
                                if latest_progress != last_progress_update:
                                    spinner.update_message(
                                        f"Running {func.__name__} in cloud: {latest_progress} (elapsed: {int(elapsed)}s)"
                                    )
                                    last_progress_update = latest_progress
                            else:
                                spinner.update_message(
                                    f"Running {func.__name__} in cloud... (elapsed: {int(elapsed)}s)"
                                )

                            # Look for our result marker
                            if "RESULT_MARKER_BEGIN" in output_text and "RESULT_MARKER_END" in output_text:
                                start_marker = output_text.index("RESULT_MARKER_BEGIN") + len("RESULT_MARKER_BEGIN")
                                end_marker = output_text.index("RESULT_MARKER_END")
                                result_json_str = output_text[start_marker:end_marker].strip()

                                try:
                                    # Parse the result JSON
                                    result_json = json.loads(result_json_str)
                                    if "result" in result_json:
                                        result_encoded = result_json["result"]

                                        # Update spinner message while deserializing
                                        spinner.update_message(f"Retrieving results from cloud ({int(elapsed)}s)...")

                                        # Deserialize the result
                                        result_binary = base64.b64decode(result_encoded)
                                        result_decompressed = zlib.decompress(result_binary)
                                        result = pickle.loads(result_decompressed)

                                        # Stop spinner and show completion message
                                        spinner.update_message(f"Cloud computation completed in {int(elapsed)}s")
                                        spinner.stop()
                                        return result
                                except json.JSONDecodeError:
                                    spinner.update_message(f"Error parsing JSON result (elapsed: {int(elapsed)}s)")
                                    debug_print(f"Invalid JSON: {result_json_str[:100]}...")
                                except Exception as e:
                                    spinner.update_message(f"Error processing result: {e}")
                                    debug_print(traceback.format_exc())

                            # Fallback method: look for any JSON with our result pattern
                            for line in output_text.split('\n'):
                                if line.startswith("{") and "result" in line and "result_size" in line:
                                    try:
                                        debug_print(f"Found potential result line: {line[:50]}...")
                                        result_json = json.loads(line)
                                        if "result" in result_json and "result_size" in result_json:
                                            result_encoded = result_json["result"]

                                            # Update spinner message while deserializing
                                            spinner.update_message(f"Retrieving results from cloud ({int(elapsed)}s)...")

                                            # Deserialize the result
                                            result_binary = base64.b64decode(result_encoded)
                                            result_decompressed = zlib.decompress(result_binary)
                                            result = pickle.loads(result_decompressed)

                                            # Stop spinner and show completion message
                                            spinner.update_message(f"Cloud computation completed in {int(elapsed)}s")
                                            spinner.stop()
                                            return result
                                    except Exception as e:
                                        debug_print(f"Error processing line as result: {e}")


                        # Do a manual check every 30 checks if we're still running
                        if check_count % 30 == 0 and elapsed > 60:
                            spinner.stop()
                            check_job_manually(job_id)
                            spinner.start()

                        # If we've been running for more than 3 minutes and still don't have results,
                        # show the full response to help debug
                        if elapsed > 180 and check_count % 60 == 0:
                            spinner.stop()
                            print("\n==== FULL RESPONSE AFTER 3+ MINUTES ====")
                            print(f"Full response text: {result_response.text}")

                            # Look for any line that has "error", "exception", or "traceback"
                            for line in result_response.text.lower().split('\n'):
                                if any(word in line for word in ["error", "exception", "traceback"]):
                                    print(f"Found potential error: {line}")
                            print("========================================\n")
                            spinner.start()

                        # Wait before checking again
                        wait_time = 1 if elapsed < 10 else (3 if elapsed < 60 else 5)
                        time.sleep(wait_time)

                    except requests.exceptions.RequestException as e:
                        # Network error
                        spinner.update_message(f"Network error: {str(e)[:30]} (elapsed: {int(elapsed)}s)")
                        time.sleep(5)
                    except Exception as e:
                        # Other errors
                        spinner.update_message(f"Error checking status: {str(e)[:30]} (elapsed: {int(elapsed)}s)")
                        debug_print(traceback.format_exc())
                        time.sleep(5)

            except KeyboardInterrupt:
                # Handle Ctrl+C gracefully
                spinner.stop()
                print("\n⚠️ Operation interrupted by user")
                print(f"Job ID: {job_id} (you can check its status later if needed)")
                raise
            except Exception as e:
                # Make sure to stop the spinner on any exception
                spinner.stop()
                print(f"❌ Error: {str(e)}")
                debug_print(traceback.format_exc())
                raise
            finally:
                # Ensure spinner is always stopped
                if spinner.running:
                    spinner.stop()

            # If we somehow get here without returning a result or raising an exception
            if result is None:
                print(f"\n❌ Cloud execution of {func.__name__} failed")
                raise Exception(f"Cloud execution of {func.__name__} failed - see error details above")

            return result

        return wrapper
    return decorator