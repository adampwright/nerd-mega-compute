from nerd_mega_compute import cloud_compute, set_nerd_compute_api_key
import time

# Set your API key (replace with your actual Test API key)
set_nerd_compute_api_key("<your_api_key>")

# Define a compute-intensive function to run in the cloud
@cloud_compute(cores=2)
def find_primes(limit):
    """Find all prime numbers up to a given limit."""
    primes = []
    for num in range(2, limit):
        is_prime = all(num % i != 0 for i in range(2, int(num ** 0.5) + 1))
        if is_prime:
            primes.append(num)
    return primes

# Run the test
if __name__ == "__main__":
    print("Starting a compute-intensive cloud operation...")
    start_time = time.time()
    result = find_primes(100000)  # Adjust the limit for a ~1-minute operation
    end_time = time.time()
    print(f"Found {len(result)} primes in {end_time - start_time:.2f} seconds.")