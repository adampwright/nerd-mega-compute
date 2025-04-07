# Import the cloud_compute decorator directly for cleaner syntax
from nerd_mega_compute import cloud_compute, set_nerd_compute_api_key

# Set the API key at the beginning of your script with a more specific function name
set_nerd_compute_api_key("H4fBc9MlQZ5bDvYi1XZ0C2iQOTC5zNbh3t8Rjw9u")

# Add decorator to any function you want to run in the cloud
@cloud_compute(cores=2)
def add_numbers(a, b):
    """A simple function that runs in the cloud"""
    print("Starting cloud calculation...")
    result = a + b
    print(f"Cloud result: {result}")
    return result

# Call the function like normal - it runs in the cloud automatically!
print("Running a simple test...")
result = add_numbers(40, 2)
print(f"The answer is {result}")