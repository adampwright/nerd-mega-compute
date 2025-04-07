# NerdMegaCompute

Run compute-intensive Python functions in the cloud with a simple decorator.

## Installation

### From TestPyPI (for testing purposes)

To install the package from TestPyPI, use the following command:

```bash
pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple nerd-mega-compute
```

### From PyPI (once published)

Once the package is published to the main PyPI repository, you can install it with:

```bash
pip install nerd-mega-compute
```

## Quick Start

1. Set up your API key:

   Create a `.env` file in your project directory containing:

   ```
   API_KEY=your_api_key_here
   ```

2. Use the decorator to run functions in the cloud:

   ```python
   from nerd_megacompute import cloud_compute

   @cloud_compute(cores=8)  # Choose how many CPU cores to use
   def my_intensive_function(data):
        # This code runs in the cloud!
        result = process_data(data)
        return result

   # Call the function normally - it automatically runs in the cloud
   result = my_intensive_function(my_data)
   ```

## Example

```python
from nerd_megacompute import cloud_compute

@cloud_compute(cores=2)
def add_numbers(a, b):
     print("Starting simple addition...")
     result = a + b
     print(f"Result: {result}")
     return result

print("Running a simple test...")
result = add_numbers(40, 2)
print(f"The answer is {result}")
```

## Features

- Run computationally intensive tasks on powerful cloud servers
- Scale up to many CPU cores for faster processing
- Automatically handles data transfer to and from the cloud
- Real-time progress updates while your code runs

## Configuration

```python
# Set your API key in code rather than using .env file
from nerd_megacompute import set_api_key
set_api_key('your_api_key_here')

# Enable debug mode for troubleshooting
from nerd_megacompute import set_debug_mode
set_debug_mode(True)
```

## Parameters

The `@cloud_compute` decorator accepts the following parameters:

- `cores` (default: 8): Number of CPU cores to use in the cloud
- `timeout` (default: 1800): Maximum time in seconds to wait for results

## Limitations

- The function and all its data must be serializable
- Not suitable for tasks requiring specialized hardware (use GPU-specific services instead)
- Internet connection required during execution
