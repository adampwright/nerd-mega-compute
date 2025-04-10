"""
NerdMegaCompute: Run Python functions on powerful cloud servers with a simple decorator.
"""

from .cloud import cloud_compute, set_nerd_compute_api_key
from .utils import enable_debug_mode

__all__ = ["cloud_compute", "set_nerd_compute_api_key", "enable_debug_mode"]
__version__ = "0.1.0"