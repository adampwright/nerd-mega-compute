"""
NerdMegaCompute: Run Python functions on powerful cloud servers with a simple decorator.
"""

from .core import cloud_compute, set_nerd_compute_api_key, set_debug_mode

__all__ = ["cloud_compute", "set_nerd_compute_api_key", "set_debug_mode"]
__version__ = "0.1.0"