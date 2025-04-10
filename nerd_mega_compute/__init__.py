"""
NerdMegaCompute: Run Python functions on powerful cloud servers with a simple decorator.
"""

from .cloud import cloud_compute
from .config import set_debug_mode
from .utils import check_job_manually
from .api import set_nerd_compute_api_key, get_nerd_compute_api_key

__all__ = ["cloud_compute", "set_debug_mode", "check_job_manually", "set_nerd_compute_api_key", "get_nerd_compute_api_key"]
__version__ = "0.1.0"