import warnings

from .ufs import *  # noqa: F401, F403

warnings.warn("_rrfs_cmaq_mm module is deprecated. Use ufs instead.", DeprecationWarning)
