
import sys
from unittest.mock import MagicMock
import pytest

# Mock fcntl for Windows
if sys.platform.startswith("win"):
    if "fcntl" not in sys.modules:
        mock_fcntl = MagicMock()
        mock_fcntl.LOCK_EX = 1
        mock_fcntl.LOCK_NB = 2
        mock_fcntl.LOCK_UN = 8
        sys.modules["fcntl"] = mock_fcntl
