import os
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def is_ci():
    return os.environ.get("CI", "false").lower() in {"true", "yes", "1", ""}


@pytest.fixture
def data_dir() -> Path:
    return Path(__file__).parent / "data"
