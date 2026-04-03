from pathlib import Path

import pytest

from monetio.util import _on_ci


@pytest.fixture(scope="session")
def is_ci():
    return _on_ci()


@pytest.fixture
def data_dir() -> Path:
    return Path(__file__).parent / "data"
