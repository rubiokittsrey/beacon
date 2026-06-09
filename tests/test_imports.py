from __future__ import annotations

import subprocess
import sys

import pytest


@pytest.mark.parametrize(
    "entry_module",
    [
        "beacon.mqtt",
        "beacon.core",
        "beacon.core.app",
    ],
)
def test_package_imports_standalone(entry_module: str) -> None:
    # regression: beacon.core re-exporting the app module made importing
    # beacon.mqtt first fail with a circular ImportError; import order can't
    # be exercised in-process once the package is loaded, hence a subprocess
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", f"import {entry_module}"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
