#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unit / integration tests for wind_cogtiff_worker.py (process_timestep + main()).

Run with: python3 -m pytest test_wind_cogtiff_worker.py -v
"""

import os
import sys
import shutil
import tempfile
import zipfile
import subprocess

import numpy as np
import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from wind_cogtiff_worker import process_timestep, main  # noqa: E402


U10_ZIP = '/home/leon/src/atm_service/tmp/data/WRF_REAL_20260506000000_u10_single_00000.zip'
V10_ZIP = '/home/leon/src/atm_service/tmp/data/WRF_REAL_20260506000000_v10_single_00000.zip'


# ---------------------------------------------------------------------------
# process_timestep()
# ---------------------------------------------------------------------------

@pytest.fixture
def small_uv_grid():
    """Small synthetic 4D u/v arrays + coordinates for fast unit tests."""
    rng = np.random.RandomState(0)
    times, levels, ySize, xSize = 2, 1, 5, 5
    u_4d = rng.uniform(-5, 5, size=(times, levels, ySize, xSize)).astype(np.float32)
    v_4d = rng.uniform(-5, 5, size=(times, levels, ySize, xSize)).astype(np.float32)
    lon_array = np.linspace(105.0, 111.0, xSize).astype(np.float32)
    lat_array = np.linspace(28.0, 33.0, ySize).astype(np.float32)
    return u_4d, v_4d, lon_array, lat_array


def test_process_timestep_success(small_uv_grid, tmp_path):
    u_4d, v_4d, lon_array, lat_array = small_uv_grid

    success, filename, error = process_timestep(
        0, "0", 0, "0", u_4d, v_4d, lon_array, lat_array, str(tmp_path)
    )

    assert success is True
    assert filename == "wind_0_0.tif"
    assert error is None
    assert os.path.exists(os.path.join(tmp_path, filename))


def test_process_timestep_all_nan(small_uv_grid, tmp_path):
    u_4d, v_4d, lon_array, lat_array = small_uv_grid
    u_4d = u_4d.copy()
    v_4d = v_4d.copy()
    u_4d[0, 0, :, :] = np.nan
    v_4d[0, 0, :, :] = np.nan

    success, filename, error = process_timestep(
        0, "0", 0, "0", u_4d, v_4d, lon_array, lat_array, str(tmp_path)
    )

    assert success is False
    assert filename == "wind_0_0.tif"
    assert error == "All NaN data"
    assert not os.path.exists(os.path.join(tmp_path, filename))


def test_process_timestep_error_returns_tuple(small_uv_grid, tmp_path):
    """Bad output dir (points to a file, not dir) should raise inside
    save_wind_cogtiff and be reported as (False, filename, error) rather
    than propagating."""
    u_4d, v_4d, lon_array, lat_array = small_uv_grid

    # Pass a lon array too short to trigger a ValueError inside
    # save_wind_cogtiff, and confirm process_timestep captures it.
    bad_lon = lon_array[:1]

    success, filename, error = process_timestep(
        0, "0", 0, "0", u_4d, v_4d, bad_lon, lat_array, str(tmp_path)
    )

    assert success is False
    assert filename == "wind_0_0.tif"
    assert error is not None


# ---------------------------------------------------------------------------
# main() - argument validation without touching real data
# ---------------------------------------------------------------------------

def test_main_missing_input_file_returns_1(tmp_path, monkeypatch):
    missing_u = str(tmp_path / "no_such_u.zip")
    missing_v = str(tmp_path / "no_such_v.zip")
    output_zip = str(tmp_path / "out.zip")

    monkeypatch.setattr(sys, 'argv', [
        'wind_cogtiff_worker.py',
        '--u-input', missing_u,
        '--v-input', missing_v,
        '--output', output_zip,
    ])

    assert main() == 1
    assert not os.path.exists(output_zip)


# ---------------------------------------------------------------------------
# main() - full integration test against real ZIP fixtures
# ---------------------------------------------------------------------------

@pytest.mark.skipif(
    not (os.path.exists(U10_ZIP) and os.path.exists(V10_ZIP)),
    reason="real u10/v10 ZIP fixtures not available in this environment"
)
def test_main_end_to_end_with_real_data(tmp_path):
    output_zip = str(tmp_path / "wind_output.zip")

    result = subprocess.run(
        [
            sys.executable, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'wind_cogtiff_worker.py'),
            '--u-input', U10_ZIP,
            '--v-input', V10_ZIP,
            '--output', output_zip,
        ],
        capture_output=True,
        text=True,
        timeout=600,
    )

    assert result.returncode == 0, f"stderr:\n{result.stderr}\nstdout:\n{result.stdout}"
    assert os.path.exists(output_zip)

    with zipfile.ZipFile(output_zip, 'r') as zf:
        names = zf.namelist()
        assert len(names) > 0
        assert all(n.startswith('wind_') and n.endswith('.tif') for n in names)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, '-v']))
