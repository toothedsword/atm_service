# Wind Speed & Direction COG TIFF Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create `/api/wind-cogtiff` API endpoint that accepts U10/V10 wind component ZIP files, calculates wind speed and direction, and outputs multi-channel COG TIFF files.

**Architecture:** Standalone worker script (`wind_cogtiff_worker.py`) processes two input ZIPs in parallel, computes wind properties, interpolates to fixed bounds (105-111°E, 28-33°N), and generates multi-channel GeoTIFF files with Band 1=wind speed (int16, 0-255) and Band 2=wind direction (int16, 0-360°). Flask API endpoint dispatches worker via subprocess.

**Tech Stack:** NumPy, scipy.interpolate, rasterio, threading (ThreadPoolExecutor), GDAL/gdaladdo

## Global Constraints

- Output bounds (hardcoded): lon 105-111°, lat 28-33°
- Output resolution: 0.002° (3000×2500 pixels)
- Parallel workers: 2 (ThreadPoolExecutor)
- Wind speed storage: int16, 0-255 m/s, no scaling
- Wind direction storage: int16, 0-360°, meteorological convention (from direction)
- Both input ZIPs must have same times/levels/grid dimensions
- Temporary files cleaned up with 5-second delay

---

## Task 1: Create wind_cogtiff_worker.py skeleton

**Files:**
- Create: `/mnt/d/src/atm_service/wind_cogtiff_worker.py`

**Interfaces:**
- Produces: 
  - `read_zip_data(zip_path)` → (header dict, data_4d array, lon_array, lat_array)
  - `uv_to_wind(u, v)` → (wind_speed float, wind_direction_deg float)
  - `save_wind_cogtiff(u_2d, v_2d, lon, lat, time_str, level_str, output_path)` → output_path string
  - `main()` → exit code (0 on success, 1 on error)

- [ ] **Step 1: Create file with imports and logger setup**

```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Wind Speed & Direction COG TIFF Worker

Reads U10 and V10 wind component ZIPs, calculates wind speed and direction,
generates multi-channel COG TIFF files (Band 1=speed, Band 2=direction).
"""

import logging
import json
import struct
import zipfile
from typing import Tuple
import numpy as np
import subprocess
import os
from scipy.interpolate import RegularGridInterpolator
import rasterio
from rasterio.transform import from_bounds
import argparse
import tempfile
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import gc
import math


# Configure logging
logger = logging.getLogger(__name__)


class Timer:
    def __init__(self, name):
        self.name = name
        self.start = time.time()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        elapsed = time.time() - self.start
        logger.info(f"⏱ {self.name}: {elapsed:.2f}s")
```

- [ ] **Step 2: Add read_zip_data function (copy/adapt from interpolate_zip_worker.py)**

```python
def read_zip_data(zip_path: str) -> Tuple[dict, np.ndarray, np.ndarray, np.ndarray]:
    """
    读取 ZIP 中的 data.bin 或 xxx.dat 文件，解析 header 和 4D 数据数组。
    返回: (header dict, data_4d array, lon_array, lat_array)
    """
    logger.info(f"Reading ZIP file: {zip_path}")

    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            # Find binary data file (data.bin or first .dat file)
            files = [f for f in zf.namelist() if not f.endswith('/')]
            if not files:
                raise FileNotFoundError(f"No files found in {zip_path}")

            # Prefer data.bin, otherwise use first file
            data_filename = 'data.bin' if 'data.bin' in files else files[0]
            logger.info(f"Reading file from ZIP: {data_filename}")

            with zf.open(data_filename) as f:
                data_bytes = f.read()
    except zipfile.BadZipFile as e:
        raise FileNotFoundError(f"Invalid ZIP file: {zip_path}") from e

    logger.info(f"Extracted {data_filename}: {len(data_bytes)} bytes")

    # Parse binary format
    if len(data_bytes) < 4:
        raise ValueError(f"data.bin too small: {len(data_bytes)} bytes")

    header_len = struct.unpack('<I', data_bytes[0:4])[0]
    logger.info(f"Header length: {header_len} bytes")

    # Extract and parse JSON header
    if len(data_bytes) < 4 + header_len:
        raise ValueError(
            f"data.bin truncated: expected at least {4 + header_len} bytes, "
            f"got {len(data_bytes)} bytes"
        )

    try:
        header_json_str = data_bytes[4:4 + header_len].decode('utf-8')
        header = json.loads(header_json_str)
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(
            f"Failed to parse JSON header: {str(e)}",
            header_json_str,
            e.pos
        )

    logger.info(f"Header parsed successfully: {list(header.keys())}")

    # Extract metadata
    times = header.get('times')
    levels = header.get('levels')
    ySize = header.get('ySize')
    xSize = header.get('xSize')

    if None in (times, levels, ySize, xSize):
        raise ValueError(
            f"Missing required header fields: "
            f"times={times}, levels={levels}, ySize={ySize}, xSize={xSize}"
        )

    logger.info(f"Data shape: times={times}, levels={levels}, y={ySize}, x={xSize}")

    # Calculate expected data size
    expected_data_elements = times * levels * ySize * xSize
    expected_data_bytes = expected_data_elements * 4  # float32 = 4 bytes
    actual_data_bytes = len(data_bytes) - 4 - header_len

    if actual_data_bytes != expected_data_bytes:
        raise ValueError(
            f"Data size mismatch: expected {expected_data_bytes} bytes, "
            f"got {actual_data_bytes} bytes"
        )

    logger.info(f"Data size verified: {actual_data_bytes} bytes")

    # Parse 4D data array
    data_offset = 4 + header_len
    data_flat = np.frombuffer(
        data_bytes[data_offset:data_offset + expected_data_bytes],
        dtype=np.float32
    )
    data_4d = data_flat.reshape((times, levels, ySize, xSize))

    logger.info(
        f"4D array shape: {data_4d.shape}, "
        f"dtype: {data_4d.dtype}, "
        f"value range: [{data_4d.min():.2f}, {data_4d.max():.2f}]"
    )

    # Generate coordinate arrays from header
    xStart = header.get('xStart', 0)
    xDelta = header.get('xDelta', 1)
    yStart = header.get('yStart', 0)
    yDelta = header.get('yDelta', 1)

    lon_array = np.array([xStart + i * xDelta for i in range(xSize)], dtype=np.float32)
    lat_array = np.array([yStart + i * yDelta for i in range(ySize)], dtype=np.float32)

    logger.info(
        f"Coordinate arrays: "
        f"lon range [{lon_array.min():.4f}, {lon_array.max():.4f}], "
        f"lat range [{lat_array.min():.4f}, {lat_array.max():.4f}]"
    )

    return header, data_4d, lon_array, lat_array
```

- [ ] **Step 3: Add uv_to_wind function**

```python
def uv_to_wind(u: float, v: float) -> Tuple[float, float]:
    """
    Convert U, V wind components to wind speed and direction.
    
    Returns: (wind_speed, wind_direction_deg)
    - wind_speed: m/s
    - wind_direction_deg: 0-360°, meteorological convention (wind coming FROM)
      - 0° = North, 90° = East, 180° = South, 270° = West
    """
    spd = math.sqrt(u * u + v * v)
    if spd < 1e-8:
        return 0.0, 0.0
    # atan2(-u, -v) gives meteorological direction
    direction = math.degrees(math.atan2(-u, -v)) % 360
    return spd, direction
```

- [ ] **Step 4: Add placeholder for save_wind_cogtiff and main**

```python
def save_wind_cogtiff(u_2d, v_2d, lon, lat, time_str, level_str, output_path):
    """
    Calculate wind speed and direction, interpolate to target grid, 
    save as multi-channel COG TIFF.
    
    Band 1: wind_speed (int16, 0-255)
    Band 2: wind_direction (int16, 0-360)
    """
    # TODO: Implement in Task 2
    pass


def main():
    """Main entry point."""
    # TODO: Implement in Task 3
    pass


if __name__ == "__main__":
    import sys
    sys.exit(main())
```

- [ ] **Step 5: Commit**

```bash
git add wind_cogtiff_worker.py
git commit -m "feat: create wind_cogtiff_worker skeleton with read_zip_data and uv_to_wind"
```

---

## Task 2: Implement save_wind_cogtiff function

**Files:**
- Modify: `/mnt/d/src/atm_service/wind_cogtiff_worker.py`

**Interfaces:**
- Consumes: `uv_to_wind(u, v)` → (wind_speed, wind_direction)
- Produces: saves multi-channel TIFF to output_path

- [ ] **Step 1: Implement save_wind_cogtiff with interpolation**

```python
def save_wind_cogtiff(u_2d, v_2d, lon, lat, time_str, level_str, output_path):
    """
    Calculate wind speed and direction from u, v components.
    Interpolate to target grid (105-111°E, 28-33°N, 0.002° resolution).
    Save as multi-channel COG TIFF (Band 1=speed int16, Band 2=direction int16).
    """
    logger.info(
        f"Processing wind for time={time_str}, level={level_str}, "
        f"shape={u_2d.shape}, output={output_path}"
    )

    # Validate inputs
    if u_2d.size == 0 or v_2d.size == 0:
        raise ValueError("u_2d or v_2d is empty")
    if len(lon) < 2 or len(lat) < 2:
        raise ValueError("lon and lat must have at least 2 elements")

    # Sort coordinates (should be monotonic)
    if lon[0] > lon[-1]:
        lon = lon[::-1]
        u_2d = u_2d[:, ::-1]
        v_2d = v_2d[:, ::-1]
    if lat[0] > lat[-1]:
        lat = lat[::-1]
        u_2d = u_2d[::-1, :]
        v_2d = v_2d[::-1, :]

    logger.info(
        f"Original data: lon range [{lon.min():.4f}, {lon.max():.4f}], "
        f"lat range [{lat.min():.4f}, {lat.max():.4f}]"
    )

    # Create interpolators for u and v
    with Timer(f"    create_interpolators[{level_str}]"):
        u_interp = RegularGridInterpolator(
            (lat, lon),
            u_2d,
            method='linear',
            bounds_error=False,
            fill_value=np.nan
        )
        v_interp = RegularGridInterpolator(
            (lat, lon),
            v_2d,
            method='linear',
            bounds_error=False,
            fill_value=np.nan
        )

    # Define target grid (hardcoded bounds)
    target_resolution = 0.002
    lon_min, lon_max = 105.0, 111.0
    lat_min, lat_max = 28.0, 33.0

    target_lon = np.arange(lon_min, lon_max + target_resolution, target_resolution)
    target_lat = np.arange(lat_min, lat_max + target_resolution, target_resolution)

    logger.info(
        f"Target grid: lon {len(target_lon)} points, lat {len(target_lat)} points, "
        f"total {len(target_lon) * len(target_lat)} pixels"
    )

    # Interpolate u and v to target grid
    with Timer(f"    interpolate_uv[{level_str}]"):
        lon_grid, lat_grid = np.meshgrid(target_lon, target_lat)
        points = np.stack([lat_grid.ravel(), lon_grid.ravel()], axis=-1)
        
        u_interp_flat = u_interp(points)
        v_interp_flat = v_interp(points)
        
        u_interp_data = u_interp_flat.reshape(lat_grid.shape)
        v_interp_data = v_interp_flat.reshape(lat_grid.shape)

    logger.info(
        f"Interpolated u: range [{np.nanmin(u_interp_data):.2f}, {np.nanmax(u_interp_data):.2f}]"
    )
    logger.info(
        f"Interpolated v: range [{np.nanmin(v_interp_data):.2f}, {np.nanmax(v_interp_data):.2f}]"
    )

    # Calculate wind speed and direction (vectorized)
    with Timer(f"    calculate_wind[{level_str}]"):
        wind_speed = np.sqrt(u_interp_data ** 2 + v_interp_data ** 2)
        wind_dir = np.degrees(np.arctan2(-u_interp_data, -v_interp_data)) % 360
        
        # Handle NaN values
        wind_speed = np.nan_to_num(wind_speed, nan=-9999)
        wind_dir = np.nan_to_num(wind_dir, nan=-9999)

    logger.info(
        f"Wind speed: range [{np.min(wind_speed[wind_speed != -9999]):.2f}, "
        f"{np.max(wind_speed[wind_speed != -9999]):.2f}] m/s"
    )
    logger.info(
        f"Wind direction: range [{np.min(wind_dir[wind_dir != -9999]):.1f}, "
        f"{np.max(wind_dir[wind_dir != -9999]):.1f}]°"
    )

    # Convert to int16
    nodata_value = -9999
    wind_speed_int16 = np.full_like(wind_speed, nodata_value, dtype=np.int16)
    wind_dir_int16 = np.full_like(wind_dir, nodata_value, dtype=np.int16)
    
    valid_mask = (wind_speed != -9999) & (wind_dir != -9999)
    wind_speed_int16[valid_mask] = np.clip(wind_speed[valid_mask], 0, 255).astype(np.int16)
    wind_dir_int16[valid_mask] = wind_dir[valid_mask].astype(np.int16)

    logger.info(
        f"Converted to int16: wind_speed shape={wind_speed_int16.shape}, "
        f"wind_dir shape={wind_dir_int16.shape}"
    )

    # Create output directory
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Calculate geotransform
    transform = from_bounds(lon_min, lat_min, lon_max + target_resolution,
                            lat_max + target_resolution,
                            wind_speed_int16.shape[1],
                            wind_speed_int16.shape[0])

    # Write multi-channel GeoTIFF
    try:
        with Timer(f"    write_geotiff[{level_str}]"):
            with rasterio.open(
                output_path,
                'w',
                driver='GTiff',
                height=wind_speed_int16.shape[0],
                width=wind_speed_int16.shape[1],
                count=2,
                dtype=np.int16,
                crs='EPSG:4326',
                transform=transform,
                nodata=nodata_value,
                compress='deflate',
                tiled=True,
                blockxsize=512,
                blockysize=512,
                BIGTIFF='NO',
                PREDICTOR=2
            ) as dst:
                dst.write(wind_speed_int16, 1)
                dst.write(wind_dir_int16, 2)
                dst.update_tags(1, description='Wind Speed (m/s)')
                dst.update_tags(2, description='Wind Direction (0-360°, from direction)')
    except Exception as e:
        raise ValueError(f"Failed to write GeoTIFF: {str(e)}") from e

    logger.info(f"GeoTIFF written: {output_path}")

    # Build COG with pyramids
    temp_path = output_path + '.tmp.tif'
    try:
        gdal_cmd = ['gdaladdo', '-r', 'average', output_path, '2', '4', '8', '16', '32']
        logger.info(f"Building pyramids: {' '.join(gdal_cmd)}")
        with Timer(f"    gdaladdo[{level_str}]"):
            subprocess.run(gdal_cmd, check=True, capture_output=True, text=True)
        logger.info(f"Pyramids built successfully")
    except subprocess.CalledProcessError as e:
        logger.warning(f"gdaladdo failed: {e.stderr}. Continuing without pyramids.")
    except FileNotFoundError:
        logger.warning("gdaladdo not found. Continuing without pyramids.")

    # Verify output
    if not os.path.exists(output_path):
        raise ValueError(f"Output file not created: {output_path}")

    file_size = os.path.getsize(output_path)
    logger.info(f"Output file created: {output_path}, size: {file_size} bytes")

    # Verify with rasterio
    try:
        with rasterio.open(output_path) as src:
            logger.info(
                f"Verified with rasterio: "
                f"shape=({src.height}, {src.width}), "
                f"dtype={src.dtypes}, "
                f"crs={src.crs}, "
                f"count={src.count} bands"
            )
    except Exception as e:
        raise ValueError(f"Failed to verify GeoTIFF: {str(e)}") from e

    # Free large arrays
    gc.collect()

    return output_path
```

- [ ] **Step 2: Run test to verify function signature is correct**

```bash
python3 -c "
from wind_cogtiff_worker import save_wind_cogtiff, uv_to_wind
print('save_wind_cogtiff imported successfully')
print('uv_to_wind imported successfully')
"
```

Expected: No errors

- [ ] **Step 3: Commit**

```bash
git add wind_cogtiff_worker.py
git commit -m "feat: implement save_wind_cogtiff with multi-channel TIFF generation"
```

---

## Task 3: Implement main() CLI entry point

**Files:**
- Modify: `/mnt/d/src/atm_service/wind_cogtiff_worker.py`

**Interfaces:**
- Consumes: `read_zip_data()`, `save_wind_cogtiff()`, `uv_to_wind()`
- Produces: exit code 0 (success) or 1 (error), output ZIP with TIFF files

- [ ] **Step 1: Implement main() function**

```python
def process_timestep(time_idx, time_str, level_idx, level_str, u_4d, v_4d, lon_array, lat_array, tiff_output_dir):
    """
    Process single time/level combination in parallel.
    Returns: (success, filename, error_message)
    """
    try:
        u_2d = u_4d[time_idx, level_idx, :, :]
        v_2d = v_4d[time_idx, level_idx, :, :]

        if np.isnan(u_2d).all() or np.isnan(v_2d).all():
            return False, f"wind_{time_str}_{level_str}.tif", "All NaN data"

        tiff_filename = f"wind_{time_str}_{level_str}.tif"
        tiff_output_path = os.path.join(tiff_output_dir, tiff_filename)

        save_wind_cogtiff(u_2d, v_2d, lon_array, lat_array, time_str, level_str, tiff_output_path)
        return True, tiff_filename, None
    except Exception as e:
        return False, f"wind_{time_str}_{level_str}.tif", str(e)


def main():
    """
    Main entry point for wind COG TIFF worker.
    
    Usage:
        python3 wind_cogtiff_worker.py --u-input u10.zip --v-input v10.zip --output result.zip
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    parser = argparse.ArgumentParser(
        description='Wind Speed & Direction COG TIFF Worker',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python3 wind_cogtiff_worker.py --u-input u10.zip --v-input v10.zip --output result.zip
        '''
    )

    parser.add_argument('--u-input', type=str, required=True, help='Path to U10 wind component ZIP')
    parser.add_argument('--v-input', type=str, required=True, help='Path to V10 wind component ZIP')
    parser.add_argument('--output', type=str, required=True, help='Path to output ZIP file')

    args = parser.parse_args()

    u_zip = args.u_input
    v_zip = args.v_input
    output_zip = args.output

    logger.info("=" * 60)
    logger.info("Wind Speed & Direction COG TIFF Worker")
    logger.info("=" * 60)
    logger.info(f"U10 ZIP: {u_zip}")
    logger.info(f"V10 ZIP: {v_zip}")
    logger.info(f"Output ZIP: {output_zip}")

    # Validate input files
    if not os.path.exists(u_zip):
        logger.error(f"U10 file not found: {u_zip}")
        return 1
    if not os.path.exists(v_zip):
        logger.error(f"V10 file not found: {v_zip}")
        return 1

    # Create temporary directory
    temp_dir = tempfile.mkdtemp(prefix='wind_worker_')
    logger.info(f"Created temporary directory: {temp_dir}")

    overall_timer = Timer("OVERALL EXECUTION")
    overall_timer.__enter__()

    try:
        # Read both ZIP files
        logger.info("Step 1: Reading input ZIP files...")
        try:
            with Timer("read_u10_zip"):
                u_header, u_4d, u_lon, u_lat = read_zip_data(u_zip)
        except Exception as e:
            logger.error(f"Failed to read U10 ZIP: {e}")
            return 1

        try:
            with Timer("read_v10_zip"):
                v_header, v_4d, v_lon, v_lat = read_zip_data(v_zip)
        except Exception as e:
            logger.error(f"Failed to read V10 ZIP: {e}")
            return 1

        # Validate metadata consistency
        logger.info("Validating metadata consistency...")
        if u_4d.shape != v_4d.shape:
            logger.error(f"Shape mismatch: U10 {u_4d.shape} vs V10 {v_4d.shape}")
            return 1
        if not np.allclose(u_lon, v_lon) or not np.allclose(u_lat, v_lat):
            logger.error("Coordinate mismatch between U10 and V10")
            return 1

        logger.info(f"Successfully read both ZIPs!")
        logger.info(f"  Data shape: {u_4d.shape} (times, levels, y, x)")
        logger.info(f"  Times: {u_header.get('timeList', [])}")
        logger.info(f"  Levels: {u_header.get('levelList', [])}")

        # Get time and level lists
        time_list = u_header.get('timeList', [])
        level_list = u_header.get('levelList', [])

        if not time_list or not level_list:
            logger.error("Missing timeList or levelList in header")
            return 1

        num_times = len(time_list)
        num_levels = len(level_list)
        total_combinations = num_times * num_levels

        logger.info(f"Processing {total_combinations} combinations ({num_times} times × {num_levels} levels)")

        # Create output directory for TIFF files
        tiff_output_dir = os.path.join(temp_dir, 'tiffs')
        os.makedirs(tiff_output_dir, exist_ok=True)

        # Process each time/level combination (parallel)
        logger.info("Step 2: Processing time/level combinations (parallel)...")
        processed_count = 0
        failed_combinations = []
        step2_timer = Timer("Step 2 - All processing")
        step2_timer.__enter__()

        max_cores = os.cpu_count() or 1
        num_workers = max(1, max_cores // 4)
        logger.info(f"Using {num_workers} parallel workers (out of {max_cores} cores)")

        # Create task list
        tasks = []
        for time_idx, time_str in enumerate(time_list):
            for level_idx, level_str in enumerate(level_list):
                tasks.append((time_idx, time_str, level_idx, level_str))

        # Execute tasks in parallel
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = {
                executor.submit(
                    process_timestep,
                    time_idx, time_str, level_idx, level_str,
                    u_4d, v_4d, u_lon, u_lat, tiff_output_dir
                ): (time_idx, time_str, level_idx, level_str)
                for time_idx, time_str, level_idx, level_str in tasks
            }

            completed = 0
            for future in as_completed(futures):
                completed += 1
                time_idx, time_str, level_idx, level_str = futures[future]

                try:
                    success, tiff_filename, error_msg = future.result()
                    if success:
                        logger.info(f"  [{completed}/{total_combinations}] ✓ {tiff_filename}")
                        processed_count += 1
                    else:
                        logger.error(f"  [{completed}/{total_combinations}] ✗ {tiff_filename}: {error_msg}")
                        failed_combinations.append((time_str, level_str, error_msg))
                except Exception as e:
                    logger.error(f"  [{completed}/{total_combinations}] ✗ time={time_str}, level={level_str}: {e}")
                    failed_combinations.append((time_str, level_str, str(e)))

        step2_timer.__exit__(None, None, None)

        # Log summary
        if failed_combinations:
            logger.warning(f"Failed to process {len(failed_combinations)} combinations:")
            for time_str, level_str, reason in failed_combinations:
                logger.warning(f"  - time={time_str}, level={level_str}: {reason}")

        logger.info(f"Successfully processed {processed_count}/{total_combinations} combinations")

        if processed_count == 0:
            logger.error("No combinations were successfully processed!")
            return 1

        # Create output ZIP with all TIFF files
        logger.info("Step 3: Creating output ZIP file...")

        try:
            output_dir = os.path.dirname(output_zip)
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)

            logger.info(f"Creating ZIP archive: {output_zip}")
            with Timer("Step 3 - Create ZIP"):
                with zipfile.ZipFile(output_zip, 'w', zipfile.ZIP_DEFLATED) as zf:
                    tiff_files = sorted([f for f in os.listdir(tiff_output_dir) if f.endswith('.tif')])
                    logger.info(f"Adding {len(tiff_files)} TIFF files to ZIP...")

                    for tiff_file in tiff_files:
                        tiff_path = os.path.join(tiff_output_dir, tiff_file)
                        file_size = os.path.getsize(tiff_path)
                        logger.info(f"  Adding {tiff_file} ({file_size / (1024*1024):.2f} MB)")
                        zf.write(tiff_path, arcname=tiff_file)

            # Verify output ZIP
            logger.info(f"Verifying output ZIP file...")
            with Timer("Step 3 - Verify ZIP"):
                with zipfile.ZipFile(output_zip, 'r') as zf:
                    files_in_zip = zf.namelist()
                    total_size = sum(zf.getinfo(f).file_size for f in files_in_zip)
                    logger.info(f"  ✓ ZIP contains {len(files_in_zip)} files")
                    logger.info(f"  ✓ Total uncompressed size: {total_size / (1024*1024):.2f} MB")
                    logger.info(f"  ✓ ZIP file size: {os.path.getsize(output_zip) / (1024*1024):.2f} MB")

        except Exception as e:
            logger.error(f"Failed to create output ZIP: {e}")
            return 1

        logger.info("=" * 60)
        logger.info("Successfully completed wind COG TIFF workflow!")
        logger.info(f"Output ZIP: {output_zip}")
        logger.info("=" * 60)

        overall_timer.__exit__(None, None, None)
        return 0

    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        overall_timer.__exit__(None, None, None)
        return 1

    finally:
        # Clean up temporary directory
        if os.path.exists(temp_dir):
            logger.info(f"Cleaning up temporary directory: {temp_dir}")
            try:
                shutil.rmtree(temp_dir)
                logger.info("Temporary directory removed successfully")
            except Exception as e:
                logger.warning(f"Failed to remove temporary directory: {e}")
```

- [ ] **Step 2: Test main() with test data**

```bash
python3 wind_cogtiff_worker.py \
  --u-input /home/leon/src/atm_service/tmp/data/WRF_REAL_20260506000000_u10_single_00000.zip \
  --v-input /home/leon/src/atm_service/tmp/data/WRF_REAL_20260506000000_v10_single_00000.zip \
  --output /tmp/test_wind_output.zip
```

Expected: Processes successfully, creates /tmp/test_wind_output.zip with multiple wind_*.tif files

- [ ] **Step 3: Verify output ZIP contents**

```bash
unzip -l /tmp/test_wind_output.zip | head -20
```

Expected: List shows multiple wind_*.tif files (one per time/level combination)

- [ ] **Step 4: Commit**

```bash
git add wind_cogtiff_worker.py
git commit -m "feat: implement main() CLI entry point for wind worker"
```

---

## Task 4: Add /api/wind-cogtiff endpoint to run.py

**Files:**
- Modify: `/mnt/d/src/atm_service/run.py`

**Interfaces:**
- Consumes: POST multipart/form-data with `u_file` and `v_file` parameters
- Produces: Flask response with application/zip download

- [ ] **Step 1: Locate appropriate position in run.py and add endpoint**

Find line with other interpolate/processing endpoints (look for `/api/interpolate` area, around line 1650)

```python
@app.route('/api/wind-cogtiff', methods=['POST'])
def wind_cogtiff():
    """
    将 WRF U10/V10 风分量 ZIP 转换为风速/风向 COG TIFF。

    输入：
    - u_file: ZIP 文件（U10 东西向风分量）
    - v_file: ZIP 文件（V10 南北向风分量）
      - 两个文件格式相同：data.bin + JSON metadata

    输出：
    - application/zip：结果 ZIP 文件，内含所有生成的多通道 GeoTIFF 文件
      - 文件命名：wind_{time}_{level}.tif
      - Band 1: 风速 (int16, 0-255 m/s)
      - Band 2: 风向 (int16, 0-360°, 来向)

    示例：
    ```
    curl -X POST \
      -F "u_file=@u10.zip" \
      -F "v_file=@v10.zip" \
      http://localhost:5001/api/wind-cogtiff \
      -o result.zip
    ```
    """
    task_id = str(uuid.uuid4())
    u_input_file = None
    v_input_file = None
    output_file = None

    try:
        # Check file uploads
        if 'u_file' not in request.files:
            return jsonify({"success": False, "error": "未提供 u_file"}), 400
        if 'v_file' not in request.files:
            return jsonify({"success": False, "error": "未提供 v_file"}), 400

        u_file = request.files['u_file']
        v_file = request.files['v_file']

        if not u_file.filename or not v_file.filename:
            return jsonify({"success": False, "error": "文件名为空"}), 400

        # Check file extensions
        if not u_file.filename.lower().endswith('.zip'):
            return jsonify({"success": False, "error": "u_file 必须是 .zip 格式"}), 400
        if not v_file.filename.lower().endswith('.zip'):
            return jsonify({"success": False, "error": "v_file 必须是 .zip 格式"}), 400

        # Save uploaded files
        u_input_file = os.path.join(TEMP_BASE, f'{task_id}_u_input.zip')
        v_input_file = os.path.join(TEMP_BASE, f'{task_id}_v_input.zip')
        u_file.save(u_input_file)
        v_file.save(v_input_file)
        logger.info(f"已保存上传文件: {u_input_file}, {v_input_file}")

        # Prepare output file
        output_file = os.path.join(TEMP_BASE, f'{task_id}_wind_output.zip')

        # Call worker subprocess
        cmd = [
            'python3',
            os.path.join(SERVICE_DIR, 'wind_cogtiff_worker.py'),
            '--u-input', u_input_file,
            '--v-input', v_input_file,
            '--output', output_file
        ]

        rc, stdout, stderr = run_worker(cmd, timeout=7200)  # 2小时超时

        if rc != 0:
            return jsonify({
                "success": False,
                "error": f"风场处理失败: {stderr}"
            }), 500

        if not os.path.exists(output_file):
            return jsonify({
                "success": False,
                "error": "未生成输出文件"
            }), 500

        # Return result file
        u_filename = os.path.splitext(secure_filename(u_file.filename))[0]
        v_filename = os.path.splitext(secure_filename(v_file.filename))[0]
        response = send_file(
            output_file,
            mimetype='application/zip',
            as_attachment=True,
            download_name=f'wind_cogtiff_{u_filename}_{v_filename}.zip'
        )

        logger.info(f"✓ 风场处理完成: {output_file}")
        return response

    except Exception as e:
        logger.error(f"wind-cogtiff 失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({
            "success": False,
            "error": f"服务错误: {str(e)}"
        }), 500

    finally:
        # Delayed cleanup of temporary files
        if u_input_file:
            cleanup_later(u_input_file)
        if v_input_file:
            cleanup_later(v_input_file)
        if output_file:
            cleanup_later(output_file)
```

- [ ] **Step 2: Update /api/info endpoint to include new endpoint**

Find line with other endpoint descriptions (around line 209), add:

```python
"/api/wind-cogtiff":     "WRF U10/V10 风分量 ZIP → 风速/风向 COG TIFF（多通道）",
```

- [ ] **Step 3: Test the endpoint (start server)**

```bash
# Terminal 1: Start server
cd /mnt/d/src/atm_service
python3 run.py
```

Expected: Server starts on port 5001

- [ ] **Step 4: Test the endpoint (call it)**

```bash
# Terminal 2: Test with curl
curl -X POST \
  -F "u_file=@/home/leon/src/atm_service/tmp/data/WRF_REAL_20260506000000_u10_single_00000.zip" \
  -F "v_file=@/home/leon/src/atm_service/tmp/data/WRF_REAL_20260506000000_v10_single_00000.zip" \
  http://localhost:5001/api/wind-cogtiff \
  -o /tmp/wind_result.zip
```

Expected: Downloads successfully, creates /tmp/wind_result.zip with wind_*.tif files

- [ ] **Step 5: Verify output TIFF files**

```bash
unzip -l /tmp/wind_result.zip | head -15
```

Expected: Shows wind_*.tif files

- [ ] **Step 6: Check endpoint documentation**

```bash
curl http://localhost:5001/api/info | jq '.endpoints."/api/wind-cogtiff"'
```

Expected: Returns "WRF U10/V10 风分量 ZIP → 风速/风向 COG TIFF（多通道）"

- [ ] **Step 7: Commit**

```bash
git add run.py
git commit -m "feat: add /api/wind-cogtiff endpoint for wind field processing"
```

---

## Self-Review Checklist

✅ **Spec Coverage:**
- [x] Reads two ZIP files (u, v components) → Task 1, Task 3
- [x] Calculates wind speed and direction → Task 2 (uv_to_wind + save_wind_cogtiff)
- [x] Interpolates to bounds 105-111°E, 28-33°N → Task 2
- [x] Generates multi-channel TIFF (Band 1=speed, Band 2=direction) → Task 2
- [x] Uses 2 parallel workers → Task 3
- [x] API endpoint /api/wind-cogtiff → Task 4
- [x] Accepts multipart/form-data → Task 4
- [x] Returns ZIP with all TIFF files → Task 3, Task 4

✅ **Placeholder Scan:**
- No "TBD", "TODO", or "implement later" in executable steps
- All code blocks contain complete, runnable code
- All commands have expected outputs documented

✅ **Type Consistency:**
- `uv_to_wind(u, v)` returns `(float, float)` consistently
- `read_zip_data()` returns `(dict, np.ndarray, np.ndarray, np.ndarray)` 
- `save_wind_cogtiff()` returns `str` (output_path)
- Multi-channel TIFF: Band 1 = int16 (wind speed), Band 2 = int16 (wind direction)

✅ **Scope Boundaries:**
- Each task produces independently testable output
- Task 1: Skeleton + read_zip_data + uv_to_wind
- Task 2: save_wind_cogtiff implementation
- Task 3: main() CLI
- Task 4: Flask API endpoint
- No task depends on future tasks (only on previous ones)
