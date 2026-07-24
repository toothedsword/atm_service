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


def read_zip_data(zip_path: str) -> Tuple[dict, np.ndarray, np.ndarray, np.ndarray]:
    """
    读取 ZIP 中的 data.bin 文件，解析 header 和 4D 数据数组。

    Parameters:
    -----------
    zip_path : str
        输入 ZIP 文件路径

    Returns:
    --------
    Tuple[dict, np.ndarray, np.ndarray, np.ndarray]
        - header: 包含元数据的字典
        - data_4d: 形状为 (times, levels, ySize, xSize) 的 float32 数组
        - lon_array: 经度数组（1D）
        - lat_array: 纬度数组（1D）

    Raises:
    -------
    FileNotFoundError
        当 ZIP 文件不存在或不包含 data.bin 文件时
    json.JSONDecodeError
        当 JSON header 解析失败时
    ValueError
        当数据格式无效或数据大小不匹配时
    """
    logger.info(f"Reading ZIP file: {zip_path}")

    # Open ZIP file and extract binary data file
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            # Find binary data file (any file that's not data.bin specifically, but first non-dir file)
            files = [f for f in zf.namelist() if not f.endswith('/')]
            if not files:
                raise FileNotFoundError(f"No files found in {zip_path}")

            # Prefer data.bin if it exists, otherwise use first file
            data_filename = 'data.bin' if 'data.bin' in files else files[0]
            logger.info(f"Reading file from ZIP: {data_filename}")

            # Extract data file to memory
            with zf.open(data_filename) as f:
                data_bytes = f.read()
    except zipfile.BadZipFile as e:
        raise FileNotFoundError(f"Invalid ZIP file: {zip_path}") from e

    logger.info(f"Extracted {data_filename}: {len(data_bytes)} bytes")

    # Parse binary format
    # First 4 bytes: little-endian unsigned int (header length)
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

    # Extract metadata from header
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

    # Create 1D coordinate arrays
    lon_array = np.array([xStart + i * xDelta for i in range(xSize)], dtype=np.float32)
    lat_array = np.array([yStart + i * yDelta for i in range(ySize)], dtype=np.float32)

    logger.info(
        f"Coordinate arrays: "
        f"lon range [{lon_array.min():.4f}, {lon_array.max():.4f}], "
        f"lat range [{lat_array.min():.4f}, {lat_array.max():.4f}]"
    )

    return header, data_4d, lon_array, lat_array


def uv_to_wind(u: float, v: float) -> Tuple[float, float]:
    """
    Calculate wind speed and direction from U and V wind components.

    Parameters:
    -----------
    u : float
        U wind component (m/s, east-west, positive = eastward)
    v : float
        V wind component (m/s, north-south, positive = northward)

    Returns:
    --------
    Tuple[float, float]
        - wind_speed: magnitude of wind (m/s)
        - wind_direction: direction wind is coming FROM (0-360°, meteorological convention)
                         0° = from north, 90° = from east, 180° = from south, 270° = from west

    Notes:
    ------
    - If wind speed < 1e-8 m/s, returns (0.0, 0.0)
    - Wind direction uses meteorological convention (from direction, not to direction)
    - Calculated as: direction = atan2(-u, -v) * 180/π, normalized to [0, 360)
    """
    # Calculate wind speed
    wind_speed = math.sqrt(u**2 + v**2)

    # Handle calm wind
    if wind_speed < 1e-8:
        return 0.0, 0.0

    # Calculate wind direction using meteorological convention
    # atan2(-u, -v) gives direction the wind is coming FROM
    wind_direction_rad = math.atan2(-u, -v)
    wind_direction_deg = wind_direction_rad * 180.0 / math.pi

    # Normalize to [0, 360)
    if wind_direction_deg < 0:
        wind_direction_deg += 360.0

    return wind_speed, wind_direction_deg


def save_wind_cogtiff(u_2d, v_2d, lon, lat, time_str, level_str, output_path):
    """
    计算风速/风向，插值到目标网格，保存为多通道 COG TIFF。

    Band 1: wind_speed (int16, 0-255 m/s, 不缩放)
    Band 2: wind_direction (int16, 0-360°, 气象学惯例：风的来向)

    Parameters:
    -----------
    u_2d : np.ndarray
        U 风分量 2D 数组 (ySize, xSize)
    v_2d : np.ndarray
        V 风分量 2D 数组 (ySize, xSize)
    lon : np.ndarray
        经度数组 (1D)
    lat : np.ndarray
        纬度数组 (1D)
    time_str : str
        时次标识符（来自 timeList）
    level_str : str
        层次标识符（来自 levelList）
    output_path : str
        输出 TIFF 文件路径

    Returns:
    --------
    str
        生成的 TIFF 文件路径

    Raises:
    -------
    ValueError
        插值或保存过程中的错误
    """
    logger.info(
        f"Interpolating wind for time={time_str}, level={level_str}, "
        f"shape={u_2d.shape}, output={output_path}"
    )

    # --- Validation ---
    if u_2d.size == 0 or v_2d.size == 0:
        raise ValueError("u_2d/v_2d is empty")
    if u_2d.shape != v_2d.shape:
        raise ValueError(f"u_2d and v_2d shape mismatch: {u_2d.shape} vs {v_2d.shape}")
    if len(lon) < 2 or len(lat) < 2:
        raise ValueError("lon and lat must have at least 2 elements")

    # Sort coordinates (RegularGridInterpolator requires increasing order)
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

    # --- Create interpolators (one for u, one for v) ---
    with Timer(f"    create_interpolator[{level_str}]"):
        u_interpolator = RegularGridInterpolator(
            (lat, lon),  # (y, x) order for 2D array
            u_2d,
            method='linear',
            bounds_error=False,
            fill_value=np.nan
        )
        v_interpolator = RegularGridInterpolator(
            (lat, lon),
            v_2d,
            method='linear',
            bounds_error=False,
            fill_value=np.nan
        )

    # --- Define target grid with 0.002° resolution ---
    target_resolution = 0.002
    # Output bounds (hardcoded): lon 105-111°, lat 28-33°
    lon_min, lon_max = 105.0, 111.0
    lat_min, lat_max = 28.0, 33.0

    target_lon = np.arange(lon_min, lon_max + target_resolution, target_resolution)
    target_lat = np.arange(lat_min, lat_max + target_resolution, target_resolution)

    logger.info(
        f"Target grid: lon {len(target_lon)} points, lat {len(target_lat)} points, "
        f"total {len(target_lon) * len(target_lat)} pixels"
    )

    # --- Interpolate u and v to target grid ---
    with Timer(f"    meshgrid_and_interpolate[{level_str}]"):
        lon_grid, lat_grid = np.meshgrid(target_lon, target_lat)
        points = np.stack([lat_grid.ravel(), lon_grid.ravel()], axis=-1)

        u_interp = u_interpolator(points).reshape(lat_grid.shape)
        v_interp = v_interpolator(points).reshape(lat_grid.shape)

    logger.info(f"Interpolated u/v shape: {u_interp.shape}")

    # --- Vectorized wind speed / direction calculation ---
    wind_speed = np.sqrt(u_interp ** 2 + v_interp ** 2)

    # atan2(-u, -v) gives the direction the wind is coming FROM (meteorological)
    wind_direction = np.degrees(np.arctan2(-u_interp, -v_interp))
    wind_direction = np.where(wind_direction < 0, wind_direction + 360.0, wind_direction)

    # Calm wind (speed below threshold) -> direction defined as 0, matching uv_to_wind()
    calm_mask = wind_speed < 1e-8
    wind_direction = np.where(calm_mask, 0.0, wind_direction)

    logger.info(
        f"Wind speed range: [{np.nanmin(wind_speed):.2f}, {np.nanmax(wind_speed):.2f}], "
        f"direction range: [{np.nanmin(wind_direction):.2f}, {np.nanmax(wind_direction):.2f}]"
    )

    # --- Convert to int16 with nodata handling ---
    nodata_value = -9999

    valid_mask = ~(np.isnan(u_interp) | np.isnan(v_interp))

    speed_int16 = np.full(wind_speed.shape, nodata_value, dtype=np.int16)
    direction_int16 = np.full(wind_direction.shape, nodata_value, dtype=np.int16)

    # Wind speed: clip to 0-255, no scaling, direct truncation
    clipped_speed = np.clip(wind_speed[valid_mask], 0, 255)
    speed_int16[valid_mask] = clipped_speed.astype(np.int16)

    # Wind direction: clip to 0-360
    clipped_direction = np.clip(wind_direction[valid_mask], 0, 360)
    direction_int16[valid_mask] = clipped_direction.astype(np.int16)

    logger.info(
        f"Converted to int16: speed dtype={speed_int16.dtype}, "
        f"direction dtype={direction_int16.dtype}"
    )

    # --- Create output directory if needed ---
    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    # --- Calculate geotransform ---
    transform = from_bounds(
        lon_min, lat_min,
        lon_max + target_resolution, lat_max + target_resolution,
        speed_int16.shape[1], speed_int16.shape[0]
    )

    # --- Write multi-channel GeoTIFF with COG settings ---
    try:
        with Timer(f"    write_geotiff[{level_str}]"):
            with rasterio.open(
                output_path,
                'w',
                driver='GTiff',
                height=speed_int16.shape[0],
                width=speed_int16.shape[1],
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
                dst.write(speed_int16[::-1, :], 1)     # Band 1 (flipped north-up)
                dst.write(direction_int16[::-1, :], 2)  # Band 2 (flipped north-up)

                dst.update_tags(1, description=f'wind_speed (m/s, 0-255) t={time_str} level={level_str}')
                dst.update_tags(2, description=f'wind_direction (deg, 0-360, meteorological) t={time_str} level={level_str}')

                if valid_mask.any():
                    dst.update_tags(
                        1,
                        STATISTICS_MINIMUM=str(speed_int16[valid_mask].min()),
                        STATISTICS_MAXIMUM=str(speed_int16[valid_mask].max()),
                        STATISTICS_MEAN=str(speed_int16[valid_mask].mean()),
                        STATISTICS_STDDEV=str(speed_int16[valid_mask].std()),
                    )
                    dst.update_tags(
                        2,
                        STATISTICS_MINIMUM=str(direction_int16[valid_mask].min()),
                        STATISTICS_MAXIMUM=str(direction_int16[valid_mask].max()),
                        STATISTICS_MEAN=str(direction_int16[valid_mask].mean()),
                        STATISTICS_STDDEV=str(direction_int16[valid_mask].std()),
                    )
    except Exception as e:
        raise ValueError(f"Failed to write GeoTIFF: {str(e)}") from e

    logger.info(f"GeoTIFF written: {output_path}")

    # --- Build COG pyramids using gdaladdo ---
    try:
        gdal_cmd = [
            'gdaladdo',
            '-r', 'average',
            output_path,
            '2', '4', '8', '16', '32'
        ]

        logger.info(f"Building pyramids: {' '.join(gdal_cmd)}")
        with Timer(f"    gdaladdo[{level_str}]"):
            subprocess.run(gdal_cmd, check=True, capture_output=True, text=True)
        logger.info("Pyramids built successfully")

    except subprocess.CalledProcessError as e:
        logger.warning(f"gdaladdo failed: {e.stderr}. Continuing without pyramids.")
    except FileNotFoundError:
        logger.warning("gdaladdo not found. Continuing without pyramids.")

    # --- Verify output file ---
    if not os.path.exists(output_path):
        raise ValueError(f"Output file not created: {output_path}")

    file_size = os.path.getsize(output_path)
    logger.info(f"Output file created: {output_path}, size: {file_size} bytes")

    try:
        with rasterio.open(output_path) as src:
            logger.info(
                f"Verified with rasterio: "
                f"shape=({src.height}, {src.width}), "
                f"dtype={src.dtypes[0]}, "
                f"crs={src.crs}, "
                f"nodata={src.nodata}, "
                f"count={src.count}"
            )
            if src.count != 2:
                raise ValueError(f"Expected 2 bands, got {src.count}")
    except Exception as e:
        raise ValueError(f"Failed to verify GeoTIFF: {str(e)}") from e

    # --- Memory cleanup ---
    gc.collect()

    return output_path


def process_timestep(time_idx, time_str, level_idx, level_str, u_4d, v_4d, lon_array, lat_array, tiff_output_dir):
    """
    独立处理单个时间/层次组合的函数，用于并行执行。

    Parameters:
    -----------
    time_idx, level_idx : int
        时间/层次索引
    time_str, level_str : str
        时间/层次标识符（来自 timeList/levelList）
    u_4d, v_4d : np.ndarray
        完整的 4D U/V 风分量数组，形状 (times, levels, ySize, xSize)
    lon_array, lat_array : np.ndarray
        坐标数组
    tiff_output_dir : str
        输出目录

    Returns:
    --------
    tuple (success, filename, error_message)
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
    Main entry point for Wind Speed & Direction COG TIFF Worker.

    Usage:
        python3 wind_cogtiff_worker.py --u-input <u10.zip> --v-input <v10.zip> --output <output.zip>

    Reads U10/V10 wind component ZIPs, computes wind speed & direction for
    every time/level combination, and writes all resulting multi-band COG
    TIFF files into a single output ZIP archive.
    """
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Wind Speed & Direction COG TIFF Worker',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python3 wind_cogtiff_worker.py --u-input u10.zip --v-input v10.zip --output wind.zip
  python3 wind_cogtiff_worker.py --u-input /path/to/u10.zip --v-input /path/to/v10.zip --output /path/to/wind.zip
        '''
    )

    parser.add_argument(
        '--u-input',
        type=str,
        required=True,
        help='Path to input ZIP file containing U10 wind component data'
    )
    parser.add_argument(
        '--v-input',
        type=str,
        required=True,
        help='Path to input ZIP file containing V10 wind component data'
    )
    parser.add_argument(
        '--output',
        type=str,
        required=True,
        help='Path to output ZIP file for processed wind TIFF files'
    )

    args = parser.parse_args()

    u_input_zip = args.u_input
    v_input_zip = args.v_input
    output_zip = args.output

    logger.info("=" * 60)
    logger.info("Wind Speed & Direction COG TIFF Worker")
    logger.info("=" * 60)
    logger.info(f"U10 Input ZIP: {u_input_zip}")
    logger.info(f"V10 Input ZIP: {v_input_zip}")
    logger.info(f"Output ZIP: {output_zip}")

    # Validate input files
    if not os.path.exists(u_input_zip):
        logger.error(f"U10 input file not found: {u_input_zip}")
        return 1
    if not os.path.exists(v_input_zip):
        logger.error(f"V10 input file not found: {v_input_zip}")
        return 1

    # Create temporary directory for processing
    temp_dir = tempfile.mkdtemp(prefix='wind_cogtiff_worker_')
    logger.info(f"Created temporary directory: {temp_dir}")

    overall_timer = Timer("OVERALL EXECUTION")
    overall_timer.__enter__()

    try:
        # Step 1: Read both ZIP files
        logger.info("Step 1: Reading U10/V10 input ZIP data...")
        try:
            with Timer("read_zip_data[u10]"):
                u_header, u_4d, u_lon_array, u_lat_array = read_zip_data(u_input_zip)
            with Timer("read_zip_data[v10]"):
                v_header, v_4d, v_lon_array, v_lat_array = read_zip_data(v_input_zip)
        except Exception as e:
            logger.error(f"Failed to read input ZIP(s): {e}")
            return 1

        logger.info("Successfully read U10/V10 ZIP data!")
        logger.info(f"  U10 data 4D shape: {u_4d.shape}")
        logger.info(f"  V10 data 4D shape: {v_4d.shape}")

        # Step 2: Validate consistency between U10 and V10
        logger.info("Step 2: Validating U10/V10 metadata consistency...")

        if u_4d.shape != v_4d.shape:
            logger.error(
                f"U10/V10 shape mismatch: u_4d.shape={u_4d.shape}, v_4d.shape={v_4d.shape}"
            )
            return 1

        if not np.allclose(u_lon_array, v_lon_array):
            logger.error("U10/V10 longitude coordinates do not match")
            return 1

        if not np.allclose(u_lat_array, v_lat_array):
            logger.error("U10/V10 latitude coordinates do not match")
            return 1

        logger.info("U10/V10 metadata consistency verified")

        # Get time and level information (from u_header; v_header should be consistent)
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

        # Step 3: Process each time/level combination (parallel)
        logger.info("Step 3: Processing time/level combinations (parallel)...")
        processed_count = 0
        failed_combinations = []
        step3_timer = Timer("Step 3 - All processing")
        step3_timer.__enter__()

        # Determine number of workers (quarter of available cores, for an
        # 8-core system this yields 2 workers). Using ThreadPoolExecutor
        # instead of ProcessPoolExecutor to avoid u/v array serialization
        # overhead across process boundaries.
        max_cores = os.cpu_count() or 1
        num_workers = max(1, max_cores // 4)
        logger.info(f"Using {num_workers} parallel workers (out of {max_cores} cores)")

        # Create list of tasks
        tasks = []
        for time_idx, time_str in enumerate(time_list):
            for level_idx, level_str in enumerate(level_list):
                tasks.append((time_idx, time_str, level_idx, level_str))

        # Execute tasks in parallel
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            # Submit all tasks
            futures = {
                executor.submit(
                    process_timestep,
                    time_idx, time_str, level_idx, level_str,
                    u_4d, v_4d, u_lon_array, u_lat_array, tiff_output_dir
                ): (time_idx, time_str, level_idx, level_str)
                for time_idx, time_str, level_idx, level_str in tasks
            }

            # Process results as they complete
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

        step3_timer.__exit__(None, None, None)

        # Log summary of failures
        if failed_combinations:
            logger.warning(f"Failed to process {len(failed_combinations)} combinations:")
            for time_str, level_str, reason in failed_combinations:
                logger.warning(f"  - time={time_str}, level={level_str}: {reason}")

        logger.info(f"Successfully processed {processed_count}/{total_combinations} combinations")

        if processed_count == 0:
            logger.error("No combinations were successfully processed!")
            return 1

        # Step 4: Create output ZIP with all TIFF files
        logger.info("Step 4: Creating output ZIP file...")

        try:
            # Create output directory if it doesn't exist
            output_dir = os.path.dirname(output_zip)
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)

            # Create ZIP file with all TIFF files
            logger.info(f"Creating ZIP archive: {output_zip}")
            with Timer("Step 4 - Create ZIP"):
                with zipfile.ZipFile(output_zip, 'w', zipfile.ZIP_DEFLATED) as zf:
                    tiff_files = sorted([f for f in os.listdir(tiff_output_dir) if f.endswith('.tif')])
                    logger.info(f"Adding {len(tiff_files)} TIFF files to ZIP...")

                    for tiff_file in tiff_files:
                        tiff_path = os.path.join(tiff_output_dir, tiff_file)
                        file_size = os.path.getsize(tiff_path)
                        logger.info(f"  Adding {tiff_file} ({file_size / (1024*1024):.2f} MB)")
                        zf.write(tiff_path, arcname=tiff_file)

            # Verify output ZIP
            logger.info("Verifying output ZIP file...")
            with Timer("Step 4 - Verify ZIP"):
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


if __name__ == "__main__":
    import sys
    sys.exit(main())
