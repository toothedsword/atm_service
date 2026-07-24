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
