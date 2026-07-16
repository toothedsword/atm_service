"""
ZIP to COG TIFF Interpolation Worker

This module provides utilities to read and process atmospheric data
from ZIP archives containing binary data files.
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


# Configure logging
logger = logging.getLogger(__name__)


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

    # Open ZIP file and extract data.bin
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            # Check if data.bin exists in ZIP
            if 'data.bin' not in zf.namelist():
                raise FileNotFoundError(f"data.bin not found in {zip_path}")

            # Extract data.bin to memory
            with zf.open('data.bin') as f:
                data_bytes = f.read()
    except zipfile.BadZipFile as e:
        raise FileNotFoundError(f"Invalid ZIP file: {zip_path}") from e

    logger.info(f"Extracted data.bin: {len(data_bytes)} bytes")

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


def interpolate_and_save_tif(data_2d, lon, lat, time_str, level_str, output_path):
    """
    将 2D 数据插值到更高分辨率并保存为 COG TIFF。

    Parameters:
    -----------
    data_2d : np.ndarray
        2D 数据切片 (ySize, xSize)
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
        f"Interpolating data for time={time_str}, level={level_str}, "
        f"shape={data_2d.shape}, output={output_path}"
    )

    # Validate inputs
    if data_2d.size == 0:
        raise ValueError("data_2d is empty")
    if len(lon) < 2 or len(lat) < 2:
        raise ValueError("lon and lat must have at least 2 elements")

    # Sort coordinates (they should be monotonic)
    if lon[0] > lon[-1]:
        lon = lon[::-1]
        data_2d = data_2d[:, ::-1]
    if lat[0] > lat[-1]:
        lat = lat[::-1]
        data_2d = data_2d[::-1, :]

    logger.info(
        f"Original data: lon range [{lon.min():.4f}, {lon.max():.4f}], "
        f"lat range [{lat.min():.4f}, {lat.max():.4f}]"
    )

    # Create interpolator
    # RegularGridInterpolator expects coordinates in increasing order
    interpolator = RegularGridInterpolator(
        (lat, lon),  # (y, x) order for 2D array
        data_2d,
        method='linear',
        bounds_error=False,
        fill_value=np.nan
    )

    # Define target grid with 0.001° resolution
    target_resolution = 0.001
    lon_min, lon_max = lon.min(), lon.max()
    lat_min, lat_max = lat.min(), lat.max()

    # Create target grid
    target_lon = np.arange(lon_min, lon_max + target_resolution, target_resolution)
    target_lat = np.arange(lat_min, lat_max + target_resolution, target_resolution)

    logger.info(
        f"Target grid: lon {len(target_lon)} points, lat {len(target_lat)} points, "
        f"total {len(target_lon) * len(target_lat)} pixels"
    )

    # Create meshgrid and interpolate
    lon_grid, lat_grid = np.meshgrid(target_lon, target_lat)
    points = np.stack([lat_grid.ravel(), lon_grid.ravel()], axis=-1)
    interpolated_flat = interpolator(points)
    interpolated_data = interpolated_flat.reshape(lat_grid.shape)

    logger.info(
        f"Interpolated data: shape={interpolated_data.shape}, "
        f"value range [{np.nanmin(interpolated_data):.2f}, {np.nanmax(interpolated_data):.2f}]"
    )

    # Convert to int16 (multiply by 10 and convert)
    # Handle NaN values by replacing with NODATA value
    nodata_value = -9999
    interpolated_int16 = np.full_like(interpolated_data, nodata_value, dtype=np.int16)
    valid_mask = ~np.isnan(interpolated_data)
    interpolated_int16[valid_mask] = (interpolated_data[valid_mask] * 10).astype(np.int16)

    logger.info(
        f"Converted to int16: shape={interpolated_int16.shape}, "
        f"dtype={interpolated_int16.dtype}"
    )

    # Create output directory if not exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Calculate geotransform
    # GDAL expects bounds as (minx, miny, maxx, maxy)
    transform = from_bounds(lon_min, lat_min, lon_max + target_resolution,
                            lat_max + target_resolution,
                            interpolated_int16.shape[1],
                            interpolated_int16.shape[0])

    # Write GeoTIFF with COG settings
    try:
        with rasterio.open(
            output_path,
            'w',
            driver='GTiff',
            height=interpolated_int16.shape[0],
            width=interpolated_int16.shape[1],
            count=1,
            dtype=interpolated_int16.dtype,
            crs='EPSG:4326',
            transform=transform,
            nodata=nodata_value,
            compress='deflate',
            tiled=True,
            blockxsize=512,
            blockysize=512,
            BIGTIFF='NO'
        ) as dst:
            dst.write(interpolated_int16, 1)
            # Write statistics
            dst.update_tags(1, STATISTICS_MINIMUM=str(interpolated_int16[valid_mask].min()),
                           STATISTICS_MAXIMUM=str(interpolated_int16[valid_mask].max()),
                           STATISTICS_MEAN=str(interpolated_int16[valid_mask].mean()),
                           STATISTICS_STDDEV=str(interpolated_int16[valid_mask].std()))
    except Exception as e:
        raise ValueError(f"Failed to write GeoTIFF: {str(e)}") from e

    logger.info(f"GeoTIFF written: {output_path}")

    # Build COG with pyramids using gdal_translate
    temp_path = output_path + '.tmp.tif'
    try:
        # Build pyramids with gdaladdo
        gdal_cmd = [
            'gdaladdo',
            '-r', 'average',
            output_path,
            '2', '4', '8', '16', '32'
        ]

        logger.info(f"Building pyramids: {' '.join(gdal_cmd)}")
        subprocess.run(gdal_cmd, check=True, capture_output=True, text=True)
        logger.info(f"Pyramids built successfully")

    except subprocess.CalledProcessError as e:
        logger.warning(f"gdaladdo failed: {e.stderr}. Continuing without pyramids.")
    except FileNotFoundError:
        logger.warning("gdaladdo not found. Continuing without pyramids.")

    # Verify the output file
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
                f"dtype={src.dtypes[0]}, "
                f"crs={src.crs}, "
                f"nodata={src.nodata}, "
                f"count={src.count}"
            )
    except Exception as e:
        raise ValueError(f"Failed to verify GeoTIFF: {str(e)}") from e

    return output_path


if __name__ == "__main__":
    # Example usage and testing
    logging.basicConfig(level=logging.INFO)

    # Test with sample data if available
    test_zip = "/home/leon/Downloads/atm_service/tmp/data/WRF_REAL_20260506000000_t_all_00000.zip"

    try:
        header, data_4d, lon_array, lat_array = read_zip_data(test_zip)
        print(f"\nSuccessfully read ZIP data!")
        print(f"Header metadata: {list(header.keys())}")
        print(f"Data 4D shape: {data_4d.shape}")
        print(f"Lon array shape: {lon_array.shape}, range: [{lon_array.min():.4f}, {lon_array.max():.4f}]")
        print(f"Lat array shape: {lat_array.shape}, range: [{lat_array.min():.4f}, {lat_array.max():.4f}]")

        # Test interpolate_and_save_tif with first time and level
        print("\n" + "=" * 60)
        print("Testing interpolate_and_save_tif...")
        print("=" * 60)

        # Extract first time and level
        data_2d = data_4d[0, 0, :, :]  # First time, first level
        time_str = header.get('timeList', ['00000'])[0]
        level_str = header.get('levelList', ['1000'])[0]

        # Create output directory
        output_dir = "/tmp/atm_service_test"
        os.makedirs(output_dir, exist_ok=True)

        # Generate output path
        output_tiff = os.path.join(output_dir, f"test_t{time_str}_lv{level_str}.tif")

        # Call interpolate_and_save_tif
        result_path = interpolate_and_save_tif(
            data_2d, lon_array, lat_array,
            time_str, level_str,
            output_tiff
        )

        print(f"\nTIFF file generated: {result_path}")
        print(f"File size: {os.path.getsize(result_path) / (1024*1024):.2f} MB")

        # Verify with rasterio
        print("\nVerifying TIFF file...")
        with rasterio.open(result_path) as src:
            print(f"  Shape: ({src.height}, {src.width})")
            print(f"  Data type: {src.dtypes[0]}")
            print(f"  CRS: {src.crs}")
            print(f"  NODATA value: {src.nodata}")
            print(f"  Transform: {src.transform}")
            data_sample = src.read(1)
            print(f"  Data range: [{np.nanmin(data_sample)}, {np.nanmax(data_sample)}]")
            print(f"  Data statistics: min={np.nanmin(data_sample)}, max={np.nanmax(data_sample)}, mean={np.nanmean(data_sample):.2f}")

        # Try to get gdalinfo output if available
        print("\n" + "=" * 60)
        print("GDAL Info:")
        print("=" * 60)
        try:
            result = subprocess.run(['gdalinfo', result_path], capture_output=True, text=True, timeout=10)
            print(result.stdout)
        except Exception as e:
            print(f"Could not run gdalinfo: {e}")

    except Exception as e:
        import traceback
        print(f"Error: {type(e).__name__}: {e}")
        traceback.print_exc()
