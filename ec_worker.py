#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ec_worker.py - EC 预报数据提取工作进程
从指定目录的 GRIB2 文件中找到最近时次，提取指定变量/层级的二维数据，输出为 zip 格式。

用法:
    python3 ec_worker.py --input <params.json> --output <output.zip>

params.json 字段:
    data_dir      str   GRIB2 文件目录 (必填)
    datetime      str   目标有效时间，格式 yyyymmddhh 或 yyyymmddHHMM (必填)
    variable      str   GRIB2 shortName，如 t / u / v / 2t / 10u (必填)
    level         int   层级值，如 850 / 500 / 2 / 10 / 0 (可选，默认 0)
    typeOfLevel   str   层级类型，如 isobaricInhPa / heightAboveGround / surface (可选，自动推断)
    minLat        float 南边界 (可选)
    maxLat        float 北边界 (可选)
    minLon        float 西边界 (可选)
    maxLon        float 东边界 (可选)
"""

import argparse
import json
import logging
import os
import re
import struct
import sys
import zipfile
from datetime import datetime, timedelta
from io import BytesIO

import numpy as np

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# GRIB2 文件名正则: 20260330000000-12h-oper-fc.grib2
_FILENAME_RE = re.compile(
    r'(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})-(\d+)h-oper-fc\.grib2$'
)

# 默认 typeOfLevel 推断规则（根据常见变量名）
_DEFAULT_LEVEL_TYPE = {
    '2t': 'heightAboveGround', '2d': 'heightAboveGround',
    'mn2t3': 'heightAboveGround', 'mx2t3': 'heightAboveGround',
    '10u': 'heightAboveGround', '10v': 'heightAboveGround',
    '10fg': 'heightAboveGround', '100u': 'heightAboveGround', '100v': 'heightAboveGround',
    'sp': 'surface', 'msl': 'surface', 'lsm': 'surface',
    'ssrd': 'surface', 'strd': 'surface', 'ssr': 'surface', 'str': 'surface',
    'tprate': 'surface', 'tp': 'surface', 'ptype': 'surface',
    'asn': 'surface', 'rsn': 'surface',
    'tcw': 'entireAtmosphere', 'tcwv': 'entireAtmosphere', 'tcc': 'entireAtmosphere',
    'sot': 'soilLayer', 'vsw': 'soilLayer',
}


def parse_datetime(s):
    """解析时间字符串为 datetime 对象，按长度精确匹配格式"""
    s = s.strip()
    fmt_map = {
        10: '%Y%m%d%H',      # yyyymmddhh
        12: '%Y%m%d%H%M',    # yyyymmddHHMM
        14: '%Y%m%d%H%M%S',  # yyyymmddHHMMSS
    }
    fmt = fmt_map.get(len(s))
    if fmt:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    raise ValueError(f"无法解析时间字符串: {s!r}，支持格式: yyyymmddhh / yyyymmddHHMM / yyyymmddHHMMSS")


def scan_files(data_dir):
    """
    扫描目录下所有 *-oper-fc.grib2 文件，返回列表：
    [(valid_time: datetime, step_hours: int, filepath: str), ...]
    """
    results = []
    for fname in os.listdir(data_dir):
        m = _FILENAME_RE.match(fname)
        if not m:
            continue
        yr, mo, dy, hh, mm, ss, step = m.groups()
        base_time = datetime(int(yr), int(mo), int(dy), int(hh), int(mm), int(ss))
        step_h = int(step)
        valid_time = base_time + timedelta(hours=step_h)
        results.append((valid_time, step_h, os.path.join(data_dir, fname)))
    results.sort(key=lambda x: x[0])
    return results


def find_nearest_file(data_dir, target_dt):
    """返回有效时间最接近 target_dt 的文件路径及其有效时间"""
    files = scan_files(data_dir)
    if not files:
        raise FileNotFoundError(f"目录中未找到 *-oper-fc.grib2 文件: {data_dir}")
    best = min(files, key=lambda x: abs((x[0] - target_dt).total_seconds()))
    logger.info(f"目标时间: {target_dt}, 最近时次: {best[0]} (step={best[1]}h), 文件: {best[2]}")
    return best[2], best[0]


def read_grib2_field(filepath, variable, level, type_of_level):
    """
    用 xarray + cfgrib 读取单个二维字段。
    返回 (data_2d: np.ndarray float32, lat: np.ndarray, lon: np.ndarray)
    lat 从小到大（南→北），lon 从小到大（西→东）
    """
    import xarray as xr

    filter_keys = {'shortName': variable, 'typeOfLevel': type_of_level}
    # 只有等压面和土壤层需要按 level 过滤；地面/高度层/整层变量 level 已隐含在 shortName 中
    if type_of_level in ('isobaricInhPa', 'soilLayer') and int(level) != 0:
        filter_keys['level'] = int(level)

    try:
        ds = xr.open_dataset(
            filepath,
            engine='cfgrib',
            filter_by_keys=filter_keys,
            indexpath=None,         # 不使用 .index 缓存文件，避免路径问题
        )
    except Exception as e:
        raise RuntimeError(
            f"读取失败: variable={variable}, typeOfLevel={type_of_level}, level={level}\n"
            f"文件: {filepath}\n错误: {e}"
        )

    # 取第一个数据变量
    varnames = list(ds.data_vars)
    if not varnames:
        raise ValueError(f"未在文件中找到变量 {variable} (typeOfLevel={type_of_level}, level={level})")

    data = ds[varnames[0]].values.astype(np.float32)   # shape: (lat, lon)
    lat = ds.latitude.values.astype(np.float64)
    lon = ds.longitude.values.astype(np.float64)

    # 保证 lat 从南到北
    if lat[0] > lat[-1]:
        data = data[::-1, :]
        lat = lat[::-1]

    # 保证 lon 从 -180 到 180（cfgrib 通常已经是这样）
    return data, lat, lon


def subset_region(data, lat, lon, min_lat, max_lat, min_lon, max_lon):
    """按经纬度范围裁切"""
    lat_mask = (lat >= min_lat) & (lat <= max_lat)
    lon_mask = (lon >= min_lon) & (lon <= max_lon)
    data = data[np.ix_(lat_mask, lon_mask)]
    return data, lat[lat_mask], lon[lon_mask]


def save_to_zip(data_2d, lat, lon, level, valid_time_dt, variable, output_path):
    """将二维数据保存为与 converter_worker.py 相同格式的 zip 文件"""
    ny, nx = data_2d.shape

    data_4d = data_2d[np.newaxis, np.newaxis, :, :].astype(np.float32)  # (1,1,ny,nx)

    time_str = valid_time_dt.strftime('%Y%m%d%H')

    header = {
        "dataType":   "Float32",
        "dataScale":  1.0,
        "dataOffset": 0.0,
        "xSize":  nx,
        "ySize":  ny,
        "levels": 1,
        "times":  1,
        "levelList": [str(level)],
        "timeList":  [time_str],
        "units":    variable,
        "element":  variable,
        "dataCode": "EC-OPER-FC",
        "xStart": float(lon[0]),
        "xEnd":   float(lon[-1]),
        "xDelta": float(lon[1] - lon[0]) if nx > 1 else 0.0,
        "yStart": float(lat[0]),
        "yEnd":   float(lat[-1]),
        "yDelta": float(lat[1] - lat[0]) if ny > 1 else 0.0,
        "lon": lon.tolist(),
        "lat": lat.tolist(),
        "nx": nx,
        "ny": ny,
        "dataMin": float(np.nanmin(data_2d)),
        "dataMax": float(np.nanmax(data_2d)),
        "validTime": time_str,
        "forecastLevel": int(level),
    }

    header_bytes = json.dumps(header, separators=(',', ':')).encode('utf-8')
    header_len   = len(header_bytes)

    buf = BytesIO()
    buf.write(struct.pack('<I', header_len))
    buf.write(header_bytes)
    buf.write(data_4d.tobytes(order='C'))
    buf.seek(0)

    with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr('data.bin', buf.getvalue())

    logger.info(f"输出: {output_path}  shape=({ny},{nx})  "
                f"range=[{header['dataMin']:.3f}, {header['dataMax']:.3f}]")


def main():
    parser = argparse.ArgumentParser(description='EC预报数据提取工作进程')
    parser.add_argument('--input',  required=True, help='参数 JSON 文件路径')
    parser.add_argument('--output', required=True, help='输出 ZIP 文件路径')
    args = parser.parse_args()

    try:
        with open(args.input, 'r', encoding='utf-8') as f:
            params = json.load(f)

        # ---- 解析参数 ----
        data_dir    = params.get('data_dir', '/home/leon/Downloads/ec-oper-fc')
        variable    = params['variable']
        level       = int(params.get('level', 0))
        target_dt   = parse_datetime(params['datetime'])

        # typeOfLevel: 优先用参数，否则按变量名推断，最后默认 isobaricInhPa
        type_of_level = (
            params.get('typeOfLevel')
            or _DEFAULT_LEVEL_TYPE.get(variable)
            or 'isobaricInhPa'
        )

        # 可选空间裁切
        min_lat = params.get('minLat')
        max_lat = params.get('maxLat')
        min_lon = params.get('minLon')
        max_lon = params.get('maxLon')

        logger.info(f"请求: variable={variable}, typeOfLevel={type_of_level}, "
                    f"level={level}, datetime={target_dt}")

        # ---- 查找最近时次文件 ----
        filepath, valid_time_dt = find_nearest_file(data_dir, target_dt)

        # ---- 读取二维字段 ----
        data, lat, lon = read_grib2_field(filepath, variable, level, type_of_level)

        # ---- 可选区域裁切 ----
        if all(v is not None for v in [min_lat, max_lat, min_lon, max_lon]):
            data, lat, lon = subset_region(
                data, lat, lon,
                float(min_lat), float(max_lat), float(min_lon), float(max_lon)
            )
            logger.info(f"裁切后 shape: {data.shape}")

        # ---- 保存 zip ----
        save_to_zip(data, lat, lon, level, valid_time_dt, variable, args.output)

        logger.info("完成")
        sys.exit(0)

    except Exception as e:
        logger.error(f"失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == '__main__':
    main()
