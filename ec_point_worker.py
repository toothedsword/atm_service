#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ec_point_worker.py - EC 预报点值时间序列提取工作进程
提取指定经纬度、变量、层级在给定起始时间之后的所有预报时次值，输出为 JSON。

用法:
    python3 ec_point_worker.py --input <params.json> --output <result.json>

params.json 字段:
    data_dir      str   GRIB2 文件目录 (可选，默认 /home/leon/Downloads/ec-oper-fc)
    datetime      str   起始时间（包含），格式 yyyymmddhh，例如 "2026033003"  (必填)
    variable      str   GRIB2 shortName，例如 "t" / "2t" / "u"              (必填)
    lat           float 纬度，例如 40.0                                       (必填)
    lon           float 经度，例如 116.0                                      (必填)
    level         int   层级值，等压面用 hPa；地面/高度层变量可省略 (默认 0)
    typeOfLevel   str   层级类型，可省略，自动推断
"""

import argparse
import json
import logging
import os
import re
import sys
from datetime import datetime, timedelta

import numpy as np

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

_FILENAME_RE = re.compile(
    r'(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})-(\d+)h-oper-fc\.grib2$'
)

_DEFAULT_LEVEL_TYPE = {
    '2t': 'heightAboveGround', '2d': 'heightAboveGround',
    'mn2t3': 'heightAboveGround', 'mx2t3': 'heightAboveGround',
    '10u': 'heightAboveGround', '10v': 'heightAboveGround',
    '10fg': 'heightAboveGround', '100u': 'heightAboveGround', '100v': 'heightAboveGround',
    'sp': 'surface', 'msl': 'surface', 'lsm': 'surface',
    'ssrd': 'surface', 'strd': 'surface', 'ssr': 'surface', 'str': 'surface',
    'tprate': 'surface', 'tp': 'surface', 'ptype': 'surface',
    'asn': 'surface', 'rsn': 'surface', 'skt': 'surface',
    'sd': 'surface', 'sf': 'surface', 'ro': 'surface',
    'tcw': 'entireAtmosphere', 'tcwv': 'entireAtmosphere', 'tcc': 'entireAtmosphere',
    'sot': 'soilLayer', 'vsw': 'soilLayer',
    'msl': 'meanSea',
}


def parse_datetime(s):
    s = s.strip()
    fmt_map = {
        10: '%Y%m%d%H',
        12: '%Y%m%d%H%M',
        14: '%Y%m%d%H%M%S',
    }
    fmt = fmt_map.get(len(s))
    if fmt:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    raise ValueError(f"无法解析时间: {s!r}，支持格式: yyyymmddhh / yyyymmddHHMM")


def scan_files_after(data_dir, start_dt):
    """返回 valid_time >= start_dt 的文件列表，按有效时间排序"""
    results = []
    for fname in os.listdir(data_dir):
        m = _FILENAME_RE.match(fname)
        if not m:
            continue
        yr, mo, dy, hh, mm, ss, step = m.groups()
        base_time = datetime(int(yr), int(mo), int(dy), int(hh), int(mm), int(ss))
        valid_time = base_time + timedelta(hours=int(step))
        if valid_time >= start_dt:
            results.append((valid_time, int(step), os.path.join(data_dir, fname)))
    results.sort(key=lambda x: x[0])
    return results


def extract_point(filepath, variable, level, type_of_level, lat, lon):
    """
    从 grib2 文件中提取指定经纬度的点值（双线性插值）。
    返回 float，若缺测返回 None。
    """
    import xarray as xr

    filter_keys = {'shortName': variable, 'typeOfLevel': type_of_level}
    if type_of_level in ('isobaricInhPa', 'soilLayer') and int(level) != 0:
        filter_keys['level'] = int(level)

    ds = xr.open_dataset(
        filepath,
        engine='cfgrib',
        filter_by_keys=filter_keys,
        indexpath=None,
    )

    varnames = list(ds.data_vars)
    if not varnames:
        raise ValueError(f"未找到变量 {variable} (typeOfLevel={type_of_level}, level={level})")

    da = ds[varnames[0]]

    # 双线性插值到请求点
    value = float(da.interp(latitude=lat, longitude=lon, method='linear').values)

    if np.isnan(value):
        return None
    return round(value, 6)


def main():
    parser = argparse.ArgumentParser(description='EC预报点值时间序列提取')
    parser.add_argument('--input',  required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()

    try:
        with open(args.input, 'r', encoding='utf-8') as f:
            params = json.load(f)

        data_dir     = params.get('data_dir', '/home/leon/Downloads/ec-oper-fc')
        variable     = params['variable']
        req_lat      = float(params['lat'])
        req_lon      = float(params['lon'])
        level        = int(params.get('level', 0))
        start_dt     = parse_datetime(params['datetime'])
        type_of_level = (
            params.get('typeOfLevel')
            or _DEFAULT_LEVEL_TYPE.get(variable)
            or 'isobaricInhPa'
        )

        logger.info(f"请求: variable={variable} level={level} "
                    f"lat={req_lat} lon={req_lon} start={start_dt}")

        files = scan_files_after(data_dir, start_dt)
        if not files:
            raise FileNotFoundError(f"未找到 >= {start_dt} 的预报文件")

        logger.info(f"共找到 {len(files)} 个时次文件")

        times, values = [], []
        for valid_time, step_h, filepath in files:
            logger.info(f"  读取 step={step_h}h  valid={valid_time}")
            try:
                val = extract_point(filepath, variable, level, type_of_level, req_lat, req_lon)
                times.append(valid_time.strftime('%Y%m%d%H'))
                values.append(val)
            except Exception as e:
                logger.warning(f"  跳过 {filepath}: {e}")

        if not times:
            raise RuntimeError("所有时次提取均失败")

        result = {
            "variable":     variable,
            "typeOfLevel":  type_of_level,
            "level":        level,
            "lat":          req_lat,
            "lon":          req_lon,
            "start_time":   start_dt.strftime('%Y%m%d%H'),
            "count":        len(times),
            "times":        times,
            "values":       values,
        }

        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        logger.info(f"完成，输出 {len(times)} 个时次到 {args.output}")
        sys.exit(0)

    except Exception as e:
        logger.error(f"失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == '__main__':
    main()
