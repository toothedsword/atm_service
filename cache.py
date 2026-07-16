#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
cache.py - Open-Meteo 数据后台缓存进程
以请求点为中心，抓取周围 5×5 格点（0.125° 间距）的地面温压湿风降水时间序列并缓存到本地。
每个格点单独存一个 JSON 文件，文件名含时间戳，缓存有效期 1 小时。
文件命名: {lat:.3f}_{lon:.3f}_{yyyymmddhh}.json

用法（由 run.py 后台调度，也可手动运行）:
    python3 cache.py --lat 40.0 --lon 116.0
"""

import argparse
import json
import logging
import os
import time
from datetime import datetime, timezone

import requests

# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------
CACHE_DIR     = '/tmp/atm_service/openmeteo_cache'
CACHE_TTL     = 3600     # 缓存有效期（秒）
GRID_RES      = 0.125    # Open-Meteo 格距（度），用于对齐格点
CACHE_EXTENT  = 2.5      # 缓存范围半径（度），覆盖 5°×5° 区域
CACHE_SAMPLE  = 0.5      # 缓存采样间距（度），5°×5° 内按 0.5° 采样，共 11×11=121 点
REQUEST_DELAY = 0.15     # 每次请求间隔（秒），避免频率过高

# 固定缓存的变量组合（地面温压湿风降水）
CACHE_VARIABLES = [
    'temperature_2m',
    'relativehumidity_2m',
    'precipitation',
    'windspeed_10m',
    'winddirection_10m',
    'windgusts_10m',
    'surface_pressure',
    'cloudcover',
]

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------

def snap_to_grid(val):
    """将经纬度对齐到 0.125° 格点"""
    return round(round(val / GRID_RES) * GRID_RES, 3)


def cache_filename(lat, lon, ts=None):
    """生成带时间戳的缓存文件名，ts 为 yyyymmddhh 字符串，默认当前小时"""
    if ts is None:
        ts = datetime.now(timezone.utc).strftime('%Y%m%d%H')
    return f'{lat:.3f}_{lon:.3f}_{ts}.json'


def cache_path(lat, lon, ts=None):
    return os.path.join(CACHE_DIR, cache_filename(lat, lon, ts))


def find_latest_cache(lat, lon):
    """
    查找该格点最新的缓存文件。
    返回 (path, ts_str) 或 (None, None)，ts_str 为文件名中的 yyyymmddhh。
    """
    import glob
    pattern = os.path.join(CACHE_DIR, f'{lat:.3f}_{lon:.3f}_*.json')
    files = glob.glob(pattern)
    if not files:
        return None, None
    # 文件名末尾是 yyyymmddhh，字典序即时间序，max 直接取最新
    latest = max(files)
    ts = os.path.basename(latest).rsplit('_', 1)[-1].replace('.json', '')
    return latest, ts


def is_cache_fresh(lat, lon, requested_dt=None):
    """
    用文件名中的时间戳判断缓存是否在有效期内。
    requested_dt: datetime 对象（UTC），若提供，则还要求 cache 对该时间也足够新鲜。
    两个条件同时满足才算命中：
      1. now - cache_ts < TTL          （相对当前时间不过期）
      2. requested_dt - cache_ts < TTL  （相对请求时间不过期，仅 requested_dt > cache_ts 时检查）
    """
    path, ts = find_latest_cache(lat, lon)
    if path is None:
        return False
    try:
        cache_dt = datetime.strptime(ts, '%Y%m%d%H').replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)

        if (now - cache_dt).total_seconds() >= CACHE_TTL:
            return False

        if requested_dt is not None and requested_dt > cache_dt:
            if (requested_dt - cache_dt).total_seconds() >= CACHE_TTL:
                return False

        return True
    except ValueError:
        return False


def load_cache(lat, lon):
    path, _ = find_latest_cache(lat, lon)
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_cache(lat, lon, om_response, requested_dt=None):
    """
    将 Open-Meteo 原始响应存为带时间戳的缓存文件，同名文件已存在则跳过。
    文件时间戳：若 requested_dt 晚于当前时间则用 requested_dt，否则用当前小时。
    """
    os.makedirs(CACHE_DIR, exist_ok=True)
    ts = (requested_dt.strftime('%Y%m%d%H')
          if requested_dt
          else datetime.now(timezone.utc).strftime('%Y%m%d%H'))
    path = cache_path(lat, lon, ts)
    if os.path.exists(path):
        logger.info(f"  同名缓存已存在，跳过: {os.path.basename(path)}")
        return
    payload = {
        'cached_at': ts,
        'lat': lat,
        'lon': lon,
        'data': om_response,
    }
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, separators=(',', ':'))
    logger.info(f"  缓存已保存: ({lat:.3f}, {lon:.3f})  时次={ts}")


def fetch_from_openmeteo(lat, lon, forecast_days=7):
    """请求 Open-Meteo，返回原始 JSON"""
    url = 'https://api.open-meteo.com/v1/forecast'
    params = {
        'latitude':     lat,
        'longitude':    lon,
        'hourly':       ','.join(CACHE_VARIABLES),
        'forecast_days': forecast_days,
        'timezone':     'UTC',
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def gen_area_points(center_lat, center_lon):
    """
    在 5°×5° 范围内（center ± CACHE_EXTENT）按 CACHE_SAMPLE 间距采样，
    每个采样点再对齐到 0.125° Open-Meteo 格点，去重后返回。
    """
    import numpy as np
    lats = np.arange(center_lat - CACHE_EXTENT,
                     center_lat + CACHE_EXTENT + CACHE_SAMPLE * 0.5,
                     CACHE_SAMPLE)
    lons = np.arange(center_lon - CACHE_EXTENT,
                     center_lon + CACHE_EXTENT + CACHE_SAMPLE * 0.5,
                     CACHE_SAMPLE)
    points = set()
    for lat in lats:
        for lon in lons:
            points.add((snap_to_grid(lat), snap_to_grid(lon)))
    return sorted(points)


# ---------------------------------------------------------------------------
# 主逻辑
# ---------------------------------------------------------------------------

def run_cache(center_lat, center_lon, requested_dt=None):
    snapped_lat = snap_to_grid(center_lat)
    snapped_lon = snap_to_grid(center_lon)
    points = gen_area_points(snapped_lat, snapped_lon)

    logger.info(f"开始缓存 5°×5° 区域，中心 ({snapped_lat:.3f}, {snapped_lon:.3f})，"
                f"采样间距 {CACHE_SAMPLE}°，共 {len(points)} 个点"
                + (f"，请求时间 {requested_dt.strftime('%Y%m%d%H')}" if requested_dt else ""))

    ok, skipped, failed = 0, 0, 0
    for lat, lon in points:
        if is_cache_fresh(lat, lon, requested_dt):
            logger.info(f"  跳过 ({lat:.3f}, {lon:.3f}) - 缓存新鲜")
            skipped += 1
            continue
        try:
            data = fetch_from_openmeteo(lat, lon)
            save_cache(lat, lon, data, requested_dt)
            ok += 1
            time.sleep(REQUEST_DELAY)
        except Exception as e:
            logger.error(f"  失败 ({lat:.3f}, {lon:.3f}): {e}")
            failed += 1

    logger.info(f"缓存完成: 新增={ok}  跳过={skipped}  失败={failed}")


def main():
    parser = argparse.ArgumentParser(description='Open-Meteo 5°×5° 区域后台缓存')
    parser.add_argument('--lat',      type=float, required=True, help='中心纬度')
    parser.add_argument('--lon',      type=float, required=True, help='中心经度')
    parser.add_argument('--datetime', type=str,   default=None,
                        help='请求起始时间 yyyymmddhh，用于判断缓存是否对该时间足够新鲜')
    args = parser.parse_args()

    requested_dt = None
    if args.datetime:
        try:
            requested_dt = datetime.strptime(args.datetime, '%Y%m%d%H').replace(tzinfo=timezone.utc)
        except ValueError:
            logger.error(f"--datetime 格式错误: {args.datetime}，应为 yyyymmddhh")

    run_cache(args.lat, args.lon, requested_dt)


if __name__ == '__main__':
    main()
