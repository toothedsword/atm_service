#!/bin/bash
# 测试 /api/wrf_slice 接口
# 返回 ZIP（含 4 张剖面图 PNG），解压到当前目录

HOST="${1:-http://172.17.0.3:5001}"
OUT="wrf_slice_out.zip"

# waypoints 每个点含经纬度和时间，worker 同时做空间+时间插值
# base_time + time_step_hours 用于将数据文件中的整数时间索引转换为实际时间
# 若数据 timeList 已是 yyyymmddhh 字符串，可省略 base_time / time_step_hours

curl -X POST "${HOST}/api/wrf_slice" \
  -H "Content-Type: application/json" \
  -d '{"files":[
    "/.out/home/leon/Downloads/atm_service/tmp/data/WRF_REAL_20260506000000_t_all_00000.zip",
    "/.out/home/leon/Downloads/atm_service/tmp/data/WRF_REAL_20260506000000_u_all_00000.zip",
    "/.out/home/leon/Downloads/atm_service/tmp/data/WRF_REAL_20260506000000_v_all_00000.zip",
    "/.out/home/leon/Downloads/atm_service/tmp/data/WRF_REAL_20260506000000_rh_all_00000.zip",
    "/.out/home/leon/Downloads/atm_service/tmp/data/WRF_REAL_20260506000000_dzdt_all_00000.zip",
    "/.out/home/leon/Downloads/atm_service/tmp/data/WRF_REAL_20260506000000_cf1_single_00000.zip",
    "/.out/home/leon/Downloads/atm_service/tmp/data/WRF_REAL_20260506000000_cf2_single_00000.zip",
    "/.out/home/leon/Downloads/atm_service/tmp/data/WRF_REAL_20260506000000_cf3_single_00000.zip",
    "/.out/home/leon/Downloads/atm_service/tmp/data/WRF_REAL_20260506000000_dcf1_single_00000.zip",
    "/.out/home/leon/Downloads/atm_service/tmp/data/WRF_REAL_20260506000000_dcf2_single_00000.zip",
    "/.out/home/leon/Downloads/atm_service/tmp/data/WRF_REAL_20260506000000_dcf3_single_00000.zip"
  ],
  "base_time": "2026050600",
  "time_step_hours": 1,
  "waypoints": [
    {"lon": 100, "lat": 30, "time": "202605060000"},
    {"lon": 102, "lat": 31, "time": "202605060100"},
    {"lon": 104, "lat": 32, "time": "202605060200"},
    {"lon": 106, "lat": 33, "time": "202605060300"},
    {"lon": 108, "lat": 34, "time": "202605060400"}
  ],
  "flight_height_km": 6,
  "max_height_km": 8.0,
  "nx_points": 200,
  "label": "test",
  "plot_types": ["rh", "ws", "dzdt", "cf"]}' \
  -o "${OUT}" && unzip -o "${OUT}"
