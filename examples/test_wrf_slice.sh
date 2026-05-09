#!/bin/bash
# 测试 /api/wrf_slice 接口
# 返回 ZIP（含 4 张剖面图 PNG），解压到当前目录

HOST="${1:-http://localhost:5001}"
OUT="wrf_slice_out.zip"

curl -X POST "${HOST}/api/wrf_slice" \
  -H "Content-Type: application/json" \
  -d '{
    "data_dir": "/home/leon/Downloads/atm_service/tmp/data",
    "lons": [100, 102, 104, 106, 108],
    "lats": [30,  31,  32,  33,  34],
    "time_idx": 0,
    "flight_height_km": 6,
    "max_height_km": 8.0,
    "nx_points": 200,
    "label": "test",
    "plot_types": ["rh", "ws", "dzdt", "cf"]
  }' \
  -o "${OUT}" && unzip -o "${OUT}"
