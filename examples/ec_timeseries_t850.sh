#!/bin/bash
# 提取北京(40N,116E) 850hPa温度预报序列，从2026033003时次开始
curl -X POST http://localhost:5001/api/ec-timeseries \
  -H "Content-Type: application/json" \
  -d '{
    "datetime": "2026033003",
    "variable": "t",
    "level": 850,
    "lat": 40.0,
    "lon": 116.0
  }'
