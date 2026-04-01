#!/bin/bash
# 提取某点500hPa U/V风预报序列
curl -X POST http://localhost:5001/api/ec-timeseries \
  -H "Content-Type: application/json" \
  -d '{
    "datetime": "2026033000",
    "variable": "u",
    "level": 500,
    "lat": 35.0,
    "lon": 105.0
  }'
