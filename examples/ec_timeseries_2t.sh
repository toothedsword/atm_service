#!/bin/bash
# 提取上海(31N,121E) 2米温度预报序列，从2026033000时次开始（地面变量不填level）
curl -X POST http://localhost:5001/api/ec-timeseries \
  -H "Content-Type: application/json" \
  -d '{
    "datetime": "2026033000",
    "variable": "2t",
    "lat": 31.0,
    "lon": 121.0
  }'
