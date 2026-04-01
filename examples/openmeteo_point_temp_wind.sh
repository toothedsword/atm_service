#!/bin/bash
# 上海 只取温度和风（缓存命中时极快）
curl -X POST http://localhost:5001/api/openmeteo \
  -H "Content-Type: application/json" \
  -d '{
    "lat": 31.2,
    "lon": 121.5,
    "variables": ["temperature_2m", "windspeed_10m", "winddirection_10m"]
  }'
