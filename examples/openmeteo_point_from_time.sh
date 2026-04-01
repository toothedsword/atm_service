#!/bin/bash
# 北京 从指定时间开始的降水序列
curl -X POST http://localhost:5001/api/openmeteo \
  -H "Content-Type: application/json" \
  -d '{
    "lat": 40.0,
    "lon": 116.0,
    "variables": ["precipitation"],
    "datetime": "2026033106"
  }'
