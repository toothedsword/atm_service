#!/bin/bash
# 提取850hPa相对湿度，裁切到中国区域
curl -X POST http://localhost:5001/api/ec-forecast \
  -H "Content-Type: application/json" \
  -d '{
    "datetime": "2026033006",
    "variable": "r",
    "level": 850,
    "minLat": 18.0,
    "maxLat": 55.0,
    "minLon": 73.0,
    "maxLon": 135.0
  }' \
  -o /tmp/r_850_china.zip
