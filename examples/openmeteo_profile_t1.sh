#!/bin/bash

# 默认高度层（全部15层 100~30000m）
echo "=== 默认15层 (100~30000m) ==="
curl -s -X POST http://localhost:5001/api/openmeteo_profile \
  -H "Content-Type: application/json" \
  -d '{
    "lat": 40.0,
    "lon": 116.0
  }'
