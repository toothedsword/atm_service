#!/bin/bash
# 垂直高度层剖面：北京 15层(100~30000m)风速/风向/温度时间序列
# 等压面 geopotential_height 定位各层实际高度；缺失层用标准大气压高公式插值
# 超出 30 hPa(≈23.8 km) 的高度(如 30000 m)用最近两层线性外推
# 风速单位: m/s  温度单位: °C

# 默认高度层（全部15层）
echo "=== 默认15层 (100~30000m) ==="
curl -s -X POST http://localhost:5001/api/openmeteo_profile \
  -H "Content-Type: application/json" \
  -d '{
    "lat": 40.0,
    "lon": 116.0
  }' | python3 -m json.tool

echo ""
echo "=== 指定高度层 + 起始时间 ==="
curl -s -X POST http://localhost:5001/api/openmeteo_profile \
  -H "Content-Type: application/json" \
  -d '{
    "lat": 40.0,
    "lon": 116.0,
    "heights": [100, 900, 3000, 7000, 10000],
    "datetime": "2026050900",
    "forecast_days": 3
  }' | python3 -m json.tool
