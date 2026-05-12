#!/bin/bash
# 垂直高度层剖面接口
# 固定返回 2m（无风）和 10m（含10m风）近地面层，再加指定高度层（风+温度）
# 输出格式: {"code":1000, "message":"成功", "data":[{height,sfp,cld,tem,dp,pre,windS,windD,vis,rh,forecastTime},...]}
# 数据顺序: 2m所有时次 → 10m所有时次 → 各profile高度所有时次
# 风速单位: m/s  温度: °C  气压: hPa  能见度: m  forecastTime: "YYYY-MM-DD HH:MM:SS"

# 默认高度层（全部15层 100~30000m）
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
    "datetime": "2026051100",
    "forecast_days": 3
  }' | python3 -m json.tool
