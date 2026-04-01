#!/bin/bash
# 北京 所有地面变量时间序列（温压湿风降水云）
# 首次请求走 Open-Meteo，同时后台缓存周边 5×5 格点
curl -X POST http://localhost:5001/api/openmeteo \
  -H "Content-Type: application/json" \
  -d '{"lat": 40.0, "lon": 116.0}'
