#!/bin/bash
# 北京 2米温度逐小时预报序列（未来7天）
curl -s "https://api.open-meteo.com/v1/forecast?latitude=39.9&longitude=116.4&hourly=temperature_2m&forecast_days=7" \
  | python3 -m json.tool
