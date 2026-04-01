#!/bin/bash
# 北京 2米相对湿度逐小时预报序列
curl -s "https://api.open-meteo.com/v1/forecast?latitude=39.9&longitude=116.4&hourly=relativehumidity_2m&forecast_days=7" \
  | python3 -m json.tool
