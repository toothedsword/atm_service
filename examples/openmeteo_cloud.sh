#!/bin/bash
# 北京 云量逐小时预报序列（总云量、低云、中云、高云）
curl -s "https://api.open-meteo.com/v1/forecast?latitude=39.9&longitude=116.4&hourly=cloudcover,cloudcover_low,cloudcover_mid,cloudcover_high&forecast_days=7" \
  | python3 -m json.tool
