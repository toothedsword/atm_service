#!/bin/bash
# 北京 10米风速风向逐小时预报序列
curl -s "https://api.open-meteo.com/v1/forecast?latitude=39.9&longitude=116.4&hourly=windspeed_10m,winddirection_10m,windgusts_10m&forecast_days=7" \
  | python3 -m json.tool
