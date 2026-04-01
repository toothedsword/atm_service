#!/bin/bash
# 北京 逐小时降水量预报序列
curl -s "https://api.open-meteo.com/v1/forecast?latitude=39.9&longitude=116.4&hourly=precipitation&forecast_days=7" \
  | python3 -m json.tool
