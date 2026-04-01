#!/bin/bash
# 北京 850hPa 温度、相对湿度、U/V风逐小时预报序列
curl -s "https://api.open-meteo.com/v1/forecast?latitude=39.9&longitude=116.4&hourly=temperature_850hPa,relativehumidity_850hPa,windspeed_850hPa,winddirection_850hPa&forecast_days=7" \
  | python3 -m json.tool
