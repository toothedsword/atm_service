#!/bin/bash
# 北京 当前时刻实况（温度、湿度、降水、风速风向、云量）
curl -s "https://api.open-meteo.com/v1/forecast?latitude=39.9&longitude=116.4&current=temperature_2m,relativehumidity_2m,precipitation,windspeed_10m,winddirection_10m,cloudcover" \
  | python3 -m json.tool
