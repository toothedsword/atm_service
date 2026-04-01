#!/bin/bash
# 北京 多变量组合：温度、湿度、降水、风、云量、气压（地面）
curl -s "https://api.open-meteo.com/v1/forecast?latitude=39.9&longitude=116.4&hourly=temperature_2m,relativehumidity_2m,precipitation,windspeed_10m,winddirection_10m,cloudcover,surface_pressure&forecast_days=3&timezone=Asia%2FShanghai" \
  | python3 -m json.tool
