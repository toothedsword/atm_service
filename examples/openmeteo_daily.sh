#!/bin/bash
# 北京 逐日预报：最高最低气温、日总降水、最大风速
curl -s "https://api.open-meteo.com/v1/forecast?latitude=39.9&longitude=116.4&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max,winddirection_10m_dominant&forecast_days=7&timezone=Asia%2FShanghai" \
  | python3 -m json.tool
