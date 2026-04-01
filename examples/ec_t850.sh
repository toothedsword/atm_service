#!/bin/bash
# 提取850hPa温度，自动匹配最近时次
curl -X POST http://localhost:5001/api/ec-forecast \
  -H "Content-Type: application/json" \
  -d '{"datetime":"2026033006","variable":"t","level":850}' \
  -o /tmp/t_850.zip
