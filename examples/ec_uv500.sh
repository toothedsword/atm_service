#!/bin/bash
# 提取500hPa U、V风分量
curl -X POST http://localhost:5001/api/ec-forecast \
  -H "Content-Type: application/json" \
  -d '{"datetime":"2026033006","variable":"u","level":500}' \
  -o /tmp/u_500.zip

curl -X POST http://localhost:5001/api/ec-forecast \
  -H "Content-Type: application/json" \
  -d '{"datetime":"2026033006","variable":"v","level":500}' \
  -o /tmp/v_500.zip
