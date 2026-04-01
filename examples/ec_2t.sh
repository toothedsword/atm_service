#!/bin/bash
# 提取2米温度（地面变量，不需要指定level）
curl -X POST http://localhost:5001/api/ec-forecast \
  -H "Content-Type: application/json" \
  -d '{"datetime":"2026033006","variable":"2t"}' \
  -o /tmp/2t_2026033006.zip
