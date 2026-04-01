#!/bin/bash
# 提取总降水量（地面变量）
curl -X POST http://localhost:5001/api/ec-forecast \
  -H "Content-Type: application/json" \
  -d '{"datetime":"2026033012","variable":"tp"}' \
  -o /tmp/tp.zip
