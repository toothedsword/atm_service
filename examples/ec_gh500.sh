#!/bin/bash
# 提取500hPa位势高度
curl -X POST http://localhost:5001/api/ec-forecast \
  -H "Content-Type: application/json" \
  -d '{"datetime":"2026033012","variable":"gh","level":500}' \
  -o /tmp/gh_500.zip
