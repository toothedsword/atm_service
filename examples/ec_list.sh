#!/bin/bash
# 查询EC预报可用时次列表
curl -s http://localhost:5001/api/ec-list | python3 -m json.tool
