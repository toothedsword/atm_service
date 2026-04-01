#!/bin/bash
# test_ec_forecast.sh
# EC预报数据提取接口测试示例
# 用法: bash test_ec_forecast.sh
#
# 前提: 服务已启动  python3 run.py

HOST="http://localhost:5001"
OUT="/tmp/ec_test_output"
mkdir -p "$OUT"

echo "========================================"
echo "EC预报数据提取接口测试"
echo "========================================"

# ----------------------------------------------------------------
# 1. 查询可用时次列表
# ----------------------------------------------------------------
echo ""
echo "[1] 查询可用时次列表"
curl -s "${HOST}/api/ec-list" | python3 -m json.tool


# ----------------------------------------------------------------
# 2. 850hPa 温度（全球，自动匹配最近时次）
# ----------------------------------------------------------------
echo ""
echo "[2] 850hPa 温度  datetime=2026033006"
curl -s -X POST "${HOST}/api/ec-forecast" \
  -H "Content-Type: application/json" \
  -d '{
    "datetime": "2026033006",
    "variable": "t",
    "level": 850
  }' \
  -o "${OUT}/t_850_2026033006.zip"
echo "  -> ${OUT}/t_850_2026033006.zip  $(du -h ${OUT}/t_850_2026033006.zip | cut -f1)"


# ----------------------------------------------------------------
# 3. 500hPa 位势高度
# ----------------------------------------------------------------
echo ""
echo "[3] 500hPa 位势高度  datetime=2026033012"
curl -s -X POST "${HOST}/api/ec-forecast" \
  -H "Content-Type: application/json" \
  -d '{
    "datetime": "2026033012",
    "variable": "gh",
    "level": 500
  }' \
  -o "${OUT}/gh_500_2026033012.zip"
echo "  -> ${OUT}/gh_500_2026033012.zip  $(du -h ${OUT}/gh_500_2026033012.zip | cut -f1)"


# ----------------------------------------------------------------
# 4. 500hPa 风场（U、V分量）
# ----------------------------------------------------------------
echo ""
echo "[4] 500hPa U分量  datetime=2026033009"
curl -s -X POST "${HOST}/api/ec-forecast" \
  -H "Content-Type: application/json" \
  -d '{
    "datetime": "2026033009",
    "variable": "u",
    "level": 500
  }' \
  -o "${OUT}/u_500_2026033009.zip"
echo "  -> ${OUT}/u_500_2026033009.zip  $(du -h ${OUT}/u_500_2026033009.zip | cut -f1)"

echo ""
echo "[4] 500hPa V分量  datetime=2026033009"
curl -s -X POST "${HOST}/api/ec-forecast" \
  -H "Content-Type: application/json" \
  -d '{
    "datetime": "2026033009",
    "variable": "v",
    "level": 500
  }' \
  -o "${OUT}/v_500_2026033009.zip"
echo "  -> ${OUT}/v_500_2026033009.zip  $(du -h ${OUT}/v_500_2026033009.zip | cut -f1)"


# ----------------------------------------------------------------
# 5. 2米温度（地面变量，不需要指定level）
# ----------------------------------------------------------------
echo ""
echo "[5] 2米温度  datetime=2026033003"
curl -s -X POST "${HOST}/api/ec-forecast" \
  -H "Content-Type: application/json" \
  -d '{
    "datetime": "2026033003",
    "variable": "2t"
  }' \
  -o "${OUT}/2t_2026033003.zip"
echo "  -> ${OUT}/2t_2026033003.zip  $(du -h ${OUT}/2t_2026033003.zip | cut -f1)"


# ----------------------------------------------------------------
# 6. 10米风速（U分量）
# ----------------------------------------------------------------
echo ""
echo "[6] 10米U风  datetime=2026033006"
curl -s -X POST "${HOST}/api/ec-forecast" \
  -H "Content-Type: application/json" \
  -d '{
    "datetime": "2026033006",
    "variable": "10u"
  }' \
  -o "${OUT}/10u_2026033006.zip"
echo "  -> ${OUT}/10u_2026033006.zip  $(du -h ${OUT}/10u_2026033006.zip | cut -f1)"


# ----------------------------------------------------------------
# 7. 地面气压
# ----------------------------------------------------------------
echo ""
echo "[7] 地面气压  datetime=2026033000"
curl -s -X POST "${HOST}/api/ec-forecast" \
  -H "Content-Type: application/json" \
  -d '{
    "datetime": "2026033000",
    "variable": "sp"
  }' \
  -o "${OUT}/sp_2026033000.zip"
echo "  -> ${OUT}/sp_2026033000.zip  $(du -h ${OUT}/sp_2026033000.zip | cut -f1)"


# ----------------------------------------------------------------
# 8. 总降水（累积量）
# ----------------------------------------------------------------
echo ""
echo "[8] 总降水  datetime=2026033012"
curl -s -X POST "${HOST}/api/ec-forecast" \
  -H "Content-Type: application/json" \
  -d '{
    "datetime": "2026033012",
    "variable": "tp"
  }' \
  -o "${OUT}/tp_2026033012.zip"
echo "  -> ${OUT}/tp_2026033012.zip  $(du -h ${OUT}/tp_2026033012.zip | cut -f1)"


# ----------------------------------------------------------------
# 9. 区域裁切示例：中国区域 850hPa 相对湿度
# ----------------------------------------------------------------
echo ""
echo "[9] 中国区域 850hPa 相对湿度  datetime=2026033006"
curl -s -X POST "${HOST}/api/ec-forecast" \
  -H "Content-Type: application/json" \
  -d '{
    "datetime": "2026033006",
    "variable": "r",
    "level": 850,
    "minLat": 18.0,
    "maxLat": 55.0,
    "minLon": 73.0,
    "maxLon": 135.0
  }' \
  -o "${OUT}/r_850_china_2026033006.zip"
echo "  -> ${OUT}/r_850_china_2026033006.zip  $(du -h ${OUT}/r_850_china_2026033006.zip | cut -f1)"


# ----------------------------------------------------------------
# 10. 最近时次自动匹配（请求时间不在整点上）
# ----------------------------------------------------------------
echo ""
echo "[10] 自动匹配最近时次  datetime=2026033005 (应匹配06h步长)"
curl -s -X POST "${HOST}/api/ec-forecast" \
  -H "Content-Type: application/json" \
  -d '{
    "datetime": "2026033005",
    "variable": "t",
    "level": 700
  }' \
  -o "${OUT}/t_700_nearest.zip"
echo "  -> ${OUT}/t_700_nearest.zip  $(du -h ${OUT}/t_700_nearest.zip | cut -f1)"


echo ""
echo "========================================"
echo "输出文件列表:"
ls -lh "${OUT}/"
echo "========================================"

# ----------------------------------------------------------------
# 11. 错误处理演示：缺少必填字段
# ----------------------------------------------------------------
echo ""
echo "[11] 错误处理 - 缺少variable字段"
curl -s -X POST "${HOST}/api/ec-forecast" \
  -H "Content-Type: application/json" \
  -d '{"datetime": "2026033006"}' \
  | python3 -m json.tool

echo ""
echo "[11] 错误处理 - 不存在的变量名"
curl -s -X POST "${HOST}/api/ec-forecast" \
  -H "Content-Type: application/json" \
  -d '{"datetime":"2026033006","variable":"xyz","level":500}' \
  | python3 -m json.tool
