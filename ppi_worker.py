#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PPI 最新完整圈出图 worker
用法: python3 ppi_worker.py <config_json> <output_dir>

config_json 格式:
{
    "files": [
        "/path/to/file1.csv",
        "/path/to/file2.csv",
        "/path/to/file3.csv"
    ]
}

逻辑：
  1. 按时间顺序解析所有 CSV 中的波束
  2. 利用方位角跨越 0° 的跳变识别 PPI 圈边界
  3. 从后往前找最新的完整圈（起始 az < AZ_START_MAX，结束 az > AZ_END_MIN，波束数 ≥ MIN_BEAMS）
  4. 生成 PNG + GeoTIFF 并输出文件路径
"""

import csv
import json
import math
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib import font_manager

import numpy as np
from scipy.interpolate import griddata
import rasterio
from rasterio.transform import from_bounds
from rasterio.crs import CRS

# ── 中文字体 ──────────────────────────────────────────────────────────────────
def _setup_cjk_font():
    _name_candidates = [
        "Noto Sans CJK SC", "Noto Serif CJK SC",
        "Noto Serif SC", "Noto Sans SC",
        "AR PL UMing TW", "AR PL UKai TW",
    ]
    for _n in _name_candidates:
        if any(f.name == _n for f in font_manager.fontManager.ttflist):
            matplotlib.rcParams["font.family"] = _n
            return
    _path_candidates = [
        os.path.join(matplotlib.get_data_path(), "fonts", "ttf", "NotoSansCJK-Regular.ttc"),
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/NotoSerifSC-VF.ttf",
        "/usr/local/share/fonts/opentype/noto/NotoSerifCJK-Medium.ttc",
        "/usr/share/fonts/msyh.ttc",
        os.path.join(os.path.dirname(__file__), "msyh.ttc"),
    ]
    for _p in _path_candidates:
        if os.path.exists(_p):
            font_manager.fontManager.addfont(_p)
            _name = font_manager.FontProperties(fname=_p).get_name()
            matplotlib.rcParams["font.family"] = _name
            return

_setup_cjk_font()

# ── 参数 ──────────────────────────────────────────────────────────────────────
RANGE_STEP     = 60        # 距离库步长（米）
NUM_GATES      = 300       # 距离库数量
MAX_RANGE_M    = 10_000    # 出图最大距离（米）
GRID_RES_M     = 100.0     # 网格分辨率（米）
MIN_BEAMS      = 60        # 完整一圈最少波束数
WRAP_THRESHOLD = 200.0     # 方位角跳变检测阈值（度）
AZ_START_MAX   = 10.0      # 完整圈起始方位角上限（度）
AZ_END_MIN     = 350.0     # 完整圈结束方位角下限（度）
VEL_RANGE      = (-15, 15) # 色标范围（m/s）
EARTH_R        = 6_371_000.0


# ── CSV 解析 ──────────────────────────────────────────────────────────────────
def parse_file(filepath: str) -> list:
    beams = []
    try:
        with open(filepath, encoding="gbk") as f:
            reader = csv.reader(f)
            try:
                next(reader)  # 跳过表头
            except StopIteration:
                return beams
            for row in reader:
                if len(row) < 16:
                    continue
                try:
                    az  = float(row[5])
                    el  = float(row[6])
                    lon = float(row[3])
                    lat = float(row[4])
                    t   = row[0]
                except ValueError:
                    continue
                vel = np.full(NUM_GATES, np.nan)
                for i in range(NUM_GATES):
                    base = 10 + i * 6
                    if base >= len(row):
                        break
                    try:
                        v = float(row[base])
                        if v != -9999:
                            vel[i] = v
                    except ValueError:
                        pass
                beams.append({"time": t, "az": az, "el": el,
                              "lon": lon, "lat": lat, "vel": vel,
                              "_src": filepath})
    except Exception as exc:
        print(f"[warn] 解析失败 {filepath}: {exc}", file=sys.stderr)
    return beams


# ── 识别完整 PPI 段 ────────────────────────────────────────────────────────────
def find_latest_complete_ppi(all_beams: list) -> list | None:
    """
    将 all_beams 按方位角跳变切分为若干段，
    从后往前找最新的满足条件的完整圈。
    返回该圈的波束列表，若无则返回 None。
    """
    # 切分成段
    segments = []
    seg = []
    prev_az = None
    for beam in all_beams:
        az = beam["az"]
        if prev_az is not None and (prev_az - az) > WRAP_THRESHOLD:
            segments.append(seg)
            seg = []
        seg.append(beam)
        prev_az = az
    if seg:
        segments.append(seg)

    # 从后往前找最新完整圈
    for seg in reversed(segments):
        if len(seg) < MIN_BEAMS:
            continue
        az_first = seg[0]["az"]
        az_last  = seg[-1]["az"]
        if az_first <= AZ_START_MAX and az_last >= AZ_END_MIN:
            return seg

    return None


# ── 极坐标 → 笛卡尔 ───────────────────────────────────────────────────────────
def beams_to_cartesian(beams, max_r):
    el_rad = math.radians(beams[0]["el"])
    ng = int(max_r / RANGE_STEP)
    rm = np.arange(1, ng + 1) * RANGE_STEP
    xs, ys, vs = [], [], []
    for b in beams:
        ar = math.radians(b["az"])
        h  = rm * math.cos(el_rad)
        e  = h * math.sin(ar)
        n  = h * math.cos(ar)
        v  = b["vel"][:ng]
        ok = ~np.isnan(v)
        xs.extend(e[ok]); ys.extend(n[ok]); vs.extend(v[ok])
    return np.array(xs), np.array(ys), np.array(vs)


# ── 插值 ──────────────────────────────────────────────────────────────────────
def to_grid(xs, ys, vs, ext, res):
    n  = int(2 * ext / res) + 1
    ge = np.linspace(-ext, ext, n)
    gn = np.linspace(-ext, ext, n)
    GE, GN = np.meshgrid(ge, gn)
    g = griddata((xs, ys), vs, (GE, GN), method="linear", fill_value=np.nan)
    return g, ge, gn, n


# ── GeoTIFF ───────────────────────────────────────────────────────────────────
def save_geotiff(grid, n, rlon, rlat, ext, path):
    lr  = math.radians(rlat)
    dpl = 1.0 / (EARTH_R * math.pi / 180.0)
    dpo = 1.0 / (EARTH_R * math.cos(lr) * math.pi / 180.0)
    W = rlon - ext * dpo;  E = rlon + ext * dpo
    S = rlat - ext * dpl;  N = rlat + ext * dpl
    tf = from_bounds(W, S, E, N, n, n)
    with rasterio.open(path, "w", driver="GTiff",
                       height=n, width=n, count=1,
                       dtype=rasterio.float32,
                       crs=CRS.from_epsg(4326),
                       transform=tf, nodata=np.nan) as dst:
        dst.write(grid[::-1, :].astype(np.float32), 1)


# ── 专题图 PNG ────────────────────────────────────────────────────────────────
def save_png(grid, ge, gn, rlon, rlat, ext, beams, path):
    lr  = math.radians(rlat)
    el  = math.radians(beams[0]["el"])
    dpl = 1.0 / (EARTH_R * math.pi / 180.0)
    dpo = 1.0 / (EARTH_R * math.cos(lr) * math.pi / 180.0)
    LON, LAT = np.meshgrid(rlon + ge * dpo, rlat + gn * dpl)

    fig, ax = plt.subplots(figsize=(9, 9))
    fig.patch.set_facecolor("#1a1a2e")
    ax.set_facecolor("#1a1a2e")

    pcm = ax.pcolormesh(LON, LAT, grid, cmap="RdBu_r",
                        vmin=VEL_RANGE[0], vmax=VEL_RANGE[1],
                        shading="auto", zorder=1)

    # 距离环
    th = np.linspace(0, 2 * math.pi, 360)
    for km in [2, 4, 6, 8, 10]:
        r = km * 1000
        if r > ext:
            continue
        rx = rlon + r * np.sin(th) * math.cos(el) * dpo
        ry = rlat + r * np.cos(th) * math.cos(el) * dpl
        ax.plot(rx, ry, "--", color="white", lw=0.6, alpha=0.5, zorder=2)
        ax.text(rlon, rlat + r * math.cos(el) * dpl,
                f"{km}km", color="white", fontsize=7,
                ha="center", va="bottom", alpha=0.7, zorder=3)

    # 方位标注
    for az_d, lbl in [(0, "N"), (90, "E"), (180, "S"), (270, "W")]:
        ar = math.radians(az_d)
        lr2 = ext * 0.93
        lx = rlon + lr2 * math.sin(ar) * math.cos(el) * dpo
        ly = rlat + lr2 * math.cos(ar) * math.cos(el) * dpl
        ax.text(lx, ly, lbl, color="white", fontsize=10,
                fontweight="bold", ha="center", va="center", zorder=4)

    ax.plot(rlon, rlat, "w^", ms=8, zorder=5)

    cb = plt.colorbar(pcm, ax=ax, fraction=0.03, pad=0.02)
    cb.set_label("径向风速 (m/s)", color="white", fontsize=11)
    cb.ax.yaxis.set_tick_params(color="white")
    plt.setp(cb.ax.yaxis.get_ticklabels(), color="white")

    ax.tick_params(colors="white", labelsize=8)
    for sp in ax.spines.values():
        sp.set_edgecolor("white")
    ax.set_xlabel("经度 (°E)", color="white", fontsize=10)
    ax.set_ylabel("纬度 (°N)", color="white", fontsize=10)
    ax.xaxis.set_major_formatter(mticker.FormatStrFormatter("%.3f°"))
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.3f°"))

    t0 = beams[0]["time"]
    t1 = beams[-1]["time"][-8:]
    ax.set_title(
        f"PPI 径向风速\n"
        f"{t0} ~ {t1}   仰角 {beams[0]['el']}°   "
        f"{len(beams)} 波束   最大距离 {MAX_RANGE_M/1000:.0f} km",
        color="white", fontsize=11, pad=10,
    )
    ax.set_aspect("equal")
    plt.tight_layout()
    plt.savefig(path, dpi=150, bbox_inches="tight",
                facecolor=fig.get_facecolor())
    plt.close()


# ── 结果写出 ──────────────────────────────────────────────────────────────────
def _write_result(output_dir: str, result: dict):
    """将结果写入 output_dir/result.json，供调用方读取。"""
    os.makedirs(output_dir, exist_ok=True)
    with open(os.path.join(output_dir, "result.json"), "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False)


# ── 主处理函数 ────────────────────────────────────────────────────────────────
def process(config_file: str, output_dir: str) -> dict:
    """
    读取 config_file（JSON），处理并输出 PNG + TIF 到 output_dir。
    返回 {"png": <path>, "tif": <path>, "beams": <int>} 或 {"error": <msg>}。
    """
    with open(config_file, encoding="utf-8") as f:
        config = json.load(f)

    files = config.get("files", [])
    if not files:
        return {"error": "config 中未指定 files"}

    # 按顺序（时间从旧到新）读取所有波束
    all_beams = []
    for fp in files:
        if not os.path.exists(fp):
            print(f"[warn] 文件不存在: {fp}", file=sys.stderr)
            continue
        all_beams.extend(parse_file(fp))

    if not all_beams:
        return {"error": "所有文件均无法解析或为空"}

    # 找最新完整圈
    ppi_beams = find_latest_complete_ppi(all_beams)
    if ppi_beams is None:
        return {"error": f"未找到完整 PPI（需要起始 az<{AZ_START_MAX}°、结束 az>{AZ_END_MIN}°、至少 {MIN_BEAMS} 波束）"}

    # 如果最新文件完全未参与该完整圈，说明上次调用时已经生成过，跳过
    latest_file = os.path.abspath(files[-1])
    ppi_files = {os.path.abspath(b["_src"]) for b in ppi_beams}
    if latest_file not in ppi_files:
        print(f"[skip] 最新文件 {os.path.basename(latest_file)} 未参与最新完整圈，跳过出图")
        result = {"skipped": True, "reason": "最新文件未参与最新完整圈，结果已在上次调用时生成"}
        _write_result(output_dir, result)
        return result

    rlon = ppi_beams[0]["lon"]
    rlat = ppi_beams[0]["lat"]
    ext  = MAX_RANGE_M * math.cos(math.radians(ppi_beams[0]["el"]))

    xs, ys, vs = beams_to_cartesian(ppi_beams, MAX_RANGE_M)
    if len(vs) < 20:
        return {"error": f"有效径向风速数据点不足（{len(vs)}）"}

    grid, ge, gn, n = to_grid(xs, ys, vs, ext, GRID_RES_M)

    os.makedirs(output_dir, exist_ok=True)
    ts = ppi_beams[0]["time"].replace(":", "").replace(" ", "_").replace("-", "")
    tif_path = os.path.join(output_dir, f"ppi_{ts}.tif")
    png_path = os.path.join(output_dir, f"ppi_{ts}.png")

    save_geotiff(grid, n, rlon, rlat, ext, tif_path)
    save_png(grid, ge, gn, rlon, rlat, ext, ppi_beams, png_path)

    valid_px = int(np.sum(~np.isnan(grid)))
    print(f"[ok] {len(ppi_beams)} 波束 / {len(vs)} 点 / {valid_px} 有效像素")
    print(f"[ok] PNG: {png_path}")
    print(f"[ok] TIF: {tif_path}")

    result = {"png": png_path, "tif": tif_path, "beams": len(ppi_beams)}
    _write_result(output_dir, result)
    return result


# ── 命令行入口 ─────────────────────────────────────────────────────────────────
def main():
    if len(sys.argv) != 3:
        print("Usage: python3 ppi_worker.py <config_json> <output_dir>")
        sys.exit(1)
    config_file = sys.argv[1]
    output_dir  = sys.argv[2]
    if not os.path.exists(config_file):
        print(f"Error: config not found: {config_file}")
        sys.exit(1)
    result = process(config_file, output_dir)
    if "error" in result:
        _write_result(output_dir, result)
        print(f"Error: {result['error']}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
