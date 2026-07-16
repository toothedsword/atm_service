#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PPI VAD 风场反演 worker
用法: python3 ppi_vad_worker.py <config_json> <output_dir>

在 ppi_worker 的基础上，额外用 VAD 方法反演水平风场 U/V：
  对每个距离库 r，在全方位角上最小二乘拟合
      V_r(φ) = A·sin(φ) + B·cos(φ) + C
  U = A / cos(el)，V = B / cos(el)
将 U(r)/V(r) 投影到 2D 笛卡尔格点，额外输出：
  • wind_speed_*.tif  —— 风速 GeoTIFF
  • wind_uv_*.png     —— 风速填色 + 白色黑边风矢量 PNG

同时保留原有输出（径向风速 ppi_*.tif / ppi_*.png）。
"""

import json
import math
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.patheffects as pe
from matplotlib.font_manager import FontProperties

import numpy as np
import rasterio
from rasterio.transform import from_bounds
from rasterio.crs import CRS

# 复用 ppi_worker 的解析、常量、出图逻辑
import ppi_worker as _pw

# ── 中文字体 ──────────────────────────────────────────────────────────────────
yh_font = FontProperties(fname='/usr/share/fonts/msyh.ttc')

# ── 共用常量别名 ───────────────────────────────────────────────────────────────
RANGE_STEP  = _pw.RANGE_STEP
NUM_GATES   = _pw.NUM_GATES
MAX_RANGE_M = _pw.MAX_RANGE_M
GRID_RES_M  = _pw.GRID_RES_M
EARTH_R     = _pw.EARTH_R
VEL_RANGE   = _pw.VEL_RANGE

# VAD 专用参数
VAD_MIN_AZ_SAMPLES = 12   # 每个距离库最少有效方位角样本数才做拟合


# ── VAD 反演 ──────────────────────────────────────────────────────────────────
def vad_inversion(beams: list, max_r: float):
    """
    对每个距离库做 VAD 拟合，返回 U[ng], V[ng] (m/s)。
    U: 东西分量（正东为正），V: 南北分量（正北为正）。
    无法拟合的库填 NaN。
    """
    el_rad = math.radians(beams[0]["el"])
    cos_el = math.cos(el_rad)
    ng = int(max_r / RANGE_STEP)

    # 收集所有波束的方位角（弧度）和速度矩阵
    azs = np.array([math.radians(b["az"]) for b in beams])
    # vel_mat: shape (n_beams, ng)
    vel_mat = np.vstack([b["vel"][:ng] if len(b["vel"]) >= ng
                         else np.pad(b["vel"], (0, ng - len(b["vel"])),
                                     constant_values=np.nan)
                         for b in beams])

    U = np.full(ng, np.nan)
    V = np.full(ng, np.nan)

    sin_az = np.sin(azs)
    cos_az = np.cos(azs)
    ones   = np.ones_like(azs)
    A_full = np.column_stack([sin_az, cos_az, ones])  # (n_beams, 3)

    for i in range(ng):
        vr = vel_mat[:, i]
        ok = ~np.isnan(vr)
        if ok.sum() < VAD_MIN_AZ_SAMPLES:
            continue
        try:
            coeffs, _, _, _ = np.linalg.lstsq(A_full[ok], vr[ok], rcond=None)
            U[i] = coeffs[0] / cos_el
            V[i] = coeffs[1] / cos_el
        except Exception:
            pass

    return U, V


# ── VAD U/V 投影到 2D 格点 ────────────────────────────────────────────────────
def vad_to_grid(U_r: np.ndarray, V_r: np.ndarray, ext: float, res: float):
    """
    将 VAD 给出的 U(r)/V(r) 插值到均匀笛卡尔格点。
    返回 U_grid, V_grid, speed_grid, ge, gn, n。
    """
    n  = int(2 * ext / res) + 1
    ge = np.linspace(-ext, ext, n)
    gn = np.linspace(-ext, ext, n)
    GE, GN = np.meshgrid(ge, gn)

    R = np.sqrt(GE**2 + GN**2)                     # 每格点到雷达距离（米）
    idx = np.round(R / RANGE_STEP).astype(int) - 1  # 对应距离库索引
    ng = len(U_r)
    idx_clip = np.clip(idx, 0, ng - 1)

    U_grid = U_r[idx_clip]
    V_grid = V_r[idx_clip]

    # 超出探测范围或对应库无效的点设 NaN
    out_of_range = R > ext
    U_grid[out_of_range] = np.nan
    V_grid[out_of_range] = np.nan
    U_grid[np.isnan(U_r)[idx_clip]] = np.nan
    V_grid[np.isnan(V_r)[idx_clip]] = np.nan

    speed = np.sqrt(U_grid**2 + V_grid**2)
    return U_grid, V_grid, speed, ge, gn, n


# ── 风速 GeoTIFF ──────────────────────────────────────────────────────────────
def save_speed_geotiff(speed, n, rlon, rlat, ext, path):
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
        dst.write(speed[::-1, :].astype(np.float32), 1)


# ── 风矢量 PNG ────────────────────────────────────────────────────────────────
def save_wind_png(U_grid, V_grid, speed, ge, gn, rlon, rlat, ext, beams, path):
    lr  = math.radians(rlat)
    el  = math.radians(beams[0]["el"])
    dpl = 1.0 / (EARTH_R * math.pi / 180.0)
    dpo = 1.0 / (EARTH_R * math.cos(lr) * math.pi / 180.0)
    LON, LAT = np.meshgrid(rlon + ge * dpo, rlat + gn * dpl)

    fig, ax = plt.subplots(figsize=(9, 9))
    fig.patch.set_facecolor("#1a1a2e")
    ax.set_facecolor("#1a1a2e")

    # 风速填色（jet 色表）
    vmax = np.nanpercentile(speed, 98) if np.any(~np.isnan(speed)) else 20.0
    vmax = max(vmax, 1.0)
    pcm = ax.pcolormesh(LON, LAT, speed, cmap="jet",
                        vmin=0, vmax=vmax,
                        shading="auto", zorder=1)

    # 风矢量（稀疏采样，白色修长箭头+黑色描边）
    n = len(ge)
    step = max(1, n // 20)          # 约 20×20 个箭头
    sl = slice(None, None, step)
    LON_q = LON[sl, sl]
    LAT_q = LAT[sl, sl]
    U_q   = U_grid[sl, sl]
    V_q   = V_grid[sl, sl]
    valid = ~(np.isnan(U_q) | np.isnan(V_q))
    if np.any(valid):
        q = ax.quiver(
            LON_q[valid], LAT_q[valid],
            U_q[valid],   V_q[valid],
            color="white",
            scale=150,              # scale 越大箭头越短
            width=0.0018,
            headwidth=3,
            headlength=6,
            headaxislength=5,
            zorder=5,
        )
        if False:
            q.set_path_effects([
                pe.Stroke(linewidth=2.0, foreground="black"),
                pe.Normal(),
        ])
        # 图例箭头
        ref_spd = max(round(vmax * 0.4), 1)
        ax.quiverkey(q, 0.88, 0.05, ref_spd,
                     f"{ref_spd} m/s",
                     labelpos="E", color="white",
                     labelcolor="white", fontproperties={"size": 9},
                     zorder=6)

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
    cb.set_label("风速 (m/s)", color="white", fontsize=11,
                 fontproperties=yh_font)
    cb.ax.yaxis.set_tick_params(color="white")
    plt.setp(cb.ax.yaxis.get_ticklabels(), color="white")

    ax.tick_params(colors="white", labelsize=8)
    for sp in ax.spines.values():
        sp.set_edgecolor("white")
    ax.set_xlabel("经度 (°E)", color="white", fontsize=10,
                  fontproperties=yh_font)
    ax.set_ylabel("纬度 (°N)", color="white", fontsize=10,
                  fontproperties=yh_font)
    ax.xaxis.set_major_formatter(mticker.FormatStrFormatter("%.3f°"))
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.3f°"))

    t0 = beams[0]["time"]
    t1 = beams[-1]["time"][-8:]
    ax.set_title(
        f"VAD 反演水平风场\n"
        f"{t0} ~ {t1}   仰角 {beams[0]['el']}°   {len(beams)} 波束",
        color="white", fontsize=11, pad=10,
        fontproperties=yh_font,
    )
    ax.set_aspect(dpo / dpl)   # 1/cos(lat)，使物理圆在屏幕上仍为圆
    plt.tight_layout()
    plt.savefig(path, dpi=150, bbox_inches="tight",
                facecolor=fig.get_facecolor())
    plt.close()


# ── 主处理函数 ────────────────────────────────────────────────────────────────
def process(config_file: str, output_dir: str) -> dict:
    """
    在 ppi_worker.process 输出（径向速度 PNG + TIF）基础上，
    额外生成 VAD 风速 TIF 和风矢量 PNG。
    """
    # ① 先跑原有径向速度出图流程
    ppi_result = _pw.process(config_file, output_dir)
    if "error" in ppi_result:
        return ppi_result
    if ppi_result.get("skipped"):
        return ppi_result

    # ② 重新解析 config，拿到文件列表和已识别的完整圈波束
    with open(config_file, encoding="utf-8") as f:
        config = json.load(f)
    files = config.get("files", [])

    all_beams = []
    for fp in files:
        if os.path.exists(fp):
            all_beams.extend(_pw.parse_file(fp))

    ppi_beams = _pw.find_latest_complete_ppi(all_beams)
    if ppi_beams is None:
        return {**ppi_result, "vad_error": "无法重新定位完整 PPI 波束"}

    # ③ VAD 反演
    rlon = ppi_beams[0]["lon"]
    rlat = ppi_beams[0]["lat"]
    ext  = MAX_RANGE_M * math.cos(math.radians(ppi_beams[0]["el"]))

    U_r, V_r = vad_inversion(ppi_beams, MAX_RANGE_M)
    valid_gates = int(np.sum(~np.isnan(U_r)))
    print(f"[vad] {valid_gates}/{int(MAX_RANGE_M/RANGE_STEP)} 个距离库拟合成功")

    U_grid, V_grid, speed, ge, gn, n = vad_to_grid(U_r, V_r, ext, GRID_RES_M)

    # ④ 输出文件
    ts = ppi_beams[0]["time"].replace(":", "").replace(" ", "_").replace("-", "")
    tif_path = os.path.join(output_dir, f"wind_speed_{ts}.tif")
    png_path = os.path.join(output_dir, f"wind_uv_{ts}.png")

    save_speed_geotiff(speed, n, rlon, rlat, ext, tif_path)
    save_wind_png(U_grid, V_grid, speed, ge, gn, rlon, rlat, ext, ppi_beams, png_path)

    print(f"[vad] TIF: {tif_path}")
    print(f"[vad] PNG: {png_path}")

    result = {
        **ppi_result,
        "wind_speed_tif": tif_path,
        "wind_uv_png":    png_path,
        "vad_gates":      valid_gates,
    }
    _pw._write_result(output_dir, result)
    return result


# ── 命令行入口 ─────────────────────────────────────────────────────────────────
def main():
    if len(sys.argv) != 3:
        print("Usage: python3 ppi_vad_worker.py <config_json> <output_dir>")
        sys.exit(1)
    config_file = sys.argv[1]
    output_dir  = sys.argv[2]
    if not os.path.exists(config_file):
        print(f"Error: config not found: {config_file}")
        sys.exit(1)
    result = process(config_file, output_dir)
    if "error" in result:
        print(f"Error: {result['error']}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
