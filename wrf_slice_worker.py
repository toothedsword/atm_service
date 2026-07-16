#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
wrf_slice_worker.py - WRF 模式数据剖面图绘制工作进程

用法:
    python3 wrf_slice_worker.py <config_json> <output_dir>

config_json 字段:
    waypoints       list  航迹控制点，每个点含 lon/lat/time，例如：
                          [{"lon":100,"lat":30,"time":"202605060000"}, ...]
                          time 格式：yyyymmddhh 或 yyyymmddhhmm
                          提供 waypoints 时自动做时空联合插值；
                          也可只提供 lons/lats（无时间，用 time_idx）
    lons            list  剖面经度控制点列表（waypoints 的替代写法）
    lats            list  剖面纬度控制点列表（waypoints 的替代写法）
    time_idx        int   时间索引（仅在无 waypoints 时生效，默认 0）
    base_time       str   WRF 数据起始时间 yyyymmddhh（timeList 为整数时必填）
    time_step_hours float WRF 数据时间间隔小时数（timeList 为整数时必填，默认 1）
    files           list  变量文件路径列表（自动从文件名识别变量名）
    data_dir        str   WRF 数据目录（files 未覆盖的变量从此目录查找）
    flight_height_km float 航线高度(km)（默认 2.5）
    nx_points       int   剖面插值点数（默认 200）
    max_height_km   float 纵轴最大高度 km（默认 6.0）
    label           str   标签（默认 WRF）
    plot_types      list  绘图类型：rh / ws / dzdt / cf（默认全部）
"""

import json
import os
import re
import struct
import sys
import zipfile
import glob

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import colors
from matplotlib.font_manager import FontProperties
from scipy.interpolate import RegularGridInterpolator, interp1d

# ── 字体 ──────────────────────────────────────────────────────────────────────
def _load_font():
    for path in ('/usr/share/fonts/msyh.ttc', './msyh.ttc',
                 os.path.join(os.path.dirname(__file__), 'msyh.ttc')):
        if os.path.exists(path):
            return FontProperties(fname=path)
    return FontProperties()

yh_font = _load_font()

# ── 地形文件路径 ───────────────────────────────────────────────────────────────
_TOPO_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          'topo', 'topo_0~180.nc')

# ── 自定义 jet 色表 ────────────────────────────────────────────────────────────
def _make_jet():
    rgb = ((0.8,0.8,1),(0,0,1),(0,1,1),(1,1,0),(1,0,0),(1,0,1),(0.4,0,0.4))
    ns  = [30, 40, 40, 40, 40, 30]
    ccc = np.zeros((sum(ns), 3))
    i0  = 0
    for i, n in enumerate(ns):
        i1 = i0 + n
        for j in range(3):
            if n > 1:
                ccc[i0:i1, j] = np.linspace(rgb[i][j], rgb[i+1][j], n)
        i0 = i1
    return colors.ListedColormap(ccc, name='ccc_jet')

CCC_JET = _make_jet()


# ═══════════════════════════════════════════════════════════════════════════════
#  数据读取
# ═══════════════════════════════════════════════════════════════════════════════

def read_wrf_file(path):
    """
    读取 WRF dat 或 zip 文件。
    返回: (header, data[times,levels,lat,lon], lon, lat, lev_hpa)
    """
    if path.endswith('.zip'):
        with zipfile.ZipFile(path) as z:
            raw = z.read('data.bin')
    else:
        with open(path, 'rb') as f:
            raw = f.read()

    hlen   = struct.unpack('<I', raw[:4])[0]
    header = json.loads(raw[4:4+hlen])

    arr = np.frombuffer(raw[4+hlen:], dtype=np.float32).copy()
    nt, nz, ny, nx = (header['times'], header['levels'],
                      header['ySize'],  header['xSize'])
    arr = arr.reshape(nt, nz, ny, nx)

    # 缺测替换
    undef = float(header.get('undef', 999999))
    arr[np.abs(arr - undef) < 1] = np.nan

    lon = np.linspace(header['xStart'], header['xEnd'], nx)
    lat = np.linspace(header['yStart'], header['yEnd'], ny)
    lev = np.array(header['levelList'], dtype=float)

    return header, arr, lon, lat, lev


def find_var_file(data_dir, varname):
    """在 data_dir 中寻找变量文件，zip 优先于 dat。"""
    # zip 优先
    zip_candidates = [os.path.join(data_dir, f'WRF_REAL_{varname}_test.zip')]
    zip_candidates += glob.glob(os.path.join(data_dir, f'*_{varname}_all_*.zip'))
    zip_candidates += glob.glob(os.path.join(data_dir, f'*_{varname}_single_*.zip'))

    for p in zip_candidates:
        if os.path.exists(p):
            return p

    # 回退到 dat
    dat_candidates = [os.path.join(data_dir, f'WRF_REAL_{varname}_test.dat')]
    dat_candidates += glob.glob(os.path.join(data_dir, f'*_{varname}_all_*.dat'))
    dat_candidates += glob.glob(os.path.join(data_dir, f'*_{varname}_single_*.dat'))

    for p in dat_candidates:
        if os.path.exists(p):
            return p

    return None


def load_topo():
    """加载地形数据，返回 (lon_arr, lat_arr, topo_arr[lon,lat])。"""
    import netCDF4 as nc4
    with nc4.Dataset(_TOPO_FILE) as f:
        topo = f.variables['topo'][:]
    # dim0=lon(0-180°), dim1=lat(-90 to 90°)
    lon_topo = np.linspace(0,  180, topo.shape[0])
    lat_topo = np.linspace(-90, 90, topo.shape[1])
    return lon_topo, lat_topo, topo


# ═══════════════════════════════════════════════════════════════════════════════
#  时间工具
# ═══════════════════════════════════════════════════════════════════════════════

from datetime import datetime, timedelta

def parse_dt(s):
    """解析时间字符串为 datetime，支持 yyyymmddhh / yyyymmddhhmm。"""
    s = str(s).strip()
    for fmt in ('%Y%m%d%H%M', '%Y%m%d%H'):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    raise ValueError(f'无法解析时间: {s!r}')


def parse_wrf_times(time_list, base_time=None, step_hours=1.0):
    """
    将 WRF header 里的 timeList 转为 datetime 列表。
    - 若元素是 yyyymmddhh(mm) 字符串，直接解析。
    - 若是整数/纯数字，当作从 base_time 开始的小时偏移。
    """
    result = []
    for t in time_list:
        s = str(t).strip()
        if re.match(r'^\d{10,12}$', s):
            result.append(parse_dt(s))
        else:
            # 纯数字索引，转为小时偏移
            if base_time is None:
                raise ValueError(
                    'timeList 为整数索引，请在 config 中提供 base_time (yyyymmddhh)')
            result.append(base_time + timedelta(hours=float(s) * step_hours))
    return result


# ═══════════════════════════════════════════════════════════════════════════════
#  剖面生成
# ═══════════════════════════════════════════════════════════════════════════════

def p2h(p):
    """气压(hPa) → 标准大气高度(km)。"""
    return -8.5 * np.log(np.asarray(p, dtype=float) / 1013.0)


def latlon2dis(lat1, lon1, lat2, lon2):
    R = 6371.0
    pi = np.pi / 180
    dlat, dlon = (lat2-lat1)*pi, (lon2-lon1)*pi
    a = np.sin(dlat/2)**2 + np.cos(lat1*pi)*np.cos(lat2*pi)*np.sin(dlon/2)**2
    return R * 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))


def gen_track(lons, lats, nx=200):
    """生成沿控制点的剖面航迹坐标及累积距离。"""
    lons, lats = np.array(lons), np.array(lats)
    segs = [latlon2dis(lats[i], lons[i], lats[i+1], lons[i+1])
            for i in range(len(lons)-1)]
    total = sum(segs)
    pts_per_seg = [max(2, int(nx * s / total)) for s in segs]

    x, y = np.array([]), np.array([])
    for i in range(len(lons)-1):
        xi = np.linspace(lons[i], lons[i+1], pts_per_seg[i])
        yi = np.linspace(lats[i], lats[i+1], pts_per_seg[i])
        if i > 0:
            xi, yi = xi[1:], yi[1:]
        x, y = np.append(x, xi), np.append(y, yi)

    dis = np.zeros(len(x))
    for i in range(1, len(x)):
        dis[i] = dis[i-1] + latlon2dis(y[i-1], x[i-1], y[i], x[i])

    return x, y, dis


def gen_track_times(track_dis, wp_lons, wp_lats, wp_times):
    """
    沿航迹按累积距离线性插值时间。
    wp_times: 各控制点的 datetime 列表（与 wp_lons/wp_lats 一一对应）。
    返回每个插值点的 datetime 列表。
    """
    # 控制点对应的累积距离
    n_wp = len(wp_lons)
    wp_dis = [0.0]
    for i in range(1, n_wp):
        wp_dis.append(wp_dis[-1] + latlon2dis(wp_lats[i-1], wp_lons[i-1],
                                               wp_lats[i],   wp_lons[i]))
    wp_dis = np.array(wp_dis)

    # 将时间转为秒偏移，然后插值
    t0 = wp_times[0]
    wp_sec = np.array([(t - t0).total_seconds() for t in wp_times])
    track_sec = np.interp(track_dis, wp_dis, wp_sec)
    return [t0 + timedelta(seconds=float(s)) for s in track_sec]


def interp_profile(var4d, lon_data, lat_data, lev_hpa,
                   track_lons, track_lats, z_km_out, t_idx=0):
    """
    将 4D 变量插值到剖面上。
    返回 data_2d[len(z_km_out), len(track_lons)]
    """
    data3d = var4d[t_idx]                         # (nz, ny, nx)
    lev_km = p2h(lev_hpa)                         # 各压力层对应高度(km)

    # 按高度升序排列（从低到高）
    sort_idx = np.argsort(lev_km)
    lev_km_s = lev_km[sort_idx]
    data3d_s = data3d[sort_idx]                   # (nz, ny, nx) 高→低排列变低→高

    # 3D 插值器：(lat, lon, lev_km)
    f3d = RegularGridInterpolator(
        (lat_data, lon_data, lev_km_s),
        data3d_s.transpose(1, 2, 0),              # (ny, nx, nz)
        method='linear', bounds_error=False, fill_value=np.nan)

    nz_out = len(z_km_out)
    nx_out = len(track_lons)
    out = np.full((nz_out, nx_out), np.nan, dtype=np.float32)

    for i, (tx, ty) in enumerate(zip(track_lons, track_lats)):
        pts = np.column_stack([
            np.full(nz_out, ty),
            np.full(nz_out, tx),
            z_km_out
        ])
        out[:, i] = f3d(pts)

    return out


def interp_profile_with_time(var4d, lon_data, lat_data, lev_hpa, wrf_times,
                              track_lons, track_lats, track_times, z_km_out):
    """
    时空联合插值：4D(time,lev,lat,lon) → 2D(z,x)。
    每个航迹点有独立时间，先空间插值相邻两个时次，再线性加权。
    """
    # WRF 时间轴（秒）
    t0 = wrf_times[0]
    wrf_sec = np.array([(t - t0).total_seconds() for t in wrf_times])
    track_sec = np.array([(t - t0).total_seconds() for t in track_times])

    # 各航迹点的时间分数索引
    track_sec_c = np.clip(track_sec, wrf_sec[0], wrf_sec[-1])
    t_frac = np.interp(track_sec_c, wrf_sec, np.arange(len(wrf_times)))
    t_lo_arr = np.clip(np.floor(t_frac).astype(int), 0, len(wrf_times) - 2)
    t_hi_arr = t_lo_arr + 1
    alpha_arr = (t_frac - t_lo_arr).astype(np.float32)

    # 只计算实际用到的时次（避免重复计算）
    needed_t = sorted(set(t_lo_arr.tolist() + t_hi_arr.tolist()))
    profiles = {}
    for ti in needed_t:
        profiles[ti] = interp_profile(var4d, lon_data, lat_data, lev_hpa,
                                       track_lons, track_lats, z_km_out, t_idx=ti)

    nz = len(z_km_out)
    nx = len(track_lons)
    out = np.full((nz, nx), np.nan, dtype=np.float32)
    for i in range(nx):
        lo = profiles[t_lo_arr[i]][:, i]
        hi = profiles[t_hi_arr[i]][:, i]
        a  = alpha_arr[i]
        valid = ~np.isnan(lo) & ~np.isnan(hi)
        out[valid, i] = (1 - a) * lo[valid] + a * hi[valid]
        only_lo = ~np.isnan(lo) & np.isnan(hi)
        only_hi = np.isnan(lo) & ~np.isnan(hi)
        out[only_lo, i] = lo[only_lo]
        out[only_hi, i] = hi[only_hi]
    return out


def interp_2d_with_time(arr4d, lon_data, lat_data, wrf_times,
                         track_lons, track_lats, track_times):
    """单层 2D 字段的时空插值，返回 (n_track,)。"""
    t0 = wrf_times[0]
    wrf_sec = np.array([(t - t0).total_seconds() for t in wrf_times])
    track_sec = np.clip(
        np.array([(t - t0).total_seconds() for t in track_times]),
        wrf_sec[0], wrf_sec[-1])
    t_frac = np.interp(track_sec, wrf_sec, np.arange(len(wrf_times)))
    t_lo_arr = np.clip(np.floor(t_frac).astype(int), 0, len(wrf_times) - 2)
    t_hi_arr = t_lo_arr + 1
    alpha_arr = (t_frac - t_lo_arr).astype(np.float32)

    pts = np.column_stack([track_lats, track_lons])
    needed_t = sorted(set(t_lo_arr.tolist() + t_hi_arr.tolist()))
    slices = {}
    for ti in needed_t:
        f2d = RegularGridInterpolator(
            (lat_data, lon_data), arr4d[ti, 0],
            method='linear', bounds_error=False, fill_value=np.nan)
        slices[ti] = f2d(pts)

    out = np.full(len(track_lons), np.nan, dtype=np.float32)
    for i in range(len(track_lons)):
        lo, hi, a = slices[t_lo_arr[i]][i], slices[t_hi_arr[i]][i], alpha_arr[i]
        if not np.isnan(lo) and not np.isnan(hi):
            out[i] = (1 - a) * lo + a * hi
        elif not np.isnan(lo):
            out[i] = lo
        elif not np.isnan(hi):
            out[i] = hi
    return out


def get_terrain(track_lons, track_lats, lon_topo, lat_topo, topo):
    """沿航迹插值地形高度(m)。"""
    f = RegularGridInterpolator(
        (lon_topo, lat_topo), topo,
        method='linear', bounds_error=False, fill_value=0.0)
    pts = np.column_stack([track_lons, track_lats])
    return f(pts)


# ═══════════════════════════════════════════════════════════════════════════════
#  绘图
# ═══════════════════════════════════════════════════════════════════════════════

def plot_cross_section(cfg, lv_vars, track_x, track_y, track_dis,
                       terrain_km, z_km, plot_type, output_path):
    """
    绘制单张剖面图。

    lv_vars  : dict of 2D arrays [nz, nx], 已插值到 (z_km, track) 网格
    track_x  : 经度数组
    track_y  : 纬度数组
    terrain_km: 地形高度(km)
    z_km     : 高度网格(km)
    plot_type: 'rh' / 'ws' / 'dzdt' / 'cf'
    """
    fz = (20, 5)
    axpos = [0.05, 0.10, 0.88, 0.84]

    fig = plt.figure(figsize=fz, dpi=100)
    ax  = fig.add_axes(axpos)

    # X 轴用经度
    xarr = track_x
    XX, ZZ = np.meshgrid(xarr, z_km)

    ylim = [0, z_km.max()]
    ax.set_ylim(ylim)
    ax.set_xlim([xarr.min(), xarr.max()])

    # ── 温度填色 ────────────────────────────────────────────────────────────
    if 't' in lv_vars:
        t2d = lv_vars['t']
        vmin, vmax = np.nanpercentile(t2d, 1), np.nanpercentile(t2d, 99)
        levels_t = np.linspace(vmin, vmax, 200)
        cf = ax.contourf(XX, ZZ, t2d, levels=levels_t,
                         cmap=CCC_JET, vmin=vmin, vmax=vmax, extend='both')
        cax = fig.add_axes([0.94, 0.10, 0.012, 0.84])
        cbar = plt.colorbar(cf, cax=cax)
        cbar.set_label('温度(℃)', fontproperties=yh_font, fontsize=11)

    # ── 相对湿度等值线 ──────────────────────────────────────────────────────
    if plot_type == 'rh' and 'rh' in lv_vars:
        try:
            ct = ax.contour(XX, ZZ, lv_vars['rh'], [25, 50],
                            colors='k', linestyles='--', linewidths=0.6)
            ax.clabel(ct, fmt='%g', fontsize=7)
            ct2 = ax.contour(XX, ZZ, lv_vars['rh'], [75, 85],
                             colors='k', linestyles='-', linewidths=0.6)
            ax.clabel(ct2, fmt='%g', fontsize=7)
        except Exception as e:
            print(f'[RH contour] {e}')

    # ── 风切变等值线 ────────────────────────────────────────────────────────
    if plot_type == 'ws' and 'u' in lv_vars and 'v' in lv_vars:
        u2d, v2d = lv_vars['u'], lv_vars['v']
        du = np.diff(u2d, axis=0)
        dv = np.diff(v2d, axis=0)
        ws = np.sqrt(du**2 + dv**2)
        ws_full = np.full_like(u2d, np.nan)
        ws_full[1:, :] = ws
        try:
            ct = ax.contour(XX, ZZ, ws_full,
                            levels=np.arange(1, 12, 1),
                            colors='k', linestyles='-', linewidths=0.5)
            ax.clabel(ct, fmt='%g', fontsize=7)
        except Exception as e:
            print(f'[WS contour] {e}')

    # ── 垂直速度等值线 ──────────────────────────────────────────────────────
    if plot_type == 'dzdt' and 'dzdt' in lv_vars:
        dz = lv_vars['dzdt']
        vabs = max(abs(np.nanpercentile(dz, 2)), abs(np.nanpercentile(dz, 98)), 0.1)
        levs = np.linspace(-vabs, vabs, 20)
        levs = levs[levs != 0]
        try:
            ct = ax.contour(XX, ZZ, dz, levels=levs,
                            colors='k', linewidths=0.5)
            ax.clabel(ct, fmt='%.2g', fontsize=7)
        except Exception as e:
            print(f'[dzdt contour] {e}')

    # ── 液态/冰水含量等值线 ──────────────────────────────────────────────────
    if plot_type == 'cf':
        if 'cf' in lv_vars:
            cf_d = lv_vars['cf']
            vmax_cf = np.nanpercentile(cf_d[cf_d > 0], 95) if np.any(cf_d > 0) else 1
            try:
                ct = ax.contour(XX, ZZ, cf_d,
                                levels=np.linspace(0.01*vmax_cf, vmax_cf, 8),
                                colors='k', linestyles='-', linewidths=0.6)
                ax.clabel(ct, fmt='%.1g', fontsize=7)
            except Exception as e:
                print(f'[CF contour] {e}')
        if 'dcf' in lv_vars:
            dcf_d = lv_vars['dcf']
            vmax_d = np.nanpercentile(dcf_d[dcf_d > 0], 95) if np.any(dcf_d > 0) else 1
            try:
                ct2 = ax.contour(XX, ZZ, dcf_d,
                                 levels=np.linspace(0.01*vmax_d, vmax_d, 8),
                                 colors='k', linestyles='--', linewidths=0.6)
                ax.clabel(ct2, fmt='%.1g', fontsize=7)
            except Exception as e:
                print(f'[DCF contour] {e}')

    # ── 水平风向杆 ─────────────────────────────────────────────────────────
    if 'u' in lv_vars and 'v' in lv_vars:
        skip_z = max(1, len(z_km) // 10)
        skip_x = max(1, len(xarr) // 20)
        ax.barbs(XX[::skip_z, ::skip_x], ZZ[::skip_z, ::skip_x],
                 lv_vars['u'][::skip_z, ::skip_x],
                 lv_vars['v'][::skip_z, ::skip_x],
                 length=7, linewidth=0.8, pivot='middle',
                 barb_increments=dict(half=2, full=4, flag=20))

    # ── 地形填色 ────────────────────────────────────────────────────────────
    topo_x = np.append(xarr, [xarr[-1], xarr[0]])
    topo_y = np.append(terrain_km, [0, 0])
    ax.fill(topo_x, topo_y, color=[0.5, 0.5, 0.5], zorder=10)
    ax.plot(xarr, terrain_km, 'k-', linewidth=0.8, zorder=11)

    # ── 航线 ─────────────────────────────────────────────────────────────────
    flight_km = float(cfg.get('flight_height_km', 2.5))
    n = len(xarr)
    ramp = int(n * 0.12)
    h_track = np.full(n, flight_km)
    h_track[:ramp]  = np.linspace(terrain_km[0],   flight_km, ramp)
    h_track[-ramp:] = np.linspace(flight_km, terrain_km[-1], ramp)
    h_track = np.maximum(h_track, terrain_km + 0.05)
    ax.plot(xarr, h_track, 'k-', linewidth=2, zorder=12)

    # 航向箭头（叠加在轨迹上，箭头方向为水平方向）
    ax1 = fig.add_axes(axpos)
    ax1.set_axis_off()
    ax1.set_xlim([0, fz[0]*axpos[2]])
    ax1.set_ylim([0, fz[1]*axpos[3]])
    step = max(1, n//20)
    ux = np.diff(track_x, append=track_x[-1]) * 13
    uy = np.diff(track_y, append=track_y[-1]) * 13
    xa = (xarr[::step] - xarr.min()) / (xarr.max() - xarr.min()) * fz[0]*axpos[2]
    ya = h_track[::step] / ylim[1] * fz[1]*axpos[3]
    ax1.quiver(xa, ya, ux[::step], uy[::step],
               angles='xy', scale_units='xy', scale=1,
               width=0.002, color='white', zorder=13)

    # ── 坐标轴标签 ──────────────────────────────────────────────────────────
    ax.set_xlabel('经度（度）', fontproperties=yh_font, fontsize=12)
    ax.set_ylabel('高度（公里）', fontproperties=yh_font, fontsize=12)
    ax.grid(True, alpha=0.25, linestyle='--', linewidth=0.5)

    _TITLES = {
        'rh':   '温度+航线+风向杆+相对湿度',
        'ws':   '温度+航线+风向杆+风切变',
        'dzdt': '温度+航线+风向杆+垂直速度',
        'cf':   '温度+航线+风向杆+液态水(实线)+冰水(虚线)',
    }
    ax.set_title(_TITLES.get(plot_type, plot_type),
                 fontproperties=yh_font, fontsize=12)

    fig.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f'Saved: {output_path}')
    return output_path


# ═══════════════════════════════════════════════════════════════════════════════
#  主流程
# ═══════════════════════════════════════════════════════════════════════════════

def process(config_file, output_dir):
    with open(config_file, encoding='utf-8') as f:
        cfg = json.load(f)

    nx_pts     = int(cfg.get('nx_points', 200))
    label      = cfg.get('label', 'WRF')
    plot_types = cfg.get('plot_types', ['rh', 'ws', 'dzdt', 'cf'])

    # ── 航迹控制点（支持带时间的 waypoints 或纯坐标的 lons/lats）──────────────
    if 'waypoints' in cfg:
        wps = cfg['waypoints']
        lons     = [w['lon']  for w in wps]
        lats     = [w['lat']  for w in wps]
        wp_times = [parse_dt(w['time']) for w in wps]
        use_time_interp = True
    else:
        lons     = cfg['lons']
        lats     = cfg['lats']
        wp_times = None
        use_time_interp = False

    t_idx = int(cfg.get('time_idx', 0))  # 仅 use_time_interp=False 时用

    # ── 文件映射 ─────────────────────────────────────────────────────────────
    raw_files = cfg.get('files', {})
    if isinstance(raw_files, list):
        data_files = {}
        for fp in raw_files:
            m = re.search(r'_([a-zA-Z0-9]+)_(all|single)_', os.path.basename(fp))
            if m:
                data_files[m.group(1)] = fp
    else:
        data_files = raw_files

    data_dir = cfg.get('data_dir')

    def find_file(vname):
        if vname in data_files:
            return data_files[vname]
        if data_dir:
            return find_var_file(data_dir, vname)
        return None

    print(f'Track: {list(zip(lons, lats))}')

    # ── 生成航迹 ─────────────────────────────────────────────────────────────
    track_x, track_y, track_dis = gen_track(lons, lats, nx_pts)
    n = len(track_x)

    # 按距离插值出每个航迹点的时间
    if use_time_interp:
        track_times = gen_track_times(track_dis, lons, lats, wp_times)

    # ── 高度网格（0 ~ max_km km，50 层）────────────────────────────────────
    max_z_km = float(cfg.get('max_height_km', 6.0))
    z_km = np.linspace(0, max_z_km, 50)

    # ── 地形 ─────────────────────────────────────────────────────────────────
    lon_topo, lat_topo, topo_arr = load_topo()
    terrain_m  = get_terrain(track_x, track_y, lon_topo, lat_topo, topo_arr)
    terrain_km = terrain_m / 1000.0

    # ── 读取所需变量 ──────────────────────────────────────────────────────────
    required_by_type = {
        'rh':   ['t', 'rh', 'u', 'v'],
        'ws':   ['t', 'u', 'v'],
        'dzdt': ['t', 'dzdt', 'u', 'v'],
        'cf':   ['t', 'u', 'v'],
    }
    needed = set()
    for pt in plot_types:
        needed |= set(required_by_type.get(pt, ['t']))

    # cf 变量需单独处理（多层拼合）
    if 'cf' in plot_types:
        needed -= {'cf', 'dcf'}

    # base_time / time_step 用于 timeList 是整数索引的情况
    base_time_cfg  = parse_dt(cfg['base_time']) if 'base_time' in cfg else None
    time_step_hrs  = float(cfg.get('time_step_hours', 1.0))

    raw_vars = {}   # varname -> (arr4d, lon, lat, lev, wrf_times_or_None)
    for vname in needed:
        fp = find_file(vname)
        if fp is None:
            print(f'Warning: variable {vname} not found')
            continue
        print(f'Reading {vname}: {fp}')
        hdr, arr4d, lon_d, lat_d, lev_d = read_wrf_file(fp)
        wrf_t = (parse_wrf_times(hdr['timeList'], base_time_cfg, time_step_hrs)
                 if use_time_interp else None)
        raw_vars[vname] = (arr4d, lon_d, lat_d, lev_d, wrf_t)

    # ── 插值到剖面网格 ────────────────────────────────────────────────────────
    lv_vars_cache = {}

    def get_lv(vname):
        if vname in lv_vars_cache:
            return lv_vars_cache[vname]
        if vname not in raw_vars:
            return None
        arr4d, lon_d, lat_d, lev_d, wrf_t = raw_vars[vname]
        print(f'Interpolating {vname}...')
        if use_time_interp:
            result = interp_profile_with_time(arr4d, lon_d, lat_d, lev_d, wrf_t,
                                               track_x, track_y, track_times, z_km)
        else:
            ti = min(t_idx, arr4d.shape[0] - 1)
            result = interp_profile(arr4d, lon_d, lat_d, lev_d,
                                    track_x, track_y, z_km, ti)
        lv_vars_cache[vname] = result
        return result

    # ── 逐类型绘图 ────────────────────────────────────────────────────────────
    os.makedirs(output_dir, exist_ok=True)
    output_files = []

    for pt in plot_types:
        lv = {}
        for vname in required_by_type.get(pt, []):
            arr = get_lv(vname)
            if arr is not None:
                lv[vname] = arr

        # cf/dcf：把 cf1+cf2+cf3 在高度维拼合成单一 2D 场
        if pt == 'cf':
            cf_parts, dcf_parts = [], []
            # 假设 cf1 对应低层 (1-3km)，cf2 中层 (3-5km)，cf3 高层 (5+km)
            cf_height_ranges  = [('cf1',  0.0, 3.0), ('cf2',  3.0, 5.5), ('cf3',  5.5, max_z_km)]
            dcf_height_ranges = [('dcf1', 0.0, 3.0), ('dcf2', 3.0, 5.5), ('dcf3', 5.5, max_z_km)]

            def _merge_single_level(ranges):
                merged = np.full((len(z_km), n), np.nan)
                for (vname, z_lo, z_hi) in ranges:
                    fp = find_file(vname)
                    if fp is None:
                        continue
                    hdr_s, arr4d, lon_d, lat_d, _ = read_wrf_file(fp)
                    wrf_t = (parse_wrf_times(hdr_s['timeList'], base_time_cfg, time_step_hrs)
                             if use_time_interp else None)
                    if use_time_interp:
                        track_vals = interp_2d_with_time(
                            arr4d, lon_d, lat_d, wrf_t,
                            track_x, track_y, track_times)
                    else:
                        ti = min(t_idx, arr4d.shape[0] - 1)
                        f2d = RegularGridInterpolator(
                            (lat_d, lon_d), arr4d[ti, 0],
                            method='linear', bounds_error=False, fill_value=np.nan)
                        track_vals = f2d(np.column_stack([track_y, track_x]))
                    mask = (z_km >= z_lo) & (z_km < z_hi)
                    merged[mask, :] = track_vals[np.newaxis, :]
                return merged

            cf_2d  = _merge_single_level(cf_height_ranges)
            dcf_2d = _merge_single_level(dcf_height_ranges)
            if not np.all(np.isnan(cf_2d)):
                lv['cf'] = cf_2d
            if not np.all(np.isnan(dcf_2d)):
                lv['dcf'] = dcf_2d

        outfile = os.path.join(output_dir, f'wrf_slice_{label}_{pt}.png')
        try:
            plot_cross_section(cfg, lv, track_x, track_y, track_dis,
                               terrain_km, z_km, pt, outfile)
            output_files.append(outfile)
        except Exception as e:
            import traceback
            print(f'Error plotting {pt}: {e}')
            traceback.print_exc()

    # 写 result.json
    result = {'output_files': [os.path.basename(f) for f in output_files]}
    with open(os.path.join(output_dir, 'result.json'), 'w') as f:
        json.dump(result, f, ensure_ascii=False)

    return output_files


# ═══════════════════════════════════════════════════════════════════════════════
#  命令行入口
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    if len(sys.argv) != 3:
        print('Usage: python3 wrf_slice_worker.py <config.json> <output_dir>')
        sys.exit(1)

    config_file, output_dir = sys.argv[1], sys.argv[2]

    if not os.path.exists(config_file):
        print(f'Error: config file not found: {config_file}')
        sys.exit(1)

    try:
        files = process(config_file, output_dir)
        print(f'\nSuccess! Generated {len(files)} plot(s):')
        for fp in files:
            print(f'  - {fp}')
        sys.exit(0)
    except Exception as e:
        import traceback
        print(f'Error: {e}')
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
