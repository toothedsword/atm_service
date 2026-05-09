#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
wrf_slice_worker.py - WRF 模式数据剖面图绘制工作进程

用法:
    python3 wrf_slice_worker.py <config_json> <output_dir>

config_json 字段:
    data_dir        str   WRF 数据目录（含 .dat 或 .zip 文件）
    lons            list  剖面经度控制点列表
    lats            list  剖面纬度控制点列表
    time_idx        int   时间索引（默认 0）
    flight_height_km float 航线高度(km)（默认 2.5）
    nx_points       int   剖面插值点数（默认 200）
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

    data_dir = cfg['data_dir']
    lons     = cfg['lons']
    lats     = cfg['lats']
    t_idx    = int(cfg.get('time_idx', 0))
    nx_pts   = int(cfg.get('nx_points', 200))
    label    = cfg.get('label', 'WRF')
    plot_types = cfg.get('plot_types', ['rh', 'ws', 'dzdt', 'cf'])

    print(f'Data dir: {data_dir}')
    print(f'Track: {list(zip(lons, lats))}')

    # ── 生成航迹 ─────────────────────────────────────────────────────────────
    track_x, track_y, track_dis = gen_track(lons, lats, nx_pts)
    n = len(track_x)

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

    raw_vars = {}   # varname -> (header, arr4d, lon, lat, lev)
    for vname in needed:
        fp = find_var_file(data_dir, vname)
        if fp is None:
            print(f'Warning: variable {vname} not found in {data_dir}')
            continue
        print(f'Reading {vname}: {fp}')
        raw_vars[vname] = read_wrf_file(fp)

    # ── 插值到剖面网格 ────────────────────────────────────────────────────────
    lv_vars_cache = {}

    def get_lv(vname):
        if vname in lv_vars_cache:
            return lv_vars_cache[vname]
        if vname not in raw_vars:
            return None
        hdr, arr4d, lon_d, lat_d, lev_d = raw_vars[vname]
        print(f'Interpolating {vname}...')
        result = interp_profile(arr4d, lon_d, lat_d, lev_d,
                                track_x, track_y, z_km, t_idx)
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
                    fp = find_var_file(data_dir, vname)
                    if fp is None:
                        continue
                    _, arr4d, lon_d, lat_d, _ = read_wrf_file(fp)
                    # 单层 2D 数据：arr4d shape=(nt,1,ny,nx)
                    arr2d = arr4d[t_idx, 0]            # (ny, nx)
                    f2d = RegularGridInterpolator(
                        (lat_d, lon_d), arr2d,
                        method='linear', bounds_error=False, fill_value=np.nan)
                    pts = np.column_stack([track_y, track_x])
                    track_vals = f2d(pts)              # (n,)
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
