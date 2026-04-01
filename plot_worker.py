"""
绘图工作脚本 - plot_worker.py
负责实际的数据处理和图像生成
"""
import json
import numpy as np
import matplotlib
matplotlib.use('Agg')  # 使用非GUI后端
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
import sys

# 设置中文字体
yh_font = FontProperties(fname='./msyh.ttc')

def plot_ppi_data(input_file, output_file):
    """生成PPI图像"""
    # 读取JSON文件
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # 选择要可视化的数据记录（这里选择第一条）
    record = data['content'][0]
    
    # 解析数据
    height = json.loads(record['height'])
    wind_speed = json.loads(record['windSpeed'])
    wind_direction = json.loads(record['windDirection'])
    # snr = json.loads(record['snr'])
    azimuth = float(record['azimuth'])
    pitch = float(record['pitch'])
    
    # 创建图形
    fig, axes = plt.subplots(1, 2, figsize=(16, 7))
    
    # ========== 左图：风速PPI分布（使用pcolor） ==========
    ax1 = axes[0]
    
    # 收集所有数据并组织成网格
    height_data = json.loads(data['content'][0]['height'])
    n_range = len(height_data)  # 距离门数
    n_azimuth = len(data['content'])  # 方位角数
    
    # 创建数据矩阵
    azimuth_array = []
    ws_matrix = np.full((n_azimuth, n_range), np.nan)
    
    for idx, rec in enumerate(data['content']):
        h = json.loads(rec['height'])
        ws = json.loads(rec['windSpeed'])
        az = float(rec['azimuth'])
        azimuth_array.append(az)
        
        for i, w in enumerate(ws):
            if w != 999.0:
                ws_matrix[idx, i] = w
    
    # 创建网格坐标
    azimuth_rad = np.radians(azimuth_array)
    range_grid = np.array(height_data)
    
    # 创建笛卡尔坐标网格
    X = np.outer(np.sin(azimuth_rad), range_grid)
    Y = np.outer(np.cos(azimuth_rad), range_grid)
    
    # 使用pcolor绘制
    pcm = ax1.pcolormesh(X, Y, ws_matrix, cmap='jet', shading='auto', 
                         vmin=0, vmax=15, alpha=0.8)
    ax1.set_xlabel('X (米)', fontsize=12, fontproperties=yh_font)
    ax1.set_ylabel('Y (米)', fontsize=12, fontproperties=yh_font)
    ax1.set_title(f'风速平面位置显示\n俯仰角: {pitch}°', 
                  fontsize=14, fontweight='bold', fontproperties=yh_font)
    ax1.grid(True, alpha=0.3)
    ax1.set_aspect('equal')
    cbar1 = plt.colorbar(pcm, ax=ax1)
    cbar1.set_label('风速 (米/秒)', fontsize=11, fontproperties=yh_font)
    
    # 保存所有数据用于右图
    all_x, all_y, all_ws = [], [], []
    for rec in data['content']:
        h = json.loads(rec['height'])
        ws = json.loads(rec['windSpeed'])
        wd = json.loads(rec['windDirection'])
        az = float(rec['azimuth'])
        
        for i, (w, d) in enumerate(zip(ws, wd)):
            if w != 999.0 and d != 999.0:
                r = h[i]
                theta = np.radians(az)
                x = r * np.sin(theta)
                y = r * np.cos(theta)
                all_x.append(x)
                all_y.append(y)
                all_ws.append(w)
    
    # ========== 右图：风向矢量场（灰色打底，彩色箭头） ==========
    ax2 = axes[1]
    
    # 使用灰色pcolormesh作为背景
    pcm2 = ax2.pcolormesh(X, Y, ws_matrix, cmap='gray', shading='auto', 
                          vmin=0, vmax=15, alpha=0.2)
    
    # 收集风向数据
    all_wd = []
    for rec in data['content']:
        h = json.loads(rec['height'])
        ws = json.loads(rec['windSpeed'])
        wd = json.loads(rec['windDirection'])
        
        for i, (w, d) in enumerate(zip(ws, wd)):
            if w != 999.0 and d != 999.0:
                all_wd.append(d)
    
    # 对数据进行采样以避免箭头过密
    sample_step = 10  # 每隔10个点采样一次
    sampled_x = all_x[::sample_step]
    sampled_y = all_y[::sample_step]
    sampled_ws = all_ws[::sample_step]
    sampled_wd = all_wd[::sample_step]
    
    # 计算u, v分量（风向是气象学惯例：从哪里来）
    u = [-sampled_ws[i] * np.sin(np.radians(sampled_wd[i])) 
         for i in range(len(sampled_ws))]
    v = [-sampled_ws[i] * np.cos(np.radians(sampled_wd[i])) 
         for i in range(len(sampled_ws))]
    
    # 绘制彩色风向箭头
    quiver = ax2.quiver(sampled_x, sampled_y, u, v, sampled_ws,
                        scale=300, width=0.002, alpha=0.9, 
                        cmap='jet', clim=(0, 15),
                        edgecolors='black', linewidth=0.3)
    
    ax2.set_xlabel('X (米)', fontsize=12, fontproperties=yh_font)
    ax2.set_ylabel('Y (米)', fontsize=12, fontproperties=yh_font)
    ax2.set_title('风速风向平面位置显示', fontsize=14, fontweight='bold', fontproperties=yh_font)
    ax2.grid(True, alpha=0.3)
    ax2.set_aspect('equal')
    cbar2 = plt.colorbar(pcm2, ax=ax2)
    cbar2.set_label('风速 (米/秒)', fontsize=11, fontproperties=yh_font)
    
    # 计算对称的范围（基于有效数据）
    valid_x = X[~np.isnan(ws_matrix)]
    valid_y = Y[~np.isnan(ws_matrix)]
    max_extent = max(abs(valid_x).max(), abs(valid_y).max())
    
    ax1.set_xlim(-max_extent, max_extent)
    ax1.set_ylim(-max_extent, max_extent)
    ax2.set_xlim(-max_extent, max_extent)
    ax2.set_ylim(-max_extent, max_extent)
    
    # 添加日期时间信息
    fig.suptitle(f'激光雷达PPI数据 - {record["dateTime"]}', 
                 fontsize=16, fontweight='bold', y=0.98, fontproperties=yh_font)
    
    plt.tight_layout()
    
    # 保存到文件
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    plt.close(fig)
    
    # 打印统计信息
    print(f"可视化完成！")
    print(f"数据时间: {record['dateTime']}")
    print(f"有效数据点数: {len(all_x)}")
    print(f"风速范围: {min(all_ws):.2f} - {max(all_ws):.2f} m/s")

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("用法: python plot_worker.py <input.json> <output.png>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    try:
        plot_ppi_data(input_file, output_file)
        sys.exit(0)
    except Exception as e:
        print(f"错误: {str(e)}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
