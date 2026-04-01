
import numpy as np
import os
import matplotlib.pyplot as plt
import re
import json
from matplotlib import colors
from matplotlib.font_manager import FontProperties
yh_font = FontProperties(fname='/usr/share/fonts/msyh.ttc')


def p2h(p):
    p = np.array(p)
    h = -8.5*np.log(p/1013)
    return h


def get_single_label_positions(contour_set):
    """为每条等高线返回一个标签位置(取中点)"""
    positions = []
    for contour in contour_set.collections:
        paths = contour.get_paths()
        for path in paths:
            if len(path.vertices) > 0:
                mid_idx = len(path.vertices) // 2
                positions.append(path.vertices[mid_idx])
    return np.array(positions)


def grid2sites(lon, lat, data, xi, yi):
    # 格点到站点插值
    from scipy.interpolate import RegularGridInterpolator
    f = RegularGridInterpolator((lon, lat), data,
                bounds_error=False, fill_value=np.nan)
    tmp = f((xi, yi))
    return tmp


def latlon2dis(lat1, lon1, lat2, lon2):
    pi = 3.141592654
    R = 6371
    dLat = (lat2-lat1)*pi/180
    dLon = (lon2-lon1)*pi/180
    a = np.sin(dLat/2) * np.sin(dLat/2) + \
        np.cos(lat1*pi/180) * np.cos(lat2*pi/180) * \
        np.sin(dLon/2) * np.sin(dLon/2)
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))
    d = R * c
    return d


def gen_ccc(rgb, ns, gamma=np.ones(1000)):
    ccc = np.zeros((sum(ns), 3), dtype=float)
    i0 = 0
    for i in range(0, len(ns)):
        i1 = i0+ns[i]
        for j in range(0, 3):
            if ns[i] > 1:
                ccc[i0:i1, j] = np.linspace(rgb[i][j], rgb[i+1][j], ns[i])**gamma[i]
        i0 = i1
    return ccc


def parse_griddata_txt(filename):
    """解析GridData格式的txt文件"""
    with open(filename, 'r') as f:
        content = f.read()
    
    # 提取gridParams部分
    params_match = re.search(r'gridParams=({[^}]+})', content)
    if params_match:
        params_str = params_match.group(1)
        # 修正JSON格式
        params_str = params_str.replace("'", '"')
        params = json.loads(params_str)
    else:
        raise ValueError("Cannot parse gridParams")
    
    # 提取gridData部分
    data_match = re.search(r'gridData=(\[.*?\](?=,\s*gridData3D))', content, re.DOTALL)
    if data_match:
        data_str = data_match.group(1)
        # 解析数据数组
        data = eval(data_str)  # 使用eval解析Python列表格式
        data = np.array(data)
    else:
        raise ValueError("Cannot parse gridData")
    
    return params, data


class GridDataReader():
    """读取GridData格式的txt文件"""
    
    def __init__(self):
        self.vars = {}
        self.prodir = os.path.dirname(os.path.abspath(__file__))
        rgb = ((0.8, 0.8, 1), (0, 0, 1), (0, 1, 1), (1, 1, 0), (1, 0, 0), (1, 0, 1), (0.4, 0, 0.4))
        ns = [30, 40, 40, 40, 40, 30]
        cc0 = gen_ccc(rgb, ns)
        self.cc0_jet = cc0.tolist()
        self.ccc_jet = colors.ListedColormap(cc0.tolist(), name='ccc')

    def get_topo_global(self):
        # 不再需要地形数据,保留空函数避免错误
        pass

    def read_txt_data(self, filename, varname):
        """读取txt格式的GridData"""
        print(f'read: {filename} as {varname}')
        params, data = parse_griddata_txt(filename)
        
        # 第一次读取时设置维度信息
        if not hasattr(self, 'lon'):
            self.lon = np.array(params['lon'])
            self.lat = np.array(params['lat'])
            self.lev = np.array([float(x) for x in params['levelList']])  # 高度层(km)
            self.hgt = self.lev  # 直接使用高度
            self.nx = params['xSize']
            self.ny = params['ySize']
            self.nz = params['levels']
        
        # 数据维度: [time, level, lat, lon] -> 转换为 [lat, lon, level]
        data = np.squeeze(data)  # 移除time维度
        if data.ndim == 3:  # [level, lat, lon]
            data = data.transpose([1, 2, 0])  # -> [lat, lon, level]
        
        self.vars[varname] = data
        return data

    def gen_track(self, height=3, nx=200):
        """生成航迹路径"""
        a = self
        a.h = np.array([])
        a.x = np.array([])
        a.y = np.array([])
        a.get_topo_global()

        dis = []
        for i in range(0, len(a.lons)-1):
            lat1 = a.lats[i]
            lat2 = a.lats[i+1]
            lon1 = a.lons[i]
            lon2 = a.lons[i+1]
            dis.append(latlon2dis(lat1, lon1, lat2, lon2))
        dis = np.array(dis)
        tdis = np.sum(dis)
        ndis = dis/np.sum(dis)*nx*0.8
        ndis = ndis.astype(np.int16)
        ndis[-1] = int(nx*0.8-np.sum(ndis[0:-1]))
        
        for i in range(0, len(a.lons)-1):
            lat1 = a.lats[i]
            lat2 = a.lats[i+1]
            lon1 = a.lons[i]
            lon2 = a.lons[i+1]
            x = np.linspace(lon1, lon2, round(ndis[i]))
            y = np.linspace(lat1, lat2, round(ndis[i]))
            a.x = np.append(a.x, x)
            a.y = np.append(a.y, y)
            z = x*0+height
            if i == 0:
                z = np.linspace(0, 1, x.size)*(height)
            if i == len(a.lons)-2:
                z = np.linspace(1, 0, x.size)*(height)
            a.h = np.append(a.h, z)

        lon1 = self.x[0:-1]
        lat1 = self.y[0:-1]
        lon2 = self.x[1:]
        lat2 = self.y[1:]
        tmp = latlon2dis(lat1, lon1, lat2, lon2)
        tmp = np.cumsum(tmp)
        self.dis = np.append(0, tmp)
        return

    def gen_line_height(self, var, name):
        """生成高度剖面数据 - 插值到规则的相对地面高度层"""
        print('gen_line:', name)
        from scipy.interpolate import RegularGridInterpolator, interp1d
        
        x1 = self.x
        y1 = self.y
        
        # 定义规则的相对地面高度层用于输出
        if not hasattr(self, 'z_output'):
            max_agl = np.max(self.lev)
            # 创建规则的相对地面高度网格
            self.z_output = np.linspace(0, max_agl, 50)  # 50个高度层
        
        # 输出网格
        tmp = np.zeros((len(self.z_output), len(x1)))
        
        # GridData的插值器: [lat, lon, height_agl]
        f_grid = RegularGridInterpolator(
                (self.lat, self.lon, self.hgt), var,
                bounds_error=False, fill_value=np.nan)
        
        # 对每个航迹点进行插值
        for i in range(len(x1)):
            # 1. 先从GridData插值得到该点各相对高度层的值
            points = np.column_stack([
                np.full(len(self.lev), y1[i]),  # lat
                np.full(len(self.lev), x1[i]),  # lon
                self.lev  # 相对地面高度
            ])
            var_agl = f_grid(points)  # 各相对高度层的变量值
            
            # 2. 在相对高度坐标系中插值到规则网格
            if np.sum(~np.isnan(var_agl)) > 1:
                # 使用有效数据点进行插值
                valid_data = ~np.isnan(var_agl)
                if np.sum(valid_data) > 1:
                    f_vert = interp1d(self.lev[valid_data], var_agl[valid_data], 
                                     kind='linear', bounds_error=False, fill_value=np.nan)
                    tmp[:, i] = f_vert(self.z_output)
        
        if not hasattr(self, 'line_var'):
            self.line_var = {}
        self.line_var[name] = tmp
        
        return tmp

    def plot_line_height(self, var):
        """绘制高度剖面图"""
        print('plot:', var)
        
        fz = (20, 5)
        # 使用规则的相对地面高度网格
        xx1, zz1 = np.meshgrid(self.dis, self.z_output)
        
        fig = plt.figure(figsize=fz, dpi=100)

        axpos = [0.04, 0.10, 0.91, 0.84]
        ax = fig.add_axes(axpos)
        
        # y轴显示相对地面高度
        ylim = [0, self.z_output.max()]
        ax.set_ylim(ylim)
        ax.set_xlim([self.dis.min(), self.dis.max()])

        # 计算风速切变
        if 'u' in self.line_var and 'v' in self.line_var:
            du = self.line_var['u'][1::, :] - self.line_var['u'][0:-1, :]
            dv = self.line_var['v'][1::, :] - self.line_var['v'][0:-1, :]
            ws = np.sqrt(du**2+dv**2)
            self.line_var['ws'] = self.line_var['u']+np.nan
            self.line_var['ws'][1::, :] = ws

        # 绘制温度填色
        if re.search('-t-', var) and 't' in self.line_var:
            valid_data = ~np.isnan(self.line_var['t'])
            if np.any(valid_data):
                vmin, vmax = np.nanmin(self.line_var['t']), np.nanmax(self.line_var['t'])
                levels = np.arange(vmin, vmax+0.1, 0.01)
                f = ax.contourf(xx1, zz1, self.line_var['t'],
                        levels=levels,
                        cmap=self.ccc_jet, vmax=vmax,
                        vmin=vmin, extend='both')
                cax = fig.add_axes([0.96, 0.10, 0.01, 0.84])
                cbar = plt.colorbar(f, cax=cax)
                cbar.set_label('温度(℃)', fontproperties=yh_font)

        # 绘制相对湿度等值线
        if re.search('-rrr-', var) and 'rh' in self.line_var:
            try:
                ct = ax.contour(xx1, zz1, self.line_var['rh'], [25, 50],
                        colors='#000000', linestyles='--', linewidths=0.5)
                ax.clabel(ct, inline=True, fmt='%1.0f', fontsize=8)
                ct = ax.contour(xx1, zz1, self.line_var['rh'], [75, 84],
                        colors='#000000', linestyles='-', linewidths=0.5)
                ax.clabel(ct, inline=True, fmt='%1.0f', fontsize=8)
            except Exception as e:
                print(f"RH contour error: {e}")

        # 绘制相对湿度等值线
        if re.search('-r-', var) and 'rh' in self.line_var:
            try:
                ct = ax.contour(xx1, zz1, self.line_var['rh'], np.arange(5, 100, 5),
                        colors='#000000', linestyles='--', linewidths=0.5)
                ax.clabel(ct, inline=True, fmt='%1.0f', fontsize=8)
            except Exception as e:
                print(f"RH contour error: {e}")

        # 绘制风速切变
        if re.search('-ws-', var) and 'ws' in self.line_var:
            try:
                ct = ax.contourf(xx1, zz1, self.line_var['ws'],
                        np.arange(0, 11, 0.01),
                        colors='#000000', linestyles='--',
                        linewidths=2)
                ax.clabel(ct, inline=True, fmt='%2.0f', fontsize=8)
            except Exception as e:
                print(f"WS contour error: {e}")


        # 绘制风速切变
        if re.search('-wsf-', var) and 'ws' in self.line_var:
            vmin = np.nanmin(self.line_var['ws'])
            vmax = np.nanmax(self.line_var['ws'])
            try:
                ct = ax.contourf(xx1, zz1, self.line_var['ws'],
                        levels=np.arange(vmin, vmax, 0.01),
                        cmap=self.ccc_jet, vmin=vmin, vmax=vmax)
                cax = fig.add_axes([0.96, 0.10, 0.01, 0.84])
                cbar = plt.colorbar(ct, cax=cax)
                cbar.set_label('风切变(m/s)', fontproperties=yh_font)
            except Exception as e:
                print(f"WS contour error: {e}")


        # 绘制风矢量
        if re.search('-uv-', var) and 'u' in self.line_var and 'v' in self.line_var:
            try:
                # 简化的风矢量绘制 - 更稀疏的采样
                skip_z = max(1, len(self.z_output)//15)  # 垂直方向采样
                skip_x = max(1, len(self.dis)//30)    # 水平方向采样
                
                # 只绘制有效数据点
                u_plot = self.line_var['u'][::skip_z, ::skip_x]
                v_plot = self.line_var['v'][::skip_z, ::skip_x]
                valid = ~(np.isnan(u_plot) | np.isnan(v_plot))
                
                if np.any(valid) and False:
                    Q = ax.quiver(xx1[::skip_z, ::skip_x], zz1[::skip_z, ::skip_x],
                             u_plot, v_plot,
                             scale=500, width=0.001, alpha=0.7)
                    # 添加风矢量图例
                    ax.quiverkey(Q, 0.9, 0.95, 10, '10 m/s', 
                                labelpos='E', coordinates='axes')
                else:
                    import windpy
                    windpy.wind_flag(xx1, zz1, self.line_var['u'],
                            self.line_var['v'], rgb='kkk', # 'bgr',
                            linewidth=0.5, ns=[10,10], ix=3, iy=5, ax=ax)
                    label_positions = get_single_label_positions(ct)
                    ax.clabel(ct, inline=True, manual=label_positions, fmt='%1.0f', fontsize=8)


            except Exception as e:
                print(f"Wind vector error: {e}")

        ax.set_xlabel('距离(公里)', fontproperties=yh_font, fontsize=12)
        ax.set_ylabel('相对地面高度(公里)', fontproperties=yh_font, fontsize=12)
        ax.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)
        
        # 绘制飞行高度轨迹
        ax.plot(self.dis, self.h, 'k-', linewidth=2, label='飞行轨迹', zorder=11)

        # 添加航迹方向箭头
        ax1 = fig.add_axes(axpos)
        ax1.set_axis_off()
        ax1.set_xlim([0, fz[0]*axpos[2]])
        ax1.set_ylim([0, fz[1]*axpos[3]])
        u = np.append(self.x[1:]-self.x[0:-1], np.nan)*40
        v = np.append(self.y[1:]-self.y[0:-1], np.nan)*40
        i = 10
        x_arrow = self.dis[::i]/self.dis[-1]*fz[0]*axpos[2]
        y_arrow = self.h[::i]/ylim[1]*fz[1]*axpos[3]
        ax1.quiver(x_arrow, y_arrow, u[::i], v[::i],
                  angles='xy', scale_units='xy', scale=1,
                  width=0.002, linewidth=0.2, color='white', zorder=12)

        os.makedirs('line', exist_ok=True)
        fig.savefig('line/line_'+self.lt+'_'+var+'.png', dpi=150, bbox_inches='tight')
        plt.close()
        print(f"Saved: line/line_{self.lt}_{var}.png")


# 主程序
if __name__ == '__main__':
    e = GridDataReader()
    
    # 读取txt数据文件
    e.read_txt_data('tdata.txt', 't')
    e.read_txt_data('rhdata.txt', 'rh')
    e.read_txt_data('udata.txt', 'u')
    e.read_txt_data('vdata.txt', 'v')
    e.hgt[0] = 0
    
    # 设置航迹点
    e.lons = [115.5, 115.8, 116.0, 116.3, 116.8]
    e.lats = [40.0, 40.3, 40.5, 40.7, 41.0]
    
    # 生成航迹
    e.gen_track(height=1, nx=200)
    
    # 生成剖面数据
    e.gen_line_height(e.vars['t'], 't')
    e.gen_line_height(e.vars['rh'], 'rh')
    e.gen_line_height(e.vars['u'], 'u')
    e.gen_line_height(e.vars['v'], 'v')
    
    # 绘制剖面图
    e.lt = 'griddata'
    e.plot_line_height('-t-r-uv-')
    e.plot_line_height('-wsf-uv-')
    
    print("Done! Check ./line/ folder for output images")
