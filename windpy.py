
import numpy as np
import wind_flag as wfg
import matplotlib.pyplot as plt
import re


def gen_ccc(rgb0, ns):
    # {{{
    if type(rgb0) == type('rgb'):
        rgb = []
        for i in range(0, len(rgb0)):
            if rgb0[i] == 'w':
                rgb.append([1, 1, 1])
            if rgb0[i] == 'r':
                rgb.append([1, 0, 0])
            if rgb0[i] == 'g':
                rgb.append([0, 1, 0])
            if rgb0[i] == 'a':
                rgb.append([0.5, 0.5, 0.5])
            if rgb0[i] == 'b':
                rgb.append([0, 0, 1])
            if rgb0[i] == 'k':
                rgb.append([0, 0, 0])
            if rgb0[i] == 'c':
                rgb.append([0, 1, 1])
            if rgb0[i] == 'p':
                rgb.append([1, 0, 1])
            if rgb0[i] == 'y':
                rgb.append([1, 1, 0])
            if rgb0[i] == 'o':
                rgb.append([1, 0.5, 0])
    else:
        rgb = rgb0
    ccc = np.zeros((sum(ns), 3), dtype=float)
    i0 = 0
    for i in range(0, len(ns)):
        i1 = i0+ns[i]
        for j in range(0, 3):
            if ns[i] > 1:
                ccc[i0:i1, j] = np.linspace(
                    rgb[i][j], rgb[i+1][j], ns[i])
        i0 = i1
    ccc = ccc.tolist()
    return ccc
    # }}}


def wind_flag(xx, yy, u, v, xs=0, ys=0, linewidth=0.5,
              # {{{
              rgb=[[0, 0, 0.5], [0, 0, 1], [0, 0.8, 0.8], [0, 0.8, 0],
                   [1, 0.8, 0], [1, 0, 0], [1, 0, 1]],
              ns=[5, 10, 10, 10, 10, 5], xc='figure', 
              plot='yes', addtext=False,
              ix=1, iy=1, scp=[-200, 100], zz='', 
              zorder=1000, lat='', ax='', mi=0, ma=-1, ccc=''):
    # xx, 1y position of wind
    if re.search('str', str(type(ax))):
        ax = plt.gca()
    p3d = True
    if re.search('str', str(type(zz))):
        zz = xx
        p3d = False

    if re.search('str', str(type(lat))):
        if lat == '':
            lat = yy * 0+1

    if iy > 1:
        xx = xx[0::iy, :]
        yy = yy[0::iy, :]
        zz = zz[0::iy, :]
        u = u[0::iy, :]
        v = v[0::iy, :]
        lat = lat[0::iy, :]

    if ix > 1:
        xx = xx[:, 0::ix]
        yy = yy[:, 0::ix]
        zz = zz[:, 0::ix]
        u = u[:, 0::ix]
        v = v[:, 0::ix]
        lat = lat[:, 0::ix]

    ap = re.findall(r'([\d\.]+)', re.sub(r'.*\(','',str(ax)))
    print(ap)
    for i in range(0, len(ap)):
        ap[i] = float(ap[i])
    fp = re.findall(r'([\d\.]+)', str(plt.gcf()))
    for i in range(0, len(fp)):
        fp[i] = float(fp[i])

    if xc == 'figure':
        xlim = ax.get_xlim()
        ylim = ax.get_ylim()
        xc = xlim[1]-xlim[0]
        yc = ylim[1]-ylim[0]

    if xc == 'data':
        xc = np.max(xx.flatten()) - np.min(xx.flatten())
        yc = np.max(yy.flatten()) - np.min(yy.flatten())


    if xs > 0 and ys <= 0:
        ys = xs*ap[2]*fp[0]*yc/(ap[3]*fp[1]*xc)  # 风标缩放的y向的比例
    if ys > 0 and xs <= 0:
        xs = ys*ap[3]*fp[1]*xc/(ap[2]*fp[0]*yc)  # 风标缩放的x向的比例

    if xs <= 0 and ys <=0:
        if xx.ndim > 1:
            xs = xx[1, 1] - xx[0, 0]
        else:
            ux = np.unique(xx)
            xs = ux[1] - ux[0]
        ys = xs*ap[2]*fp[0]*yc/(ap[3]*fp[1]*xc)  # 风标缩放的y向的比例

    id = np.where(
        (u > scp[0]) & (v > scp[0]) &
        (u < scp[1]) & (v < scp[1]) &
        (xx < np.max(xlim)) & (xx > np.min(xlim)) &
        (yy < np.max(ylim)) & (yy > np.min(ylim)) )
    id = np.where(
        (u > scp[0]) & (v > scp[0]) &
        (u < scp[1]) & (v < scp[1]) )
    if True:
        u = u[id]
        v = v[id]
        xx = xx[id]
        yy = yy[id]
        zz = zz[id]
        lat = lat[id]
        if addtext:
            for i in range(u.size):
                ax.text(xx[i], yy[i], str(round(np.sqrt(u[i]**2+v[i]**2)/2)*2))
    
    print('yy:',yy.shape)
    xw, yw, sw = wfg.flag_lat(xx.flatten(), yy.flatten(),
                                u.flatten(), v.flatten(),
                                lat.flatten(), xs, ys)
    _, zw = np.meshgrid(xw[0, :], zz.flatten())

    id = np.where(sw > -990)
    xw = xw[id]
    yw = yw[id]
    zw = zw[id]
    sw = sw[id]
    xw[np.where(xw > 99999900)] = np.NaN

    usw = np.unique(sw)

    if mi > ma:
        mi = np.min(usw)
        ma = np.max(usw)
        if mi == ma:
            mi = mi-1
            ma = ma+1

    if str(type(ccc)) == str(type(' ')):
        jet = gen_ccc(rgb, ns)
    else:
        jet = ccc
    wim = []
    for us in usw:
        id = np.where(sw == us)
        ic = int((us-mi)/(ma-mi)*(len(jet)-1))
        if ic > len(jet)-1:
            ic = len(jet)-1
        c = jet[ic]
        # print(us, c)
        if p3d:
            ax.plot3D(xw[id], yw[id], zw[id], color=list(c),
                    linewidth=linewidth, zorder=zorder)
            # ax.plot(xw[id], yw[id], color=list(c), linewidth=linewidth)
        else:
            ax.plot(xw[id], yw[id], color=list(c), linewidth=linewidth)

        # ax.plot(xw[id], yw[id], color='r', linewidth=linewidth)
        # wim.append(ax.images[-1])
    # }}}
    return xw[id], yw[id], zw[id], jet 


def wind_flag_plotly(xx, yy, u, v, xs=0, ys=0, linewidth=0.5,
              # {{{
              rgb=[[0, 0, 0.5], [0, 0, 1], [0, 0.8, 0.8], [0, 0.8, 0],
                   [1, 0.8, 0], [1, 0, 0], [1, 0, 1]],
              ns=[5, 10, 10, 10, 10, 5], xc='figure', 
              plot='yes', addtext=False,
              xlim=[0, 100], ylim=[0,100],
              ix=1, iy=1, scp=[-200, 100], zz='', 
              zorder=1000, lat='', ax='', mi=0, ma=-1, ccc=''):
    import plotly.graph_objects as go
    p3d = True
    if re.search('str', str(type(zz))):
        zz = xx
        p3d = False

    if re.search('str', str(type(lat))):
        if lat == '':
            lat = yy * 0+1

    if iy > 1:
        xx = xx[0::iy, :]
        yy = yy[0::iy, :]
        zz = zz[0::iy, :]
        u = u[0::iy, :]
        v = v[0::iy, :]
        lat = lat[0::iy, :]

    if ix > 1:
        xx = xx[:, 0::ix]
        yy = yy[:, 0::ix]
        zz = zz[:, 0::ix]
        u = u[:, 0::ix]
        v = v[:, 0::ix]
        lat = lat[:, 0::ix]

    ap = [0, 0, 1, 1]
    fp = [10, 10]

    if xc == 'figure':
        xc = xlim[1]-xlim[0]
        yc = ylim[1]-ylim[0]

    if xc == 'data':
        xc = np.max(xx.flatten()) - np.min(xx.flatten())
        yc = np.max(yy.flatten()) - np.min(yy.flatten())


    if xs > 0 and ys <= 0:
        ys = xs*ap[2]*fp[0]*yc/(ap[3]*fp[1]*xc)  # 风标缩放的y向的比例
    if ys > 0 and xs <= 0:
        xs = ys*ap[3]*fp[1]*xc/(ap[2]*fp[0]*yc)  # 风标缩放的x向的比例

    if xs <= 0 and ys <=0:
        if xx.ndim > 1:
            xs = xx[1, 1] - xx[0, 0]
        else:
            ux = np.unique(xx)
            xs = ux[1] - ux[0]
        ys = xs*ap[2]*fp[0]*yc/(ap[3]*fp[1]*xc)  # 风标缩放的y向的比例

    id = np.where(
        (u > scp[0]) & (v > scp[0]) &
        (u < scp[1]) & (v < scp[1]) &
        (xx < np.max(xlim)) & (xx > np.min(xlim)) &
        (yy < np.max(ylim)) & (yy > np.min(ylim)) )
    id = np.where(
        (u > scp[0]) & (v > scp[0]) &
        (u < scp[1]) & (v < scp[1]) )
    if True:
        u = u[id]
        v = v[id]
        xx = xx[id]
        yy = yy[id]
        zz = zz[id]
        lat = lat[id]
        if addtext:
            for i in range(u.size):
                ax.text(xx[i], yy[i], str(round(np.sqrt(u[i]**2+v[i]**2)/2)*2))
    
    print('yy:',yy.shape)
    xw, yw, sw = wfg.flag_lat(xx.flatten(), yy.flatten(),
                                u.flatten(), v.flatten(),
                                lat.flatten(), xs, ys)
    _, zw = np.meshgrid(xw[0, :], zz.flatten())

    id = np.where(sw > -990)
    xw = xw[id]
    yw = yw[id]
    zw = zw[id]
    sw = sw[id]
    xw[np.where(xw > 99999900)] = np.nan

    usw = np.unique(sw)

    if mi > ma:
        mi = np.min(usw)
        ma = np.max(usw)
        if mi == ma:
            mi = mi-1
            ma = ma+1

    if str(type(ccc)) == str(type(' ')):
        jet = gen_ccc(rgb, ns)
    else:
        jet = ccc
    wim = []
    figs = []
    for us in usw:
        id = np.where(sw == us)
        ic = int((us-mi)/(ma-mi)*(len(jet)-1))
        if ic > len(jet)-1:
            ic = len(jet)-1
        c = jet[ic]
        # print(us, c)
        fig = go.Scatter3d(
                x=xw[id],
                y=yw[id],
                z=zw[id],
                mode='lines',
                line=dict(color=list(c)),
                showlegend=False
            )
        figs.append(fig)

        # ax.plot(xw[id], yw[id], color='r', linewidth=linewidth)
        # wim.append(ax.images[-1])
    return figs
    # }}}


def dwind_flag(xx, yy, u, v, xs=0, ys=0, linewidth=0.5,
              # {{{
               rgb=[[0, 0, 0.5], [0, 0, 1], [0, 0.8, 0.8], [0, 0.8, 0],
                   [1, 0.8, 0], [1, 0, 0], [1, 0, 1]],
               ns=[5, 10, 10, 10, 10, 5], xc='figure', plot='yes', zz=-1,
               ix=1, iy=1, scp=[-200, 100]):
    # xx, 1y position of wind

    if iy > 1:
        xx = xx[0::iy, :]
        yy = yy[0::iy, :]
        u = u[0::iy, :]
        v = v[0::iy, :]
    if ix > 1:
        xx = xx[:, 0::ix]
        yy = yy[:, 0::ix]
        u = u[:, 0::ix]
        v = v[:, 0::ix]

    ap = re.findall(r'([\d\.]+)', str(plt.gca()))
    for i in range(0, len(ap)):
        ap[i] = float(ap[i])
    fp = re.findall(r'([\d\.]+)', str(plt.gcf()))
    for i in range(0, len(fp)):
        fp[i] = float(fp[i])

    if xc == 'figure':
        xlim = plt.xlim()
        ylim = plt.ylim()
        xc = xlim[1]-xlim[0]
        yc = ylim[1]-ylim[0]

    if xc == 'data':
        xc = np.max(xx.flatten()) - np.min(xx.flatten())
        yc = np.max(yy.flatten()) - np.min(yy.flatten())

    if xs <= 0:
        if xx.ndim > 1:
            xs = xx[1, 1] - xx[0, 0]
        else:
            ux = np.unique(xx)
            xs = ux[1] - ux[0]

    if ys <= 0:
        ys = xs*ap[2]*fp[0]*yc/(ap[3]*fp[1]*xc)  # 风标缩放的y向的比例

    id = np.where(
        (u > scp[0]) & (v > scp[0]) &
        (u < scp[1]) & (v < scp[1]))
    u = u[id]
    v = v[id]
    xx = xx[id]
    yy = yy[id]

    xw, yw, sw = wfg.flag(xx.flatten(), yy.flatten(),
                                u.flatten(), v.flatten(),
                                xs, ys)

    id = np.where(sw > -990)
    xw = xw[id]
    yw = yw[id]
    sw = sw[id]
    # xw[np.where(xw > 99990)] = np.NaN

    usw = np.unique(sw)
    mi = np.min(usw)
    ma = np.max(usw)
    if mi == ma:
        mi = mi-1
        ma = ma+1

    jet = gen_ccc(rgb, ns)
    return jet, usw, sw, mi, ma, xw, yw
    # }}}


if __name__ == '__main__':
    x,y = np.meshgrid(np.linspace(0,10,11), np.linspace(0,1,2))

    fig = plt.figure()
    ax = fig.add_axes([0,0,1,1])
    ax.set_facecolor('w')
    ax.plot(x, y)
    ax.set_xlim([-1, 1])
    ax.set_ylim([-1, 2])
    wind_flag(x, x, x*0, x*0-26, xs=0, ys=0, linewidth=8, rgb='ccc',
              ns=[10, 10], xc='figure', plot='yes')
 
    plt.show()
