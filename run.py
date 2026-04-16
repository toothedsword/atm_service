#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run.py - 主服务入口
负责接收请求、解析参数、调度子进程执行计算，自身不加载重型库，长期运行不会内存溢出。

Worker脚本:
  converter_worker.py  -- 数据格式转换 (txt/json/surface/height/time -> zip)
  plot_worker.py       -- 激光雷达PPI绘图
  slice_worker.py      -- 航迹剖面图绘制
  ec_worker.py         -- EC GRIB2 二维字段提取
  ec_point_worker.py   -- EC GRIB2 点值时间序列提取
  cache.py             -- Open-Meteo 5×5格点后台缓存

启动: python3 run.py
端口: 5001
"""

import json
import logging
import os
import re
import subprocess
import tempfile
import threading
import time
import uuid
import zipfile

from flask import Flask, jsonify, request, send_file
from werkzeug.utils import secure_filename

# ---------------------------------------------------------------------------
# 日志
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Flask应用
# ---------------------------------------------------------------------------
app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB

ALLOWED_EXTENSIONS = {'txt', 'json'}
TEMP_BASE = '/tmp/atm_service'
os.makedirs(TEMP_BASE, exist_ok=True)

# 当前脚本所在目录，用于定位worker脚本
SERVICE_DIR = os.path.dirname(os.path.abspath(__file__))


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def make_task_dirs(*subdirs):
    """在 TEMP_BASE 下创建任务子目录，返回路径列表"""
    paths = []
    for s in subdirs:
        p = os.path.join(TEMP_BASE, s)
        os.makedirs(p, exist_ok=True)
        paths.append(p)
    return paths if len(paths) > 1 else paths[0]


def cleanup_later(*paths, delay=5):
    """在后台线程中延迟删除文件或目录"""
    def _do():
        time.sleep(delay)
        import shutil
        for p in paths:
            if not p:
                continue
            try:
                if os.path.isdir(p):
                    shutil.rmtree(p)
                elif os.path.isfile(p):
                    os.remove(p)
            except Exception as e:
                logger.warning(f"清理失败 {p}: {e}")
    threading.Thread(target=_do, daemon=True).start()


def run_worker(cmd, timeout=300):
    """
    运行子进程 worker，返回 (returncode, stdout, stderr)
    cmd: list，例如 ['python3', 'converter_worker.py', '--input', ...]
    """
    logger.info(f"调度子进程: {' '.join(cmd)}")
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
        cwd=SERVICE_DIR
    )
    if result.returncode != 0:
        logger.error(f"子进程失败 (code={result.returncode}): {result.stderr}")
    else:
        logger.info(f"子进程完成: {result.stdout.strip()}")
    return result.returncode, result.stdout, result.stderr


def parse_txt_content(content):
    """解析 txt 文件内容，提取 URL 参数和 JSON 数据"""
    lines = content.strip().split('\n', 1)
    url_line = lines[0] if lines else ''
    json_content = lines[1] if len(lines) > 1 else content

    param_patterns = {
        'dataCode': r'dataCode=([^&\s]+)',
        'datetime':  r'datetime=([^&\s]+)',
        'element':   r'element=([^&\s]+)',
        'minLat':    r'minLat=([\d.]+)',
        'maxLat':    r'maxLat=([\d.]+)',
        'minLon':    r'minLon=([\d.]+)',
        'maxLon':    r'maxLon=([\d.]+)',
    }
    params = {}
    for key, pattern in param_patterns.items():
        m = re.search(pattern, url_line)
        if m:
            params[key] = m.group(1)

    try:
        json_data = json.loads(json_content)
    except json.JSONDecodeError as e:
        raise ValueError(f"JSON格式错误: {e}")

    return params, json_data


def call_converter(input_data, conversion_type):
    """
    将 input_data 写入临时文件，调用 converter_worker.py，返回输出 zip 路径。
    调用方负责在使用完成后删除输出文件。
    """
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False,
                                     dir=TEMP_BASE) as f:
        json.dump(input_data, f, ensure_ascii=False)
        input_file = f.name

    output_file = tempfile.mktemp(suffix='.zip', dir=TEMP_BASE)

    try:
        cmd = [
            'python3', os.path.join(SERVICE_DIR, 'converter_worker.py'),
            '--input', input_file,
            '--output', output_file,
            '--type', conversion_type,
        ]
        rc, _, stderr = run_worker(cmd)
        if rc != 0:
            raise RuntimeError(f"转换失败: {stderr}")
        return output_file
    finally:
        try:
            os.unlink(input_file)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# 健康检查 / 服务信息
# ---------------------------------------------------------------------------

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy", "service": "atm-service"})


EC_DATA_DIR = os.environ.get('EC_DATA_DIR', '/home/leon/Downloads/ec-oper-fc')


@app.route('/api/info', methods=['GET'])
def get_service_info():
    return jsonify({
        "service": "atm-service",
        "version": "3.1.0",
        "description": "大气数据处理服务：格式转换 + PPI绘图 + 航迹剖面图 + EC预报提取",
        "endpoints": {
            "/health":              "健康检查",
            "/api/info":            "服务信息",
            "/api/convert-txt":     "txt文件 -> zip",
            "/api/convert-json":    "JSON数据 -> zip",
            "/api/convert-surface": "地面二维数组 -> zip",
            "/api/convert-height":  "高度层数据 -> zip",
            "/api/convert-time":    "时间序列数据 -> zip",
            "/api/parse-info":      "解析txt预览（不转换）",
            "/api/plot":            "激光雷达PPI绘图",
            "/api/slice":           "航迹剖面图",
            "/api/ec-forecast":    "EC预报数据提取 -> zip",
            "/api/ec-timeseries":  "EC预报点值时间序列 -> JSON",
            "/api/ec-list":        "列出EC预报可用时次",
            "/api/openmeteo":      "Open-Meteo点值时间序列（带缓存）-> JSON",
            "/api/ppi_latest":     "激光雷达PPI最新完整圈 径向风速 PNG+TIF -> ZIP",
            "/api/ppi_vad_latest": "激光雷达PPI最新完整圈 VAD反演风场 PNG+TIF -> ZIP",
        },
        "workers": ["converter_worker.py", "plot_worker.py", "slice_worker.py",
                    "ec_worker.py", "ec_point_worker.py", "cache.py",
                    "ppi_worker.py", "ppi_vad_worker.py"],
        "max_file_size": "100MB",
    })


# ---------------------------------------------------------------------------
# 数据转换接口（均通过 converter_worker.py 子进程处理）
# ---------------------------------------------------------------------------

@app.route('/api/convert-txt', methods=['POST'])
def convert_txt_file():
    """txt文件（含URL参数行 + JSON体）-> zip"""
    output_file = None
    try:
        content = None
        filename = 'api_response'

        if 'file' in request.files:
            f = request.files['file']
            if not f.filename:
                return jsonify({"success": False, "error": "未选择文件"}), 400
            if not allowed_file(f.filename):
                return jsonify({"success": False, "error": "只支持.txt或.json格式文件"}), 400
            filename = secure_filename(f.filename).rsplit('.', 1)[0]
            content = f.read().decode('utf-8')
        elif request.content_type == 'text/plain':
            content = request.data.decode('utf-8')
        elif request.is_json:
            data = request.get_json()
            content = data.get('content')
            filename = data.get('filename', filename)

        if not content:
            return jsonify({"success": False, "error": "未提供有效内容"}), 400

        params, json_data = parse_txt_content(content)
        output_file = call_converter({'params': params, 'json_data': json_data}, 'txt')

        element = params.get('element', 'data')
        return send_file(output_file, as_attachment=True,
                         download_name=f"{filename}_{element}.zip",
                         mimetype='application/zip')

    except ValueError as e:
        return jsonify({"success": False, "error": f"数据格式错误: {e}"}), 400
    except Exception as e:
        logger.error(f"convert-txt 失败: {e}")
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if output_file:
            cleanup_later(output_file)


@app.route('/api/convert-json', methods=['POST'])
def convert_json_directly():
    """直接 JSON 数据 -> zip"""
    output_file = None
    try:
        if not request.is_json:
            return jsonify({"success": False, "error": "请提供JSON格式数据"}), 400
        data = request.get_json()

        params = {
            'dataCode': data.get('dataCode', 'RISE'),
            'element':  data.get('element', 'unknown'),
            'datetime': data.get('datetime', ''),
            'minLat': str(data.get('minLat', 40.0)),
            'maxLat': str(data.get('maxLat', 41.0)),
            'minLon': str(data.get('minLon', 115.5)),
            'maxLon': str(data.get('maxLon', 116.8)),
        }
        json_data = data if 'code' in data else {
            'code': 200, 'message': '操作成功', 'data': data.get('data', [])
        }

        output_file = call_converter({'params': params, 'json_data': json_data}, 'json')
        element = params.get('element', 'data')
        return send_file(output_file, as_attachment=True,
                         download_name=f"{element}_data.zip",
                         mimetype='application/zip')

    except Exception as e:
        logger.error(f"convert-json 失败: {e}")
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if output_file:
            cleanup_later(output_file)


def _convert_direct(conversion_type):
    """公共逻辑：文件上传或 JSON body -> converter_worker -> zip"""
    output_file = None
    try:
        json_data = None
        if 'file' in request.files:
            f = request.files['file']
            if not f.filename:
                return jsonify({"success": False, "error": "未选择文件"}), 400
            if not allowed_file(f.filename):
                return jsonify({"success": False, "error": "只支持.txt或.json格式文件"}), 400
            json_data = json.loads(f.read().decode('utf-8'))
        elif request.is_json:
            json_data = request.get_json()
        else:
            return jsonify({"success": False, "error": "请提供JSON格式数据或上传JSON文件"}), 400

        if not json_data:
            return jsonify({"success": False, "error": "未提供有效数据"}), 400

        output_file = call_converter(json_data, conversion_type)

        element = json_data.get('element', conversion_type + '_data')
        datetime_str = json_data.get('datetime', '000000')
        if conversion_type == 'time':
            time_list = [item.get('time', '') for item in json_data.get('data', [])]
            first_time = time_list[0].replace(' ', '_').replace(':', '') if time_list else '000000'
            download_name = f"{element}_{first_time}.zip"
        else:
            download_name = f"{element}_{datetime_str}.zip"

        return send_file(output_file, as_attachment=True,
                         download_name=download_name,
                         mimetype='application/zip')

    except Exception as e:
        logger.error(f"convert-{conversion_type} 失败: {e}")
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if output_file:
            cleanup_later(output_file)


@app.route('/api/convert-surface', methods=['POST'])
def convert_surface_data():
    return _convert_direct('surface')


@app.route('/api/convert-height', methods=['POST'])
def convert_height_data():
    return _convert_direct('height')


@app.route('/api/convert-time', methods=['POST'])
def convert_time_series_data():
    return _convert_direct('time')


@app.route('/api/parse-info', methods=['POST'])
def parse_info_only():
    """解析 txt 文件，预览参数，不做转换"""
    try:
        content = None
        if 'file' in request.files:
            f = request.files['file']
            if f.filename and allowed_file(f.filename):
                content = f.read().decode('utf-8')
        elif request.content_type == 'text/plain':
            content = request.data.decode('utf-8')
        elif request.is_json:
            content = request.get_json().get('content')

        if not content:
            return jsonify({"success": False, "error": "未提供有效内容"}), 400

        params, json_data = parse_txt_content(content)
        data_list = json_data.get('data', [])
        return jsonify({
            "success": True,
            "params": params,
            "data_info": {
                "times": len(data_list),
                "time_list": [item.get('time') for item in data_list],
                "shape": {
                    "y": len(data_list[0]['data']) if data_list and data_list[0].get('data') else 0,
                    "x": len(data_list[0]['data'][0]) if data_list and data_list[0].get('data') and data_list[0]['data'] else 0,
                } if data_list else {"y": 0, "x": 0},
                "api_code": json_data.get('code'),
                "api_message": json_data.get('message'),
            }
        })

    except Exception as e:
        logger.error(f"parse-info 失败: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


# ---------------------------------------------------------------------------
# PPI 绘图（通过 plot_worker.py 子进程处理）
# ---------------------------------------------------------------------------

@app.route('/api/plot', methods=['POST'])
def plot():
    """激光雷达 PPI 数据绘图"""
    task_id = str(uuid.uuid4())
    input_file = os.path.join(TEMP_BASE, f'{task_id}_input.json')
    output_file = os.path.join(TEMP_BASE, f'{task_id}_output.png')

    try:
        if 'file' in request.files:
            request.files['file'].save(input_file)
        else:
            with open(input_file, 'wb') as f:
                f.write(request.get_data())

        cmd = ['python3', os.path.join(SERVICE_DIR, 'plot_worker.py'),
               input_file, output_file]
        rc, _, stderr = run_worker(cmd)

        if rc != 0:
            return jsonify({"success": False, "error": f"绘图失败: {stderr}"}), 500
        if not os.path.exists(output_file):
            return jsonify({"success": False, "error": "未生成图片文件"}), 500

        return send_file(output_file, mimetype='image/png',
                         as_attachment=True, download_name='ppi_plot.png')

    except Exception as e:
        logger.error(f"plot 失败: {e}")
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        cleanup_later(input_file, output_file)


# ---------------------------------------------------------------------------
# 航迹剖面图（通过 slice_worker.py 子进程处理）
# ---------------------------------------------------------------------------

@app.route('/api/slice', methods=['POST'])
def generate_slice():
    """生成航迹剖面图，支持文件上传"""
    task_id = str(uuid.uuid4())
    config_file = os.path.join(TEMP_BASE, f'{task_id}_config.json')
    output_dir = os.path.join(TEMP_BASE, f'{task_id}_out')
    data_dir = os.path.join(TEMP_BASE, f'{task_id}_data')
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(data_dir, exist_ok=True)

    zip_file = None

    try:
        config_data = None
        uploaded_files = {}

        ct = request.content_type or ''
        if 'multipart/form-data' in ct:
            if 'config' in request.form:
                config_data = json.loads(request.form['config'])
            elif 'lons' in request.form and 'lats' in request.form:
                config_data = {
                    'lons':          json.loads(request.form.get('lons', '[]')),
                    'lats':          json.loads(request.form.get('lats', '[]')),
                    'flight_height': float(request.form.get('flight_height', 1)),
                    'nx_points':     int(request.form.get('nx_points', 200)),
                    'label':         request.form.get('label', 'griddata'),
                    'plot_types':    json.loads(request.form.get('plot_types', '["-t-r-uv-"]')),
                    'data_files':    {},
                }

            for key in request.files:
                if key.startswith('data_'):
                    var_name = key[5:]  # 去掉 'data_' 前缀
                    f = request.files[key]
                    if f and f.filename:
                        file_path = os.path.join(data_dir, secure_filename(f.filename))
                        f.save(file_path)
                        uploaded_files[var_name] = file_path
                        logger.info(f"上传数据文件 {var_name}: {file_path}")

        elif request.is_json:
            config_data = request.get_json()
        elif 'file' in request.files:
            config_data = json.load(request.files['file'])
        else:
            return jsonify({"success": False, "error": "请提供配置数据（JSON或multipart表单）"}), 400

        if not config_data:
            return jsonify({"success": False, "error": "配置数据为空"}), 400

        if uploaded_files:
            config_data.setdefault('data_files', {}).update(uploaded_files)

        if not config_data.get('data_files'):
            return jsonify({"success": False, "error": "缺少data_files字段或未上传数据文件"}), 400
        if 'lons' not in config_data or 'lats' not in config_data:
            return jsonify({"success": False, "error": "缺少lons或lats字段"}), 400

        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(config_data, f, ensure_ascii=False, indent=2)

        cmd = ['python3', os.path.join(SERVICE_DIR, 'slice_worker.py'),
               config_file, output_dir]
        rc, _, stderr = run_worker(cmd, timeout=600)

        if rc != 0:
            return jsonify({"success": False, "error": f"剖面图生成失败: {stderr}"}), 500

        output_files = [os.path.join(output_dir, fn)
                        for fn in os.listdir(output_dir) if fn.endswith('.png')]

        if not output_files:
            return jsonify({"success": False, "error": "未生成任何图片文件"}), 500

        if len(output_files) == 1:
            return send_file(output_files[0], mimetype='image/png',
                             as_attachment=True, download_name='slice_plot.png')

        zip_file = os.path.join(TEMP_BASE, f'{task_id}_plots.zip')
        with zipfile.ZipFile(zip_file, 'w') as zf:
            for fp in output_files:
                zf.write(fp, os.path.basename(fp))

        return send_file(zip_file, mimetype='application/zip',
                         as_attachment=True, download_name='slice_plots.zip')

    except Exception as e:
        logger.error(f"slice 失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        cleanup_later(config_file, output_dir, data_dir,
                      *([] if zip_file is None else [zip_file]))


# ---------------------------------------------------------------------------
# EC 预报数据接口（通过 ec_worker.py 子进程处理）
# ---------------------------------------------------------------------------

@app.route('/api/ec-timeseries', methods=['POST'])
def ec_timeseries():
    """
    提取指定经纬度点在起始时间之后的预报时间序列，返回 JSON。

    请求体 (JSON):
        datetime     str   起始时间（含），格式 yyyymmddhh，例如 "2026033003"  (必填)
        variable     str   GRIB2 shortName，例如 "t" / "2t" / "u"             (必填)
        lat          float 纬度，例如 40.0                                      (必填)
        lon          float 经度，例如 116.0                                     (必填)
        level        int   层级值，等压面用 hPa；地面变量可省略 (默认 0)
        typeOfLevel  str   层级类型，可省略，自动推断
        data_dir     str   GRIB2 目录路径 (可选)

    返回 JSON:
        {variable, typeOfLevel, level, lat, lon, start_time, count, times[], values[]}
    """
    result_file = None
    try:
        if not request.is_json:
            return jsonify({"success": False, "error": "请提供 JSON 格式请求体"}), 400

        params = request.get_json()

        for field in ('datetime', 'variable', 'lat', 'lon'):
            if field not in params:
                return jsonify({"success": False, "error": f"缺少必填字段: {field}"}), 400

        params.setdefault('data_dir', EC_DATA_DIR)

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False,
                                         dir=TEMP_BASE) as f:
            json.dump(params, f, ensure_ascii=False)
            input_file = f.name

        result_file = tempfile.mktemp(suffix='.json', dir=TEMP_BASE)

        cmd = ['python3', os.path.join(SERVICE_DIR, 'ec_point_worker.py'),
               '--input', input_file,
               '--output', result_file]

        rc, _, stderr = run_worker(cmd, timeout=120)

        try:
            os.unlink(input_file)
        except OSError:
            pass

        if rc != 0:
            return jsonify({"success": False, "error": f"提取失败: {stderr}"}), 500

        with open(result_file, 'r', encoding='utf-8') as f:
            result = json.load(f)

        return jsonify({"success": True, "data": result})

    except Exception as e:
        logger.error(f"ec-timeseries 失败: {e}")
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if result_file:
            cleanup_later(result_file)


@app.route('/api/ec-list', methods=['GET'])
def ec_list():
    """列出 EC 预报目录中所有可用的有效时次"""
    import re as _re
    from datetime import datetime as _dt, timedelta as _td
    _re_fn = _re.compile(
        r'(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})-(\d+)h-oper-fc\.grib2$'
    )
    data_dir = request.args.get('data_dir', EC_DATA_DIR)
    try:
        times = []
        for fname in sorted(os.listdir(data_dir)):
            m = _re_fn.match(fname)
            if not m:
                continue
            yr, mo, dy, hh, mm, ss, step = m.groups()
            base = _dt(int(yr), int(mo), int(dy), int(hh), int(mm), int(ss))
            valid = base + _td(hours=int(step))
            times.append({
                "filename": fname,
                "base_time": base.strftime('%Y%m%d%H'),
                "step_hours": int(step),
                "valid_time": valid.strftime('%Y%m%d%H'),
            })
        return jsonify({"success": True, "count": len(times), "data": times})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/ec-forecast', methods=['POST'])
def ec_forecast():
    """
    从 EC GRIB2 预报文件中提取最近时次的二维字段，返回 zip。

    请求体 (JSON):
        datetime     str   目标有效时间，格式 yyyymmddhh，例如 "2026033006"  (必填)
        variable     str   GRIB2 shortName，例如 "t" / "u" / "2t" / "10u"   (必填)
        level        int   层级值，等压面用 hPa，地面/高度层用实际值，例如 850 / 2 / 0
                           (可选，默认 0)
        typeOfLevel  str   层级类型，可选；不填则按变量名自动推断
                           常用值: isobaricInhPa / heightAboveGround / surface / entireAtmosphere
        data_dir     str   GRIB2 目录路径 (可选，默认 EC_DATA_DIR 环境变量)
        minLat       float 裁切南边界 (可选)
        maxLat       float 裁切北边界 (可选)
        minLon       float 裁切西边界 (可选)
        maxLon       float 裁切东边界 (可选)

    返回: application/zip，内含 data.bin（与 convert-* 系列接口格式相同）
    """
    output_file = None
    try:
        if not request.is_json:
            return jsonify({"success": False, "error": "请提供 JSON 格式请求体"}), 400

        params = request.get_json()

        if 'datetime' not in params:
            return jsonify({"success": False, "error": "缺少必填字段: datetime"}), 400
        if 'variable' not in params:
            return jsonify({"success": False, "error": "缺少必填字段: variable"}), 400

        # 注入默认数据目录
        if 'data_dir' not in params:
            params['data_dir'] = EC_DATA_DIR

        # 写临时参数文件
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False,
                                         dir=TEMP_BASE) as f:
            json.dump(params, f, ensure_ascii=False)
            input_file = f.name

        output_file = tempfile.mktemp(suffix='.zip', dir=TEMP_BASE)

        cmd = ['python3', os.path.join(SERVICE_DIR, 'ec_worker.py'),
               '--input', input_file,
               '--output', output_file]

        rc, stdout, stderr = run_worker(cmd, timeout=120)

        try:
            os.unlink(input_file)
        except OSError:
            pass

        if rc != 0:
            return jsonify({"success": False, "error": f"提取失败: {stderr}"}), 500
        if not os.path.exists(output_file):
            return jsonify({"success": False, "error": "未生成输出文件"}), 500

        variable  = params['variable']
        level     = params.get('level', 0)
        dt_str    = params['datetime']
        download_name = f"ec_{variable}_{level}_{dt_str}.zip"

        return send_file(output_file, as_attachment=True,
                         download_name=download_name,
                         mimetype='application/zip')

    except Exception as e:
        logger.error(f"ec-forecast 失败: {e}")
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if output_file:
            cleanup_later(output_file)


# ---------------------------------------------------------------------------
# Open-Meteo 代理接口（带本地缓存）
# ---------------------------------------------------------------------------

# 与 cache.py 共享的常量
_OM_CACHE_DIR       = '/tmp/atm_service/openmeteo_cache'
_OM_CACHE_TTL       = 3600
_OM_GRID_RES        = 0.125
_OM_ALL_VARS        = [
    'temperature_2m', 'relativehumidity_2m', 'precipitation',
    'windspeed_10m', 'winddirection_10m', 'windgusts_10m',
    'surface_pressure', 'cloudcover',
]
# 最后访问位置记录文件
_OM_LAST_POS_FILE   = os.path.join(TEMP_BASE, 'openmeteo_last_pos.json')
# 自动刷新检查间隔（秒），略小于缓存有效期，确保到期前主动续期
_OM_AUTO_REFRESH_INTERVAL = 1800   # 30 分钟检查一次


def _om_snap(val):
    return round(round(val / _OM_GRID_RES) * _OM_GRID_RES, 3)


def _om_save_last_pos(lat, lon):
    """记录最后访问的格点坐标及时间"""
    with open(_OM_LAST_POS_FILE, 'w') as f:
        json.dump({
            'lat': lat,
            'lon': lon,
            'time': time.strftime('%Y%m%d%H%M%S', time.gmtime()),
        }, f)


def _om_load_last_pos():
    """读取最后访问位置，返回 (lat, lon) 或 (None, None)"""
    if not os.path.exists(_OM_LAST_POS_FILE):
        return None, None
    try:
        with open(_OM_LAST_POS_FILE, 'r') as f:
            d = json.load(f)
        return float(d['lat']), float(d['lon'])
    except Exception:
        return None, None


def _om_trigger_cache(lat, lon, requested_dt=None):
    """后台启动 cache.py，可选传入请求时间"""
    cmd = ['python3', os.path.join(SERVICE_DIR, 'cache.py'),
           '--lat', str(lat), '--lon', str(lon)]
    if requested_dt is not None:
        cmd += ['--datetime', requested_dt.strftime('%Y%m%d%H')]
    subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    dt_str = requested_dt.strftime('%Y%m%d%H') if requested_dt else 'None'
    logger.info(f"[自动刷新] 已触发 cache.py for ({lat}, {lon}) datetime={dt_str}")


def _om_find_latest_cache(lat, lon):
    """返回该格点最新缓存文件路径，或 None。文件名末尾是 yyyymmddhh，max 即最新。"""
    import glob
    pattern = os.path.join(_OM_CACHE_DIR, f'{lat:.3f}_{lon:.3f}_*.json')
    files = glob.glob(pattern)
    return max(files) if files else None


def _om_cache_fresh(lat, lon, requested_dt=None):
    """
    判断缓存是否命中：
    - 若提供 requested_dt：直接找对应时间戳的文件，存在即命中，不做TTL检查。
    - 若未提供 requested_dt：找最新文件，检查 now - cache_ts < TTL。
    """
    from datetime import datetime as _dt, timezone as _tz
    if requested_dt is not None:
        ts = requested_dt.strftime('%Y%m%d%H')
        p = os.path.join(_OM_CACHE_DIR, f'{lat:.3f}_{lon:.3f}_{ts}.json')
        return os.path.exists(p)
    else:
        p = _om_find_latest_cache(lat, lon)
        if p is None:
            return False
        try:
            ts = os.path.basename(p).rsplit('_', 1)[-1].replace('.json', '')
            cache_dt = _dt.strptime(ts, '%Y%m%d%H').replace(tzinfo=_tz.utc)
            now = _dt.now(_tz.utc)
            return (now - cache_dt).total_seconds() < _OM_CACHE_TTL
        except ValueError:
            return False


def _om_parse_times(time_list):
    """将 Open-Meteo ISO8601 时间列表转为 yyyymmddhh 格式"""
    from datetime import datetime as _dt
    result = []
    for t in time_list:
        # "2026-03-31T06:00" -> "2026033106"
        result.append(_dt.strptime(t, '%Y-%m-%dT%H:%M').strftime('%Y%m%d%H'))
    return result


def _om_fetch_live(lat, lon, variables, forecast_days=7):
    """直接请求 Open-Meteo，返回解析后的数据字典"""
    import urllib.request
    import urllib.parse

    params = urllib.parse.urlencode({
        'latitude':      lat,
        'longitude':     lon,
        'hourly':        ','.join(variables),
        'forecast_days': forecast_days,
        'timezone':      'UTC',
    })
    url = f'https://api.open-meteo.com/v1/forecast?{params}'
    with urllib.request.urlopen(url, timeout=30) as resp:
        return json.loads(resp.read().decode('utf-8'))


def _om_extract(om_data, variables, start_time=None):
    """
    从 Open-Meteo 响应（原始或缓存）中提取指定变量的时间序列。
    start_time: yyyymmddhh 字符串，只返回此时间之后的数据；None 表示全部返回。
    """
    from datetime import datetime as _dt
    hourly   = om_data.get('hourly', {})
    raw_times = hourly.get('time', [])
    times    = _om_parse_times(raw_times)

    # 按起始时间过滤
    if start_time:
        cutoff = start_time
        idx_start = next((i for i, t in enumerate(times) if t >= cutoff), 0)
    else:
        idx_start = 0

    times = times[idx_start:]
    result = {'times': times}
    for var in variables:
        vals = hourly.get(var, [])
        result[var] = vals[idx_start:] if vals else []

    return result


@app.route('/api/openmeteo', methods=['POST'])
def openmeteo_point():
    """
    提取 Open-Meteo 点值时间序列，优先读本地缓存；
    缓存未命中时实时请求并后台触发 cache.py 缓存周边 5×5 格点。

    请求体 (JSON):
        lat           float  纬度 (必填)
        lon           float  经度 (必填)
        variables     list   变量列表，默认全部 (可选)
                             可选值: temperature_2m / relativehumidity_2m / precipitation /
                                     windspeed_10m / winddirection_10m / windgusts_10m /
                                     surface_pressure / cloudcover
        datetime      str    起始时间 yyyymmddhh，只返回此时间之后的数据 (可选)
        forecast_days int    预报天数，默认 7 (可选，仅缓存未命中时生效)

    返回 JSON:
        {success, source("cache"|"openmeteo"), lat, lon, units, times[], var1[], var2[], ...}
    """
    try:
        if not request.is_json:
            return jsonify({"success": False, "error": "请提供 JSON 格式请求体"}), 400

        params = request.get_json()
        if 'lat' not in params or 'lon' not in params:
            return jsonify({"success": False, "error": "缺少必填字段: lat / lon"}), 400

        req_lat  = float(params['lat'])
        req_lon  = float(params['lon'])
        variables = params.get('variables', _OM_ALL_VARS)
        start_time = params.get('datetime')
        forecast_days = int(params.get('forecast_days', 7))

        # 对齐到 Open-Meteo 格点
        snap_lat = _om_snap(req_lat)
        snap_lon = _om_snap(req_lon)

        os.makedirs(_OM_CACHE_DIR, exist_ok=True)

        # 记录本次访问位置，供后台自动刷新使用
        _om_save_last_pos(snap_lat, snap_lon)

        # 将 start_time 字符串解析为 datetime，用于缓存新鲜度判断
        from datetime import datetime as _dt, timezone as _tz
        requested_dt = None
        if start_time:
            try:
                requested_dt = _dt.strptime(start_time, '%Y%m%d%H').replace(tzinfo=_tz.utc)
            except ValueError:
                pass

        # ---- 判断缓存 ----
        if _om_cache_fresh(snap_lat, snap_lon, requested_dt):
            source = 'cache'
            if requested_dt is not None:
                ts_key = requested_dt.strftime('%Y%m%d%H')
                cache_file = os.path.join(_OM_CACHE_DIR, f'{snap_lat:.3f}_{snap_lon:.3f}_{ts_key}.json')
            else:
                cache_file = _om_find_latest_cache(snap_lat, snap_lon)
            with open(cache_file, 'r') as f:
                cached = json.load(f)
            om_data = cached['data']
            logger.info(f"openmeteo 缓存命中: ({snap_lat}, {snap_lon})  文件={os.path.basename(cache_file)}")
        else:
            source = 'openmeteo'
            logger.info(f"openmeteo 缓存未命中，实时请求: ({snap_lat}, {snap_lon})")
            om_data = _om_fetch_live(snap_lat, snap_lon, _OM_ALL_VARS, forecast_days)

            # 保存当前格点缓存，文件名用请求时间（有则用，无则用当前小时）
            ts = (requested_dt.strftime('%Y%m%d%H')
                  if requested_dt
                  else time.strftime('%Y%m%d%H', time.gmtime()))
            cache_file = os.path.join(_OM_CACHE_DIR, f'{snap_lat:.3f}_{snap_lon:.3f}_{ts}.json')
            if not os.path.exists(cache_file):
                with open(cache_file, 'w') as f:
                    json.dump({
                        'cached_at': ts,
                        'lat': snap_lat, 'lon': snap_lon,
                        'data': om_data,
                    }, f, separators=(',', ':'))

            # 后台触发 5°×5° 区域缓存，带上请求时间
            _om_trigger_cache(snap_lat, snap_lon, requested_dt)

        # ---- 提取请求的变量 ----
        # 只取缓存中存在的变量
        avail_vars = [v for v in variables if v in om_data.get('hourly', {})]
        ts = _om_extract(om_data, avail_vars, start_time)

        # 单位信息
        units = {v: om_data.get('hourly_units', {}).get(v, '') for v in avail_vars}

        return jsonify({
            "success":   True,
            "source":    source,
            "lat":       snap_lat,
            "lon":       snap_lon,
            "units":     units,
            "count":     len(ts['times']),
            **ts,
        })

    except Exception as e:
        logger.error(f"openmeteo 失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({"success": False, "error": str(e)}), 500


# ---------------------------------------------------------------------------
# PPI 最新完整圈（ppi_worker / ppi_vad_worker）
# ---------------------------------------------------------------------------

def _ppi_common(worker_script, zip_name):
    """
    公共逻辑：解析请求体中的 files 列表，调用指定 worker，
    读取 result.json 判断是否跳过，否则打包 PNG+TIF 返回 ZIP。
    """
    task_id     = str(uuid.uuid4())
    work_dir    = os.path.join(TEMP_BASE, task_id)
    config_file = os.path.join(TEMP_BASE, f'{task_id}_config.json')
    zip_path    = os.path.join(TEMP_BASE, f'{task_id}_result.zip')

    try:
        body = request.get_json(force=True, silent=True)
        if not body or 'files' not in body:
            return jsonify({"error": "请求体须为 JSON，包含 files 字段（文件路径列表）"}), 400

        files = body['files']
        if not isinstance(files, list) or len(files) == 0:
            return jsonify({"error": "files 须为非空列表"}), 400

        os.makedirs(work_dir, exist_ok=True)
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump({"files": files}, f, ensure_ascii=False)

        cmd = ['python3', os.path.join(SERVICE_DIR, worker_script),
               config_file, work_dir]
        rc, _, stderr = run_worker(cmd, timeout=300)
        if rc != 0:
            return jsonify({"error": f"{worker_script} 返回错误码 {rc}: {stderr}"}), 500

        result_json = os.path.join(work_dir, 'result.json')
        if not os.path.exists(result_json):
            return jsonify({"error": "worker 未写出 result.json"}), 500
        with open(result_json, encoding='utf-8') as rf:
            worker_result = json.load(rf)

        if 'error' in worker_result:
            return jsonify(worker_result), 500
        if worker_result.get('skipped'):
            return jsonify(worker_result), 200

        out_files = [os.path.join(work_dir, fn) for fn in os.listdir(work_dir)
                     if fn.endswith('.png') or fn.endswith('.tif')]
        if not out_files:
            return jsonify({"error": "worker 未生成任何输出文件"}), 500

        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            for fp in out_files:
                zf.write(fp, os.path.basename(fp))

        return send_file(zip_path, mimetype='application/zip',
                         as_attachment=True, download_name=zip_name)

    except Exception as e:
        logger.error(f"{worker_script} 失败: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        cleanup_later(config_file, zip_path, work_dir)


@app.route('/api/ppi_latest', methods=['POST'])
def ppi_latest():
    """
    从给定 CSV 文件中找最新完整 PPI 圈，生成径向风速 PNG + GeoTIFF。

    请求体 (JSON):
        files  list  CSV 文件绝对路径列表，按时间从旧到新排列（必填）

    返回: ZIP（ppi_*.png + ppi_*.tif），或跳过时返回 JSON {"skipped": true}
    """
    return _ppi_common('ppi_worker.py', 'ppi_latest.zip')


@app.route('/api/ppi_vad_latest', methods=['POST'])
def ppi_vad_latest():
    """
    从给定 CSV 文件中找最新完整 PPI 圈，在径向风速输出基础上额外
    用 VAD 方法反演水平风场，生成风速 GeoTIFF + 风矢量 PNG。

    请求体 (JSON):
        files  list  CSV 文件绝对路径列表，按时间从旧到新排列（必填）

    返回: ZIP（ppi_*.png + ppi_*.tif + wind_speed_*.tif + wind_uv_*.png），
          或跳过时返回 JSON {"skipped": true}
    """
    return _ppi_common('ppi_vad_worker.py', 'ppi_vad_latest.zip')


# ---------------------------------------------------------------------------
# 错误处理
# ---------------------------------------------------------------------------

@app.errorhandler(413)
def too_large(e):
    return jsonify({"success": False, "error": "文件过大，最大支持100MB"}), 413

@app.errorhandler(404)
def not_found(e):
    return jsonify({"success": False, "error": "接口不存在"}), 404

@app.errorhandler(500)
def internal_error(e):
    return jsonify({"success": False, "error": "服务器内部错误"}), 500


# ---------------------------------------------------------------------------
# 启动
# ---------------------------------------------------------------------------

def _start_auto_refresh():
    """
    后台守护线程：每隔 _OM_AUTO_REFRESH_INTERVAL 秒检查一次。
    若最后访问位置的缓存已过期，自动触发 cache.py 刷新。
    """
    def _worker():
        logger.info("[自动刷新] 后台线程已启动，检查间隔 %ds", _OM_AUTO_REFRESH_INTERVAL)
        while True:
            time.sleep(_OM_AUTO_REFRESH_INTERVAL)
            try:
                lat, lon = _om_load_last_pos()
                if lat is None:
                    logger.debug("[自动刷新] 尚无访问记录，跳过")
                    continue

                if _om_cache_fresh(lat, lon):
                    cache_file = _om_find_latest_cache(lat, lon)
                    logger.info("[自动刷新] 缓存仍新鲜，无需刷新: (%s, %s)  文件=%s",
                                lat, lon, os.path.basename(cache_file))
                else:
                    logger.info("[自动刷新] 缓存过期，触发刷新: (%s, %s)", lat, lon)
                    _om_trigger_cache(lat, lon)   # 自动刷新不带 requested_dt，只看当前时间
            except Exception as e:
                logger.error("[自动刷新] 检查异常: %s", e)

    t = threading.Thread(target=_worker, daemon=True, name='om-auto-refresh')
    t.start()


if __name__ == '__main__':
    logger.info("=" * 60)
    logger.info("大气数据处理服务 run.py 启动")
    logger.info("服务地址: http://0.0.0.0:5001")
    logger.info(f"Worker目录: {SERVICE_DIR}")
    logger.info(f"临时文件目录: {TEMP_BASE}")
    logger.info("=" * 60)
    _start_auto_refresh()
    app.run(host='0.0.0.0', port=5001, debug=False)
