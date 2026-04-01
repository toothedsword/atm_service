#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from flask import Flask, request, jsonify, send_file
import json
import subprocess
import tempfile
import os
import logging
import re
from werkzeug.utils import secure_filename
import time
import uuid

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024

ALLOWED_EXTENSIONS = {'txt', 'json'}


def allowed_file(filename):
    """检查文件扩展名是否允许"""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def parse_txt_content(content):
    """解析txt文件内容，提取API URL参数和JSON数据"""
    lines = content.strip().split('\n', 1)
    url_line = lines[0] if len(lines) > 0 else ""
    json_content = lines[1] if len(lines) > 1 else content
    
    params = {}
    param_patterns = {
        'dataCode': r'dataCode=([^&\s]+)',
        'datetime': r'datetime=([^&\s]+)',
        'element': r'element=([^&\s]+)',
        'minLat': r'minLat=([\d.]+)',
        'maxLat': r'maxLat=([\d.]+)',
        'minLon': r'minLon=([\d.]+)',
        'maxLon': r'maxLon=([\d.]+)',
    }
    
    for key, pattern in param_patterns.items():
        match = re.search(pattern, url_line)
        if match:
            params[key] = match.group(1)
    
    try:
        json_data = json.loads(json_content)
    except json.JSONDecodeError as e:
        logger.error(f"JSON解析失败: {e}")
        raise ValueError(f"JSON格式错误: {e}")
    
    return params, json_data


def call_converter_subprocess(input_data, conversion_type):
    """
    调用独立的转换子进程
    
    参数:
    input_data: 输入的JSON数据
    conversion_type: 转换类型 ('txt', 'json', 'surface', 'height', 'time')
    
    返回:
    output_file: 生成的zip文件路径
    """
    # 创建临时文件
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(input_data, f, ensure_ascii=False)
        input_file = f.name
    
    # 创建输出文件路径
    output_file = tempfile.mktemp(suffix='.zip')
    
    try:
        # 调用转换器子进程
        cmd = [
            'python3', 'converter_worker.py',
            '--input', input_file,
            '--output', output_file,
            '--type', conversion_type
        ]
        
        logger.info(f"调用转换进程: {' '.join(cmd)}")
        
        # 使用subprocess.run，等待子进程完成
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300  # 5分钟超时
        )
        
        if result.returncode != 0:
            logger.error(f"转换进程失败: {result.stderr}")
            raise RuntimeError(f"转换失败: {result.stderr}")
        
        logger.info(f"转换完成: {output_file}")
        
        return output_file
        
    finally:
        # 清理输入临时文件
        try:
            os.unlink(input_file)
        except:
            pass


@app.route('/health', methods=['GET'])
def health_check():
    """健康检查接口"""
    return jsonify({"status": "healthy", "service": "txt-to-zip-converter"})


@app.route('/api/info', methods=['GET'])
def get_service_info():
    """获取服务信息"""
    return jsonify({
        "service": "txt-to-zip-converter",
        "version": "2.0.0",
        "description": "将API响应的txt格式转换为二进制zip格式的服务（内存优化版）",
        "endpoints": {
            "/health": "健康检查",
            "/api/convert-txt": "转换txt文件为zip格式",
            "/api/convert-json": "直接转换JSON数据为zip格式",
            "/api/convert-surface": "转换地面二维数组数据为zip格式",
            "/api/convert-height": "转换高度层数据为zip格式",
            "/api/convert-time": "转换时间序列数据为zip格式",
            "/api/parse-info": "解析预览",
            "/api/info": "服务信息"
        },
        "supported_formats": ["txt", "json"],
        "max_file_size": "100MB"
    })


@app.route('/api/convert-txt', methods=['POST'])
def convert_txt_file():
    """转换txt文件为zip格式"""
    output_file = None
    try:
        content = None
        filename = "api_response"
        
        if 'file' in request.files:
            file = request.files['file']
            
            if file.filename == '':
                return jsonify({"success": False, "error": "未选择文件"}), 400
            
            if not allowed_file(file.filename):
                return jsonify({"success": False, "error": "只支持.txt或.json格式文件"}), 400
            
            filename = secure_filename(file.filename).rsplit('.', 1)[0]
            content = file.read().decode('utf-8')
        
        elif request.content_type == 'text/plain':
            content = request.data.decode('utf-8')
        
        elif request.is_json:
            data = request.get_json()
            content = data.get('content')
            filename = data.get('filename', 'api_response')
        
        if not content:
            return jsonify({"success": False, "error": "未提供有效内容"}), 400
        
        logger.info(f"开始处理转换请求，文件名: {filename}")
        
        # 解析txt内容
        params, json_data = parse_txt_content(content)
        
        # 准备转换数据
        conversion_data = {
            'params': params,
            'json_data': json_data
        }
        
        # 调用子进程转换
        output_file = call_converter_subprocess(conversion_data, 'txt')
        
        # 生成输出文件名
        element = params.get('element', 'data')
        output_filename = f"{filename}_{element}.zip"
        
        # 返回文件
        return send_file(
            output_file,
            as_attachment=True,
            download_name=output_filename,
            mimetype='application/zip'
        )
        
    except ValueError as ve:
        logger.error(f"数据验证错误: {str(ve)}")
        return jsonify({"success": False, "error": f"数据格式错误: {str(ve)}"}), 400
        
    except Exception as e:
        logger.error(f"转换失败: {str(e)}")
        return jsonify({"success": False, "error": f"转换失败: {str(e)}"}), 500
    
    finally:
        # 清理输出临时文件
        if output_file and os.path.exists(output_file):
            try:
                os.unlink(output_file)
            except:
                pass


@app.route('/api/convert-json', methods=['POST'])
def convert_json_directly():
    """直接转换JSON数据为zip格式"""
    output_file = None
    try:
        if not request.is_json:
            return jsonify({"success": False, "error": "请提供JSON格式数据"}), 400
        
        data = request.get_json()
        
        # 准备参数
        params = {
            'dataCode': data.get('dataCode', 'RISE'),
            'element': data.get('element', 'unknown'),
            'datetime': data.get('datetime', ''),
            'minLat': str(data.get('minLat', 40.0)),
            'maxLat': str(data.get('maxLat', 41.0)),
            'minLon': str(data.get('minLon', 115.5)),
            'maxLon': str(data.get('maxLon', 116.8)),
        }
        
        json_data = data if 'code' in data else {
            'code': 200,
            'message': '操作成功',
            'data': data.get('data', [])
        }
        
        conversion_data = {
            'params': params,
            'json_data': json_data
        }
        
        output_file = call_converter_subprocess(conversion_data, 'json')
        
        element = params.get('element', 'data')
        output_filename = f"{element}_data.zip"
        
        return send_file(
            output_file,
            as_attachment=True,
            download_name=output_filename,
            mimetype='application/zip'
        )
        
    except Exception as e:
        logger.error(f"转换失败: {str(e)}")
        return jsonify({"success": False, "error": f"转换失败: {str(e)}"}), 500
    
    finally:
        if output_file and os.path.exists(output_file):
            try:
                os.unlink(output_file)
            except:
                pass


@app.route('/api/convert-surface', methods=['POST'])
def convert_surface_data():
    """转换地面数据格式"""
    output_file = None
    try:
        json_data = None
        
        if 'file' in request.files:
            file = request.files['file']
            if file.filename == '':
                return jsonify({"success": False, "error": "未选择文件"}), 400
            if not allowed_file(file.filename):
                return jsonify({"success": False, "error": "只支持.txt或.json格式文件"}), 400
            
            content = file.read().decode('utf-8')
            json_data = json.loads(content)
        
        elif request.is_json:
            json_data = request.get_json()
        
        else:
            return jsonify({"success": False, "error": "请提供JSON格式数据或上传JSON文件"}), 400
        
        if not json_data:
            return jsonify({"success": False, "error": "未提供有效数据"}), 400
        
        output_file = call_converter_subprocess(json_data, 'surface')
        
        element = json_data.get('element', 'surface_data')
        datetime_str = json_data.get('datetime', '2024010100')
        output_filename = f"{element}_{datetime_str}.zip"
        
        return send_file(
            output_file,
            as_attachment=True,
            download_name=output_filename,
            mimetype='application/zip'
        )
        
    except Exception as e:
        logger.error(f"转换失败: {str(e)}")
        return jsonify({"success": False, "error": f"转换失败: {str(e)}"}), 500
    
    finally:
        if output_file and os.path.exists(output_file):
            try:
                os.unlink(output_file)
            except:
                pass


@app.route('/api/convert-height', methods=['POST'])
def convert_height_data():
    """转换高度层数据格式"""
    output_file = None
    try:
        json_data = None
        
        if 'file' in request.files:
            file = request.files['file']
            if file.filename == '':
                return jsonify({"success": False, "error": "未选择文件"}), 400
            if not allowed_file(file.filename):
                return jsonify({"success": False, "error": "只支持.txt或.json格式文件"}), 400
            
            content = file.read().decode('utf-8')
            json_data = json.loads(content)
        
        elif request.is_json:
            json_data = request.get_json()
        
        else:
            return jsonify({"success": False, "error": "请提供JSON格式数据或上传JSON文件"}), 400
        
        if not json_data:
            return jsonify({"success": False, "error": "未提供有效数据"}), 400
        
        output_file = call_converter_subprocess(json_data, 'height')
        
        element = json_data.get('element', 'height_data')
        datetime_str = json_data.get('datetime', '2024010100')
        output_filename = f"{element}_{datetime_str}.zip"
        
        return send_file(
            output_file,
            as_attachment=True,
            download_name=output_filename,
            mimetype='application/zip'
        )
        
    except Exception as e:
        logger.error(f"转换失败: {str(e)}")
        return jsonify({"success": False, "error": f"转换失败: {str(e)}"}), 500
    
    finally:
        if output_file and os.path.exists(output_file):
            try:
                os.unlink(output_file)
            except:
                pass


@app.route('/api/convert-time', methods=['POST'])
def convert_time_series_data():
    """转换时间序列数据格式"""
    output_file = None
    try:
        json_data = None
        
        if 'file' in request.files:
            file = request.files['file']
            if file.filename == '':
                return jsonify({"success": False, "error": "未选择文件"}), 400
            if not allowed_file(file.filename):
                return jsonify({"success": False, "error": "只支持.txt或.json格式文件"}), 400
            
            content = file.read().decode('utf-8')
            json_data = json.loads(content)
        
        elif request.is_json:
            json_data = request.get_json()
        
        else:
            return jsonify({"success": False, "error": "请提供JSON格式数据或上传JSON文件"}), 400
        
        if not json_data:
            return jsonify({"success": False, "error": "未提供有效数据"}), 400
        
        output_file = call_converter_subprocess(json_data, 'time')
        
        element = json_data.get('element', 'time_series')
        time_list = [item.get('time', '') for item in json_data.get('data', [])]
        first_time = time_list[0].replace(' ', '_').replace(':', '') if time_list else '000000'
        output_filename = f"{element}_{first_time}.zip"
        
        return send_file(
            output_file,
            as_attachment=True,
            download_name=output_filename,
            mimetype='application/zip'
        )
        
    except Exception as e:
        logger.error(f"转换失败: {str(e)}")
        return jsonify({"success": False, "error": f"转换失败: {str(e)}"}), 500
    
    finally:
        if output_file and os.path.exists(output_file):
            try:
                os.unlink(output_file)
            except:
                pass


@app.route('/api/parse-info', methods=['POST'])
def parse_info_only():
    """仅解析txt文件，返回参数信息，不进行转换"""
    try:
        content = None
        
        if 'file' in request.files:
            file = request.files['file']
            if file.filename != '' and allowed_file(file.filename):
                content = file.read().decode('utf-8')
        elif request.content_type == 'text/plain':
            content = request.data.decode('utf-8')
        elif request.is_json:
            data = request.get_json()
            content = data.get('content')
        
        if not content:
            return jsonify({"success": False, "error": "未提供有效内容"}), 400
        
        params, json_data = parse_txt_content(content)
        
        data_list = json_data.get('data', [])
        info = {
            "success": True,
            "params": params,
            "data_info": {
                "times": len(data_list),
                "time_list": [item.get('time') for item in data_list] if data_list else [],
                "shape": {
                    "y": len(data_list[0]['data']) if data_list and data_list[0].get('data') else 0,
                    "x": len(data_list[0]['data'][0]) if data_list and data_list[0].get('data') and data_list[0]['data'] else 0
                } if data_list else {"y": 0, "x": 0},
                "api_code": json_data.get('code'),
                "api_message": json_data.get('message')
            }
        }
        
        return jsonify(info)
        
    except Exception as e:
        logger.error(f"解析失败: {str(e)}")
        return jsonify({"success": False, "error": f"解析失败: {str(e)}"}), 500


@app.errorhandler(413)
def too_large(e):
    return jsonify({"success": False, "error": "文件过大，最大支持100MB"}), 413


@app.errorhandler(404)
def not_found(e):
    return jsonify({"success": False, "error": "接口不存在"}), 404


@app.errorhandler(500)
def internal_error(e):
    return jsonify({"success": False, "error": "服务器内部错误"}), 500


@app.route('/api/plot', methods=['POST'])
def plot():
    TEMP_DIR = '/tmp/lidar_ppi'
    os.makedirs(TEMP_DIR, exist_ok=True)
    """处理绘图请求"""
    input_file = None
    output_file = None
    
    try:
        # 生成唯一的文件名
        task_id = str(uuid.uuid4())
        input_file = os.path.join(TEMP_DIR, f'{task_id}_input.json')
        output_file = os.path.join(TEMP_DIR, f'{task_id}_output.png')
        
        # 保存上传的JSON文件
        if 'file' in request.files:
            file = request.files['file']
            file.save(input_file)
        else:
            # 从请求体获取JSON数据并保存
            json_data = request.get_data()
            with open(input_file, 'wb') as f:
                f.write(json_data)
        
        # 调用绘图脚本
        cmd = f'python3 plot_worker.py {input_file} {output_file}'
        return_code = os.system(cmd)
        
        if return_code != 0:
            return f"Plot generation failed with code {return_code}", 500
        
        # 检查输出文件是否存在
        if not os.path.exists(output_file):
            return "Plot file not generated", 500
        
        # 返回生成的图像
        response = send_file(
            output_file,
            mimetype='image/png',
            as_attachment=True,
            download_name='ppi_plot.png'
        )
        
        return response
        
    except Exception as e:
        return f"Error: {str(e)}", 400
        
    finally:
        # 清理临时文件（延迟删除以确保文件已发送）
        def cleanup():
            time.sleep(2)  # 等待2秒确保文件已发送
            try:
                if input_file and os.path.exists(input_file):
                    os.remove(input_file)
                if output_file and os.path.exists(output_file):
                    os.remove(output_file)
            except:
                pass
        
        # 在后台线程中清理
        import threading
        threading.Thread(target=cleanup, daemon=True).start()



if __name__ == '__main__':
    logger.info("启动TXT到ZIP转换服务（内存优化版）...")
    logger.info("服务地址: http://0.0.0.0:5001")
    app.run(host='0.0.0.0', port=5001, debug=False)
