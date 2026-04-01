#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
独立的数据转换工作进程
每次转换完成后进程退出，释放所有内存
"""

import json
import struct
import numpy as np
import zipfile
from io import BytesIO
import sys
import argparse
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def convert_json_to_binary_format(params, json_data):
    """将JSON数据转换为4D numpy数组和header"""
    if json_data.get('code') != 200:
        raise ValueError(f"API返回错误: {json_data.get('message', '未知错误')}")
    
    data_list = json_data.get('data', [])
    if not data_list:
        raise ValueError("数据为空")
    
    times = len(data_list)
    first_data = np.array(data_list[0]['data'], dtype=np.float32)
    y_size, x_size = first_data.shape
    
    levels = 1
    level_list = ['0']
    
    data_4d = np.zeros((times, levels, y_size, x_size), dtype=np.float32)
    
    time_list = []
    for t, time_data in enumerate(data_list):
        time_list.append(time_data['time'])
        data_array = np.array(time_data['data'], dtype=np.float32)
        data_4d[t, 0, :, :] = data_array
    
    min_lat = float(params.get('minLat', 40.0))
    max_lat = float(params.get('maxLat', 41.0))
    min_lon = float(params.get('minLon', 115.5))
    max_lon = float(params.get('maxLon', 116.8))
    
    lat_array = np.linspace(min_lat, max_lat, y_size).tolist()
    lon_array = np.linspace(min_lon, max_lon, x_size).tolist()
    
    data_min = float(np.min(data_4d))
    data_max = float(np.max(data_4d))
    
    header = {
        "dataType": "Float32",
        "dataScale": 1.0,
        "dataOffset": 0.0,
        "xSize": x_size,
        "ySize": y_size,
        "levels": levels,
        "times": times,
        "levelList": level_list,
        "timeList": time_list,
        "units": params.get('element', 'unknown'),
        "element": params.get('element', 'unknown'),
        "dataCode": params.get('dataCode', 'RISE'),
        "xStart": min_lon,
        "xEnd": max_lon,
        "xDelta": (max_lon - min_lon) / (x_size - 1) if x_size > 1 else 0,
        "yStart": min_lat,
        "yEnd": max_lat,
        "yDelta": (max_lat - min_lat) / (y_size - 1) if y_size > 1 else 0,
        "lon": lon_array,
        "lat": lat_array,
        "nx": x_size,
        "ny": y_size,
        "dataMin": data_min,
        "dataMax": data_max
    }
    
    return data_4d, header


def convert_surface_data(json_data):
    """转换地面数据"""
    if json_data.get('code') != 200:
        raise ValueError(f"API返回错误: {json_data.get('message', '未知错误')}")
    
    data_2d = json_data.get('data', [])
    if not data_2d:
        raise ValueError("数据为空")
    
    element = json_data.get('element', 'surface_data')
    dataCode = json_data.get('dataCode', 'SURFACE')
    datetime_str = json_data.get('datetime', '2024010100')
    min_lat = float(json_data.get('minLat', 40.0))
    max_lat = float(json_data.get('maxLat', 41.0))
    min_lon = float(json_data.get('minLon', 115.5))
    max_lon = float(json_data.get('maxLon', 116.8))
    
    data_array = np.array(data_2d, dtype=np.float32)
    y_size, x_size = data_array.shape
    
    data_4d = np.zeros((1, 1, y_size, x_size), dtype=np.float32)
    data_4d[0, 0, :, :] = data_array
    
    lat_array = np.linspace(min_lat, max_lat, y_size).tolist()
    lon_array = np.linspace(min_lon, max_lon, x_size).tolist()
    
    data_min = float(np.min(data_4d))
    data_max = float(np.max(data_4d))
    
    header = {
        "dataType": "Float32",
        "dataScale": 1.0,
        "dataOffset": 0.0,
        "xSize": x_size,
        "ySize": y_size,
        "levels": 1,
        "times": 1,
        "levelList": ['0'],
        "timeList": [datetime_str],
        "units": element,
        "element": element,
        "dataCode": dataCode,
        "xStart": min_lon,
        "xEnd": max_lon,
        "xDelta": (max_lon - min_lon) / (x_size - 1) if x_size > 1 else 0,
        "yStart": min_lat,
        "yEnd": max_lat,
        "yDelta": (max_lat - min_lat) / (y_size - 1) if y_size > 1 else 0,
        "lon": lon_array,
        "lat": lat_array,
        "nx": x_size,
        "ny": y_size,
        "dataMin": data_min,
        "dataMax": data_max
    }
    
    return data_4d, header


def convert_height_data(json_data):
    """转换高度层数据"""
    if json_data.get('code') != 200:
        raise ValueError(f"API返回错误: {json_data.get('message', '未知错误')}")
    
    height_data_list = json_data.get('data', [])
    if not height_data_list:
        raise ValueError("数据为空")
    
    element = json_data.get('element', 'height_data')
    dataCode = json_data.get('dataCode', 'HEIGHT')
    datetime_str = json_data.get('datetime', '2024010100')
    min_lat = float(json_data.get('minLat', 40.0))
    max_lat = float(json_data.get('maxLat', 41.0))
    min_lon = float(json_data.get('minLon', 115.5))
    max_lon = float(json_data.get('maxLon', 116.8))
    
    levels = len(height_data_list)
    level_list = []
    
    first_layer = np.array(height_data_list[0]['data'], dtype=np.float32)
    y_size, x_size = first_layer.shape
    
    data_4d = np.zeros((1, levels, y_size, x_size), dtype=np.float32)
    
    for level_idx, layer_info in enumerate(height_data_list):
        height = layer_info.get('height', level_idx)
        level_list.append(str(height))
        
        data_array = np.array(layer_info['data'], dtype=np.float32)
        data_4d[0, level_idx, :, :] = data_array
    
    lat_array = np.linspace(min_lat, max_lat, y_size).tolist()
    lon_array = np.linspace(min_lon, max_lon, x_size).tolist()
    
    data_min = float(np.min(data_4d))
    data_max = float(np.max(data_4d))
    
    header = {
        "dataType": "Float32",
        "dataScale": 1.0,
        "dataOffset": 0.0,
        "xSize": x_size,
        "ySize": y_size,
        "levels": levels,
        "times": 1,
        "levelList": level_list,
        "timeList": [datetime_str],
        "units": element,
        "element": element,
        "dataCode": dataCode,
        "xStart": min_lon,
        "xEnd": max_lon,
        "xDelta": (max_lon - min_lon) / (x_size - 1) if x_size > 1 else 0,
        "yStart": min_lat,
        "yEnd": max_lat,
        "yDelta": (max_lat - min_lat) / (y_size - 1) if y_size > 1 else 0,
        "lon": lon_array,
        "lat": lat_array,
        "nx": x_size,
        "ny": y_size,
        "dataMin": data_min,
        "dataMax": data_max
    }
    
    return data_4d, header


def convert_time_series_data(json_data):
    """转换时间序列数据"""
    if json_data.get('code') != 200:
        raise ValueError(f"API返回错误: {json_data.get('message', '未知错误')}")
    
    time_data_list = json_data.get('data', [])
    if not time_data_list:
        raise ValueError("数据为空")
    
    element = json_data.get('element', 'time_series')
    dataCode = json_data.get('dataCode', 'TIMESERIES')
    min_lat = float(json_data.get('minLat', 40.0))
    max_lat = float(json_data.get('maxLat', 41.0))
    min_lon = float(json_data.get('minLon', 115.5))
    max_lon = float(json_data.get('maxLon', 116.8))
    
    first_data = np.array(time_data_list[0]['data'], dtype=np.float32)
    y_size, x_size = first_data.shape
    times = len(time_data_list)
    
    data_4d = np.zeros((times, 1, y_size, x_size), dtype=np.float32)
    
    time_list = []
    for t, time_info in enumerate(time_data_list):
        time_str = time_info.get('time', f'time_{t}')
        time_list.append(time_str)
        
        data_array = np.array(time_info['data'], dtype=np.float32)
        data_4d[t, 0, :, :] = data_array
    
    lat_array = np.linspace(min_lat, max_lat, y_size).tolist()
    lon_array = np.linspace(min_lon, max_lon, x_size).tolist()
    
    data_min = float(np.min(data_4d))
    data_max = float(np.max(data_4d))
    
    header = {
        "dataType": "Float32",
        "dataScale": 1.0,
        "dataOffset": 0.0,
        "xSize": x_size,
        "ySize": y_size,
        "levels": 1,
        "times": times,
        "levelList": ['0'],
        "timeList": time_list,
        "units": element,
        "element": element,
        "dataCode": dataCode,
        "xStart": min_lon,
        "xEnd": max_lon,
        "xDelta": (max_lon - min_lon) / (x_size - 1) if x_size > 1 else 0,
        "yStart": min_lat,
        "yEnd": max_lat,
        "yDelta": (max_lat - min_lat) / (y_size - 1) if y_size > 1 else 0,
        "lon": lon_array,
        "lat": lat_array,
        "nx": x_size,
        "ny": y_size,
        "dataMin": data_min,
        "dataMax": data_max
    }
    
    return data_4d, header


def save_to_zip_file(data_4d, header, output_path):
    """将数据保存为zip文件"""
    header_json = json.dumps(header, separators=(',', ':'))
    header_bytes = header_json.encode('utf-8')
    header_length = len(header_bytes)
    
    data_converted = data_4d.astype(np.float32)
    
    # 直接写入文件，避免在内存中创建中间BytesIO
    with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        # 创建二进制数据
        binary_data = BytesIO()
        binary_data.write(struct.pack('<I', header_length))
        binary_data.write(header_bytes)
        binary_data.write(data_converted.tobytes(order='C'))
        
        binary_data.seek(0)
        zf.writestr('data.bin', binary_data.getvalue())
        
        # 立即清理
        binary_data.close()
        del binary_data
    
    logger.info(f"数据已保存到: {output_path}")


def main():
    parser = argparse.ArgumentParser(description='数据转换工作进程')
    parser.add_argument('--input', required=True, help='输入JSON文件路径')
    parser.add_argument('--output', required=True, help='输出ZIP文件路径')
    parser.add_argument('--type', required=True, 
                       choices=['txt', 'json', 'surface', 'height', 'time'],
                       help='转换类型')
    
    args = parser.parse_args()
    
    try:
        logger.info(f"开始转换: {args.type}")
        logger.info(f"输入文件: {args.input}")
        logger.info(f"输出文件: {args.output}")
        
        # 读取输入数据
        with open(args.input, 'r', encoding='utf-8') as f:
            input_data = json.load(f)
        
        # 根据类型进行转换
        if args.type in ['txt', 'json']:
            params = input_data.get('params', {})
            json_data = input_data.get('json_data', {})
            data_4d, header = convert_json_to_binary_format(params, json_data)
        elif args.type == 'surface':
            data_4d, header = convert_surface_data(input_data)
        elif args.type == 'height':
            data_4d, header = convert_height_data(input_data)
        elif args.type == 'time':
            data_4d, header = convert_time_series_data(input_data)
        else:
            raise ValueError(f"不支持的转换类型: {args.type}")
        
        logger.info(f"数据维度: {data_4d.shape}")
        logger.info(f"数据范围: {header['dataMin']:.2f} - {header['dataMax']:.2f}")
        
        # 保存为zip文件
        save_to_zip_file(data_4d, header, args.output)
        
        logger.info("转换完成")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"转换失败: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == '__main__':
    main()