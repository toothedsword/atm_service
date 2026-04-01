#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试剖面图生成服务
"""

import requests
import json
import sys

def test_slice_service(config_file, output_file='slice_plot.png'):
    """
    测试剖面图生成服务
    
    参数:
        config_file: 配置文件路径
        output_file: 输出文件名
    """
    url = 'http://localhost:5001/api/slice'
    
    # 读取配置文件
    with open(config_file, 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    print(f"发送请求到: {url}")
    print(f"配置: {json.dumps(config, indent=2, ensure_ascii=False)}")
    
    # 发送POST请求
    response = requests.post(url, json=config)
    
    if response.status_code == 200:
        # 保存返回的图片或zip文件
        content_type = response.headers.get('Content-Type', '')
        
        if 'image/png' in content_type:
            with open(output_file, 'wb') as f:
                f.write(response.content)
            print(f"✓ 成功生成剖面图: {output_file}")
        elif 'application/zip' in content_type:
            zip_file = output_file.replace('.png', '.zip')
            with open(zip_file, 'wb') as f:
                f.write(response.content)
            print(f"✓ 成功生成多个剖面图: {zip_file}")
        else:
            print(f"未知的内容类型: {content_type}")
            with open('output.bin', 'wb') as f:
                f.write(response.content)
    else:
        print(f"✗ 请求失败: {response.status_code}")
        try:
            error = response.json()
            print(f"错误信息: {error}")
        except:
            print(f"响应内容: {response.text}")

def test_service_info():
    """测试服务信息接口"""
    url = 'http://localhost:5001/api/info'
    response = requests.get(url)
    
    if response.status_code == 200:
        info = response.json()
        print("服务信息:")
        print(json.dumps(info, indent=2, ensure_ascii=False))
    else:
        print(f"获取服务信息失败: {response.status_code}")

def main():
    if len(sys.argv) < 2:
        print("用法:")
        print("  测试服务信息: python3 test_slice_service.py info")
        print("  生成剖面图: python3 test_slice_service.py <config.json> [output.png]")
        sys.exit(1)
    
    if sys.argv[1] == 'info':
        test_service_info()
    else:
        config_file = sys.argv[1]
        output_file = sys.argv[2] if len(sys.argv) > 2 else 'slice_plot.png'
        test_slice_service(config_file, output_file)

if __name__ == '__main__':
    main()
