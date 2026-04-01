"""
Flask API服务 - app.py
负责接收请求并调用绘图脚本
"""
from flask import Flask, request, send_file
import os
import tempfile
import uuid
import time

app = Flask(__name__)

# 临时文件目录
TEMP_DIR = '/tmp/lidar_ppi'
os.makedirs(TEMP_DIR, exist_ok=True)

@app.route('/api/plot', methods=['POST'])
def plot():
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
        cmd = f'python plot_worker.py {input_file} {output_file}'
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

@app.route('/health', methods=['GET'])
def health():
    """健康检查接口"""
    return {"status": "ok", "service": "Lidar PPI Visualization API"}

@app.route('/cleanup', methods=['POST'])
def cleanup():
    """清理临时文件"""
    try:
        count = 0
        for filename in os.listdir(TEMP_DIR):
            file_path = os.path.join(TEMP_DIR, filename)
            # 删除超过1小时的文件
            if os.path.isfile(file_path) and time.time() - os.path.getmtime(file_path) > 3600:
                os.remove(file_path)
                count += 1
        return {"status": "ok", "cleaned": count}
    except Exception as e:
        return {"status": "error", "message": str(e)}, 500

if __name__ == '__main__':
    print("=" * 60)
    print("激光雷达PPI数据可视化API服务")
    print("=" * 60)
    print("服务地址: http://localhost:5001")
    print("API端点: POST /api/plot")
    print("临时目录:", TEMP_DIR)
    print("-" * 60)
    print("使用示例:")
    print("  curl -X POST http://localhost:5001/api/plot \\")
    print("    -F 'file=@/path/to/your/ppi.json' \\")
    print("    -o /tmp/output.png")
    print("=" * 60)
    app.run(host='0.0.0.0', port=5001, debug=False)