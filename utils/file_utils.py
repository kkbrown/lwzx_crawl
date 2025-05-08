import os
from datetime import datetime
import json

def save_to_file(data, province=""):
    timestamp = datetime.now().strftime("%Y%m%d%H%M")

    # 当前文件的路径：hebei.py 所在目录
    base_dir = os.path.dirname(os.path.abspath(__file__))
    directory = os.path.abspath(os.path.join(base_dir, "../files"))
    os.makedirs(directory, exist_ok=True)

    filename = os.path.join(directory, f"{province}_{timestamp}.json")
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print(f"数据已保存到 {filename}")