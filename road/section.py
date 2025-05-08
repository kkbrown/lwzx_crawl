import requests
import json
import logging
import time
from datetime import datetime
from utils.file_utils import save_to_file
from dbconnection.db import load_config, insert_section_data, get_mysql_connection

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)


def fetch_baidu_highway_rank():
    url = "https://jiaotong.baidu.com/trafficindex/overview/highwayroadrank?cityCode=0&top=100"
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Connection': 'keep-alive',
        'Referer': 'https://jiaotong.baidu.com/congestion/country/highway'
    }

    logging.info("请求百度交通高速排行数据...")
    response = requests.get(url, headers=headers)

    logging.info(f"响应状态码: {response.status_code}")
    logging.debug(f"响应内容: {response.text[:200]}")

    if response.status_code == 200:
        try:
            data = response.json()
            if data.get("status") == 0 and "data" in data:
                results = data["data"]
                logging.info(f"成功获取 {len(results)} 条高速拥堵数据")
                return results
            else:
                raise ValueError(f"返回数据异常: {data}")
        except json.JSONDecodeError as e:
            raise ValueError(f"解析 JSON 数据失败: {e}")
    else:
        raise ConnectionError(f"请求失败，状态码: {response.status_code}")


def process_data(results):
    rename_map = {
        "time": "publish_time",
        "provinceName": "province_name",
        "congestLength": "congest_length",
        "avgSpeed": "avg_speed"
    }

    rank = 1  # 初始化 rank 值
    for item in results:
        item["section_rank"] = rank  # 添加递增的 section_rank 字段
        item["batch_num"] = datetime.now().strftime("%Y%m%d%H%M%S")
        rank += 1

        for old_key, new_key in rename_map.items():
            if old_key in item:
                if old_key == "time":
                    time_str = str(item[old_key])
                    if len(time_str) == 12:
                        item[new_key] = datetime.strptime(time_str, "%Y%m%d%H%M").strftime("%Y-%m-%d %H:%M:%S")
                else:
                    item[new_key] = item.pop(old_key)

    return results


def run_task():
    try:
        results = fetch_baidu_highway_rank()
        process_data(results)
        save_to_file(results, "section")

        config = load_config()
        conn = get_mysql_connection(config['mysql'])
        insert_section_data(results, conn)
        conn.close()

        logging.info("任务执行完成")
    except Exception as e:
        logging.error(f"任务执行失败: {e}")


def schedule_loop():
    logging.info("定时任务已启动，每5分钟执行一次...")
    while True:
        run_task()
        logging.info("等待5分钟后继续...")
        time.sleep(5 * 60)


if __name__ == "__main__":
    import urllib3

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    schedule_loop()
