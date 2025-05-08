import requests
import json
import logging
import time
from datetime import datetime
from utils.file_utils import save_to_file
from dbconnection.db import load_config, insert_station_data, get_mysql_connection

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)


def fetch_baidu_toll_data():
    url = "https://jiaotong.baidu.com/trafficindex/toll/list?nodeId=0&provinceCode=0"
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Connection': 'keep-alive',
        'Referer': 'https://jiaotong.baidu.com/trafficindex/toll/list?nodeId=0&provinceCode=0'
    }

    logging.info("请求百度交通收费站数据...")
    response = requests.get(url, headers=headers)
    logging.info(f"响应状态码: {response.status_code}")
    logging.debug(f"响应内容: {response.text[:200]}")

    if response.status_code == 200:
        try:
            data = response.json()
            if data.get("status") == 0 and "data" in data:
                results = data["data"]
                logging.info(f"成功获取 {len(results)} 条收费站数据")
                return results
            else:
                raise ValueError(f"返回数据异常: {data}")
        except json.JSONDecodeError as e:
            raise ValueError(f"解析 JSON 数据失败: {e}")
    else:
        raise ConnectionError(f"请求失败，状态码: {response.status_code}")


def process_data(results):
    rename_map = {
        "dataTime": "publish_time",
        "provinceName": "province_name",
        "cityName": "city_name",
        "roadName": "road_name",
        "name": "station_name",
        "congestLengthInarow34": "congest_length",
        "avgSpeed": "avg_speed",
    }

    rank = 1
    for item in results:
        item["station_rank"] = rank
        item["batch_num"] = datetime.now().strftime("%Y%m%d%H%M%S")
        rank += 1

        for old_key, new_key in rename_map.items():
            if old_key in item:
                if old_key == "dataTime":
                    time_str = str(item[old_key])
                    if len(time_str) == 12:
                        item[new_key] = datetime.strptime(time_str, "%Y%m%d%H%M").strftime("%Y-%m-%d %H:%M:%S")
                else:
                    # 处理 name->station_name 并在这里就加上入口/出口
                    if old_key == "name":
                        station_name = item.pop(old_key)
                        if "inOut" in item:
                            if item["inOut"] == 1:
                                station_name += "(入口)"
                            elif item["inOut"] == 2:
                                station_name += "(出口)"
                        item[new_key] = station_name
                    else:
                        item[new_key] = item.pop(old_key)

    return results


def run_task():
    try:
        results = fetch_baidu_toll_data()
        process_data(results)
        save_to_file(results, "station")
        config = load_config()
        conn = get_mysql_connection(config['mysql'])
        insert_station_data(results, conn)
        conn.close()
        logging.info("本次任务执行完成。")
    except Exception as e:
        logging.error(f"任务执行失败: {e}")


def schedule_loop():
    logging.info("定时任务已启动，每 5 分钟执行一次...")
    while True:
        run_task()
        logging.info("等待 5 分钟后继续...")
        time.sleep(5 * 60)  # 每 5 分钟执行一次


if __name__ == "__main__":
    import urllib3

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    schedule_loop()
