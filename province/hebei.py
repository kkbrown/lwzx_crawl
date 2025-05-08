import logging
import os
import sys
import time
import requests
import re
from datetime import datetime

from condition_enum.event_category import EventCategory
from dbconnection.db import load_config, insert_traffic_data, get_mysql_connection
from utils.browser import get_session_hebei_cookie_with_selenium
from utils.file_utils import save_to_file
from utils.info_extract import classify_event_type

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)


def get_traffic_data(session_cookie):
    """获取交通数据"""
    timestamp = int(time.time() * 1000)
    url = f"http://www.hebecc.com/appTraffic/getTrafficByRoadCode.json?_={timestamp}"

    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Referer': 'http://www.hebecc.com/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest',
        'Proxy-Connection': 'keep-alive'
    }

    cookies = {'SESSION': session_cookie}

    response = requests.get(url, headers=headers, cookies=cookies, verify=False)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"请求失败，状态码: {response.status_code}, 响应: {response.text[:200]}...")


def fetch_with_retry():
    max_retry = 1000
    retry_delay = 10  # 秒
    retry_count = 0

    while retry_count < max_retry:
        try:
            session_cookie = get_session_hebei_cookie_with_selenium()
            logging.info("开始获取河北高速路况数据...")
            traffic_data = get_traffic_data(session_cookie)
            if traffic_data.get("code") == "20000" and traffic_data.get("result"):
                logging.info(f"成功获取数据，第 {retry_count + 1} 次尝试")
                return traffic_data["result"]
            else:
                logging.warning(
                    f"尝试第 {retry_count + 1} 次，失败：code={traffic_data.get('code')} msg={traffic_data.get('msg')}")
        except Exception as e:
            logging.warning(f"尝试第 {retry_count + 1} 次，异常: {e}")

        retry_count += 1
        time.sleep(retry_delay)

    raise Exception("重试次数过多，依然失败，终止任务")


def process_data(results):
    rename_map = {
        "actionResult": "publish_content",
        "startDate": "publish_time",
        "roadId": "roadCode",
    }

    for item in results:
        for old_key, new_key in rename_map.items():
            if old_key in item:
                item[new_key] = item.pop(old_key)
        item["province"] = "河北"
        raw_road_name = item.get("roadName", "")
        match = re.search(r"([\u4e00-\u9fa5]+高速)", raw_road_name)
        if match:
            item["roadName"] = match.group(1)
        else:
            item["roadName"] = raw_road_name
        item["start_time"] = None
        item["end_time"] = None
        item["event_category"] = EventCategory.REALTIME.description
        # 匹配事件类型
        event_type_raw = item.get("reason").strip()
        item["event_type_name"] = classify_event_type(event_type_raw)


def run_task():
    try:
        results = fetch_with_retry()
        process_data(results)
        save_to_file(results, "hebei")

        config = load_config()
        conn = get_mysql_connection(config['mysql'])
        insert_traffic_data("河北", results, conn)
        conn.close()

    except Exception as e:
        logging.error(f"任务执行失败: {e}")


def schedule_loop():
    logging.info("定时任务已启动，每30分钟执行一次...")
    while True:
        run_task()
        logging.info("等待30分钟后继续...")
        time.sleep(30 * 60)  # 每30分钟运行一次


if __name__ == "__main__":
    import urllib3

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    schedule_loop()
