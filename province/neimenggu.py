import logging
import time
import requests
from datetime import datetime
from bs4 import BeautifulSoup

from utils.info_extract import extract_first_highway, classify_event_type
from condition_enum.event_category import EventCategory
from utils.file_utils import save_to_file
from dbconnection.db import load_config, insert_traffic_data, get_mysql_connection

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)


def clean_html(raw_html):
    soup = BeautifulSoup(raw_html, "html.parser")
    return soup.get_text(strip=True)


def fetch_inner_mongolia_event_data():
    url = "https://jtyst.nmg.gov.cn/traffic_data_interactive/tra/place/road/news/select"
    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Referer": "https://jtyst.nmg.gov.cn/public/page/case/public/travel/realtime",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/136.0.0.0 Safari/537.36",
    }
    params = {"pageIndex": 1, "pageSize": 100}
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json().get("data", [])


def run_task():
    try:
        logging.info("开始爬取内蒙古高速路况信息...")
        data = None
        for attempt in range(10):
            try:
                data = fetch_inner_mongolia_event_data()
                if data:
                    break
            except Exception as e:
                logging.warning(f"第 {attempt + 1} 次尝试失败：{e}")
                time.sleep(10)

        if not data:
            logging.error("连续重试失败，终止本轮任务")
            return

        valid_data = []
        for item in data:
            try:
                record = {
                    "province": "内蒙古",
                    "publish_time": item.get("publishTime"),
                    "publish_content": clean_html(item.get("content", "")),
                    "event_category": EventCategory.REALTIME.description,
                    "start_time": None,
                    "end_time": None,
                    "roadCode": item.get("sectionCode", ""),
                    "roadName": item.get("sectionName", ""),
                }
                record["event_type_name"] = classify_event_type(record["publish_content"])

                # 如果包含“畅通”，跳过该条
                if "畅通" in record["publish_content"]:
                    continue

                if not record["roadCode"] or not record["roadName"]:
                    info = extract_first_highway(record["publish_content"])
                    record["roadCode"] = info["road_code"]
                    record["roadName"] = info["road_name"]

                valid_data.append(record)
            except Exception as e:
                logging.warning(f"处理数据异常：{e}")

        logging.info(f"成功采集内蒙古路况：{len(valid_data)} 条")
        save_to_file(valid_data, "neimenggu")

        config = load_config()
        conn = get_mysql_connection(config['mysql'])
        insert_traffic_data("内蒙古", valid_data, conn)
        conn.close()

    except Exception as e:
        logging.error(f"任务执行失败: {e}")


def schedule_loop():
    logging.info("内蒙古定时任务已启动：每30分钟执行一次")
    while True:
        run_task()
        logging.info("等待30分钟...")
        time.sleep(30 * 60)


if __name__ == "__main__":
    import urllib3

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    schedule_loop()
