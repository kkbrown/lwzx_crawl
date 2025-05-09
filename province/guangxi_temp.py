import logging
import time
import requests
from datetime import datetime
from condition_enum.event_category import EventCategory
from utils.file_utils import save_to_file
from utils.info_extract import extract_first_highway, classify_event_type
from dbconnection.db import load_config, insert_traffic_data, get_mysql_connection

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)


def fetch_guangxi_event_data():
    url = "http://221.7.196.191:8090/travel/server/lkxx/selectLkxxListByType"
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Content-Type': 'application/json; charset=UTF-8',
        'Referer': 'http://221.7.196.191:9002/',
        'Origin': 'http://221.7.196.191:9002',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
    }
    # 临时类事件
    payload = {"blockReasonRootId": 3}
    response = requests.post(url, headers=headers, json=payload, verify=False)
    response.raise_for_status()
    return response.json()


 # 事件分类：
 # 1. 实时事件：发布时间在当日，开始时间在当日，结束时间在当日
 # 2. 计划事件：发布时间在当日，开始时间在当日，结束时间跨日
def run_task():
    try:
        logging.info("开始采集广西临时类高速事件...")
        raw = fetch_guangxi_event_data()

        if not raw or "data" not in raw or not isinstance(raw["data"], list):
            raise ValueError("接口响应格式异常")

        data = raw["data"]
        valid_data = []
        error_log = []

        for item in data:
            try:
                publish_time_str = item.get("fillTime")
                start_time_str = item.get("discoverTime")
                end_time_str = item.get("estiTime")

                dt_publish = datetime.strptime(publish_time_str, "%Y-%m-%d %H:%M:%S") if publish_time_str else None
                dt_start = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S") if start_time_str else None
                dt_end = datetime.strptime(end_time_str, "%Y-%m-%d %H:%M:%S") if end_time_str else None

                if dt_publish and dt_start:
                    end_of_publish_day = datetime.combine(dt_publish.date(), datetime.max.time())

                    if dt_publish > dt_start and dt_end and dt_end <= end_of_publish_day:
                        event_category = EventCategory.REALTIME.description
                    else:
                        event_category = EventCategory.PLAN.description
                else:
                    event_category = EventCategory.PLAN.description

                new_item = {
                    "province": "广西",
                    "roadCode": item.get("roadNo"),
                    "roadName": item.get("roadName"),
                    "start_time": start_time_str,
                    "end_time": end_time_str,
                    "publish_time": publish_time_str,
                    "publish_content": item.get("description"),
                    "event_category": event_category,
                    "event_type_name": classify_event_type(item.get("blockReasonParentId")),
                }

                valid_data.append(new_item)

            except Exception as e:
                error_log.append({"error": str(e), "data": item})

        logging.info(f"广西采集临时类事件完成：成功 {len(valid_data)} 条，失败 {len(error_log)} 条")
        save_to_file(valid_data, "guangxi_temp")

        config = load_config()
        conn = get_mysql_connection(config['mysql'])
        insert_traffic_data("广西", valid_data, conn)
        conn.close()
    except Exception as e:
        logging.error(f"广西任务执行失败: {e}")


def schedule_loop():
    logging.info("定时任务启动，每30分钟执行一次")
    while True:
        run_task()
        logging.info("等待30分钟...")
        time.sleep(30 * 60)


if __name__ == "__main__":
    import urllib3

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    schedule_loop()
