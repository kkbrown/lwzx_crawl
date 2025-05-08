import logging
import os
import sys
import time
import requests
import re

from condition_enum.event_category import EventCategory
from dbconnection.db import load_config, insert_traffic_data, get_mysql_connection
from utils.file_utils import save_to_file
from utils.browser import get_zhejiang_token
from utils.info_extract import classify_event_type

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)


def get_zhejiang_data(token):
    url = "https://ddzlcx.jtyst.zj.gov.cn/zlcx2/hwEventInfo/getAllEvent"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0",
        "Origin": "https://gzcx.jtyst.zj.gov.cn",
        "Referer": "https://gzcx.jtyst.zj.gov.cn/"
    }

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        result = response.json()
        if result["code"] == 100000:
            return result["data"]
        else:
            raise Exception(f"接口返回错误: {result.get('msg')}")
    else:
        raise Exception(f"请求失败：{response.status_code} {response.text}")


def process_data(results):
    rename_map = {
        "content": "publish_content",
        "occurTime": "publish_time",
        "hwid": "roadCode",
        "hwname": "roadName",
    }

    for item in results:
        for old_key, new_key in rename_map.items():
            if old_key in item:
                item[new_key] = item.pop(old_key)
        item["province"] = "浙江"
        item["start_time"] = None
        item["end_time"] = None
        item["event_category"] = EventCategory.REALTIME.description
        # 匹配事件类型
        event_type_raw = item.get("publish_content").strip()
        item["event_type_name"] = classify_event_type(event_type_raw)


def run_zhejiang_task():
    try:
        logging.info("开始获取浙江高速事件数据...")
        token = get_zhejiang_token()
        data = get_zhejiang_data(token)
        process_data(data)
        save_to_file(data, "zhejiang")

        config = load_config()
        conn = get_mysql_connection(config['mysql'])
        insert_traffic_data("浙江", data, conn)
        conn.close()

        logging.info("浙江数据处理完成")
    except Exception as e:
        logging.error(f"任务执行失败: {e}")


def schedule_loop():
    logging.info("定时任务已启动，每30分钟执行一次...")
    while True:
        run_zhejiang_task()
        logging.info("等待30分钟后继续...")
        time.sleep(1800)


if __name__ == "__main__":
    schedule_loop()
