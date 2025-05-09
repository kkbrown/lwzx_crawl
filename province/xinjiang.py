import logging
import time
import requests
from datetime import datetime
from utils.info_extract import extract_first_highway, classify_event_type

from condition_enum.event_category import EventCategory
from utils.file_utils import save_to_file
from dbconnection.db import load_config, insert_traffic_data, get_mysql_connection

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)


def fetch_xinjiang_event_data(limit=20, page=1):
    """
    获取新疆路障列表数据。

    :param limit: 每页返回的条目数，默认为20
    :param page: 页码，默认为1
    :return: 解析后的 JSON 数据（dict 或 list）
    """
    url = "https://cxfw.jtyst.xinjiang.gov.cn/api/gzcx/roadblock/app/list"
    params = {
        'limit': limit,
        'page': page
    }
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/135.0.0.0 Safari/537.36',
    }

    response = requests.get(url, params=params, headers=headers, timeout=10)
    # print(response.json())
    response.raise_for_status()  # 若状态码不是200，会抛出 HTTPError
    return response.json()

def run_task():
    try:
        logging.info("开始爬取新疆省高速路况信息...")
        data = None
        retry_count = 0
        while retry_count < 100:
            try:
                data = fetch_xinjiang_event_data()['page']['list']
                if isinstance(data, list) and len(data) > 0:
                    break
                else:
                    raise ValueError("返回数据为空或格式异常")
            except Exception as e:
                retry_count += 1
                logging.warning(f"第 {retry_count} 次尝试失败：{e}")
                time.sleep(10)
        if data is None:
            raise RuntimeError("重试失败，终止本轮任务")

        valid_data = []
        error_log = []
        for item in data:
            try:
                # 判断type_cat 和 type_name
                event_type_name_raw = item.get("blockreasonParent") + item.get("blockreasonChild")
                # print(f"event_type_name_raw: {event_type_name_raw}")
                event_type_name = classify_event_type(event_type_name_raw)
                
                if item.get("blockreasonParent","") == "计划性施工":
                    event_category = EventCategory.PLAN.description
                else:
                    event_category = EventCategory.REALTIME.description
                    
                valid_dict = {
                    "province": "新疆",
                    "roadCode": item.get("roadcode"),
                    "roadName": item.get("roadname"),
                    "publish_content": item.get("blockdesc"),
                    "publish_time": item.get("pubtime"),
                    "start_time": item.get("blockstarttime"),
                    "end_time": item.get("blockexpecttime"),
                    "event_type_name": event_type_name,
                    "event_category": event_category,
                }
                
                valid_data.append(valid_dict)

            except Exception as e:
                error_log.append({
                    "error": str(e),
                    "data": item
                })

        logging.info(f"成功采集新疆高速路况：{len(valid_data)} 条，异常：{len(error_log)} 条")
        save_to_file(valid_data, "xinjiang")

        config = load_config()
        conn = get_mysql_connection(config['mysql'])
        insert_traffic_data("新疆", valid_data, conn)
        conn.close()
    except Exception as e:
        logging.error(f"任务执行失败: {e}")


def schedule_loop():
    logging.info("定时任务已启动：每30分钟执行一次")
    while True:
        run_task()
        logging.info("等待30分钟...")
        time.sleep(30 * 60)


if __name__ == "__main__":
    schedule_loop()
    # run_task()
