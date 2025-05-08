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


def fetch_shandong_event_data():
    url = "https://96659.sdhsg.com/intelligencewxxcxV2XL/manorJtMap/getProvinceEvtInfo"
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Referer': 'https://96659.sdhsg.com/front/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/135.0.0.0 Safari/537.36',
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def run_task():
    try:
        logging.info("开始爬取山东省高速路况信息...")
        data = None
        retry_count = 0
        while retry_count < 100:
            try:
                data = fetch_shandong_event_data()
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
                item["province"] = "山东"
                ts = item.get("occurTime") or item.get("ctime")
                if ts is not None:
                    dt = datetime.fromtimestamp(ts / 1000)
                    item["publish_time"] = dt.strftime("%Y-%m-%d %H:%M:%S")
                elif item.get("eventId"):
                    try:
                        time_str = item['eventId'][:14]
                        dt = datetime.strptime(time_str, "%Y%m%d%H%M%S")
                        item["publish_time"] = dt.strftime("%Y-%m-%d %H:%M:%S")
                    except Exception:
                        raise ValueError(f"eventId 时间格式错误: {item['eventId']}")
                else:
                    raise ValueError("缺少 occurTime/ctime/eventId")

                item["publish_content"] = item.pop("content", "")
                item["event_category"] = EventCategory.REALTIME.description
                item["start_time"] = None
                item["end_time"] = None
                # 匹配事件类型
                event_type_raw = item.get("eventTypeName") or item.get("controlEventTypeName", "").strip()
                item["event_type_name"] = classify_event_type(event_type_raw)

                # 山东高速数据来源不一致，如果没有 roadCode/roadName，则尝试手动从 publish_content 中提取
                if not item.get("roadCode") or not item.get("roadName"):
                    info = extract_first_highway(item["publish_content"])
                    item["roadCode"] = info["road_code"]
                    item["roadName"] = info["road_name"]
                valid_data.append(item)

            except Exception as e:
                error_log.append({
                    "error": str(e),
                    "data": item
                })

        logging.info(f"成功采集山东高速路况：{len(valid_data)} 条，异常：{len(error_log)} 条")
        save_to_file(valid_data, "shandong")

        config = load_config()
        conn = get_mysql_connection(config['mysql'])
        insert_traffic_data("山东", valid_data, conn)
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
