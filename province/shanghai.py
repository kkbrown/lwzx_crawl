import logging
import requests
import time
from datetime import datetime
from utils.file_utils import save_to_file
from dbconnection.db import load_config, insert_traffic_data, get_mysql_connection
from condition_enum.event_category import EventCategory
from utils.info_extract import classify_event_type

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)


def get_shanghai_traffic_data():
    """获取上海交通数据"""
    url = 'https://eeca.jtw.sh.gov.cn/vcloud-api/vcloud/jam/queryJamList'
    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
    }

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"请求失败，状态码: {response.status_code}, 响应: {response.text[:200]}...")


def process_shanghai_data(results):
    rename_map = {
        "routeNo": "roadCode",
        "routeName": "roadName",
        "description": "publish_content",
        "eventDate": "publish_time",
        "startTime": "start_time",
        "endTime": "end_time",
    }

    results[:] = [item for item in results if item.get("routeNo") and item.get("routeName")]

    for item in results:
        try:
            for old_key, new_key in rename_map.items():
                if old_key in item:
                    item[new_key] = item.pop(old_key)

            event_date = item.get("publish_time", "")
            start_time = item.get("start_time", "")
            if event_date and start_time:
                try:
                    item["publish_time"] = f"{event_date} {start_time}"
                except Exception:
                    item["publish_time"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            item["start_time"] = None
            item["end_time"] = None

            item["province"] = "上海"
            item["event_category"] = EventCategory.REALTIME.description
            publish_content = item.get("publish_content", "").strip()

            item["event_type_name"] = classify_event_type(publish_content)

            item = {key: item[key] for key in
                    ["roadCode", "roadName", "publish_content", "publish_time", "province", "event_category",
                     "event_type_name"]}

        except Exception as e:
            logging.error(f"处理数据时出错: {e}, 错误数据: {item}")


def fetch_shanghai_traffic_data():
    try:
        logging.info("开始获取上海市交通数据...")
        traffic_data = get_shanghai_traffic_data()
        if traffic_data.get("code") == "200" and traffic_data.get("result"):
            logging.info("成功获取上海市交通数据")
            return traffic_data["result"]
        else:
            logging.warning(f"获取数据失败: {traffic_data.get('msg')}")
    except Exception as e:
        logging.error(f"获取上海市交通数据失败: {e}")
        return []


def run_task():
    try:
        results = fetch_shanghai_traffic_data()
        if results:
            process_shanghai_data(results)
            save_to_file(results, "shanghai")

            config = load_config()
            conn = get_mysql_connection(config['mysql'])
            insert_traffic_data("上海", results, conn)
            conn.close()

    except Exception as e:
        logging.error(f"任务执行失败: {e}")


def schedule_loop():
    logging.info("定时任务已启动，每30分钟执行一次...")
    while True:
        run_task()
        logging.info("等待30分钟后继续...")
        time.sleep(30 * 60)


if __name__ == "__main__":
    import urllib3

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    schedule_loop()
