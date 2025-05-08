import logging
import time
import requests
from datetime import datetime

from condition_enum.event_type import EventType

from condition_enum.station_type import StationStatus
from utils.file_utils import save_to_file
from utils.browser import get_zhejiang_token
from dbconnection.db import load_config, insert_traffic_data, get_mysql_connection

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)


def fetch_zhejiang_toll_status(token):
    url = "https://ddzlcx.jtyst.zj.gov.cn/zlcx2/hwTsStatus/getHwTsStatusPage?current=1&size=9999"
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin": "https://gzcx.jtyst.zj.gov.cn",
        "Referer": "https://gzcx.jtyst.zj.gov.cn/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
        "authorization": f"Bearer {token}"
    }
    payload = {"current": 1, "size": 9999}

    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    return response.json()


def run_task():
    try:
        logging.info("开始爬取浙江省高速收费站状态信息...")
        data = None
        retry_count = 0

        while retry_count < 10:
            try:
                token = get_zhejiang_token()
                raw = fetch_zhejiang_toll_status(token)
                if raw.get("code") == 100000 and isinstance(raw.get("data", {}).get("records"), list):
                    data = raw["data"]["records"]
                    break
                else:
                    raise ValueError("数据格式异常或 code 不为 100000")
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
                status_code = item.get("status")
                if status_code == 0:
                    status = StationStatus.NORMAL.label
                elif status_code == 1:
                    status = StationStatus.CLOSED.label
                elif status_code == 2:
                    status = StationStatus.LIMITED.label
                else:
                    status = StationStatus.UNKNOWN.label
                # 只保留不是“正常”状态的数据
                if status_code == 0:
                    continue
                record = {
                    "province": "浙江",
                    "region": item.get("region"),
                    "county": item.get("county"),
                    "section": item.get("highway"),
                    "location": item.get("tsName"),
                    "publish_content": item.get("tsName") + status,
                    "lat": item.get("lat"),
                    "lon": item.get("lon"),
                    "flow_in": item.get("rlamountIn", 0),
                    "flow_out": item.get("rlamountOut", 0),
                    "source": "浙江省交通运输厅",
                    "publish_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "reason": EventType.STATION.description,
                }
                valid_data.append(record)
            except Exception as e:
                error_log.append({
                    "error": str(e),
                    "data": item
                })

        logging.info(f"成功采集浙江高速收费站数据：{len(valid_data)} 条，异常：{len(error_log)} 条")
        save_to_file(valid_data, "zhejiang_station")

        config = load_config()
        conn = get_mysql_connection(config['mysql'])
        insert_traffic_data(valid_data, conn)
        conn.close()

    except Exception as e:
        logging.error(f"任务执行失败: {e}")


def schedule_loop():
    logging.info("浙江高速收费站定时任务已启动：每30分钟执行一次")
    while True:
        run_task()
        logging.info("等待30分钟...")
        time.sleep(30 * 60)


if __name__ == "__main__":
    schedule_loop()
