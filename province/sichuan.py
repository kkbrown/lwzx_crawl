import requests
import json
import logging
import time
import requests
from datetime import datetime
from utils.info_extract import extract_first_highway, classify_event_type

from condition_enum.event_category import EventCategory
from condition_enum.event_type import EventType
from utils.file_utils import save_to_file
from dbconnection.db import load_config, insert_traffic_data, get_mysql_connection
import re
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

class DifyWorkFlowProcessor:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url
        self.api_key = api_key

    def run_workflow(self, input_text):
        url = self.base_url
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        payload = {
            "inputs": {
                "event": input_text  # 根据实际 workflow 的 schema，这里的 key 可能需要替换
            },
            "response_mode": "blocking",  # 或 "blocking"，视实际需求
            "user": 'cbm'
        }

        response = requests.post(url, headers=headers, json=payload)

        if response.status_code == 200:
            # print(response.json())
            return response.json()
        else:
            print(f"Error: {response.status_code}")
            print(response.text)
            return {}

def fetch_road_info(num_size=2):
    url = 'https://182.150.21.163:4432/roadBlockInfo/unlockRoadAuth'
    params = {
        'ak': '1rfTYWTft62DF345YKkfr',
        'sk': 'ertcmttifRTfvsgr23Jkd',
        'page': 1,
    }
    headers = {
        'Host': '182.150.21.163:4432',
        'Origin': 'https://jtt.sc.gov.cn',
        'Referer': 'https://jtt.sc.gov.cn/',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        # 如果有必要，也可以带上 Cookie、Authorization 等
    }

    resp = requests.get(url, params=params, headers=headers, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    # 从 data 中取出 content 列表
    content_list = data.get('content', [])

    # 只取第一页的条目（前 num_size 条）
    first_page = content_list[-num_size:]
    # print("first_page:", first_page)
    out_pages = []
    for js in first_page:
        result = {
            "road": js["road"],
            "content": js["road"] + js["location"] + ":" + js["description"],
            "startTime": js["startTime"],
            "roadCode": js.get("nationalStandardCode", None),  # 如果没有 roadCode 则返回 None
        }
        out_pages.append(result)
    # 转成 JSON 字符串返回
    # return json.dumps(out_pages, ensure_ascii=False)
    return out_pages

def run_task():
    try:
        logging.info("开始爬取四川省高速路况信息...")
        dify_worker = DifyWorkFlowProcessor(
            base_url="http://dify.datasw.cn/v1/workflows/run",
            api_key="app-jM4Nb7wHCCJdh6vG4rtKRRon" # v1.0
        )
        data = None
        retry_count = 0
        while retry_count < 100:
            try:
                data = fetch_road_info(num_size=10)
                
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
        # logging.info(data)
        for item in data:
            try:
                # 判断type_cat 和 type_name
                event = item.get("content", "")
                road = item.get("road", "")
                event_time = item.get("startTime", "")
                road_code = item.get("roadCode", None)
                valid_dict = {
                    "province": "四川",
                    "roadCode": road_code, #如果没有会返回None
                    "roadName": road,
                    "publish_content": event,
                    "publish_time": event_time,
                    "start_time": None,
                    "end_time": None,
                    "event_type_name": None,
                    "event_category": EventCategory.REALTIME.description,
                }
                
                valid_data.append(valid_dict)

            except Exception as e:
                error_log.append({
                    "error": str(e),
                    "data": item
                })
        # print(f"valid_data: {valid_data}")
        # 调用dify工作流对valid_data进行事件类型的判断
        post_str = ''
        for idx, item in enumerate(valid_data):
            content = item.get("publish_content", "").strip()
            if "\n" in content:
                content = content.replace("\n", " ")  # 确保每个事件描述在一行
            post_str += content + "\n"  # 按行区分，确保顺序对得上
        # print(f"post_str: {post_str}")
        # 校验环节，确保第idx个item对应第idx行
        post_lines = post_str.strip().split("\n")
        if len(post_lines) != len(valid_data):
            raise ValueError("valid_data的长度与post_str的行数不一致")
        
        # 启动Dify工作流
        try:
            ans= dify_worker.run_workflow(post_str)
            # print(type(ans))
            # print(ans.keys())
            data = ans.get("data", [])
            # print(f"Dify: data: {data}")
            class_name_list = data["outputs"]["class_name"]
            # print(f"Dify: class_name_list: {class_name_list}")
            for idx, item in enumerate(valid_data):
                valid_data[idx]["event_type_name"] = class_name_list[idx]
                # valid_data[idx]["start_time"] = start_time_list[idx] if start_time_list[idx] != '' else None
                # valid_data[idx]["end_time"] = end_time_list[idx] if end_time_list[idx] != '' else None
                # print(f"{valid_data[idx]}")
        except Exception as e:
            logging.error(f"Dify工作流调用失败: {e}")
            raise RuntimeError("Dify工作流调用失败")

        logging.info(f"成功采集四川高速路况：{len(valid_data)} 条，异常：{len(error_log)} 条")
        save_to_file(valid_data, "sichuan")
        # save_to_file(error_log, "sichuan_error")

        config = load_config()
        conn = get_mysql_connection(config['mysql'])
        insert_traffic_data("四川", valid_data, conn)
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
    # fetch_road_info(num_size=10)
    # run_task()