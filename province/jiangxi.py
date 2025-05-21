import urllib3
# 忽略 SSL 验证警告（可选）
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
#江西
import asyncio
import json
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

def fetch_jiangxi_event_data():
    url = "https://gzcx.jt.jiangxi.gov.cn/news/news/pageByCode.do"

    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Connection": "keep-alive",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Cookie": "SECKEY_ABVK=ypYAjCV5MemW+PtQAT9U2LIDl0cFQoRq+po/yRMEfR0%3D",
        "Host": "gzcx.jt.jiangxi.gov.cn",
        "Origin": "https://gzcx.jt.jiangxi.gov.cn",
        "Referer": "https://gzcx.jt.jiangxi.gov.cn/lk_news.html?lkType=0",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
        "sec-ch-ua": '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Linux"'
    }

    # 表单数据（以示例的 `type=sslk` 为主）
    data = {
        "current": "1",
        "size": "10",
        "type": "sslk",
        "search": ""
    }

    response = requests.post(url, headers=headers, data=data, verify=False)

    # 打印响应状态和内容
    print(response.status_code)
    # print(response.json())

    return response.json()

def run_task():
    try:
        logging.info("开始爬取江西省高速路况信息...")
        dify_worker = DifyWorkFlowProcessor(
            base_url="http://dify.datasw.cn/v1/workflows/run",
            api_key="app-Rx2ssBC8fOWg0hxT2488Vjmz"
        )
        data = None
        retry_count = 0
        while retry_count < 100:
            try:
                data = fetch_jiangxi_event_data()
                data = data['data']['records']
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
                #只有实时事件
                event_category = EventCategory.REALTIME.description
                    
                valid_dict = {
                    "province": "江西",
                    "roadCode": item.get("roadcode"), #如果没有会返回None
                    "roadName": item.get("road_name"),
                    "publish_content": item.get("content"),
                    "publish_time": item.get("publishTime"),
                    "start_time": item.get("blockstarttime"),
                    "end_time": item.get("blockexpecttime"),
                    "event_type_name": None,
                    "event_category": event_category,
                }
                
                valid_data.append(valid_dict)

            except Exception as e:
                error_log.append({
                    "error": str(e),
                    "data": item
                })
            
        # 调用dify工作流对valid_data进行事件类型的判断
        post_str = ''
        for idx, item in enumerate(valid_data):
            content = item.get("publish_content", "").strip()
            if "\n" in content:
                content = content.replace("\n", " ")  # 确保每个事件描述在一行
            post_str += content + "\n"  # 按行区分，确保顺序对得上
        logging.info(f"post_str: {post_str}")
        # 校验环节，确保第idx个item对应第idx行
        post_lines = post_str.strip().split("\n")
        if len(post_lines) != len(valid_data):
            raise ValueError("valid_data的长度与post_str的行数不一致")
        
        # 启动Dify工作流
        try:
            logging.info("调用Dify工作流进行事件类型判断...")
            ans= dify_worker.run_workflow(post_str)
            # print(type(ans))
            # print(ans.keys())
            data = ans.get("data", [])
            class_name_list = data["outputs"]["class_name"]
            road_name_list = data["outputs"]["road_name"]
            road_code_list = data["outputs"]["road_code"]
            print(f"Dify: class_name_list: {class_name_list}")
            for idx, item in enumerate(class_name_list):
                if item is not None:
                    valid_data[idx]["event_type_name"] = item
                else:
                    raise ValueError(f"第 {idx} 行数据的 event_type_name 为空")
                if road_name_list[idx] is not None:
                    valid_data[idx]["roadName"] = road_name_list[idx]
                else:
                    raise ValueError(f"第 {idx} 行数据的 road_name 为空")
                if road_code_list[idx] is not None:
                    valid_data[idx]["roadCode"] = road_code_list[idx]
                else:
                    raise ValueError(f"第 {idx} 行数据的 road_code 为空")
        except Exception as e:
            logging.error(f"Dify工作流调用失败: {e}")
            raise RuntimeError("Dify工作流调用失败")

        logging.info(f"成功采集江西高速路况：{len(valid_data)} 条，异常：{len(error_log)} 条")
        save_to_file(valid_data, "jiangxi")
        # save_to_file(error_log, "jiangxi_error")

        config = load_config()
        conn = get_mysql_connection(config['mysql'])
        insert_traffic_data("江西", valid_data, conn)
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
