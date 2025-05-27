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


def fetch_chongqing_event_data():
    """
    获取重庆路障列表数据。

    :param limit: 每页返回的条目数，默认为20
    :param page: 页码，默认为1
    :return: 解析后的 JSON 数据（dict 或 list）
    """
    url = "https://jtj.cq.gov.cn/cx/zjcx/queryAllroadCtrlNews.html"
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/135.0.0.0 Safari/537.36',
    }

    response = requests.get(url, headers=headers, timeout=10)
    # print(response.json())
    data = response.json()
    response.raise_for_status()  # 若状态码不是200，会抛出 HTTPError
    return data["data"][:5]

def get_chongqing_date_time(code):
    """
    根据code获取重庆路况的时间
    :param code: 路况code
    :return: 时间字符串
    """
    url = "https://jtj.cq.gov.cn/cx/zjcx/queryDetailroadCtrlNews.html"
    params = {
        'code': code,
    }
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/135.0.0.0 Safari/537.36',
    }

    response = requests.get(url, params=params ,headers=headers, timeout=10)
    # print(response)
    # print(response.json())
    data = response.json()
    response.raise_for_status()  # 若状态码不是200，会抛出 HTTPError
    return data["data"][0].get("dateTime", "")  # 获取第一个事件的时间字符串

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



def run_task():
    try:
        logging.info("开始爬取重庆省高速路况信息...")
        dify_worker = DifyWorkFlowProcessor(
            base_url="http://dify.datasw.cn/v1/workflows/run",
            api_key="app-mjTAaUFsiYvdzoTd25bYi0OP"
        )
        data = None
        retry_count = 0
        while retry_count < 100:
            try:
                data = fetch_chongqing_event_data()
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
                
                # 只有实时事件
                event_category = EventCategory.REALTIME.description
                    
                valid_dict = {
                    "province": "重庆",
                    "roadCode": item.get("roadcode"),
                    "roadName": item.get("roadname"),
                    "publish_content": item.get("name"),
                    "publish_time": item.get("dateTime"),
                    "start_time": item.get("blockstarttime"),
                    "end_time": item.get("blockexpecttime"),
                    "event_type_name": item.get("event_type"),
                    "event_category": event_category,
                }
                # 根据code获取publish_time
                try:
                    code = item.get("code")
                    dateTime = get_chongqing_date_time(code)
                    valid_dict["publish_time"] = dateTime
                except Exception as e:
                    logging.warning(f"获取重庆路况时间失败: {e}")
                
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
            cat_name_list = data["outputs"]["CategoryList"]
            end_time_list = data["outputs"]["EndTimeList"]
            start_time_list = data["outputs"]["StartTimeList"]
            road_name_list = data["outputs"]["road_name"]
            road_code_list = data["outputs"]["road_code"]
            # print(f"Dify: class_name_list: {class_name_list}")
            for idx, item in enumerate(class_name_list):
                if item is not None:
                    valid_data[idx]["event_type_name"] = item
                else:
                    raise ValueError(f"第 {idx} 行数据的 event_type_name 为空")
                if cat_name_list[idx] is not None:
                    valid_data[idx]["event_category"] = cat_name_list[idx]
                else:
                    raise ValueError(f"第 {idx} 行数据的 event_category 为空")
                if end_time_list[idx] is not None:
                    valid_data[idx]["end_time"] = end_time_list[idx]
                else:
                    raise ValueError(f"第 {idx} 行数据的 end_time 为空")
                if end_time_list[idx] is not None:
                    valid_data[idx]["start_time"] = start_time_list[idx]
                else:
                    raise ValueError(f"第 {idx} 行数据的 start_time 为空")
                if road_name_list[idx] is not None:
                    valid_data[idx]["roadName"] = road_name_list[idx]
                else:
                    raise ValueError(f"第 {idx} 行数据的 road_name 为空")
                if road_code_list[idx] is not None:
                    valid_data[idx]["roadCode"] = road_code_list[idx]
                else:
                    raise ValueError(f"第 {idx} 行数据的 road_code 为空")
                # 再检查一下publish_time是否为空,如果为空则使用start_time
                if not valid_data[idx].get("publish_time"):
                    valid_data[idx]["publish_time"] = valid_data[idx].get("start_time", "")

        except Exception as e:
            logging.error(f"Dify工作流调用失败: {e}")
            raise RuntimeError("Dify工作流调用失败")

        logging.info(f"成功采集重庆高速路况：{len(valid_data)} 条，异常：{len(error_log)} 条")
        save_to_file(valid_data, "chongqing")

        # return
        config = load_config()
        conn = get_mysql_connection(config['mysql'])
        insert_traffic_data("重庆", valid_data, conn)
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
