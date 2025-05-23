import urllib3
# 忽略 SSL 验证警告（可选）
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
#福建
import asyncio
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


from urllib.parse import urlencode

def fetch_fujian_event_data(ndex=1, size=5, eventtype=1006001):
    """
    安全爬取“闽通宝”福建交通信息接口
    """
    # ---------- ① 目标 URL 和 Referer ----------
    api_url = "https://mintongbao.mgskj.com/GSTFuJianWeChatAPI/Sqlserver/getTrafficMessage"
    referer = "https://mintongbao.mgskj.com/GSTFuJianWeChatAPI/Home/TrafficInformation"

    # ---------- ② 公共请求头（尽量和浏览器保持一致） ----------
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
        ),
        "Accept": "*/*",
        "Content-Type": "application/x-www-form-urlencoded",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://mintongbao.mgskj.com",
        "Referer": referer,
    }

    def build_payload(ndex, size, eventtype):
        """
        按照 x-www-form-urlencoded 要求封装参数
        """
        return {
            "index": ndex,
            "size": size,
            "eventtype": eventtype
        }

    # ---------- ③ 发请求（带 Session，先 GET 获取 Cookie/WAF 验证） ----------
    with requests.Session() as s:
        # 第一步：通过 Referer 页面让服务器下发最新的 WAF/Cookie
        s.get(referer, headers=headers, timeout=10)

        # 第二步：POST 调接口，data 接受已经 urlencode 的字符串
        payload = build_payload(ndex, size, eventtype)
        resp = s.post(api_url, data=payload, headers=headers, timeout=10)
        resp.raise_for_status()

        # 返回解析后的 JSON 数据
        return resp.json()


# def fetch_fujian_event_data(index=1, size=5, eventtype=1006001):
#     url = "https://mintongbao.mgskj.com/GSTFuJianWeChatAPI/Sqlserver/getTrafficMessage"
#     payload = {
#         "roadoldid": "",
#         "index": index,
#         "size": size,
#         "eventtype": eventtype,  #1006001-交通事件  1006002-道路施工
#         "search": "",
#     }
#     # ——请求头——（完整复制浏览器发出的那些）
#     headers = {
#         "Accept": "*/*",
#         "Accept-Encoding": "gzip, deflate, br, zstd",
#         "Accept-Language": "zh-CN,zh;q=0.9",
#         "Connection": "keep-alive",
#         "Content-Type": "application/x-www-form-urlencoded",
#         "Host": "mintongbao.mgskj.com",
#         "Origin": "https://mintongbao.mgskj.com",
#         "Referer": "https://mintongbao.mgskj.com/GSTFuJianWeChatAPI/Home/TrafficInformation",
#         "Sec-Fetch-Dest": "empty",
#         "Sec-Fetch-Mode": "cors",
#         "Sec-Fetch-Site": "same-origin",
#         "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
#         "X-Requested-With": "XMLHttpRequest",
#         # 如果你的环境需要带更多 sec-ch-ua*，也可以一并加上
#     }

#     try:
#         resp = requests.post(url, data=payload, headers=headers, timeout=10)
#         resp.raise_for_status()  # 如果返回码不是200，会抛出异常
#     except requests.RequestException as e:
#         print(f"请求失败：{e}")
#         return None
#     try:
#         return resp.json()
#     except ValueError:
#         print("响应不是有效的JSON：", resp.text)
#         return None


def run_task():
    try:
        logging.info("开始爬取福建省高速路况信息...")
        dify_worker = DifyWorkFlowProcessor(
            base_url="http://dify.datasw.cn/v1/workflows/run",
            api_key="app-mjTAaUFsiYvdzoTd25bYi0OP"
        )
        data_acc = None
        data_plan = None
        retry_count = 0
        while retry_count < 5:
            try:
                #交通事件
                data_acc = fetch_fujian_event_data(size=5, eventtype=1006001)
                data_acc = data_acc['data']
                #施工事件
                data_plan = fetch_fujian_event_data(size=3, eventtype=1006002)
                data_plan = data_plan['data']
                if (isinstance(data_acc, list) and len(data_acc) > 0) or (isinstance(data_plan, list) and len(data_plan) > 0):
                    break
                else:
                    raise ValueError("返回数据为空或格式异常")
            except Exception as e:
                retry_count += 1
                logging.warning(f"第 {retry_count} 次尝试失败：{e}")
                time.sleep(10)
        if data_acc is None and data_plan is None:
            raise RuntimeError("重试失败，终止本轮任务")
        

        valid_data = []
        error_log = []
        # logging.info(data)
        for item in data_plan:
            try:        
                #data_plan 为施工事件，需要分析预期
                event_category = EventCategory.REALTIME.description
                    
                valid_dict = {
                    "province": "福建",
                    "roadCode": item.get("roadoldcode"), #如果没有会返回None
                    "roadName": item.get("title"),
                    "publish_content": item.get("remark"),
                    "publish_time": item.get("updatetime"),
                    "start_time": item.get("occtime"),
                    "end_time": item.get("blockexpecttime"),
                    "event_type_name": EventType.MAINTENANCE.description,
                    "event_category": None,
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
        # logging.info(f"post_str: {post_str}")
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
            # print(f"Dify: class_name_list: {class_name_list}")
            for idx, item in enumerate(class_name_list):
                if item is not None:
                    valid_data[idx]["event_type_name"] = item
                else:
                    raise ValueError(f"第 {idx} 行数据的 event_type_name 为空")
                if cat_name_list[idx] is not None:
                    valid_data[idx]["event_category"] = cat_name_list[idx]
                else:
                    raise ValueError(f"第 {idx} 行数据的 road_name 为空")
                if end_time_list[idx] is not None:
                    valid_data[idx]["end_time"] = end_time_list[idx]
                else:
                    raise ValueError(f"第 {idx} 行数据的 road_code 为空")
        except Exception as e:
            logging.error(f"Dify工作流调用失败: {e}")
            raise RuntimeError("Dify工作流调用失败")
        
        # 处理交通事件
        for item in data_acc:
            try:        
                #data_acc 只有实时事件，且为交通事故
                event_category = EventCategory.REALTIME.description
                    
                valid_dict = {
                    "province": "福建",
                    "roadCode": item.get("roadoldcode"), #如果没有会返回None
                    "roadName": item.get("title"),
                    "publish_content": item.get("remark"),
                    "publish_time": item.get("updatetime"),
                    "start_time": item.get("occtime"),
                    "end_time": item.get("blockexpecttime"),
                    "event_type_name": EventType.ACCIDENT.description,
                    "event_category": event_category,
                }
                
                valid_data.append(valid_dict)

            except Exception as e:
                error_log.append({
                    "error": str(e),
                    "data": item
                })

        logging.info(f"成功采集福建高速路况：{len(valid_data)} 条，异常：{len(error_log)} 条")
        save_to_file(valid_data, "fujian")
        # save_to_file(error_log, "fujian_error")

        # return

        config = load_config()
        conn = get_mysql_connection(config['mysql'])
        insert_traffic_data("福建", valid_data, conn)
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
    # print(fetch_fujian_event_data(eventtype=1006002))
    schedule_loop()
    # run_task()
