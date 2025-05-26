import json
import asyncio
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode
from crawl4ai.extraction_strategy import JsonCssExtractionStrategy
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

async def fetch_heilongjiang_event_data():
    # 1. Define a simple extraction schema
    schema = {
        "name": "Traffic Reports",
        "baseSelector": "div.div-cells[style*='background-color']",
        "fields": [
            {
            "name": "message",
            "selector": "div:nth-of-type(2)",
            "type": "text"
            },
            {
            "name": "publish_time",
            "selector": "div:nth-of-type(3)",
            "type": "text"
            }
        ]
    }

    # 2. Create the extraction strategy
    extraction_strategy = JsonCssExtractionStrategy(schema, verbose=True)

    # 3. Set up your crawler config (if needed)
    config = CrawlerRunConfig(
        # e.g., pass js_code or wait_for if the page is dynamic
        # wait_for="css:.crypto-row:nth-child(20)"
        cache_mode = CacheMode.BYPASS,
        extraction_strategy=extraction_strategy,
    )

    async with AsyncWebCrawler(verbose=True) as crawler:
        # 4. Run the crawl and extraction
        result = await crawler.arun(
            url="http://www.hlj12328.com/dataQueryWebsite/LoadRoadConditionsServlet",

            config=config
        )

        if not result.success:
            print("Crawl failed:", result.error_message)
            return

        # 5. Parse the extracted JSON
        data = json.loads(result.extracted_content) # type: list[dict]
        # print(f"Extracted {len(data)} coin entries")
        # print(data[0:3])
    
    return data[0:5] # 只提取前5条数据

# asyncio.run(extract_crypto_prices())

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

async def run_task():
    try:
        logging.info("开始爬取黑龙江省高速路况信息...")
        dify_worker = DifyWorkFlowProcessor(
            base_url="http://dify.datasw.cn/v1/workflows/run",
            api_key="app-mjTAaUFsiYvdzoTd25bYi0OP"
        )
        data = None
        retry_count = 0
        while retry_count < 100:
            try:
                data = await fetch_heilongjiang_event_data()
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
                # # 判断type_cat 和 type_name
                # event_type_name_raw = item.get("message")
                # # print(f"event_type_name_raw: {event_type_name_raw}")
                # event_type_name = classify_event_type(event_type_name_raw)
                
                #只有实时事件
                # event_category = EventCategory.REALTIME.description
                    
                valid_dict = {
                    "province": "黑龙江",
                    "roadCode": item.get("roadcode"), #如果没有会返回None
                    "roadName": item.get("road_name"),
                    "publish_content": item.get("message"),
                    "publish_time": item.get("publish_time"),
                    "start_time": item.get("blockstarttime"),
                    "end_time": item.get("blockexpecttime"),
                    "event_type_name": item.get("event_type"),
                    "event_category": item.get("event_category"),
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
            road_name_list = data["outputs"]["road_name"]
            start_time_list = data["outputs"]["StartTimeList"]
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
        except Exception as e:
            logging.error(f"Dify工作流调用失败: {e}")
            raise RuntimeError("Dify工作流调用失败")

        logging.info(f"成功采集黑龙江高速路况：{len(valid_data)} 条，异常：{len(error_log)} 条")
        save_to_file(valid_data, "heilongjiang")

        # config = load_config()
        # conn = get_mysql_connection(config['mysql'])
        # insert_traffic_data("", valid_data, conn)
        # conn.close()
    except Exception as e:
        logging.error(f"任务执行失败: {e}")


def schedule_loop():
    logging.info("定时任务已启动：每30分钟执行一次")
    while True:
        asyncio.run(run_task())
        logging.info("等待30分钟...")
        time.sleep(30 * 60)


if __name__ == "__main__":
    # schedule_loop()
    asyncio.run(run_task())

