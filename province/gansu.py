#甘肃
import asyncio
import json
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, BrowserConfig, CacheMode
from crawl4ai.extraction_strategy import JsonCssExtractionStrategy
from crawl4ai.deep_crawling.scorers import KeywordRelevanceScorer
from crawl4ai.deep_crawling import DFSDeepCrawlStrategy, BestFirstCrawlingStrategy, BFSDeepCrawlStrategy
from crawl4ai.deep_crawling.filters import (
    FilterChain,
    DomainFilter,
    URLPatternFilter,
    ContentTypeFilter
)
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


async def fetch_links():
    # 页面信息爬取设置
    schema = {
        "name": "Traffic Info",
        "baseSelector": "div.articlelist > ul > li", # Selects the list items within the article list
        "fields": [
            {
                "name": "time", # Field for the article title
                "selector": ".addtime", # Selects the h2 tag
                "type": "text" # Extracts the text content
            },
            {
                "name": "link", #事件类型
                "selector": "a",
                "type": "attribute",
                "attribute": "href",
            },
        ]
    }
    config = CrawlerRunConfig(
        #深度爬取设置
        # deep_crawl_strategy=strategy,
        stream=False,
        #页面信息设置
        # No caching for demonstration
        cache_mode=CacheMode.BYPASS,
        markdown_generator=DefaultMarkdownGenerator(),
#        target_elements=[".detailMain"],
        #css_selector=".portlet",
        # # Extraction strategy
        extraction_strategy=JsonCssExtractionStrategy(schema)
    )
    browser_config = BrowserConfig()


    async with AsyncWebCrawler(config=browser_config) as crawler:
        results = await crawler.arun(
            url="https://www.gsgl.org.cn/cxlk/index.html",
            config=config
        )

        # print(f"Crawled {len(results)} pages in total")
        # logging.info(results)
        data_list = []
        for result in results:
            # print(f"URL: {result.url}")
            # print(f"Depth: {result.metadata.get('depth', 0)}")
            # print(type(result.extracted_content))  #str  json
            item_dict = json.loads(result.extracted_content)
            # if len(item_dict)>0:
            #     # print(f"Extracted content: {result.extracted_content}")
            #     data_list.append(item_dict[0])
            # print("Sample extracted item:", data)  # Show first item
            # print(f"item_dict: {item_dict}")
    return item_dict[:10]

async def fetch_gansu_event_data():
    link_list = await fetch_links()
    # link_list = [
    #     {
    #         "time": "2023-10-01 12:00",
    #         "link": "https://www.gsgl.org.cn/cxlk/17439.html"
    #     }
    # ]
    # 页面信息爬取设置
    schema = {
        "name": "Traffic Info",
        "baseSelector": ".showcontent", # Selects the list items within the article list
        "fields": [
            {
                "name": "event", # Field for the article title
                "type": "text", # Extracts the text content
            },
        ]
    }
    config = CrawlerRunConfig(
        #深度爬取设置
        # deep_crawl_strategy=strategy,
        stream=False,
        #页面信息设置
        # No caching for demonstration
        cache_mode=CacheMode.BYPASS,
        markdown_generator=DefaultMarkdownGenerator(),
#        target_elements=[".detailMain"],
        #css_selector=".portlet",
        # # Extraction strategy
        extraction_strategy=JsonCssExtractionStrategy(schema)
    )
    browser_config = BrowserConfig()
    data_list = []
    for item in link_list:
        time = item.get("time", "")
        link = item.get("link", "")
        # 把http改为https
        if link.startswith("http://"):
            link = link.replace("http://", "https://")
        async with AsyncWebCrawler(config=browser_config) as crawler:
            results = await crawler.arun(
                url=link,
                config=config
            )
            # print(f"Crawled {len(results)} pages in total")
            # logging.info(results)
            
            for result in results:
                # print(type(result.extracted_content))  #str  json
                item_dict = json.loads(result.extracted_content)
                if len(item_dict)>0:
                    # print(f"Extracted content: {result.extracted_content}")
                    item_dict[0]["publish_time"] = time
                    event_raw_text = item_dict[0].get("event", "")
                    stop_keyword = "分享到"
                    # 查找 "分享到" 关键词的位置
                    keyword_index = event_raw_text.find(stop_keyword)
                    relevant_text = ""
                    if keyword_index != -1:
                        # 如果找到了关键词，截取关键词之前的部分
                        relevant_text = event_raw_text[:keyword_index]
                    else:
                        # 如果没有找到关键词，则认为整个文本都是相关的
                        relevant_text = event_raw_text
                    item_dict[0]["event"] = relevant_text.strip()
                    data_list.append(item_dict[0])
                # print("Sample extracted item:", data)  # Show first item
                # print(f"item_dict: {item_dict}")
    return data_list

async def run_task():
    try:
        logging.info("开始爬取甘肃省高速路况信息...")
        dify_worker = DifyWorkFlowProcessor(
            base_url="http://dify.datasw.cn/v1/workflows/run",
            api_key="app-Rx2ssBC8fOWg0hxT2488Vjmz" # v2.0
        )
        classify_dify_worker = DifyWorkFlowProcessor(
            base_url="http://dify.datasw.cn/v1/workflows/run",
            api_key="app-InBOnqEJt7gTApviIoTKumXn"
        )
        extract_dify_worker = DifyWorkFlowProcessor(
            base_url="http://dify.datasw.cn/v1/workflows/run",
            api_key="app-32JgLE9KStrlnjrQMzqEwLA6"
        )
        data = None
        retry_count = 0
        while retry_count < 100:
            try:
                data = await fetch_gansu_event_data()
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
        # for item in data:
        #     logging.info(item)
        for item in data:
            try:
                # 判断type_cat 和 type_name
                content = item.get("event")
                time = item.get("publish_time", "")

                logging.info(f"检查数据库中是否存在省份甘肃、发布时间{time}的事件")
                config = load_config()
                conn = get_mysql_connection(config['mysql'])
                existing_count = 0
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT COUNT(*) FROM province_road_condition WHERE province=%s AND publish_time=%s", 
                        ("甘肃", time)
                    )
                    existing_count = cursor.fetchone()[0]
                conn.close()
                if existing_count != 0:
                    logging.info(f"已存在 {existing_count} 条省份甘肃、发布时间{time}的事件，跳过当前记录")
                    continue

                ans= classify_dify_worker.run_workflow(content)
                data = ans.get("data", [])["outputs"]["text"]
                if data == "不包含路况事件":
                    logging.info(f"跳过不包含路况事件: {content}")
                    continue

                data = extract_dify_worker.run_workflow(content)
                extracted_events = data.get("data", [])["outputs"]["text"]
                # 把extracted_events按行分割成列表，并去除可能的空行
                extracted_event_list = [event.strip() for event in extracted_events.split('\n') if event.strip()]
                for event in extracted_event_list:
                    valid_dict = {
                        "province": "甘肃",
                        "roadCode": None, #如果没有会返回None
                        "roadName": None,
                        "publish_content": event,
                        "publish_time": time,
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
        # for data in valid_data:
        #     logging.info(f"valid_data: {data}")
        # 调用dify工作流对valid_data进行事件类型的判断
        # 如果valid_data为空，直接返回
        if len(valid_data) == 0:
            logging.info("没有有效的路况事件数据，跳过后续处理")
            return
        
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
            road_name_list = data["outputs"]["road_name"]
            road_code_list = data["outputs"]["road_code"]
            # print(f"Dify: class_name_list: {class_name_list}")
            for idx, item in enumerate(valid_data):
                valid_data[idx]["event_type_name"] = class_name_list[idx]
                valid_data[idx]["roadName"] = road_name_list[idx] if road_name_list[idx] != '' else None
                valid_data[idx]["roadCode"] = road_code_list[idx] if road_code_list[idx] != '' else None
                # print(f"{valid_data[idx]}")
        except Exception as e:
            logging.error(f"Dify工作流调用失败: {e}")
            raise RuntimeError("Dify工作流调用失败")

        logging.info(f"成功采集甘肃高速路况：{len(valid_data)} 条，异常：{len(error_log)} 条")
        save_to_file(valid_data, "gansu")
        # save_to_file(error_log, "gansu_error")

        

        config = load_config()
        conn = get_mysql_connection(config['mysql'])
        insert_traffic_data("甘肃", valid_data, conn)
        conn.close()
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
    # asyncio.run(fetch_gansu_event_data())
    asyncio.run(run_task())
