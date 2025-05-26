#北京
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





async def fetch_beijing_event_data():
    # 二级页面设置
    filter_chain = FilterChain([
        # Domain boundaries
        #DomainFilter(
        #    allowed_domains=["jtyst.qinghai.gov.cn"],
            #blocked_domains=["old.docs.example.com"]
       # ),
        # URL patterns to include
        URLPatternFilter(patterns=["*travelInformationDetail/*"]),

        # Content type filtering
        ContentTypeFilter(allowed_types=["text/html"])
    ])
    # Create a relevance scorer
    scorer = KeywordRelevanceScorer(
        keywords=["id"],
        weight=0.7
    )
    # Configure the strategy
    strategy = BestFirstCrawlingStrategy(
        max_depth=1,
        filter_chain=filter_chain,
        include_external=False,
        # url_scorer=scorer,
        max_pages=11,              # Maximum number of pages to crawl (optional)
    )

    # 页面信息爬取设置
    schema = {
        "name": "Traffic Info",
        "baseSelector": ".article_m",
        "fields": [
            {
                "name": "title", # Field for the article title
                "selector": "h2", # Selects the h2 tag
                "type": "text" # Extracts the text content
            },
            {
                "name": "publish_time", # Field for the publication time
                "selector": "h4 span:nth-of-type(2)", # Selects the second span within the h4 tag
                "type": "text" # Extracts the text content
            },
            {
                "name": "content", # Field for the article content
                "selector": ".article_i p:first-of-type", # Selects the first p tag within the div with class 'article_i'
                "type": "text" # Extracts the text content
            }
        ]
    }
    config = CrawlerRunConfig(
        #深度爬取设置
        deep_crawl_strategy=strategy,
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
            url="https://service.jtw.beijing.gov.cn/uservice/app/service/travelInformationMain",
            config=config
        )

        # print(f"Crawled {len(results)} pages in total")
        # Access individual results
        data_list = []
        for result in results:
            # print(f"URL: {result.url}")
            # print(f"Depth: {result.metadata.get('depth', 0)}")
            # print(type(result.extracted_content))  #str  json
            item_dict = json.loads(result.extracted_content)
            # print(type(item_dict))
            if len(item_dict)>0:
                # print(f"Extracted content: {result.extracted_content}")
                data_list.append(item_dict[0])
            # print("Sample extracted item:", data)  # Show first item
            # print(f"item_dict: {item_dict}")
    return data_list

async def run_task():
    try:
        logging.info("开始爬取北京省高速路况信息...")
        dify_worker = DifyWorkFlowProcessor(
            base_url="http://dify.datasw.cn/v1/workflows/run",
            api_key="app-mjTAaUFsiYvdzoTd25bYi0OP" # v3.0
        )
        data = None
        retry_count = 0
        while retry_count < 100:
            try:
                data = await fetch_beijing_event_data()
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
                event_type_name_raw = item.get("title")
                
                event_category = EventCategory.REALTIME.description
                
                valid_dict = {
                    "province": "北京",
                    "roadCode": None, #如果没有会返回None
                    "roadName": None,
                    "publish_content": item.get("content"),
                    "publish_time": item.get("publish_time"),
                    "start_time": None,
                    "end_time": None,
                    "event_type_name": event_type_name_raw,
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
            catecory_list = data["outputs"]["CategoryList"]
            start_time_list = data["outputs"]["StartTimeList"]
            end_time_list = data["outputs"]["EndTimeList"]
            # print(f"Dify: class_name_list: {class_name_list}")
            for idx, item in enumerate(valid_data):
                if valid_data[idx]["event_type_name"] == "交通事故":
                    valid_data[idx]["event_category"] = EventCategory.REALTIME.description
                    valid_data[idx]["event_type_name"] = EventType.ACCIDENT.description
                elif valid_data[idx]["event_type_name"] == "公路运行":
                    valid_data[idx]["event_category"] = EventCategory.REALTIME.description
                    valid_data[idx]["event_type_name"] = class_name_list[idx]
                elif valid_data[idx]["event_type_name"] == "占道施工":
                    valid_data[idx]["event_category"] = catecory_list[idx]
                    valid_data[idx]["event_type_name"] = EventType.MAINTENANCE.description
                valid_data[idx]["roadName"] = road_name_list[idx] if road_name_list[idx] != '' else None
                valid_data[idx]["roadCode"] = road_code_list[idx] if road_code_list[idx] != '' else None
                valid_data[idx]["start_time"] = start_time_list[idx] if start_time_list[idx] != '' else None
                valid_data[idx]["end_time"] = end_time_list[idx] if end_time_list[idx] != '' else None
                # print(f"{valid_data[idx]}")
        except Exception as e:
            logging.error(f"Dify工作流调用失败: {e}")
            raise RuntimeError("Dify工作流调用失败")

        logging.info(f"成功采集北京高速路况：{len(valid_data)} 条，异常：{len(error_log)} 条")
        save_to_file(valid_data, "beijing")
        # save_to_file(error_log, "beijing_error")

        config = load_config()
        conn = get_mysql_connection(config['mysql'])
        insert_traffic_data("北京", valid_data, conn)
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
    # asyncio.run(fetch_beijing_event_data())
    asyncio.run(run_task())
