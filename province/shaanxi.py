#陕西
import asyncio
import json
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, BrowserConfig, CacheMode
from crawl4ai.extraction_strategy import JsonCssExtractionStrategy
from crawl4ai.deep_crawling.scorers import KeywordRelevanceScorer
from crawl4ai.deep_crawling import DFSDeepCrawlStrategy, BestFirstCrawlingStrategy
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





async def fetch_shaanxi_event_data():
    # 二级页面设置
    filter_chain = FilterChain([
        # Domain boundaries
        #DomainFilter(
        #    allowed_domains=["jtyst.qinghai.gov.cn"],
            #blocked_domains=["old.docs.example.com"]
       # ),
        # URL patterns to include
        URLPatternFilter(patterns=["*id/[0-9]*"]),

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
        url_scorer=scorer,
        max_pages=11,              # Maximum number of pages to crawl (optional)
    )

    # 页面信息爬取设置
    schema = {
        "name": "Traffic Info",
        "baseSelector": ".detail-content-article",
        "fields": [
            {
                "name": "event_type", #事件类型
                "selector": "input#pinfotype",
                "type": "attribute",
                "attribute": "value"
            },
            {
                "name": "publish_time",
                "selector": "input#startdate",
                "type": "attribute",
                "attribute": "value"
            },
            {
                "name": "road_name",
                "selector": "input#roadname",
                "type": "attribute",
                "attribute": "value"
            },
            {
                "name": "publish_content",
                "selector": "tr:nth-child(10) td:nth-child(2)",
                "type": "text"
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
            url="https://sxsjtt.sxtm.com/sfzx/index/api_lk_list?page=1",
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

    return data_list

async def run_task():
    try:
        logging.info("开始爬取陕西省高速路况信息...")
        dify_worker = DifyWorkFlowProcessor(
            base_url="http://dify.datasw.cn/v1/workflows/run",
            api_key="app-jM4Nb7wHCCJdh6vG4rtKRRon"
        )
        data = None
        retry_count = 0
        while retry_count < 100:
            try:
                data = await fetch_shaanxi_event_data()
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
                event_type_name_raw = item.get("event_type")
                # print(f"event_type_name_raw: {event_type_name_raw}")
                event_type_name = classify_event_type(event_type_name_raw)
                
                #只有实时事件
                event_category = EventCategory.REALTIME.description
                    
                valid_dict = {
                    "province": "陕西",
                    "roadCode": item.get("roadcode"), #如果没有会返回None
                    "roadName": item.get("road_name"),
                    "publish_content": item.get("publish_content"),
                    "publish_time": item.get("publish_time"),
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
        print(f"post_str: {post_str}")
        # 校验环节，确保第idx个item对应第idx行
        post_lines = post_str.strip().split("\n")
        if len(post_lines) != len(valid_data):
            raise ValueError("valid_data的长度与post_str的行数不一致")
        
        # 启动Dify工作流
        try:
            ans= dify_worker.run_workflow(post_str)
            print(type(ans))
            # print(ans.keys())
            data = ans.get("data", [])
            class_name_list = data["outputs"]["class_name"]
            print(f"Dify: class_name_list: {class_name_list}")
            for idx, item in enumerate(class_name_list):
                if item is not None:
                    valid_data[idx]["event_type_name"] = item
                else:
                    raise ValueError(f"第 {idx} 行数据的 event_type_name 为空")
        except Exception as e:
            logging.error(f"Dify工作流调用失败: {e}")
            raise RuntimeError("Dify工作流调用失败")

        logging.info(f"成功采集陕西高速路况：{len(valid_data)} 条，异常：{len(error_log)} 条")
        save_to_file(valid_data, "shaanxi")
        # save_to_file(error_log, "shaanxi_error")

        config = load_config()
        conn = get_mysql_connection(config['mysql'])
        insert_traffic_data("陕西", valid_data, conn)
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
    schedule_loop()
    # asyncio.run(run_task())
