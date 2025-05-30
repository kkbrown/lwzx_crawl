#青海
import asyncio
import json
from urllib.parse import urljoin
import re
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, BrowserConfig, CacheMode
from crawl4ai.extraction_strategy import JsonCssExtractionStrategy
from crawl4ai.deep_crawling.scorers import KeywordRelevanceScorer
from crawl4ai.deep_crawling import DFSDeepCrawlStrategy, BestFirstCrawlingStrategy
from crawl4ai.deep_crawling import BFSDeepCrawlStrategy
from crawl4ai.content_scraping_strategy import LXMLWebScrapingStrategy
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

async def fetch_links():
    # 1. Define a simple extraction schema
    schema = {
        "name": "Traffic Info",
        "baseSelector": "ul.list > li",
        "fields": [
            {
                "name": "link", #事件类型
                "selector": "a",
                "type": "attribute",
                "attribute": "href",
            },
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
            url="https://jtyst.qinghai.gov.cn/jtyst/cxfw/lkxx/index.html",

            config=config
        )

        if not result.success:
            print("Crawl failed:", result.error_message)
            return

        # 5. Parse the extracted JSON
        data = json.loads(result.extracted_content) # type: list[dict]
        # print(f"Extracted {len(data)} coin entries")
        # print(data[0:3])
    
    return data[0:2] # 只提取前2个链接

async def fetch_qinghai_event_data():
    link_list = await fetch_links()
    # 1. Define a simple extraction schema
    schema = {
        "name": "Traffic Info",
        "baseSelector": "html",
        "fields": [
            {
                "name": "publish_date", # 发布日期
                "selector": "head > meta[name='createDate']",
                "type": "attribute",
                "attribute": "content"
            },
            {
                "name": "publish_content",
                "selector": ".detailCon.detailContent",
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
    data_list = []
    for item in link_list:
        # print(item["link"])
        prefix = "https://jtyst.qinghai.gov.cn/"
        if not item.get("link").startswith("https"):
            item["link"] = urljoin(prefix, item["link"])

        # 4. Run the crawl and extraction for each link
        async with AsyncWebCrawler(verbose=True) as crawler:
            
            result = await crawler.arun(
                url=item["link"], # 爬取每一个链接
                config=config
            )

            if not result.success:
                print("Crawl failed:", result.error_message)
                return

            # 5. Parse the extracted JSON
            data = json.loads(result.extracted_content) # type: list[dict]
            # print(f"Extracted {len(data)} coin entries")
            # print(data[0:3])
            data_list.extend(data)
    
    return data_list


def res_anaylze(res):
    # 把返回的每行事件信息拆分成list
    events = []
    for line in res.splitlines():
        if not isinstance(line, str):
            # print(f"跳过非字符串类型的行: {line}")
            continue
        content = line.strip()
        if not content:
            continue
        events.append({"content": content})
    return events


async def run_task():
    try:
        logging.info("开始爬取青海省高速路况信息...")
        dify_worker_for_classify = DifyWorkFlowProcessor(
            base_url="http://dify.datasw.cn/v1/workflows/run",
            api_key="app-mjTAaUFsiYvdzoTd25bYi0OP"
        )
        dify_worker_for_analyze = DifyWorkFlowProcessor(
            base_url="http://dify.datasw.cn/v1/workflows/run",
            api_key="app-32JgLE9KStrlnjrQMzqEwLA6"
        )
        
        # 获取原始数据，重试机制
        data = None
        retry_count = 0
        while retry_count < 100:
            try:
                data = await fetch_qinghai_event_data()
                if isinstance(data, list) and data:
                    break
                raise ValueError("返回数据为空或格式异常")
            except Exception as e:
                retry_count += 1
                logging.warning(f"第 {retry_count} 次尝试失败：{e}")
                time.sleep(10)
        if data is None:
            raise RuntimeError("重试失败，终止本轮任务")

        logging.info(f"成功下载青海高速路况数据：{len(data)} 条")
        valid_data = []
        error_log = []

        # 解析原始事件并构建 valid_data 列表
        for item in data:
            try:
                publish_date = item.get("publish_date", "")
                if not publish_date:
                    raise ValueError("publish_date 为空")

                #验重：查询数据库中相同省份和发布时间的事件数
                logging.info(f"检查数据库中是否存在省份青海、发布时间{publish_date}的事件")
                config = load_config()
                conn = get_mysql_connection(config['mysql'])
                existing_count = 0
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT COUNT(*) FROM province_road_condition WHERE province=%s AND publish_time=%s", 
                        ("青海", publish_date)
                    )
                    existing_count = cursor.fetchone()[0]
                conn.close()
                if existing_count != 0:
                    logging.info(f"已存在 {existing_count} 条省份青海、发布时间{publish_date}的事件，跳过当前记录")
                    continue
                
                
                logging.info(f"调用Dify_analyze工作流进行事件分析，内容长度：{len(item.get('publish_content', ''))}")
                ans = dify_worker_for_analyze.run_workflow(item.get("publish_content", ""))
                inc_str = ans.get("data", {}).get("outputs", {}).get("text", "")
                incident_list = res_anaylze(inc_str)

                for incident in incident_list:
                    valid_data.append({
                        "province": "青海",
                        "roadCode": incident.get("roadcode"),
                        "roadName": incident.get("road_name"),
                        "publish_content": incident.get("content"),
                        "publish_time": publish_date,
                        "start_time": incident.get("blockstarttime"),
                        "end_time": incident.get("blockexpecttime"),
                        "event_type_name": incident.get("event_type"),
                        "event_category": incident.get("event_category"),
                    })
            except Exception as e:
                logging.error(f"解析事件失败: {e}")
                error_log.append({"error": str(e), "data": item})

        # 构造 post_lines，用于分类调用
        logging.info(f"有效事件数量：{len(valid_data)}，异常事件数量：{len(error_log)}")
        post_lines = []
        for item in valid_data:
            content = item.get("publish_content", "").strip().replace("\n", " ")
            post_lines.append(content)

        if not post_lines:
            logging.info("没有有效事件，结束任务")
            return

        # 验证长度对应
        if len(post_lines) != len(valid_data):
            raise ValueError("valid_data 的长度与 post_lines 不一致")

        # 对 post_lines 分批，每批不超过 15 行
        max_lines = 15
        total = len(post_lines)
        logging.info(f"总共有 {total} 条数据，分批大小：{max_lines} 行")
        chunks = [post_lines[i:i + max_lines] for i in range(0, total, max_lines)]

        # 分批调用 Dify 分类工作流
        for batch_idx, chunk in enumerate(chunks):
            batch_str = "\n".join(chunk)
            try:
                logging.info(f"调用Dify分类工作流，批次 {batch_idx + 1}/{len(chunks)}，行数：{len(chunk)}")
                ans = dify_worker_for_classify.run_workflow(batch_str)
                outputs = ans.get("data", {}).get("outputs", {})

                class_list = outputs.get("class_name", [])
                cat_list = outputs.get("CategoryList", [])
                end_list = outputs.get("EndTimeList", [])
                start_list = outputs.get("StartTimeList", [])
                rn_list = outputs.get("road_name", [])
                rc_list = outputs.get("road_code", [])

                # 将每个批次结果映射回 valid_data
                for local_idx in range(len(chunk)):
                    global_idx = batch_idx * max_lines + local_idx
                    # event_type_name
                    if class_list and class_list[local_idx] is not None:
                        valid_data[global_idx]["event_type_name"] = class_list[local_idx]
                    else:
                        raise ValueError(f"第 {global_idx} 行数据的 event_type_name 为空")
                    # event_category
                    if cat_list and cat_list[local_idx] is not None:
                        valid_data[global_idx]["event_category"] = cat_list[local_idx]
                    else:
                        raise ValueError(f"第 {global_idx} 行数据的 event_category 为空")
                    # end_time
                    if end_list and end_list[local_idx] is not None:
                        valid_data[global_idx]["end_time"] = end_list[local_idx]
                    else:
                        raise ValueError(f"第 {global_idx} 行数据的 end_time 为空")
                    # start_time
                    if start_list and start_list[local_idx] is not None:
                        valid_data[global_idx]["start_time"] = start_list[local_idx]
                    else:
                        raise ValueError(f"第 {global_idx} 行数据的 start_time 为空")
                    # roadName
                    if rn_list and rn_list[local_idx] is not None:
                        valid_data[global_idx]["roadName"] = rn_list[local_idx]
                    else:
                        raise ValueError(f"第 {global_idx} 行数据的 road_name 为空")
                    # roadCode
                    if rc_list and rc_list[local_idx] is not None:
                        valid_data[global_idx]["roadCode"] = rc_list[local_idx]
                    else:
                        raise ValueError(f"第 {global_idx} 行数据的 road_code 为空")
                    
                    # 检查一下，计划事件必须有开始和结束时间
                    if valid_data[global_idx]["event_category"] == EventCategory.PLAN.description:
                        if not valid_data[global_idx]["start_time"]:
                            # 如果计划事件没有开始时间，就把发布时间作为开始时间
                            valid_data[global_idx]["start_time"] = valid_data[global_idx]["publish_time"]

            except Exception as e:
                logging.error(f"Dify 分类工作流批次 {batch_idx + 1} 调用失败: {e}")
                raise RuntimeError("Dify 分类工作流调用失败")

        logging.info(f"成功采集青海高速路况：{len(valid_data)} 条，异常：{len(error_log)} 条")
        save_to_file(valid_data, "qinghai")
        # return
    
        config = load_config()
        conn = get_mysql_connection(config['mysql'])
        insert_traffic_data("青海", valid_data, conn)
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
    # print(asyncio.run(fetch_qinghai_event_data()))

