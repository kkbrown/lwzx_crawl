#shanxi
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
import re
from bs4 import BeautifulSoup, Comment

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


def extract_target_comments_from_html(html_string):
    if not html_string:
        return []

    soup = BeautifulSoup(html_string, 'html.parser')

    # 查找所有注释节点
    html_comments = soup.find_all(string=lambda text_node: isinstance(text_node, Comment))

    # extracted_datetimes = []
    # 正则表达式用于匹配 "MM-DD HH:MM" 格式的日期时间
    datetime_pattern_regex = r"(\d{2}-\d{2}\s+\d{2}:\d{2})" 
    
    for comment_node in html_comments:
        comment_text = comment_node.string.strip() # 获取并清理注释的原始文本
        
        # 1. 首先检查注释是否符合整体结构预期
        #    （以'['开头，包含日期时间，以'</br>'结尾）
        #    这里的 re.search(datetime_pattern_regex, comment_text) 也顺便验证了日期时间的存在
        datetime_match_in_filter = re.search(datetime_pattern_regex, comment_text)
        
        if (comment_text.startswith('[') and
            datetime_match_in_filter and # 确保日期时间部分真的存在
            comment_text.endswith('</br>')):
            
            # 2. 如果整体结构符合，我们已经通过 datetime_match_in_filter 找到了日期时间
            #    match.group(1) (或 group(0) 如果没用捕获组) 就是我们想要的日期时间字符串
            datetime_str = datetime_match_in_filter.group(1) # group(1) 是第一个捕获组的内容
            return datetime_str
            
    return None  # 如果没有找到符合条件的注释，返回 None


async def fetch_shanxi_event_data():

    # 页面信息爬取设置
    schema = {
    "name": "WeiboTextItemExtractor",
    "baseSelector": ".sslk_list",  # 目标是页面上所有class为 "weibo-text" 的div
    "fields": [
        {
            "name": "event",  # 字段名：事件时间
            # "selector": ".",       # 表示使用 baseSelector 选中的元素本身
            "type": "text",
        },
        {
            "name": "time", # 字段名，可以自定义
            # "selector": ".",  # 表示脚本中的 'element' 就是 baseSelector 选中的元素
            "type": "html"
        }
    ]
}
    config = CrawlerRunConfig(
        stream=False,
        #页面信息设置
        # No caching for demonstration
        cache_mode=CacheMode.BYPASS,
        wait_for="css:.sslk_list",
        # markdown_generator=DefaultMarkdownGenerator(),
        css_selector=".sslk_list",
        # # Extraction strategy
        extraction_strategy=JsonCssExtractionStrategy(schema)
    )
    browser_config = BrowserConfig()


    async with AsyncWebCrawler(config=browser_config) as crawler:
        results = await crawler.arun(
            url="https://jtyst.shanxi.gov.cn/bmfw/",
            config=config
        )
        # logging.info(results)
        # print(f"Crawled {len(results)} pages in total")
        # Access individual results
        data_list = []
        for result in results:
            # print(type(result.extracted_content))  #str  json
            # print(f"Extracted content: {result.extracted_content}")
            item_dict = json.loads(result.extracted_content)
            # print(type(item_dict))
            if len(item_dict)>0:
                # print(f"Extracted content: {result.extracted_content}")
                for item in item_dict:
                    target_comment = item.get("time", "")
                    if target_comment:
                        item["time"] = "2025-" + extract_target_comments_from_html(target_comment)
                    data_list.append(item)
            # print("Sample extracted item:", data)  # Show first item
            # print(f"item_dict: {item_dict}")
        # print(data_list)
    return data_list

async def run_task():
    try:
        logging.info("开始爬取山西省高速路况信息...")
        dify_worker = DifyWorkFlowProcessor(
            base_url="http://dify.datasw.cn/v1/workflows/run",
            api_key="app-mjTAaUFsiYvdzoTd25bYi0OP" # v3.0
        )
        extract_dify_worker = DifyWorkFlowProcessor(
            base_url="http://dify.datasw.cn/v1/workflows/run",
            api_key="app-32JgLE9KStrlnjrQMzqEwLA6"
        )
        data = None
        retry_count = 0
        while retry_count < 100:
            try:
                data = await fetch_shanxi_event_data()
                
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
                content = item.get("event", "")
                time = item.get("time", "")
                
                logging.info(f"检查数据库中是否存在省份山西、发布时间{time}的事件")
                config = load_config()
                conn = get_mysql_connection(config['mysql'])
                existing_count = 0
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT COUNT(*) FROM province_road_condition WHERE province=%s AND publish_time=%s", 
                        ("山西", time)
                    )
                    existing_count = cursor.fetchone()[0]
                conn.close()
                if existing_count != 0:
                    logging.info(f"已存在 {existing_count} 条省份山西、发布时间{time}的事件，跳过当前记录")
                    continue

                data = extract_dify_worker.run_workflow(content)
                extracted_events = data.get("data", [])["outputs"]["text"]
                # logging.info(f"提取到的事件内容: {extracted_events}")
                # 把extracted_events按行分割成列表，并去除可能的空行
                extracted_event_list = [event.strip() for event in extracted_events.split('\n') if event.strip()]
                for event in extracted_event_list:
                    valid_dict = {
                        "province": "山西",
                        "roadCode": None, #如果没有会返回None
                        "roadName": None,
                        "publish_content": event,
                        "publish_time": time,
                        "start_time": None,
                        "end_time": None,
                        "event_type_name": None,
                        "event_category": None,
                    }
                    
                    valid_data.append(valid_dict)

            except Exception as e:
                error_log.append({
                    "error": str(e),
                    "data": item
                })

        if len(valid_data) == 0:
            logging.info("没有有效的路况事件数据，跳过后续处理")
            return
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
            # 把post_str每20行分割成一组，避免过长的输入
            chunk_size = 20
            # 使用列表推导式生成新的列表
            # grouped_multiline_strings 列表中的每个元素都是一个包含最多20行文本的字符串
            grouped_multiline_strings = [
                "\n".join(post_lines[i : i + chunk_size])  # 1. 取出20行； 2. 用换行符合并它们
                for i in range(0, len(post_lines), chunk_size) # 每次跳过20个元素的起始索引
            ]
            # print(f"grouped_multiline_strings: {grouped_multiline_strings}")
            # print(f"分割后的组数: {len(grouped_multiline_strings)}")
            for group_idx, post_str in enumerate(grouped_multiline_strings):
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
                for idx, item in enumerate(class_name_list):
                    valid_data[group_idx * chunk_size + idx]["event_type_name"] = class_name_list[idx]
                    valid_data[group_idx * chunk_size + idx]["event_category"] = catecory_list[idx] if catecory_list[idx] != '' else None
                    valid_data[group_idx * chunk_size + idx]["roadName"] = road_name_list[idx] if road_name_list[idx] != '' else None
                    valid_data[group_idx * chunk_size + idx]["roadCode"] = road_code_list[idx] if road_code_list[idx] != '' else None
                    valid_data[group_idx * chunk_size + idx]["start_time"] = start_time_list[idx] if start_time_list[idx] != '' else None
                    valid_data[group_idx * chunk_size + idx]["end_time"] = end_time_list[idx] if end_time_list[idx] != '' else None
                    if valid_data[group_idx * chunk_size + idx]["event_category"] == EventCategory.PLAN.description \
                        and valid_data[group_idx * chunk_size + idx]["start_time"] is None:
                        valid_data[group_idx * chunk_size + idx]["start_time"] = valid_data[group_idx * chunk_size + idx]["publish_time"]
                    # print(f"{valid_data[idx]}")
        except Exception as e:
            logging.error(f"Dify工作流调用失败: {e}")
            raise RuntimeError("Dify工作流调用失败")

        logging.info(f"成功采集山西高速路况：{len(valid_data)} 条，异常：{len(error_log)} 条")
        save_to_file(valid_data, "shanxi")
        # save_to_file(error_log, "shanxi_error")

        config = load_config()
        conn = get_mysql_connection(config['mysql'])
        insert_traffic_data("山西", valid_data, conn)
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
    # asyncio.run(fetch_shanxi_event_data())
    # asyncio.run(run_task())
