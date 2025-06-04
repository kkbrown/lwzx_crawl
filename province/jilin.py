#吉林
import asyncio
import json
from urllib.parse import urljoin
import re
from bs4 import BeautifulSoup
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


def fetch_all_links(list_url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36"
    }
    resp = requests.get(list_url, headers=headers, timeout=10)
    resp.encoding = resp.apparent_encoding
    if resp.status_code != 200:
        raise RuntimeError(f"列表页请求失败，状态码：{resp.status_code}")

    soup = BeautifulSoup(resp.text, "html.parser")

    # 1. 定位到 <ul class="list allList">，提取所有 <li> 下的 <a> 的 href
    ul = soup.select_one("ul.list.allList")
    # print(ul)
    if not ul:
        raise RuntimeError("未找到 ul.list.allList，请检查列表页结构")

    hrefs = []
    for li in ul.find_all("li"):
        a_tag = li.find("a", href=True)
        if not a_tag:
            continue
        raw_href = a_tag["href"].strip()
        # 把相对路径转换成绝对 URL
        full_href = urljoin(list_url, raw_href)
        hrefs.append(full_href)
    return hrefs[0:3]

def fetch_link_info(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36"
    }
    resp = requests.get(url, headers=headers, timeout=10)
    resp.encoding = resp.apparent_encoding
    if resp.status_code != 200:
        raise RuntimeError(f"请求失败，状态码：{resp.status_code}")

    soup = BeautifulSoup(resp.text, "html.parser")

    # === 1. 提取“发布时间” ===
    # HTML 结构示例：
    # <div class="pageMain clearfix">
    #   <div class="pageTop">
    #     <div class="pageLeft">
    #       <span></span>
    #       <span>2025-05-17 13:34</span>
    #       <span>来源：<span class="addM"></span></span>
    #     </div>
    #   </div>
    # </div>
    #
    # 因为发布时间出现在 pageLeft 内的第二个 <span> 标签里，我们可以用 nth-of-type(2) 直接定位：
    date_span = soup.select_one(".pageMain .pageTop .pageLeft span:nth-of-type(2)")
    if date_span:
        publish_date = date_span.get_text(strip=True)
    else:
        publish_date = ""  # 如果没找到，再返回空字符串

    # === 2. 定位到目标表格并提取字段 ===TRS_PreExcel.TRS_PreAppend
    all_tables = soup.find_all("table")
    table = all_tables[0] if all_tables else None
    # table = soup.select_one(".fazhan_text table")
    if not table:
        raise RuntimeError("未找到目标表格，请检查 selector 是否正确")

    # 假定只有一行数据在第二个 <tr> 中
    row = table.select_one("tr:nth-child(2)")
    if not row:
        raise RuntimeError("未找到数据行（第二行），请检查表格结构")

    # 把每个 <td> 里的文字按顺序取出来，separator=" " 保证单元格内多行文字之间用空格分隔
    tds = row.find_all("td", recursive=False)
    cols = [td.get_text(separator=" ", strip=True) for td in tds]
    if len(cols) < 8:
        raise RuntimeError("列数不足，解析到的 td 少于 8 个，可能表格结构已变化")

    # 组装成一个 dict
    data = {
        "publish_time":      publish_date,         # 新增：发布时间
        "road_name":        cols[0],   # 路线名称
        "road_code":        cols[1],   # 路线编码
        # "affected_section":  cols[2],   # 影响路段
        # "km_markers":        cols[3],   # 里程桩号
        "start_date":        cols[4],   # 开始时间（例如 "2025年5月20日"）
        "end_date":          cols[5],   # 结束时间（例如 "2025年11月30日"）
        "adjustment":        cols[6],   # 道路调整措施（绕行方案文字）
        "event_reason":      cols[7],   # 事件原因
    }
    return data

async def fetch_jilin_event_data():
    # link_list = await fetch_links()
    link_list = fetch_all_links("http://jtyst.jl.gov.cn/glj/cxfw_7623/zxlkdt/")
    data_list = []
    for item in link_list:
        result_dict = fetch_link_info(item)
        # print(type(result_dict))
        data_list.append(result_dict)             
    # print(type(data_list))
    # print(type(data_list[0]))
    return data_list # type: list[dict]

async def run_task():
    try:
        logging.info("开始爬取吉林省高速路况信息...")
        dify_worker = DifyWorkFlowProcessor(
            base_url="http://dify.datasw.cn/v1/workflows/run",
            api_key="app-mjTAaUFsiYvdzoTd25bYi0OP"
        )
        data = None
        retry_count = 0
        while retry_count < 100:
            try:
                data = await fetch_jilin_event_data()
                # print(type(data))
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
                # print(type(item))
                reformatted_date = []
                for date_str in [item.get("start_date", ""), item.get("end_date", "")]:
                    if date_str:
                        # 处理日期格式，将中文日期转换为标准格式
                        date_str = re.sub(r'(\d{4})年(\d{1,2})月(\d{1,2})日', r'\1-\2-\3', date_str)
                        date_str = datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y-%m-%d 08:00:00")
                        reformatted_date.append(date_str) #[start, end]
                

                valid_dict = {
                    "province": "吉林",
                    "roadCode": item.get("road_code"), #如果没有会返回None
                    "roadName": item.get("road_name"),
                    "publish_content": item.get("adjustment"),
                    "publish_time": item.get("publish_time"),
                    "start_time": reformatted_date[0],
                    "end_time": reformatted_date[1],
                    "event_type_name": item.get("event_reason", ""),
                    "event_category": EventCategory.PLAN.description,  # 默认设置为计划类事件
                }
                
                valid_data.append(valid_dict)

            except Exception as e:
                error_log.append({
                    "error": str(e),
                    "data": item
                })
        if error_log:
            save_to_file(error_log, "jilin_error")
        # save_to_file(valid_data, "jilin")
        # 调用dify工作流对valid_data进行事件类型的判断
        post_str = ''
        for idx, item in enumerate(valid_data):
            content = item.get("event_type_name").strip() + item.get("publish_content", "").strip()
            if "\n" in content:
                content = content.replace("\n", " ")  # 确保每个事件描述在一行
            post_str += content + "\n"  # 按行区分，确保顺序对得上
        # 校验环节，确保第idx个item对应第idx行
        post_lines = post_str.strip().split("\n")
        if len(post_lines) != len(valid_data):
            raise ValueError(f"valid_data的长度与post_str的行数不一致:valid_data={len(valid_data)}, post_str={len(post_lines)}")
        
        # 启动Dify工作流
        try:
            logging.info("调用Dify工作流进行事件类型判断...")
            ans= dify_worker.run_workflow(post_str)
            # print(type(ans))
            # print(ans.keys())
            data = ans.get("data", [])
            class_name_list = data["outputs"]["class_name"]
            cat_name_list = data["outputs"]["CategoryList"]
            # end_time_list = data["outputs"]["EndTimeList"]
            # start_time_list = data["outputs"]["StartTimeList"]
            # road_name_list = data["outputs"]["road_name"]
            # road_code_list = data["outputs"]["road_code"]
            # print(f"Dify: class_name_list: {class_name_list}")
            for idx, item in enumerate(class_name_list):
                if item is not None:
                    valid_data[idx]["event_type_name"] = item
                else:
                    raise ValueError(f"第 {idx} 行数据的 event_type_name 为空")
                if cat_name_list[idx] is not None:
                    # 如果没有开始结束时间，则为实时事件
                    if valid_data[idx]["start_time"] == "" or valid_data[idx]["end_time"] == "":
                        valid_data[idx]["event_category"] = cat_name_list[idx]
                else:
                    raise ValueError(f"第 {idx} 行数据的 event_category 为空")
        except Exception as e:
            logging.error(f"Dify工作流调用失败: {e}")
            raise RuntimeError("Dify工作流调用失败")

        logging.info(f"成功采集吉林高速路况：{len(valid_data)} 条，异常：{len(error_log)} 条")
        save_to_file(valid_data, "jilin")
        
        # return
        config = load_config()
        conn = get_mysql_connection(config['mysql'])
        insert_traffic_data("吉林", valid_data, conn)
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
    # data = asyncio.run(fetch_links())
    # print(data)
    # d=asyncio.run(fetch_jilin_event_data())
    # print(d)
