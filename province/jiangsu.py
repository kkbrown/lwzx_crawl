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
import asyncio
import json, base64
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
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)


async def crawl():
    _browser_config = BrowserConfig()
    _crawler = AsyncWebCrawler(config=_browser_config)
    result = await _crawler.arun(url='https://www.js96777.com/index.php/WebIndexApi', config=CrawlerRunConfig())
    print(result.markdown)
    return result.markdown


def fetch_jiangsu_event_data(limit=20, page=1):
    # ---------- ① 目标 URL ----------
    url = "https://www.js96777.com/index.php/WebIndexApi"

    # ---------- ② 公共请求头（保持和浏览器一致就行） ----------
    headers = {
        "User-Agent": ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"),
        "Accept": "text/html,*/*;q=0.01",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://www.js96777.com",
        "Referer": "https://www.js96777.com/Home/sslk",
    }

    def build_payload(size=8, index=1, roadoldid="", eventtype=""):
        """
        按接口要求封装 → JSON → base64 字符串
        """
        body = {
            "fun": "getEventDataByEventtype",
            "data": {
                "size": size,
                "index": index,
                "roadoldid": roadoldid,
                "eventtype": eventtype
            }
        }
        compact_json = json.dumps(body, separators=(",", ":"))          # 去掉空白
        return base64.b64encode(compact_json.encode()).decode()         # 转成 ey...==

    # ---------- ③ 发请求 ----------
    with requests.Session() as s:
        # 如果站点必须带 cookie，可先 GET 一下页面让服务器下发最新 PHPSESSID
        s.get(headers["Referer"], headers=headers, timeout=10)

        payload = build_payload(size=8, index=1)
        # 如果抓包里看到是 'data=ey...' 就这样：
        # resp = s.post(url, data={"data": payload}, headers=headers, timeout=10)

        # 抓包里若 **只有** 那串 base64 原文，就直接发字符串：
        resp = s.post(url, data=payload, headers=headers, timeout=10)

        resp.raise_for_status()
        # print(resp.json()['data']) 
        # 如果接口最终返回的还是 base64，再解一次：
        # data_json = json.loads(base64.b64decode(resp.text))
        # print(f"第 {page} 页解码后：", data_json, "\n")

    return resp.json()

def run_task():
    try:
        logging.info("开始爬取江苏省高速路况信息...")
        data = None
        retry_count = 0
        while retry_count < 100:
            try:
                data = fetch_jiangsu_event_data()['data']
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
                # 判断type_cat 和 type_name
                event_type_name_raw = item.get("eventtypename")
                # print(f"event_type_name_raw: {event_type_name_raw}")
                if event_type_name_raw == "施工养护":
                    event_type_name = EventType.MAINTENANCE
                elif event_type_name_raw == "交通事故":
                    event_type_name = EventType.ACCIDENT
                elif event_type_name_raw == "特情信息":
                    event_type_name = classify_event_type(item.get("reportout"))

                match = re.search(r'\b[SG]\d+', item.get("roadcodename"))   # 若需大小写不敏感，添加 flags=re.I
                road_code = match.group(0) if match else ''
            
                valid_dict = {
                    "province": "江苏",
                    "road_code": road_code,
                    "road_name": item.get("roadcodename")[len(road_code):],
                    "publish_content": item.get("reportout"),
                    "publish_time": item.get("occtime"),
                    # "start_time": item.get("occtime"),
                    # "end_time": item.get("planovertime"),
                    "event_type_name": event_type_name,
                    "event_category": EventCategory.REALTIME.description,
                }
                
                valid_data.append(valid_dict)

            except Exception as e:
                error_log.append({
                    "error": str(e),
                    "data": item
                })

        logging.info(f"成功采集江苏高速路况：{len(valid_data)} 条，异常：{len(error_log)} 条")
        save_to_file(valid_data, "jiangsu")
        # save_to_file(error_log, "jiangsu_error")

        # config = load_config()
        # conn = get_mysql_connection(config['mysql'])
        # insert_traffic_data("江苏", valid_data, conn)
        # conn.close()
    except Exception as e:
        logging.error(f"任务执行失败: {e}")


def schedule_loop():
    logging.info("定时任务已启动：每30分钟执行一次")
    while True:
        run_task()
        logging.info("等待30分钟...")
        time.sleep(30 * 60)


if __name__ == "__main__":
    # schedule_loop()
    run_task()
    # fetch_jiangsu_event_data()
    # asyncio.run(crawl())