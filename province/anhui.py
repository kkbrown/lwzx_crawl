#安徽
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
        "baseSelector": "ul.doc_list.list-31410351 > li",
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
            url="https://jtt.ah.gov.cn/jslk/index.html",

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

async def fetch_anhui_event_data():
    link_list = await fetch_links()
    # 1. Define a simple extraction schema
    schema = {
        "name": "Traffic Info",
        "baseSelector": ".wenzhang",
        "fields": [
            {
                "name": "publish_date", #事件类型
                "selector": "span.wz_date",
                "type": "regex",
                "pattern": r"(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2})",
            },
            {
                "name": "publish_content",
                "selector": ".j-fontContent.wzcon.minh300",
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
        logging.info("开始爬取安徽省高速路况信息...")
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
                data = await fetch_anhui_event_data()
                if isinstance(data, list) and data:
                    break
                raise ValueError("返回数据为空或格式异常")
            except Exception as e:
                retry_count += 1
                logging.warning(f"第 {retry_count} 次尝试失败：{e}")
                time.sleep(10)
        if data is None:
            raise RuntimeError("重试失败，终止本轮任务")

        logging.info(f"成功下载安徽高速路况数据：{len(data)} 条")
        valid_data = []
        error_log = []

        # 解析原始事件并构建 valid_data 列表
        for item in data:
            try:
                publish_date_str = item.get("publish_date", "")
                if not publish_date_str:
                    raise ValueError("publish_date 为空")
                # 加上秒数
                publish_date = datetime.strptime(publish_date_str, "%Y-%m-%d %H:%M").strftime("%Y-%m-%d %H:%M:00")

                #验重：查询数据库中相同省份和发布时间的事件数
                logging.info(f"检查数据库中是否存在省份安徽、发布时间{publish_date}的事件")
                config = load_config()
                conn = get_mysql_connection(config['mysql'])
                existing_count = 0
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT COUNT(*) FROM province_road_condition WHERE province=%s AND publish_time=%s", 
                        ("安徽", publish_date)
                    )
                    existing_count = cursor.fetchone()[0]
                conn.close()
                if existing_count != 0:
                    logging.info(f"已存在 {existing_count} 条省份安徽、发布时间{publish_date}的事件，跳过当前记录")
                    continue
                
                
                logging.info(f"调用Dify_analyze工作流进行事件分析，内容长度：{len(item.get('publish_content', ''))}")
                ans = dify_worker_for_analyze.run_workflow(item.get("publish_content", ""))
                inc_str = ans.get("data", {}).get("outputs", {}).get("text", "")
                incident_list = res_anaylze(inc_str)

                for incident in incident_list:
                    valid_data.append({
                        "province": "安徽",
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

        logging.info(f"成功采集安徽高速路况：{len(valid_data)} 条，异常：{len(error_log)} 条")
        save_to_file(valid_data, "anhui")
        # return
    
        config = load_config()
        conn = get_mysql_connection(config['mysql'])
        insert_traffic_data("安徽", valid_data, conn)
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
    # d=asyncio.run(fetch_anhui_event_data())
    # print(d)
    # s="合徐高速：合肥往徐州方向830公里+340米至828公里+850米处挂篮拆除施工，施工期间徐州方向封闭，合肥方向半幅双向通行，预计施工时间5月28日7:00至17:00。\n合徐高速：徐州往合肥方向828公里+850米至830公里+340米处挂篮拆除施工，施工期间合肥方向封闭，徐州方向半幅双向通行，预计施工时间5月29日7:00至17:00。\n黄浮高速：双向70公里处至80公里处进行上跨老旧电力线拆除施工，现场无法通行，施工期间黄山往江西方向车辆从祁门西站分流，江西往黄山方向车辆从闪里站分流，祁门西、牯牛降、闪里入口临时封闭，预计施工时间5月29日9:00至10:00。\n德上高速：枞阳往祁门方向912公里至914公里+700米路段边坡水毁修复施工，占据所有车道，枞阳往祁门方向所有车辆在枞阳北出口分流下高速，枞阳北入口往祁门方向临时封闭，预计施工时间5月29日7:00至19:00。\n德上高速：永城往淮南方向593公里+300米至595公里+500米处铣刨摊铺施工，占据右侧车道、应急车道，左侧车道可通行，永城往淮南方向马集服务区临时封闭，预计施工时间5月29日6:00至18:00。\n连霍高速：郑州往徐州方向王寨服务区车辆导流纠违。\n沪渝高速沿江段：铜陵东、铜陵入口禁止危化品车、三超车辆通行（5月30日17:00至6月3日8:00）。\n铜商高速：铜陵北入口禁止危化品车辆、三超车辆通行（5月30日17:00至6月3日8:00）。\n济广高速：阜阳东出口六车道进行计重设备维修改造施工，超宽车辆无法下道，预计7月15日结束。\n滁新高速：苏阜园区出口六车道进行计重设备维修改造施工，超宽车辆无法下道，预计施工时间6月3日7:30至7月15日18:00。\n滁新高速：阜阳南出口六车道进行计重设备维修改造施工，超宽车辆无法下道，预计施工7月15日结束。\n合肥绕城高速：六安往蚌埠方向105公里至105公里+600米处路口枢纽B匝道封闭施工，预计2026年6月30日结束。\n合肥绕城高速：六安往南京方向91公里处至90公里+100米处三十头枢纽半幅双向通行导改施工，南京方向封闭施工，六安方向单幅双向通行，预计5月30日结束。\n滁新高速：九梓枢纽因京台高速公路合徐南段改扩建施工，封闭淮南往蚌埠方向B匝道、合肥往淮南方向C匝道、合肥往滁州方向E匝道、滁州往蚌埠方向G匝道，预计6月30日结束。\n宣城绕城高速：铜陵往广德方向营盘山隧道封闭施工，宣城北互通至丁店枢纽（36公里至50公里处，0公里至0公里+900米处）半幅主线封闭，宣城北入口往广德、宁国方向匝道封闭，铜陵、芜湖往广德、宁国方向车辆从宣城北出口分流，预计施工6月9日结束。\n京台高速合徐段：徐州方向路口枢纽至涂山怀远互通段封闭，合肥方向正常通行，双庙、九梓、永康、蚌埠入口往徐州方向匝道封闭，路口枢纽、九梓枢纽、西泉街枢纽转徐州方向匝道封闭，合肥往徐州方向吴圩、禹会服务区关闭，预计2026年6月30日结束。\n京台高速合徐段：合肥往徐州方向807公里+515米至804公里+900米处路改桥施工，施工期间徐州方向道路封闭，改合肥方向半幅双向通行，预计7月31日结束。\n京台高速庐铜段：双向1181公里+200米至1179公里+800米路段进行G3铜陵长江公铁大桥接线段专项施工，道路临时封闭，双向车辆可借用保通便道通行，预计12月31日结束。\n芜合高速：湾沚收费站改造施工，施工期间湾沚出、入口封闭，预计6月30日结束。\n芜合高速：合肥往芜湖方向153公里处至150公里处南半幅路改桥施工，施工期间合肥往芜湖方向车辆导入保通道行驶，预计7月20日结束。\n芜合高速：122公里+150米至123公里+350米处因S18宁合高速主线上垮桥及A.B.C匝道上跨桥（合肥方向半幅）施工，芜湖至合肥方向封闭，车辆改道至芜湖方向通行；芜湖方向车辆改道至保通道通行，预计施工时间5月14日至6月30日。\n沪渝高速宣广段：广德南环205公里至217公里进行双向全幅封闭施工，广德东入口往宣城方向封闭，往浙江方向可通行；出口仅允许浙江往广德方向的车辆下道，广德入口往湖州、上海方向封闭，往宣城方向正常通行，预计7月31日结束。\n宁芜高速：芜湖枢纽立交铜陵往宣城、合肥方向匝道临时封闭，马鞍山往宣城方向匝道临时封闭。\n宁芜高速：马鞍山往铜陵方向127公里+600米至128公里+575米处进行繁昌西互通导改施工，施工期间马鞍山往铜陵方向道路封闭，车辆借用铜陵往马鞍山方向主线通行，铜陵往马鞍山方向车辆从保通匝道（129公里+100米至127公里+600米处）通行，预计8月31日结束。\n巢黄高速：因宣泾高速二期泾县枢纽建设需要，泾县收费站封闭，进行拆除施工，预计7月1日结束。\n巢黄高速：旌德双向出入口匝道进行拼宽施工，施工期间旌德出入口封闭，预计9月30日结束。\n徐明高速：双向9公里+450米处至10公里+900米处进行综合养护路基工程施工，施工期间明光往徐州方向封闭所有车道，徐州往明光方向实行半幅双向通行，预计7月30日结束。\n盐洛高速：盐城往洛阳方向355公里+600米处至357公里+850米处蒿沟东枢纽上跨施工，洛阳方向全部封闭，盐城方向半幅双向通行，预计5月30日结束。\n沪蓉高速六武安徽段：六安往武汉方向649公里+600米至651公里+400米处马鬃岭枢纽路改桥施工，施工期间六安往武汉方向主线封闭，所有车辆从保通道绕行，预计9月30日结束。\n沪蓉高速六武安徽段：武汉往六安方向651公里+700米至650公里+100米处马鬃岭枢纽路改桥施工，施工期间武汉往六安方向主线封闭，所有车辆从保通道绕行，预计9月30日结束。\n高界高速：合肥往武汉方向594公里+300米至597公里+930米处临时墩架设桥梁梁体架设封闭施工，武汉往合肥方向599公里+900米处至594公里+330米处半幅双向通行，施工结束时间待定。\n芜合高速：芜湖往宣城方向新竹服务区出入口封闭。\n宣广高速：广德服务区双向出入口封闭，预计7月31日结束。\n宣城绕城高速：双向绿锦服务区入口封闭，预计6月30日结束。\n阜淮高速：双向公桥服务区餐厅暂不营业。\n商固高速：双向临泉西服务区餐厅暂不营业。\n徐明高速：明光往徐州方向石龙湖服务区管线维修施工，0号柴油停止销售（其余油品正常销售），预计06月05日18:00结束。\n滁宁高速：南京往滁州方向87公里处嘉山服务区封闭施工，预计6月30日结束。\n合肥都市圈环线：八斗服务区、吴山服务区双向加油站、充电桩、餐饮暂未营业。\n合肥绕城高速：金寨路、包河大道、集贤路入口禁止渣土车上道（空车除外）。\n京台高速合安段：方兴大道、严店、舒城入口禁止运输渣土的车辆上道（空车除外）。"
    # a=res_anaylze(s)
    # print(a)
    # print(len(a))
