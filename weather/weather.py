import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
import json
import logging
import time
from api.dify.dify_api import weather_extract
from dbconnection.db import load_config, insert_weather_data, get_mysql_connection, check_weather_exists

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

base_api = "http://www.nmc.cn/rest/findAlarm?pageNo=1&pageSize=500&signaltype=&signallevel=&province=&_=1747273583571"
base_url = "http://www.nmc.cn"

headers = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "http://www.nmc.cn/publish/alarm.html",
    "X-Requested-With": "XMLHttpRequest",
}


def fetch_alarm_list():
    resp = requests.get(base_api, headers=headers, verify=False)
    data = resp.json()
    alarms = []

    for item in data["data"]["page"]["list"]:
        alarms.append(base_url + item["url"])
    return alarms


def parse_alarm_detail(url):
    resp = requests.get(url, headers=headers, verify=False)
    resp.encoding = "utf-8"
    soup = BeautifulSoup(resp.text, "html.parser")

    title_tag = soup.find(id="title")
    if not title_tag:
        print(f"页面中未找到标题标签，跳过：{url}")
        return None
    title = title_tag.get_text(strip=True)
    # 内容（多个 id=alarmtext）
    content_parts = soup.find_all(id="alarmtext")
    content = "\n".join([part.get_text(strip=True, separator="\n") for part in content_parts])
    # 查询数据库，是否已经存在该条记录,如果存在，结束方法
    if check_weather_exists(content):
        print(f"该条记录已经存在，标题：{title}")
        return None

    # 发布时间提取
    pub_div = soup.find("div", {"id": "pubtime", "class": "hide"})
    publish_time = None
    if pub_div:
        match = re.search(r"(\d{4})年(\d{2})月(\d{2})日(\d{2})时(\d{2})分", pub_div.text)
        if match:
            publish_time = datetime.strptime(
                f"{match.group(1)}-{match.group(2)}-{match.group(3)} {match.group(4)}:{match.group(5)}",
                "%Y-%m-%d %H:%M"
            ).strftime("%Y-%m-%d %H:%M:%S")

    return {
        "title": title,
        "content": content,
        "publish_time": publish_time,
    }


def fetch_weather_save():
    urls = fetch_alarm_list()
    for url in urls:
        detail = parse_alarm_detail(url)
        if detail is None:
            continue

        # 调用 dify 接口解析
        weather_info = weather_extract(detail["title"])
        json_str = weather_info.get("data", {}).get("outputs", {}).get("text", "")

        try:
            weather_data = json.loads(json_str)
            # 合并标题和内容
            weather_data.update({
                "title": detail["title"],
                "content": detail["content"],
                "publish_time": detail["publish_time"]
            })
            # 保存到数据库
            config = load_config()
            conn = get_mysql_connection(config['mysql'])
            insert_weather_data(weather_data, conn)
            conn.close()
        except json.JSONDecodeError as e:
            print("解析失败：", json_str)
            print("错误：", e)


def schedule_loop():
    logging.info("定时任务已启动：每30分钟执行一次")
    while True:
        fetch_weather_save()
        logging.info("等待30分钟...")
        time.sleep(30 * 60)


if __name__ == "__main__":
    schedule_loop()
