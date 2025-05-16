# dbconnection.py
import datetime

import pymysql
import hashlib
import json
import os
import logging
import uuid


def md5_hash(text):
    return hashlib.md5(text.encode('utf-8')).hexdigest()


# FIXME 数据源确认
def load_config(path="../config/config.test.json"):
    base_dir = os.path.dirname(os.path.abspath(__file__))  # 当前 db.py 的目录
    config_path = os.path.abspath(os.path.join(base_dir, path))
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def get_mysql_connection(config):
    return pymysql.connect(
        host=config['host'],
        user=config['user'],
        password=config['password'],
        database=config['database'],
        charset=config.get('charset', 'utf8mb4')
    )


def insert_traffic_data(province_name, data_list, conn):
    print(f"{province_name} 正在写入 province_road_condition 表...")

    sql = """
    INSERT IGNORE INTO province_road_condition (id, province, road_code, road_name, publish_content, publish_time,start_time,end_time,insert_time,event_type_name,event_category)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor = conn.cursor()
    error_count = 0

    for index, item in enumerate(data_list, start=1):
        try:
            id = md5_hash(item['publish_time'] + item['publish_content'])
            province = item.get("province", "")
            road_code = item.get("roadCode", "")
            road_name = item.get("roadName", "")
            publish_content = item['publish_content']
            publish_time = item['publish_time'].replace('T', ' ')
            start_time_raw = item.get('start_time')
            start_time = start_time_raw.replace('T', ' ') if start_time_raw else None
            end_time_raw = item.get('end_time')
            end_time = end_time_raw.replace('T', ' ') if end_time_raw else None
            inset_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            event_category = item.get('event_category')
            event_type_name = item.get('event_type_name')
            cursor.execute(sql, (
                id, province, road_code, road_name, publish_content, publish_time, start_time, end_time, inset_time,
                event_type_name, event_category))
        except Exception as e:
            error_count += 1
            logging.error(f"第 {index} 条数据插入失败: {e}")
            logging.error(f"出错数据内容: {item}")

    conn.commit()
    cursor.close()
    print(f"{province_name}写入完成，失败 {error_count} 条")


def insert_section_data(data_list, conn):
    print("正在写入 crawler_section_congestion 表...")

    sql = """
        INSERT INTO crawler_section_congestion (id, publish_time, province_name, road_name,section_rank,congest_length, avg_speed,batch_num,semantic)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE publish_time=VALUES(publish_time)
        """
    cursor = conn.cursor()
    error_count = 0

    for index, item in enumerate(data_list, start=1):
        try:
            id = str(uuid.uuid4())
            publish_time = item['publish_time'].replace('T', ' ')
            province_name = item.get("province_name", "")
            road_name = item.get("roadName", "")
            congest_length = item.get('congest_length', 0)
            avg_speed = item.get('avg_speed', 0)
            section_rank = item.get('section_rank', 0)
            batch_num = item.get('batch_num')
            semantic = item.get('semantic')

            cursor.execute(sql, (
                id, publish_time, province_name, road_name, section_rank, congest_length, avg_speed, batch_num,
                semantic))
        except Exception as e:
            error_count += 1
            logging.error(f"第 {index} 条数据插入失败: {e}")
            logging.error(f"出错数据内容: {item}")

    conn.commit()
    cursor.close()
    print(f"写入完成，失败 {error_count} 条")


def insert_station_data(data_list, conn):
    print("正在写入 insert_station_data 表...")

    sql = """
        INSERT INTO crawler_station_congestion (id, publish_time, province_name, city_name,road_name,station_name,station_rank,congest_length, avg_speed,batch_num)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE publish_time=VALUES(publish_time)
        """
    cursor = conn.cursor()
    error_count = 0

    for index, item in enumerate(data_list, start=1):
        try:
            id = str(uuid.uuid4())
            publish_time = item['publish_time'].replace('T', ' ')
            province_name = item.get("province_name", "")
            city_name = item.get("city_name", "")
            road_name = item.get("road_name", "")
            station_name = item.get("station_name", "")
            congest_length = item.get('congest_length', 0)
            avg_speed = item.get('avg_speed', 0)
            station_rank = item.get('station_rank', 0)
            batch_num = item.get('batch_num')

            cursor.execute(sql, (
                id, publish_time, province_name, city_name, road_name, station_name, station_rank, congest_length,
                avg_speed, batch_num))
        except Exception as e:
            error_count += 1
            logging.error(f"第 {index} 条数据插入失败: {e}")
            logging.error(f"出错数据内容: {item}")

    conn.commit()
    cursor.close()
    print(f"写入完成，失败 {error_count} 条")


def insert_weather_data(weather_data, conn):
    global title
    print("正在写入 weather 表...")

    sql = """
        INSERT IGNORE INTO weather (id, province, city, area,title,warning_level,warning_type,warning_content,publish_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
    cursor = conn.cursor()

    try:
        id = md5_hash(weather_data['content'])
        province = weather_data['province']
        city = weather_data['city']
        area = weather_data['area']
        title = weather_data['title']
        publish_time = weather_data['publish_time']
        warning_level = weather_data['grade']
        warning_type = weather_data['type']
        warning_content = weather_data['content']

        cursor.execute(sql, (
            id, province, city, area, title, warning_level, warning_type, warning_content, publish_time))
    except Exception as e:
        logging.error(f"出错数据内容: {title}", "出错原因：{e}")

    conn.commit()
    cursor.close()
    print(f"写入完成，标题 {title}")


def check_weather_exists(content: str) -> bool:
    """根据内容生成 ID 并检查是否存在于 weather 表中"""
    id = md5_hash(content)
    sql = "SELECT COUNT(*) FROM weather WHERE id = %s"

    config = load_config()
    conn = get_mysql_connection(config['mysql'])

    cursor = conn.cursor()
    cursor.execute(sql, (id,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()

    return result[0] > 0
