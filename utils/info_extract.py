import re
from condition_enum.event_type import EventType


def extract_first_highway(publish_content):
    # 匹配高速路段的正则表达式，如 S11烟海高速、G18荣乌高速
    pattern = r'([GS]\d+)([\u4e00-\u9fa5]+高速)'
    match = re.search(pattern, publish_content)
    if match:
        return {
            "road_code": match.group(1),
            "road_name": match.group(2)
        }
    else:
        return {
            "road_code": "",
            "road_name": ""
        }


def classify_event_type(event_type_raw: str) -> str:
    """根据原始事件类型字符串，返回标准分类名称（中文）"""
    event_type_raw = (event_type_raw or "").strip()

    # 定义关键词映射表
    keyword_map = {
        EventType.MAINTENANCE: ["施工", "养护", "维修", "养路"],
        EventType.ACCIDENT: ["事故", "碰撞", "追尾", "刮擦", "车辆故障"],
        EventType.CONTROL: ["管制", "封闭", "限制", "交通管制","管控","车流量","封道"],
        EventType.WEATHER: ["天气", "雨", "雪", "雾", "冰", "风"]
    }

    matched_type = None

    # 模糊匹配多个关键词
    for etype, keywords in keyword_map.items():
        if any(kw in event_type_raw for kw in keywords):
            matched_type = etype
            break

    # 精确匹配
    if not matched_type:
        for et in EventType:
            if et.value == event_type_raw:
                matched_type = et
                break

    # 默认
    if not matched_type:
        matched_type = EventType.OTHER

    return matched_type.value
