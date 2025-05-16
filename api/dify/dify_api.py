import json
import logging
import requests
import os


def _dify_extract(info_str: str, key: str, config_path: str = None):
    if not config_path:
        # 自动获取当前脚本所在目录
        base_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(base_dir, "..", "..", "config", "config.test.json")
    """通用 Dify 接口调用方法"""
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    module_conf = next((item[key] for item in config["dify"] if key in item), None)
    if not module_conf:
        raise ValueError(f"配置中未找到 {key}")

    api_key = module_conf["api_key"]
    url = module_conf["base_url"]
    user_id = module_conf["user_id"]

    headers = {
        "Authorization": f"Bearer {api_key}",
        "User-Agent": "Apifox/1.0.0 (https://apifox.com)",
        "Content-Type": "application/json",
        "Accept": "*/*",
        "Connection": "keep-alive"
    }

    payload = {
        "inputs": {
            "info": info_str
        },
        "response_mode": "blocking",
        "user": user_id
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        return response.json()
    except Exception as e:
        logging.exception("调用 Dify 接口失败")
        return response.text if 'response' in locals() else str(e)


def road_conditions_extract(info_str: str, config_path: str = None):
    return _dify_extract(info_str, key="road_conditions", config_path=config_path)


def weather_extract(info_str: str, config_path: str = None):
    return _dify_extract(info_str, key="weather", config_path=config_path)
