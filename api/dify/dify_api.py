import json
import logging

import requests


def send_dify_request(info_str: str, config_path: str = "../../config/config.test.json"):
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    api_key = config["dify"]["api_key"]
    url = config["dify"]["base_url"]
    user_id = config["dify"]["user_id"]

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
    response = requests.post(url, headers=headers, json=payload)

    try:
        return response.json()
    except Exception:
        return response.text
