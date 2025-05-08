import json
import unittest
import os
from api.dify.dify_api import send_dify_request


class TestDifyClient(unittest.TestCase):
    def test_send_dify_request(self):
        # 构造一个简单的 info 字符串
        test_info = json.dumps({
            "message": "查询成功",
            "code": 1,
            "data": {
                "eventId": "f13323e961af4081a7df173cce33a0e0",
                "roadNo": "G59",
                "roadName": "呼和浩特—北海--G59",
                "status": "2",
                "type": 1,
                "position": "G59-玉林市博白县东平镇连圹村--玉林市博白县松旺镇周北村",
                "blockArea": "玉林市 博白县",
                "direction": 1,
                "startstake1": 2755,
                "startstake2": 584,
                "endstake1": 2755,
                "endstake2": 584,
                "manageOrgid": "31100450900",
                "manageOrgname": "玉林高速公路运营有限公司",
                "manageTel": "0775-3113333",
                "fillPersonname": "黄小玉",
                "contact": "0775-3113333",
                "joinProvince": "",
                "discoverTime": "2025-04-29 16:48:15",
                "occurTime": "2025-04-29 16:48:14",
                "fillTime": "2025-04-29 16:49:48",
                "estiTime": "2025-04-29 20:00:00",
                "dealEndTime": "",
                "actualTime": "",
                "description": "G59呼北高速玉铁段北海往玉林方向K2755+584处1辆小车爆胎，已自行驶入服务区停靠，预计恢复时间待定，造成不便，敬请谅解。",
                "blockReasonRootId": "突发类",
                "blockReasonParentId": "其他",
                "blockReason": "其他",
                "measureId": "其它",
                "measureDetail": "G59呼北高速玉铁段北海往玉林方向K2755+584处1辆小车爆胎，已自行驶入服务区停靠，预计恢复时间待定，造成不便，敬请谅解。",
                "result": "",
                "influenceLong": 0.0,
                "blockLong": 0.0,
                "controlStartStake1": 2755,
                "controlStartStake2": 584,
                "controlEndStake1": 2755,
                "controlEndStake2": 584,
                "isAllBlock": 0,
                "direcitonSeq": 2,
                "ptx": "109.75008318148515",
                "pty": "21.857905811237593",
                "jsdj": "高速公路",
                "sfzdsj": "0",
                "zdsjms": "",
                "zdsjxz": "",
                "zdsjcj": "",
                "xclxr": "",
                "xclxdh": "",
                "sfzcs": "",
                "sfzcsThree": "",
                "sfzcsFive": ""
            }
        })

        # 构建 config.json 的路径（假设在项目根目录的 config/config.test.json）
        base_dir = os.path.dirname(os.path.dirname(__file__))  # 回到项目根目录
        config_path = os.path.join(base_dir, "config", "config.test.json")
        try:
            result = send_dify_request(test_info, config_path=config_path)
            print("响应结果：", result)
        except Exception as e:
            self.fail(f"请求失败，错误: {e}")


if __name__ == "__main__":
    unittest.main()

    # 调用 Dify 接口
    # info_str = json.dumps(item, ensure_ascii=False)
    # base_dir = os.path.dirname(os.path.dirname(__file__))
    # config_path = os.path.join(base_dir, "config", "config.test.json")
    #
    # response = send_dify_request(info_str, config_path=config_path)
    # if response.get("data") and response["data"].get("outputs") and response["data"]["outputs"].get("text"):
    #     try:
    #         result_data = json.loads(response["data"]["outputs"]["text"])
    #         item["reason_detail"] = result_data.get("reason", "")
    #         item["event_type"] = result_data.get("type", "")
    #     except Exception:
    #         logging.warning(f"Dify 接口返回数据格式错误: {response['data']['outputs']['text']}")
    #         pass
