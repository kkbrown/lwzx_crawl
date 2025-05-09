import json
import unittest
import os
from api.dify.dify_api import send_dify_request


class TestDifyClient(unittest.TestCase):
    def test_send_dify_request(self):
        # 构造一个简单的 info 字符串
        test_info = json.dumps({
            "eventId": "a67c64477930412ebfa3f2f8a6a0a64c",
            "roadNo": "G75",
            "roadName": "兰州—海口--G75",
            "status": "2",
            "statusName": "审核通过",
            "position": "G75-北海市合浦县石湾镇星岛湖--北海市合浦县石湾镇石湾村",
            "blockArea": "北海市 合浦县",
            "discoverTime": "2025-05-08 07:13:00",
            "occurTime": "2025-05-08 07:13:00",
            "fillTime": "2025-05-08 07:30:36",
            "estiTime": "2025-05-08 12:07:05",
            "description": "G75兰海高速(钦北路)-北海管辖，钦州往北海(上行)，K2161+000，距离（后方）星岛湖收费站2公里，属于北海市合浦县，发生一起车辆着火事件。人员受伤情况不详，路产损失不详，占用应急车道，暂无拥堵。已报北海运营分公司养护巡检值班人员、辖区交警、消防119、交通综合执法部门前往处置，后续处理情况将持续跟进汇报。",
            "blockReasonRootId": "突发类",
            "blockReasonParentId": "事故灾害",
            "blockReason": "车辆交通事故",
            "measureId": "占用行车道",
            "result": "【08日事件01终报】2025年05月08日07时13分，接内部工作人员报，G75兰海高速(钦北路)-北海管辖，钦州往北海方向(上行)K2161+450（更正初报桩号：K2161+000），距离（后方）星岛湖收费站2公里，属于北海市合浦县，发生一起1辆货车着火事件。无人员受伤，有路产损失，占用应急车道，暂无拥堵。07时27分小修队到达现场，07时29分消防车到达现场，07时45分火已灭完，07时50分消防队离开现场，07时52分巡检员到达现场，08时05分交警到达现场。09时35分辖区交通综合执法局到达现场处置，09时37分拖车到达现场，经拖车公司排障人员查看现场情况后反馈需要吊车协助，10时50分封闭第三、第四车道及应急车道进行吊车作业，10时58分吊车到达现场，12时07分事故车辆已经拖离现场，事故处理完毕，经过4小时20分钟处置，现场恢复正常通行。事故路段当时无施工点，路面无积水和障碍物，也无明显道路病害隐患附近交通安全设施齐全，两侧视线良好无遮挡。"
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
