import unittest
from utils.info_extract import extract_first_highway
from dbconnection.db import get_today_weather_ids

class TestExtractFirstHighway(unittest.TestCase):

    def test_normal_case(self):
        content = "G2001济南绕城高速K0+000至K25+200双向限速,最高限速80公里/小时,禁止不可解体物品超限运输车、车辆运输车、黄牌货车、危化品运输车通行"
        result = extract_first_highway(content)
        self.assertEqual(result["road_code"], "G2001")
        self.assertEqual(result["road_name"], "济南绕城高速")

    def test_multiple_matches(self):
        content = "胶南枢纽(G15沈海高速沈阳方向转G22青兰高速兰州方向匝道)临时关闭"
        result = extract_first_highway(content)
        self.assertEqual(result["road_code"], "G15")
        self.assertEqual(result["road_name"], "沈海高速")

    def test_no_match(self):
        content = "本路段正在施工，请绕行"
        result = extract_first_highway(content)
        self.assertEqual(result["road_code"], "")
        self.assertEqual(result["road_name"], "")

    def test_current_day_weather(self):
        ids = get_today_weather_ids()
        print(ids)

if __name__ == '__main__':
    unittest.main()
