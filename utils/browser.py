import time
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager


def get_session_hebei_cookie_with_selenium():
    """使用Selenium模拟浏览器访问获取SESSION cookie"""
    print("启动浏览器...")

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36")

    try:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        driver.get("http://www.hebecc.com/")
        print("成功访问首页")
        time.sleep(3)

        cookies = driver.get_cookies()
        session_cookie = next((cookie['value'] for cookie in cookies if cookie['name'] == 'SESSION'), None)
        driver.quit()

        if session_cookie:
            print(f"成功获取SESSION cookie: {session_cookie}")
            return session_cookie
        else:
            raise Exception("未找到SESSION cookie")

    except Exception as e:
        print(f"获取SESSION cookie失败: {e}")
        raise


def get_zhejiang_token():
    token_url = "https://ddzlcx.jtyst.zj.gov.cn/auth/oauth2/token"
    params = {
        "username": "mKn1e!S21W9ez",
        "password": "EnbBVdg2X3+kA2xWeLi/OA==",
        "grant_type": "password",
        "scope": "server"
    }
    headers_token = {
        "Accept": "application/json, text/plain, */*",
        "Authorization": "Basic emxjeF9hZzptVGp4Nk9QQVNRNmoqSzRiT1Y=",
        "TENANT_ID": "1",
        "isToken": "false",
        "User-Agent": "Mozilla/5.0",
        "Origin": "https://gzcx.jtyst.zj.gov.cn",
        "Referer": "https://gzcx.jtyst.zj.gov.cn/"
    }

    resp = requests.post(token_url, headers=headers_token, params=params)
    if resp.status_code == 200:
        return resp.json()["access_token"]
    else:
        raise Exception(f"获取 Token 失败：{resp.status_code} {resp.text}")
