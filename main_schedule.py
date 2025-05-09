import threading
import time

from province.shandong import schedule_loop as shandong_schedule
from province.hebei import schedule_loop as hebei_schedule
from province.zhejiang import schedule_loop as zhejiang_schedule
# from province.zhejiang_station import schedule_loop as zhejiang_station_schedule
from province.guangxi_plan import schedule_loop as guangxi_plan_schedule
from province.guangxi_sudden import schedule_loop as guangxi_sudden_schedule
from province.guangxi_temp import schedule_loop as guangxi_temp_schedule
from province.neimenggu import schedule_loop as neimenggu_schedule
from province.shanghai import schedule_loop as shanghai_schedule
from province.xinjiang import schedule_loop as xinjiang_schedule
from road.section import schedule_loop as section_schedule
from station.station import schedule_loop as station_schedule

def start_threads():
    thread_configs = [
        ("ShandongThread", shandong_schedule),
        ("HebeiThread", hebei_schedule),
        # ("SectionThread", section_schedule),
        # ("StationThread", station_schedule),
        ("ZhejiangThread", zhejiang_schedule),
        # ("ZhejiangStationThread", zhejiang_station_schedule),
        ("GuangXiPlanScheduleThread", guangxi_plan_schedule),
        ("GuangXiSuddenScheduleThread", guangxi_sudden_schedule),
        ("GuangXiTempScheduleThread", guangxi_temp_schedule),
        ("NeimengguScheduleThread", neimenggu_schedule),
        ("ShanghaiScheduleThread", shanghai_schedule),
        ("XinjiangScheduleThread", xinjiang_schedule),
    ]

    threads = []
    for name, func in thread_configs:
        t = threading.Thread(target=func, name=name)
        t.start()
        threads.append(t)

    return threads

if __name__ == "__main__":
    threads = start_threads()

    # 保持主线程运行，防止子线程终止
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("主线程退出，正在关闭所有任务线程...")
