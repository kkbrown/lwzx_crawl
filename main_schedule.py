import threading
from province.shandong import schedule_loop as shandong_schedule
from province.hebei import schedule_loop as hebei_schedule
from province.zhejiang import schedule_loop as zhejiang_schedule
from province.zhejiang_station import schedule_loop as zhejiang_station_schedule
from road.section import schedule_loop as section_schedule
from station.station import schedule_loop as station_schedule

if __name__ == "__main__":
    # 启动山东定时任务线程
    thread_sd = threading.Thread(target=shandong_schedule, name="ShandongThread")
    thread_sd.start()

    # 启动河北定时任务线程
    # thread_hb = threading.Thread(target=hebei_schedule, name="HebeiThread")
    # thread_hb.start()

    # 启动拥堵路段定时任务线程
    thread_st = threading.Thread(target=section_schedule, name="SectionThread")
    thread_st.start()

    # 启动拥堵收费站定时任务线程
    thread_station = threading.Thread(target=station_schedule, name="StationThread")
    thread_station.start()

    # 启动浙江定时任务线程
    thread_zj = threading.Thread(target=zhejiang_schedule, name="ZhejiangThread")
    thread_zj.start()
    # 启动浙江收费站特情定时任务线程
    thread_zjs = threading.Thread(target=zhejiang_station_schedule, name="ZhejiangStationThread")
    thread_zjs.start()

    thread_sd.join()
    thread_st.join()
    thread_station.join()
    thread_zj.join()
    thread_zjs.join()
