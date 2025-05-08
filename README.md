# lwzx_crawl
路况事件爬取

代码构成：
- api：dify_api.py 调用dify的api接口
- condition_enum:
    - event_category.py 事件类别枚举(实时事件、计划事件)
    - event_type.py 时间类型枚举（施工养护、交通事故等）
    - station_type.py 收费站事件类型（暂时只有浙江省使用）
- config: 配置文件
    - config.json 生产环境配置
    - config.test.json 开发环境配置
- db: 数据库相关
    - db.py 数据库连接代码，FIXME中修改当前使用的配置文件
- files: 存放爬取的文件
- province:每个省份的爬取脚本
- road:爬取百度拥堵路段的脚本
- sql:ddl语句
- station:爬取百度拥堵收费站的脚本
- test:测试代码
- utils:
    - browser.py 浏览器相关代码
    - file_utils.py 接口数据保存成文件相关代码
    - info_extract.py 信息提取相关代码
- main_schedule.py 主程序入口，定时任务爬取