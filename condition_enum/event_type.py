from enum import Enum

class EventType(Enum):
    MAINTENANCE = "施工养护"
    ACCIDENT = "交通事故"
    CONTROL = "交通管制"
    WEATHER = "异常天气"
    OTHER = "其他事件"

    def __init__(self, description):
        self.description = description