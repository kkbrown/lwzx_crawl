from enum import Enum


class StationStatus(Enum):
    NORMAL = ("正常", "收费站正常开启")
    CLOSED = ("关闭", "收费站关闭")
    LIMITED = ("限流", "收费站限流")
    UNKNOWN = ("未知", "未知状态")

    def __init__(self, label, description):
        self.label = label
        self.description = description
