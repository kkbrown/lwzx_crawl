from enum import Enum


class EventCategory(Enum):
    REALTIME = "实时事件"
    PLAN = "计划事件"

    def __init__(self, description):
        self.description = description
