# encoding:utf-8

from plugins import *
from bridge.context import ContextType
from bridge.reply import Reply, ReplyType
from channel.chat_message import ChatMessage
import plugins
import logging
import os
import json

# 导入主程序文件中的类
from .difytask import DatabaseManager, TimeTaskModel, TaskManager

# 确保配置文件存在
def init_config():
    config_path = os.path.join(os.path.dirname(__file__), "config.json")
    if not os.path.exists(config_path):
        default_config = {
            "debug": False,
            "move_historyTask_time": "04:00:00",
            "time_check_rate": 1
        }
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(default_config, f, indent=4, ensure_ascii=False)

# 初始化配置
init_config()

@plugins.register(
    name="difytask",
    desc="定时任务插件",
    version="1.0",
    author="xxx",
    namecn="定时任务",
    hidden=False,
    enabled=True,
    desire_priority=980
)
class DifyTask(Plugin):
    def __init__(self):
        super().__init__()
        self.task_manager = None
        
    # ... 其他代码保持不变 ...
