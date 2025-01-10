# encoding:utf-8

import json
import os
import time
import logging
from bridge.context import ContextType
from bridge.reply import Reply, ReplyType
from common.log import logger
from plugins import Plugin
import plugins
from config import conf
from plugins import Event, EventContext, EventAction
import threading
from datetime import datetime, timedelta
from croniter import croniter  # 需要先 pip install croniter
import sqlite3
from typing import Optional
from bridge.bridge import Bridge
from channel.chat_message import ChatMessage
from bridge.context import Context
from lib.gewechat import GewechatClient
from plugins.plugin_manager import PluginManager
import re
import requests

@plugins.register(
    name="DifyTask",
    desire_priority=950,
    hidden=False,
    desc="定时任务插件",
    version="1.0.3",
    author="sofs2005",
)
class DifyTask(Plugin):
    # 添加类变量来跟踪实例和初始化状态
    _instance = None
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            # 初始化类变量
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        # 确保只初始化一次
        if not self._initialized:
            super().__init__()
            self.handlers[Event.ON_HANDLE_CONTEXT] = self.on_handle_context
            # 指令前缀
            self.command_prefix = "$time"
            # 确保数据目录存在
            self.data_dir = os.path.join(os.path.dirname(__file__), "data")
            if not os.path.exists(self.data_dir):
                os.makedirs(self.data_dir)
            # 初始化数据库
            self.db_path = os.path.join(self.data_dir, "tasks.db")
            self._init_db()
            # 初始化客户端
            self.client = GewechatClient(conf().get("gewechat_base_url"), conf().get("gewechat_token"))
            self.app_id = conf().get("gewechat_app_id")
            
            # 加载插件配置
            config_path = os.path.join(os.path.dirname(__file__), "config.json")
            self.plugin_config = {}
            if os.path.exists(config_path):
                with open(config_path, "r", encoding="utf-8") as f:
                    self.plugin_config = json.load(f)
            
            # 更新群组信息
            self._update_groups()
            # 启动定时器
            self.running = True
            self.timer_thread = threading.Thread(target=self._timer_loop)
            self.timer_thread.daemon = True
            self.timer_thread.start()
            logger.info("[DifyTask] inited")
            self._initialized = True

    def __del__(self):
        """析构函数，确保线程正确退出"""
        self.running = False
        if hasattr(self, 'timer_thread') and self.timer_thread.is_alive():
            self.timer_thread.join(timeout=1)
            logger.info("[DifyTask] timer thread stopped")

    def _init_db(self):
        """初始化数据库"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 创建任务表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY,
                    time TEXT NOT NULL,
                    circle TEXT NOT NULL,
                    cron TEXT NOT NULL,
                    event TEXT NOT NULL,
                    context TEXT NOT NULL,
                    created_at INTEGER NOT NULL
                )
            ''')
            
            # 创建群组信息表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS groups (
                    wxid TEXT PRIMARY KEY,
                    nickname TEXT NOT NULL,
                    updated_at INTEGER NOT NULL
                )
            ''')
            
            conn.commit()
            conn.close()
            logger.info("[DifyTask] Database initialized")
        except Exception as e:
            logger.error(f"[DifyTask] Failed to init database: {e}")

    def _update_groups(self):
        """更新群组信息"""
        try:
            # 获取所有群聊的 wxid 列表
            response = self.client.fetch_contacts_list(self.app_id)
            logger.debug(f"[DifyTask] fetch_contacts_list response: {response}")

            if response.get('ret') == 200:
                chatrooms = response.get('data', {}).get('chatrooms', [])
                logger.info(f"[DifyTask] Total chatrooms found: {len(chatrooms)}")
                
                # 获取每个群的详细信息
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                current_time = int(time.time())
                
                for chatroom_id in chatrooms:
                    try:
                        # 获取群信息
                        group_info = self.client.get_chatroom_info(self.app_id, chatroom_id)
                        logger.debug(f"[DifyTask] get_chatroom_info response for {chatroom_id}: {group_info}")
                        
                        if group_info.get('ret') == 200:
                            data = group_info.get('data', {})
                            nickname = data.get('nickName', '')
                            
                            # 更新数据库
                            cursor.execute('''
                            INSERT OR REPLACE INTO groups (wxid, nickname, updated_at)
                            VALUES (?, ?, ?)
                            ''', (chatroom_id, nickname, current_time))
                            
                            logger.debug(f"[DifyTask] Updated group info: {chatroom_id} - {nickname}")
                    except Exception as e:
                        logger.error(f"[DifyTask] Failed to update group {chatroom_id}: {e}")
                        continue
                
                # 清理超过7天未更新的群信息
                week_ago = current_time - 7 * 24 * 3600
                cursor.execute('DELETE FROM groups WHERE updated_at < ?', (week_ago,))
                
                conn.commit()
                conn.close()
                logger.info("[DifyTask] Groups info updated")
            else:
                logger.error(f"[DifyTask] Failed to fetch contacts list: {response}")
                
        except Exception as e:
            logger.error(f"[DifyTask] Failed to update groups: {e}")

    def get_help_text(self, **kwargs):
        return f"""定时任务插件使用说明:
命令前缀: {self.command_prefix}

1. 创建定时任务
基础格式：
{self.command_prefix} 周期 时间 事件内容

支持的周期格式：
- 每天 09:30 早安
- 工作日 18:00 下班提醒
- 每周一 10:00 周报时间
- 今天 23:30 睡觉提醒
- 明天 12:00 午饭提醒
- 后天 09:00 周末快乐
- 2024-01-01 12:00 新年快乐

Cron表达式格式（高级）：
{self.command_prefix} cron[分 时 日 月 周] 事件内容
例如：
{self.command_prefix} cron[0 9 * * 1-5] 该起床了
{self.command_prefix} cron[*/30 * * * *] 喝水提醒
{self.command_prefix} cron[0 */2 * * *] 休息一下

私聊时创建群任务：
{self.command_prefix} 每天 09:30 g[测试群]早安

2. 查看任务列表
{self.command_prefix} 任务列表 密码

3. 取消任务
{self.command_prefix} 取消任务 任务ID"""

    def _get_user_nickname(self, user_id):
        """获取用户昵称"""
        try:
            response = requests.post(
                f"{conf().get('gewechat_base_url')}/contacts/getBriefInfo",
                json={
                    "appId": conf().get('gewechat_app_id'),
                    "wxids": [user_id]
                },
                headers={
                    "X-GEWE-TOKEN": conf().get('gewechat_token')
                }
            )
            if response.status_code == 200:
                data = response.json()
                if data.get('ret') == 200 and data.get('data'):
                    return data['data'][0].get('nickName', user_id)
            return user_id
        except Exception as e:
            logger.error(f"[DifyTask] 获取用户昵称失败: {e}")
            return user_id

    def _get_task_list(self):
        """获取任务列表"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 获取所有任务，并关联群组信息
            cursor.execute('''
            SELECT 
                t.id, 
                t.time, 
                t.circle, 
                t.event, 
                t.context,
                g.nickname
            FROM tasks t
            LEFT JOIN groups g ON g.wxid = json_extract(t.context, '$.msg.from_user_id')
            ORDER BY json_extract(t.context, '$.isgroup') DESC,  -- 先按是否群消息排序
                     COALESCE(g.nickname, json_extract(t.context, '$.msg.from_user_id'))  -- 再按群名/用户名排序
            ''')
            tasks = cursor.fetchall()
            
            conn.close()
            
            if not tasks:
                return "当前没有任务"
            
            # 按群/用户分组整理任务
            grouped_tasks = {}
            for task_id, time, circle, event, context_str, group_name in tasks:
                try:
                    # 解析上下文信息
                    context = json.loads(context_str)
                    msg_info = context.get('msg', {})
                    is_group = context.get('isgroup', False)
                    
                    # 获取显示名称
                    if is_group:
                        display_name = f"群：{group_name or '未知群组'}"
                    else:
                        user_id = msg_info.get('from_user_id', '')
                        nickname = self._get_user_nickname(user_id)
                        display_name = f"用户：{nickname}"
                    
                    # 添加到分组中
                    if display_name not in grouped_tasks:
                        grouped_tasks[display_name] = []
                    
                    # 添加任务信息
                    grouped_tasks[display_name].append({
                        'id': task_id,
                        'time': time,
                        'circle': circle,
                        'event': event
                    })
                    
                except Exception as e:
                    logger.error(f"[DifyTask] 解析任务信息失败: {e}")
                    continue
            
            # 生成显示文本
            result = "任务列表:\n"
            for group_name, tasks in grouped_tasks.items():
                result += f"\n{group_name}\n"
                result += "-" * 30 + "\n"
                for task in tasks:
                    result += f"[{task['id']}] {task['circle']} {task['time']} {task['event']}\n"
                result += "\n"
            
            return result.strip()
        except Exception as e:
            logger.error(f"[DifyTask] 获取任务列表失败: {e}")
            return "获取任务列表失败"

    def _validate_time_format(self, time_str):
        """验证时间格式 HH:mm"""
        try:
            logger.debug(f"[DifyTask] 验证时间格式: {time_str}")
            
            # 检查格式
            if len(time_str) != 5 or time_str[2] != ':':
                logger.debug("[DifyTask] 时间格式长度错误或分隔符不是':'")
                return False
            
            # 分割小时和分钟
            hour, minute = time_str.split(':')
            logger.debug(f"[DifyTask] 时间分割结果: hour={hour}, minute={minute}")
            
            # 转换为整数
            hour = int(hour)
            minute = int(minute)
            logger.debug(f"[DifyTask] 时间转换结果: hour={hour}, minute={minute}")
            
            # 验证范围
            if hour < 0 or hour > 23:
                logger.debug("[DifyTask] 小时超出范围")
                return False
            if minute < 0 or minute > 59:
                logger.debug("[DifyTask] 分钟超出范围")
                return False
            
            logger.debug("[DifyTask] 时间格式验证通过")
            return True
        except Exception as e:
            logger.error(f"[DifyTask] 时间格式验证异常: {e}")
            return False

    def _convert_to_cron(self, circle_str, time_str):
        """转换为cron表达式"""
        try:
            # 如果已经是cron表达式，直接返回表达式内容
            if circle_str.startswith("cron[") and circle_str.endswith("]"):
                return circle_str[5:-1].strip()
            
            # 处理普通时间格式
            hour, minute = time_str.split(':')
            
            if circle_str == "每天":
                return f"{minute} {hour} * * *"
                
            if circle_str == "工作日":
                return f"{minute} {hour} * * 1-5"
                
            if circle_str.startswith("每周"):
                week_map = {"一": "1", "二": "2", "三": "3", "四": "4", "五": "5", "六": "6", "日": "0"}
                day = circle_str[2:]
                if day in week_map:
                    return f"{minute} {hour} * * {week_map[day]}"
                    
            # 处理今天、明天、后天
            if circle_str in ["今天", "明天", "后天"]:
                today = datetime.now()
                days_map = {"今天": 0, "明天": 1, "后天": 2}
                target_date = today + timedelta(days=days_map[circle_str])
                return f"{minute} {hour} {target_date.day} {target_date.month} *"
                
            if len(circle_str) == 10:  # YYYY-MM-DD
                date = datetime.strptime(circle_str, "%Y-%m-%d")
                return f"{minute} {hour} {date.day} {date.month} *"
                
            return None
        except Exception as e:
            logger.error(f"[DifyTask] 转换cron表达式失败: {e}")
            return None

    def _validate_circle_format(self, circle_str):
        """验证周期格式"""
        try:
            logger.debug(f"[DifyTask] 验证周期格式: {circle_str}")
            
            # 每天
            if circle_str == "每天":
                return True
                
            # 工作日
            if circle_str == "工作日":
                return True
                
            # 每周几
            week_days = ["一", "二", "三", "四", "五", "六", "日"]
            if circle_str.startswith("每周"):
                day = circle_str[2:]
                if day in week_days:
                    return True
                return False
                
            # 今天、明天、后天
            if circle_str in ["今天", "明天", "后天"]:
                return True
                
            # 具体日期 YYYY-MM-DD
            if len(circle_str) == 10:
                try:
                    year = int(circle_str[0:4])
                    month = int(circle_str[5:7])
                    day = int(circle_str[8:10])
                    if circle_str[4] != '-' or circle_str[7] != '-':
                        return False
                    if year < 2024 or year > 2100:
                        return False
                    if month < 1 or month > 12:
                        return False
                    if day < 1 or day > 31:
                        return False
                    return True
                except:
                    return False
                    
            # cron表达式
            if circle_str.startswith("cron[") and circle_str.endswith("]"):
                cron_exp = circle_str[5:-1].strip()  # 移除前后空格
                try:
                    # 检查格式：必须是5个部分
                    parts = cron_exp.split()
                    if len(parts) != 5:
                        logger.debug(f"[DifyTask] cron表达式格式错误，应该有5个部分: {cron_exp}")
                        return False
                    croniter(cron_exp)
                    return True
                except Exception as e:
                    logger.debug(f"[DifyTask] cron表达式验证失败: {e}")
                    return False
                    
            return False
        except Exception as e:
            logger.error(f"[DifyTask] 周期格式验证异常: {e}")
            return False

    def _create_task(self, time_str, circle_str, event_str, context):
        """创建任务"""
        try:
            logger.debug(f"[DifyTask] 创建任务: time={time_str}, circle={circle_str}, event={event_str}")
            
            # 如果是cron表达式，直接验证cron格式
            if circle_str.startswith("cron[") and circle_str.endswith("]"):
                cron_exp = circle_str[5:-1].strip()  # 移除前后空格
                logger.debug(f"[DifyTask] 解析cron表达式: {cron_exp}")
                try:
                    # 检查格式：必须是5个部分
                    parts = cron_exp.split()
                    logger.debug(f"[DifyTask] cron表达式分割结果: {parts}")
                    if len(parts) != 5:
                        return "cron表达式必须包含5个部分：分 时 日 月 周，例如：cron[0 9 * * 1-5]"
                    croniter(cron_exp)
                    time_str = "cron"  # 对于cron表达式，time_str不需要验证
                except ValueError as e:
                    logger.error(f"[DifyTask] cron表达式数值错误: {e}")
                    return f"cron表达式数值错误: {str(e)}"
                except Exception as e:
                    logger.error(f"[DifyTask] cron表达式验证失败: {e}")
                    return "cron表达式格式错误，正确格式为：cron[分 时 日 月 周]，例如：cron[0 9 * * 1-5]"
            else:
                # 验证时间格式
                if not self._validate_time_format(time_str):
                    logger.debug("[DifyTask] 时间格式验证失败")
                    return "时间格式错误，请使用 HH:mm 格式，例如：09:30"
                
                # 验证周期格式
                if not self._validate_circle_format(circle_str):
                    logger.debug("[DifyTask] 周期格式验证失败")
                    return "周期格式错误，支持：每天、每周x、工作日、YYYY-MM-DD、今天、明天、后天"

            # 转换为cron表达式
            cron_exp = self._convert_to_cron(circle_str, time_str)
            if not cron_exp:
                return "转换cron表达式失败"
                
            # 生成任务ID
            task_id = str(int(time.time()))
            logger.debug(f"[DifyTask] 生成任务ID: {task_id}")
            
            # 获取消息相关信息
            cmsg: ChatMessage = context.get("msg", None)
            logger.debug(f"[DifyTask] 原始消息对象: {cmsg}")
            msg_info = {}
            
            # 检查是否在私聊中指定了群名
            is_group = context.get("isgroup", False)
            group_name = None
            if not is_group and event_str.startswith("g["):
                # 从事件内容中提取群名，格式：g[群名]其他内容
                match = re.match(r'g\[([^\]]+)\](.*)', event_str)
                if match:
                    group_name = match.group(1)
                    event_str = match.group(2).strip()  # 移除群名标记，保留实际内容
                    logger.debug(f"[DifyTask] 从私聊中提取群名: {group_name}, 实际内容: {event_str}")
                    
                    # 查询群组信息
                    conn = sqlite3.connect(self.db_path)
                    cursor = conn.cursor()
                    cursor.execute('SELECT wxid FROM groups WHERE nickname = ?', (group_name,))
                    result = cursor.fetchone()
                    conn.close()
                    
                    if result:
                        group_wxid = result[0]
                        is_group = True
                        # 更新消息信息，将群ID设置为from_user_id
                        if cmsg:
                            msg_info = {
                                "from_user_id": group_wxid,  # 使用群ID
                                "actual_user_id": cmsg.from_user_id,  # 保留原始发送者ID
                                "to_user_id": cmsg.to_user_id,
                                "create_time": cmsg.create_time,
                                "is_group": True
                            }
                        logger.debug(f"[DifyTask] 找到群组: {group_name}, wxid: {group_wxid}")
                    else:
                        return f"未找到群组: {group_name}"
            else:
                # 常规消息处理
                if cmsg:
                    msg_info = {
                        "from_user_id": cmsg.from_user_id,
                        "actual_user_id": getattr(cmsg, "actual_user_id", cmsg.from_user_id),
                        "to_user_id": cmsg.to_user_id,
                        "create_time": cmsg.create_time,
                        "is_group": is_group
                    }
            
            logger.debug(f"[DifyTask] 处理后的消息信息: {msg_info}")
            
            # 构建上下文信息
            context_info = {
                "type": context.type.name,
                "content": event_str,
                "isgroup": is_group,
                "msg": msg_info
            }
            logger.debug(f"[DifyTask] 上下文信息: {context_info}")
            
            # 保存到数据库
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO tasks (id, time, circle, cron, event, context, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                task_id,
                time_str,
                circle_str,
                cron_exp,
                event_str,
                json.dumps(context_info),
                int(time.time())
            ))
            
            conn.commit()
            conn.close()
            
            logger.debug(f"[DifyTask] 任务创建成功: {task_id}")
            return f"已创建任务: [{task_id}] {time_str} {circle_str} {event_str}"
        except Exception as e:
            logger.error(f"[DifyTask] 创建任务失败: {e}")
            return f"创建任务失败: {str(e)}"

    def _delete_task(self, task_id):
        """删除任务"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 先查询任务是否存在
            cursor.execute('SELECT time, circle, event FROM tasks WHERE id = ?', (task_id,))
            task = cursor.fetchone()
            
            if not task:
                conn.close()
                return f"任务不存在: {task_id}"
            
            # 删除任务
            cursor.execute('DELETE FROM tasks WHERE id = ?', (task_id,))
            conn.commit()
            conn.close()
            
            time_str, circle_str, event_str = task
            return f"已删除任务: [{task_id}] {time_str} {circle_str} {event_str}"
        except Exception as e:
            logger.error(f"[DifyTask] 删除任务失败: {e}")
            return f"删除任务失败: {str(e)}"

    def _clean_expired_tasks(self):
        """清理过期任务"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 获取所有任务
            cursor.execute('SELECT id, circle, time FROM tasks')
            tasks = cursor.fetchall()
            now = datetime.now()
            
            for task_id, circle_str, time_str in tasks:
                try:
                    # 检查是否是一次性任务（具体日期）
                    if len(circle_str) == 10:  # YYYY-MM-DD 格式
                        task_date = datetime.strptime(f"{circle_str} {time_str}", "%Y-%m-%d %H:%M")
                        # 如果任务时间已过期24小时，则删除
                        if now - task_date > timedelta(hours=24):
                            cursor.execute('DELETE FROM tasks WHERE id = ?', (task_id,))
                            logger.info(f"[DifyTask] 删除过期任务: {task_id} {circle_str} {time_str}")
                except Exception as e:
                    logger.error(f"[DifyTask] 检查任务过期失败: {task_id} {e}")
                    continue
            
            conn.commit()
            conn.close()
            logger.info("[DifyTask] 清理过期任务完成")
        except Exception as e:
            logger.error(f"[DifyTask] 清理过期任务失败: {e}")

    def _timer_loop(self):
        """定时器循环"""
        last_group_update = 0  # 记录上次更新群信息的时间
        
        while self.running:
            try:
                now = datetime.now()
                current_time = int(time.time())
                
                # 每天凌晨3点清理过期任务
                if now.hour == 3 and now.minute == 0:
                    self._clean_expired_tasks()
                
                # 每6小时更新一次群信息
                if current_time - last_group_update > 21600:  # 21600秒 = 6小时
                    self._update_groups()
                    last_group_update = current_time
                    logger.info("[DifyTask] 已更新群组信息")
                
                # 从数据库获取所有任务
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                cursor.execute('SELECT id, cron, context FROM tasks')
                tasks = cursor.fetchall()
                conn.close()
                
                # 检查每个任务
                for task_id, cron_exp, context_json in tasks:
                    try:
                        cron = croniter(cron_exp, now)
                        next_time = cron.get_prev(datetime)
                        
                        # 如果上一次执行时间在1分钟内，说明需要执行
                        if (now - next_time).total_seconds() < 60:
                            context_info = json.loads(context_json)
                            self._execute_task(task_id, context_info)
                            
                    except Exception as e:
                        logger.error(f"[DifyTask] 检查任务异常: {task_id} {str(e)}")
                
                # 每分钟检查一次
                time.sleep(60 - datetime.now().second)
            except Exception as e:
                logger.error(f"[DifyTask] 定时器异常: {e}")
                time.sleep(60)

    def _execute_task(self, task_id: str, context_info: dict):
        """执行任务"""
        try:
            logger.info(f"[DifyTask] 执行任务: {task_id}")
            logger.info(f"[DifyTask] 完整上下文信息: {context_info}")
            
            # 如果是一次性任务（具体日期），执行后删除
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('SELECT circle FROM tasks WHERE id = ?', (task_id,))
            task = cursor.fetchone()
            
            if task and len(task[0]) == 10:  # YYYY-MM-DD
                cursor.execute('DELETE FROM tasks WHERE id = ?', (task_id,))
                conn.commit()
                logger.info(f"[DifyTask] 删除一次性任务: {task_id}")
            
            conn.close()
            
            # 获取消息信息
            msg_info = context_info.get('msg', {})
            content = context_info['content']
            logger.info(f"[DifyTask] 消息信息: {msg_info}")
            
            # 判断是提醒还是命令
            if content.startswith('提醒'):
                # 提醒消息：直接发送给用户
                remind_content = content[2:].strip()  # 移除"提醒"前缀
                logger.info(f"[DifyTask] 处理提醒消息: {remind_content}")
                
                if context_info.get('isgroup', False):
                    # 群聊消息
                    room_id = msg_info.get("from_user_id", "")
                    logger.info(f"[DifyTask] 发送群提醒: room_id={room_id}, content={remind_content}")
                    if room_id:
                        try:
                            response = self.client.post_text(self.app_id, room_id, remind_content, "")
                            logger.info(f"[DifyTask] 群提醒发送响应: {response}")
                        except Exception as e:
                            logger.error(f"[DifyTask] 群提醒发送失败: {e}")
                else:
                    # 私聊消息
                    to_user = msg_info.get("from_user_id", "")
                    logger.info(f"[DifyTask] 发送私聊提醒: to_user={to_user}, content={remind_content}")
                    if to_user:
                        try:
                            response = self.client.post_text(self.app_id, to_user, remind_content, "")
                            logger.info(f"[DifyTask] 私聊提醒发送响应: {response}")
                        except Exception as e:
                            logger.error(f"[DifyTask] 私聊提醒发送失败: {e}")
            else:
                # 命令消息：构建 Context 并使用 channel.produce() 处理
                logger.info(f"[DifyTask] 处理命令消息: {content}")
                
                # 构建 ChatMessage 对象
                chat_msg = ChatMessage(msg_info.get("create_time", int(time.time())))
                chat_msg.from_user_id = msg_info.get("from_user_id", "")
                chat_msg.to_user_id = msg_info.get("to_user_id", "")
                chat_msg.actual_user_id = msg_info.get("actual_user_id", "")
                chat_msg.content = content
                chat_msg.is_group = context_info.get('isgroup', False)
                
                # 构建 Context 对象
                context = Context()
                context.type = ContextType.TEXT
                context.content = content
                context.isgroup = context_info.get('isgroup', False)
                
                # 获取 channel 实例
                from channel.chat_channel import ChatChannel
                channel = ChatChannel()
                
                # 设置接收者和会话ID
                if context_info.get('isgroup', False):
                    # 群聊消息
                    receiver = msg_info.get("from_user_id", "")  # 群ID
                    session_id = f"{receiver}_{msg_info.get('actual_user_id', '')}"
                else:
                    # 私聊消息
                    receiver = msg_info.get("from_user_id", "")
                    session_id = receiver
                
                context.kwargs = {
                    'msg': chat_msg,
                    'receiver': receiver,  # 群聊时使用群ID作为接收者
                    'session_id': session_id,
                    'channel': channel
                }
                
                # 使用 channel.produce() 处理消息
                channel.produce(context)
                
                logger.info(f"[DifyTask] 命令消息已转发: {content}, receiver={receiver}, session_id={session_id}")
            
            logger.info(f"[DifyTask] 任务执行完成: {task_id}")
            
        except Exception as e:
            logger.error(f"[DifyTask] 执行任务异常: {e}")

    def emit_event(self, event: Event, e_context: EventContext = None):
        """触发事件"""
        try:
            # 获取插件管理器
            plugin_manager = PluginManager()
            # 触发事件
            plugin_manager.emit_event(event, e_context)
        except Exception as e:
            logger.error(f"[DifyTask] 触发事件失败: {e}")

    def on_handle_context(self, e_context: EventContext):
        """处理消息"""
        if e_context['context'].type != ContextType.TEXT:
            return

        content = e_context['context'].content.strip()
        if not content:
            return
        
        # 获取是否群聊
        is_group = e_context['context'].get('isgroup', False)
        
        # 群聊时移除用户id前缀
        if is_group and ":" in content:
            parts = content.split(":", 1)
            if not self.command_prefix in parts[0]:
                content = parts[1].strip()
        
        # 处理命令
        if content.startswith(self.command_prefix):
            logger.debug(f"[DifyTask] 收到命令: {content}")
            # 移除指令前缀
            command = content.replace(self.command_prefix, "", 1).strip()
            
            # 空命令显示帮助
            if not command:
                e_context['reply'] = Reply(ReplyType.TEXT, self.get_help_text())
                e_context.action = EventAction.BREAK_PASS
                return
            
            # 处理任务列表命令
            if command.startswith("任务列表"):
                # 检查是否提供了密码
                parts = command.split()
                if len(parts) < 2:
                    e_context['reply'] = Reply(ReplyType.ERROR, "请提供访问密码")
                    e_context.action = EventAction.BREAK_PASS
                    return
                
                # 验证密码
                password = parts[1]
                config_password = self.plugin_config.get('task_list_password')
                if not config_password:
                    e_context['reply'] = Reply(ReplyType.ERROR, "管理员未设置访问密码，无法查看任务列表")
                    e_context.action = EventAction.BREAK_PASS
                    return
                
                if password != config_password:
                    e_context['reply'] = Reply(ReplyType.ERROR, "访问密码错误")
                    e_context.action = EventAction.BREAK_PASS
                    return
                
                # 密码正确，显示任务列表
                task_list = self._get_task_list()
                e_context['reply'] = Reply(ReplyType.TEXT, task_list)
                e_context.action = EventAction.BREAK_PASS
                return
            
            # 取消任务
            if command.startswith("取消任务 "):
                task_id = command.replace("取消任务 ", "", 1).strip()
                result = self._delete_task(task_id)
                e_context['reply'] = Reply(ReplyType.TEXT, result)
                e_context.action = EventAction.BREAK_PASS
                return
            
            # 创建任务
            # 先检查是否是 cron 表达式
            if "cron[" in command:
                # 使用正则表达式匹配 cron 表达式和事件内容
                import re
                match = re.match(r'cron\[(.*?)\]\s*(.*)', command)
                if match:
                    cron_exp = match.group(1).strip()
                    event_str = match.group(2).strip()
                    if not event_str:
                        e_context['reply'] = Reply(ReplyType.TEXT, "请输入事件内容")
                        e_context.action = EventAction.BREAK_PASS
                        return
                    result = self._create_task("cron", f"cron[{cron_exp}]", event_str, e_context['context'])
                    e_context['reply'] = Reply(ReplyType.TEXT, result)
                    e_context.action = EventAction.BREAK_PASS
                    return
                else:
                    e_context['reply'] = Reply(ReplyType.TEXT, "cron表达式格式错误，正确格式：$time cron[分 时 日 月 周] 事件内容")
                    e_context.action = EventAction.BREAK_PASS
                    return
            
            # 处理普通定时任务
            parts = command.split(" ", 2)
            if len(parts) == 3:
                circle_str, time_str, event_str = parts
                result = self._create_task(time_str, circle_str, event_str, e_context['context'])
                e_context['reply'] = Reply(ReplyType.TEXT, result)
                e_context.action = EventAction.BREAK_PASS
                return
            
            # 命令格式错误
            e_context['reply'] = Reply(ReplyType.TEXT, "命令格式错误，请查看帮助信息")
            e_context.action = EventAction.BREAK_PASS 