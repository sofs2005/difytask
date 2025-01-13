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
    version="1.2.5",
    author="sofs2005",
)
class DifyTask(Plugin):
    _instance = None
    _initialized = False
    _scheduler = None
    _scheduler_lock = threading.Lock()
    _running = False

    def __init__(self):
        # 确保只初始化一次
        if not self._initialized:
            super().__init__()
            # 初始化 handlers
            self.handlers = {}
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
            
            # 修改定时器初始化部分
            with self._scheduler_lock:
                # 如果存在旧的调度器，先停止它
                if self._scheduler and self._scheduler.is_alive():
                    self._running = False  # 停止旧的循环
                    self._scheduler.join(timeout=1)  # 等待旧线程结束
                
                # 启动新的调度器
                self._running = True
                self._scheduler = threading.Thread(target=self._timer_loop)
                self._scheduler.daemon = True
                self._scheduler.start()
            
            logger.info("[DifyTask] plugin initialized")
            self._initialized = True

    def reload(self):
        """重载时停止旧线程，返回 (success, message)"""
        try:
            with self._scheduler_lock:
                # 停止旧线程
                self._running = False
                if self._scheduler and self._scheduler.is_alive():
                    try:
                        self._scheduler.join(timeout=30)
                        if self._scheduler.is_alive():
                            return False, "Failed to stop timer thread"
                    except Exception as e:
                        return False, f"Error stopping timer thread: {e}"
                
                # 重新初始化 handlers
                self.handlers = {}
                self.handlers[Event.ON_HANDLE_CONTEXT] = self.on_handle_context
                
                # 重新初始化线程
                self._running = True
                self._scheduler = threading.Thread(target=self._timer_loop)
                self._scheduler.daemon = True
                self._scheduler.start()
                
                logger.info("[DifyTask] Plugin reloaded successfully")
                return True, "Timer thread restarted successfully"
        except Exception as e:
            logger.error(f"[DifyTask] Reload failed: {e}")
            return False, f"Error reloading plugin: {e}"

    def __del__(self):
        """析构函数，确保线程正确退出"""
        if hasattr(self, 'running'):
            self.running = False
        if hasattr(self, 'timer_thread') and self.timer_thread and self.timer_thread.is_alive():
            self.timer_thread.join(timeout=1)
            logger.info("[DifyTask] timer thread stopped")

    def _timer_loop(self):
        """定时器循环"""
        last_group_update = 0
        last_check_time = 0

        while self._running:
            try:
                now = datetime.now()
                current_time = int(time.time())

                # 避免在同一秒内多次检查任务
                if current_time == last_check_time:
                    time.sleep(0.1)
                    continue

                last_check_time = current_time

                # 每天凌晨3点清理过期任务
                if now.hour == 3 and now.minute == 0:
                    self._clean_expired_tasks()
                
                # 每6小时更新一次群信息
                if current_time - last_group_update > 21600:  # 21600秒 = 6小时
                    self._update_groups()
                    last_group_update = current_time
                
                # 从数据库获取所有任务
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                
                try:
                    # 开始事务
                    cursor.execute('BEGIN IMMEDIATE')
                    
                    # 获取需要执行的任务
                    cursor.execute('''
                        SELECT id, cron, context, last_executed_at 
                        FROM tasks 
                        WHERE last_executed_at < ?
                    ''', (current_time - 60,))
                    
                    tasks = cursor.fetchall()
                    tasks_to_execute = []
                    
                    # 首先尝试更新所有需要执行的任务
                    for task_id, cron_exp, context_json, last_executed_at in tasks:
                        try:
                            cron = croniter(cron_exp, now)
                            next_time = cron.get_prev(datetime)
                            time_diff = (now - next_time).total_seconds()
                            
                            # 检查是否在执行时间窗口内
                            if 0 <= time_diff < 60:
                                # 尝试更新执行时间
                                cursor.execute('''
                                    UPDATE tasks 
                                    SET last_executed_at = ? 
                                    WHERE id = ? 
                                    AND last_executed_at < ?
                                ''', (current_time, task_id, current_time - 60))
                                
                                # 如果更新成功，将任务添加到执行列表
                                if cursor.rowcount > 0:
                                    tasks_to_execute.append((task_id, context_json))
                        
                        except Exception as e:
                            logger.error(f"[DifyTask] 检查任务异常: {task_id} {str(e)}")
                            continue
                    
                    # 提交事务，确保所有更新都已完成
                    conn.commit()
                    
                    # 执行所有已更新的任务
                    for task_id, context_json in tasks_to_execute:
                        try:
                            context_info = json.loads(context_json)
                            self._execute_task(task_id, context_info)
                        except Exception as e:
                            logger.error(f"[DifyTask] 执行任务异常: {task_id} {str(e)}")
                    
                except Exception as e:
                    # 如果发生异常，回滚事务
                    conn.rollback()
                    logger.error(f"[DifyTask] 数据库操作异常: {str(e)}")
                
                finally:
                    # 确保关闭连接
                    conn.close()

                # 优化休眠时间
                next_second = (now + timedelta(seconds=1)).replace(microsecond=0)
                sleep_time = (next_second - now).total_seconds()
                time.sleep(max(0.1, sleep_time))

            except Exception as e:
                logger.error(f"[DifyTask] 定时器异常: {e}")
                time.sleep(60)

    def _init_db(self):
        """初始化数据库"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 创建任务表，添加 last_executed_at 字段
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY,
                    time TEXT NOT NULL,
                    circle TEXT NOT NULL,
                    cron TEXT NOT NULL,
                    event TEXT NOT NULL,
                    context TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    last_executed_at INTEGER DEFAULT 0
                )
            ''')
            
            # 为现有表添加 last_executed_at 字段（如果不存在）
            try:
                cursor.execute('ALTER TABLE tasks ADD COLUMN last_executed_at INTEGER DEFAULT 0')
            except sqlite3.OperationalError:
                pass  # 字段已存在，忽略错误
            
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

    def _get_last_task_id(self):
        """获取数据库中最后一个数字类型的任务ID"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            # 简单地获取所有ID，在Python中过滤
            cursor.execute('SELECT id FROM tasks')
            ids = cursor.fetchall()
            conn.close()
            
            # 过滤出纯数字ID并找出最大值
            numeric_ids = [int(id[0]) for id in ids if id[0].isdigit()]
            return max(numeric_ids) if numeric_ids else 1000
        except Exception as e:
            logger.error(f"[DifyTask] 获取最后任务ID失败: {e}")
            return 1000  # 出错时从1000开始

    def _generate_task_id(self):
        """生成新的任务ID"""
        alphabet = '23456789ABCDEFGHJKLMNPQRSTUVWXYZ'
        
        while True:
            # 生成4位ID
            result = ''
            seed = int(str(time.time_ns())[-6:])
            for _ in range(4):
                seed = (seed * 1103515245 + 12345) & 0x7fffffff
                result += alphabet[seed % len(alphabet)]
            
            # 检查是否已存在
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('SELECT 1 FROM tasks WHERE id = ?', (result,))
            exists = cursor.fetchone()
            conn.close()
            
            # 如果ID不存在，则使用它
            if not exists:
                return result

    def _create_task(self, time_str, circle_str, event_str, context):
        """创建任务"""
        try:
            # 获取消息相关信息
            cmsg: ChatMessage = context.get("msg", None)
            
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
            task_id = self._generate_task_id()
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
                    # 检查是否是一次性任务
                    if len(circle_str) == 10:  # YYYY-MM-DD 格式
                        task_date = datetime.strptime(f"{circle_str} {time_str}", "%Y-%m-%d %H:%M")
                        # 如果任务时间已过期24小时，则删除
                        if now - task_date > timedelta(hours=24):
                            cursor.execute('DELETE FROM tasks WHERE id = ?', (task_id,))
                            logger.info(f"[DifyTask] 删除过期任务: {task_id} {circle_str} {time_str}")
                    # 检查其他一次性任务（今天、明天、后天）
                    elif circle_str in ["今天", "明天", "后天"]:
                        # 解析任务时间
                        days_map = {"今天": 0, "明天": 1, "后天": 2}
                        task_date = datetime.now().replace(hour=int(time_str.split(':')[0]), 
                                                        minute=int(time_str.split(':')[1]), 
                                                        second=0, microsecond=0)
                        task_date = task_date + timedelta(days=days_map[circle_str])
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

    def _execute_task(self, task_id: str, context_info: dict):
        """执行任务"""
        try:
            msg_info = context_info.get('msg', {})
            content = context_info.get('content', '')
            is_group = context_info.get('isgroup', False)
            
            # 判断是否是提醒功能
            if content.startswith('提醒'):
                try:
                    from lib.gewechat import GewechatClient
                    
                    # 创建客户端
                    client = GewechatClient(
                        base_url=conf().get("gewechat_base_url"),
                        token=conf().get("gewechat_token")
                    )
                    
                    # 去掉"提醒"前缀并格式化消息
                    reminder_content = content[2:].strip()  # 移除"提醒"两个字
                    reminder_message = f"⏰ 定时提醒\n{'-' * 20}\n{reminder_content}\n{'-' * 20}\n发送时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    
                    # 发送提醒消息
                    client.post_text(
                        conf().get("gewechat_app_id"),
                        msg_info.get('from_user_id'),
                        reminder_message
                    )
                    logger.info(f"[DifyTask] 已发送提醒消息: {task_id}")
                    
                except Exception as e:
                    logger.error(f"[DifyTask] 发送提醒消息失败: {e}")
                    raise
            else:
                # 非提醒消息，需要转发给其他插件处理
                try:
                    chat_msg = ChatMessage({})
                    chat_msg.content = content
                    chat_msg.from_user_id = msg_info.get('from_user_id')
                    chat_msg.to_user_id = msg_info.get('to_user_id')
                    chat_msg.actual_user_id = msg_info.get('actual_user_id', msg_info.get('from_user_id'))
                    chat_msg.create_time = msg_info.get("create_time", int(time.time()))
                    chat_msg.is_group = is_group
                    chat_msg._prepared = True
                    
                    # 设置其他用户ID和昵称
                    chat_msg.other_user_id = chat_msg.from_user_id
                    
                    # 为群消息设置额外属性
                    if is_group:
                        try:
                            # 使用群聊专用接口获取群信息
                            group_info = self.client.get_chatroom_info(
                                self.app_id,
                                chat_msg.from_user_id
                            )
                            if group_info.get('ret') == 200:
                                data = group_info.get('data', {})
                                chat_msg.other_user_nickname = data.get('nickName', chat_msg.from_user_id)
                                chat_msg.actual_user_nickname = data.get('nickName', chat_msg.from_user_id)
                            else:
                                logger.error(f"[DifyTask] 获取群信息失败: {group_info}")
                                chat_msg.other_user_nickname = chat_msg.from_user_id
                                chat_msg.actual_user_nickname = chat_msg.from_user_id
                        except Exception as e:
                            logger.error(f"[DifyTask] 获取群名称失败: {e}")
                            chat_msg.other_user_nickname = chat_msg.from_user_id
                            chat_msg.actual_user_nickname = chat_msg.from_user_id
                    else:
                        # 获取用户昵称
                        try:
                            response = requests.post(
                                f"{conf().get('gewechat_base_url')}/contacts/getBriefInfo",
                                json={
                                    "appId": conf().get('gewechat_app_id'),
                                    "wxids": [chat_msg.from_user_id]
                                },
                                headers={
                                    "X-GEWE-TOKEN": conf().get('gewechat_token')
                                }
                            )
                            if response.status_code == 200:
                                data = response.json()
                                if data.get('ret') == 200 and data.get('data'):
                                    chat_msg.other_user_nickname = data['data'][0].get('nickName', chat_msg.from_user_id)
                                    chat_msg.actual_user_nickname = data['data'][0].get('nickName', chat_msg.from_user_id)
                                else:
                                    chat_msg.other_user_nickname = chat_msg.from_user_id
                                    chat_msg.actual_user_nickname = chat_msg.from_user_id
                            else:
                                chat_msg.other_user_nickname = chat_msg.from_user_id
                                chat_msg.actual_user_nickname = chat_msg.from_user_id
                        except Exception as e:
                            logger.error(f"[DifyTask] 获取用户昵称失败: {e}")
                            chat_msg.other_user_nickname = chat_msg.from_user_id
                            chat_msg.actual_user_nickname = chat_msg.from_user_id
                    
                    # 构建 Context
                    context = Context(ContextType.TEXT, content)
                    context["session_id"] = msg_info.get('from_user_id')
                    context["receiver"] = msg_info.get('from_user_id')
                    context["msg"] = chat_msg
                    context["isgroup"] = is_group
                    context["group_name"] = chat_msg.other_user_nickname if is_group else None
                    context["is_shared_session_group"] = True if is_group else False
                    context["origin_ctype"] = ContextType.TEXT
                    context["openai_api_key"] = None
                    context["gpt_model"] = None
                    context["no_need_at"] = True
                    
                    # 创建 channel 并发送消息
                    from channel.gewechat.gewechat_channel import GeWeChatChannel
                    channel = GeWeChatChannel()
                    if not channel.client:
                        channel.client = self.client
                    channel.produce(context)
                    logger.info(f"[DifyTask] 已转发消息到插件处理: {task_id}")
                    
                except Exception as e:
                    logger.error(f"[DifyTask] 转发消息失败: {e}")
                    raise

            # 检查并删除一次性任务
            try:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                cursor.execute('SELECT circle FROM tasks WHERE id = ?', (task_id,))
                result = cursor.fetchone()
                if result:
                    circle_str = result[0]
                    # 如果是一次性任务（具体日期或今天/明天/后天），则删除
                    if len(circle_str) == 10 or circle_str in ["今天", "明天", "后天"]:
                        cursor.execute('DELETE FROM tasks WHERE id = ?', (task_id,))
                        conn.commit()
                        logger.info(f"[DifyTask] 已删除一次性任务: {task_id}")
                conn.close()
            except Exception as e:
                logger.error(f"[DifyTask] 删除一次性任务失败: {task_id} {e}")
            
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