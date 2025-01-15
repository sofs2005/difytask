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
    version="1.3.0",
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
            
            # 添加默认配置
            if not self.plugin_config.get("task_list_password"):
                self.plugin_config["task_list_password"] = "123456"
            if not self.plugin_config.get("task_capacity"):
                self.plugin_config["task_capacity"] = 100
            
            with open(config_path, "w", encoding="utf-8") as f:
                json.dump(self.plugin_config, f, indent=4)
            
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
            
            # 新增：创建联系人信息表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS contacts (
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
        """更新群组和联系人信息"""
        try:
            # 获取所有群聊和联系人列表
            response = self.client.fetch_contacts_list(self.app_id)
            logger.debug(f"[DifyTask] fetch_contacts_list response: {response}")

            if response.get('ret') == 200:
                current_time = int(time.time())
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                
                # 处理群组信息（保持原有逻辑）
                chatrooms = response.get('data', {}).get('chatrooms', [])
                logger.info(f"[DifyTask] Total chatrooms found: {len(chatrooms)}")
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
                    except Exception as e:
                        logger.error(f"[DifyTask] Failed to update group {chatroom_id}: {e}")
                
                # 新增：处理联系人信息
                friends = response.get('data', {}).get('friends', [])
                logger.info(f"[DifyTask] Total friends found: {len(friends)}")
                if friends:
                    try:
                        # 批量获取联系人信息
                        friend_info_response = requests.post(
                            f"{conf().get('gewechat_base_url')}/contacts/getBriefInfo",
                            json={
                                "appId": conf().get('gewechat_app_id'),
                                "wxids": friends
                            },
                            headers={
                                "X-GEWE-TOKEN": conf().get('gewechat_token')
                            }
                        )
                        
                        if friend_info_response.status_code == 200:
                            data = friend_info_response.json()
                            if data.get('ret') == 200 and data.get('data'):
                                for contact in data['data']:
                                    wxid = contact.get('userName')
                                    nickname = contact.get('nickName', '')
                                    if wxid:
                                        cursor.execute('''
                                            INSERT OR REPLACE INTO contacts (wxid, nickname, updated_at)
                                            VALUES (?, ?, ?)
                                        ''', (wxid, nickname, current_time))
                    except Exception as e:
                        logger.error(f"[DifyTask] Failed to update contacts: {e}")
                
                # 清理过期数据（保持原有逻辑并添加联系人清理）
                week_ago = current_time - 7 * 24 * 3600
                cursor.execute('DELETE FROM groups WHERE updated_at < ?', (week_ago,))
                cursor.execute('DELETE FROM contacts WHERE updated_at < ?', (week_ago,))
                
                conn.commit()
                conn.close()
                logger.info("[DifyTask] Groups and contacts info updated")
            else:
                logger.error(f"[DifyTask] Failed to fetch contacts list: {response}")
                
        except Exception as e:
            logger.error(f"[DifyTask] Failed to update groups and contacts: {e}")

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
            # 检查格式
            if ':' not in time_str and '：' not in time_str:
                return False, "时间格式错误，请使用 HH:mm 格式"
            
            # 统一处理中文冒号
            time_str = time_str.replace('：', ':')
            
            # 解析时间
            hour, minute = time_str.split(':')
            hour = int(hour)
            minute = int(minute)
            
            # 验证范围
            if hour < 0 or hour > 23:
                return False, "小时必须在0-23之间"
            if minute < 0 or minute > 59:
                return False, "分钟必须在0-59之间"
            
            # 检查时间是否过期
            now = datetime.now()
            if hour < now.hour or (hour == now.hour and minute < now.minute):
                # 仅当指定"今天"时才提示过期
                return True, "today_expired"
            
            logger.debug("[DifyTask] 时间格式验证通过")
            return True, None
            
        except ValueError:
            return False, "时间格式错误，请使用 HH:mm 格式，例如：09:30"
        except Exception as e:
            logger.error(f"[DifyTask] 时间格式验证失败: {e}")
            return False, "时间格式验证失败"

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
            # 再次验证时间格式（双重检查）
            if not circle_str.startswith("cron["):  # 非 cron 表达式才需要验证时间
                is_valid, error_msg = self._validate_time_format(time_str)
                if not is_valid:
                    return error_msg
                
                # 验证时间值
                hour, minute = time_str.replace('：', ':').split(':')
                hour = int(hour)
                minute = int(minute)
                
                if hour > 23 or minute > 59:
                    return "时间格式错误：小时必须在0-23之间，分钟必须在0-59之间"
                
                # 检查是否过期
                if circle_str == "今天":
                    if hour < datetime.now().hour or (hour == datetime.now().hour and minute < datetime.now().minute):
                        return "指定的时间已过期，请设置未来的时间"
                elif circle_str == "明天" or circle_str == "后天":
                    # 这两种情况不需要检查，因为必定是未来时间
                    pass
                elif len(circle_str) == 10:  # YYYY-MM-DD 格式
                    try:
                        target_date = datetime.strptime(f"{circle_str} {time_str}", "%Y-%m-%d %H:%M")
                        if target_date <= datetime.now():
                            return "指定的时间已过期，请设置未来的时间"
                    except ValueError:
                        return "日期格式错误"
            
            # 连接数据库
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            try:
                 # 保存原始时间用于比较
                original_time = time_str
                # 检查任务数量是否超出限制
                cursor.execute('SELECT COUNT(*) FROM tasks')
                task_count = cursor.fetchone()[0]
                task_capacity = self.plugin_config.get("task_capacity", 100)
                if task_count >= task_capacity:
                    return f"任务数量已达上限（{task_capacity}），请先删除一些任务"
                
                # 检查并调整时间以避免冲突
                adjusted_time = self._adjust_time_for_conflicts(time_str, circle_str, cursor)
                if adjusted_time is None:
                    return "无法调整时间，可能已超出有效范围"
                if adjusted_time != time_str:
                    time_str = adjusted_time
                    logger.info(f"[DifyTask] 任务时间已自动调整为: {time_str} 以避免冲突")
                # 保存到数据库前的最后验证
                if not self._is_valid_task_data(time_str, circle_str, event_str):
                    return "任务数据验证失败，请检查输入"
                    
                # 获取消息相关信息
                cmsg: ChatMessage = context.get("msg", None)
                
                # 检查是否在私聊中指定了用户或群组
                is_group = context.get("isgroup", False)
                group_name = None
                target_user = None

                if not is_group:
                    # 检查是否指定了用户
                    if event_str.startswith("u["):
                        # 从事件内容中提取用户名和密码，格式：u[用户名] 密码 其他内容
                        match = re.match(r'u\[([^\]]+)\]\s+(\S+)\s+(.*)', event_str)
                        if match:
                            user_name = match.group(1)
                            input_password = match.group(2)
                            event_str = match.group(3).strip()
                            logger.debug(f"[DifyTask] 从私聊中提取用户名: {user_name}, 实际内容: {event_str}")
                            
                            # 验证密码
                            password = self.plugin_config.get("task_list_password")
                            if not password:
                                return "未配置任务密码，无法创建指定用户的任务"
                            if input_password != password:
                                return "密码错误"
                            
                            # 查询用户信息
                            cursor.execute('SELECT wxid FROM contacts WHERE nickname = ?', (user_name,))
                            result = cursor.fetchone()
                            
                            if result:
                                target_user = result[0]
                                # 更新消息信息
                                if cmsg:
                                    # 创建新的 ChatMessage 对象，而不是使用字典
                                    new_msg = ChatMessage({})
                                    new_msg.from_user_id = target_user
                                    new_msg.actual_user_id = cmsg.from_user_id
                                    new_msg.to_user_id = cmsg.to_user_id
                                    new_msg.create_time = cmsg.create_time
                                    new_msg.is_group = False
                                    new_msg._prepared = True
                                    # 使用 ChatMessage 对象更新 context
                                    context["msg"] = new_msg
                                    logger.debug(f"[DifyTask] 找到用户: {user_name}, wxid: {target_user}")
                            else:
                                return f"未找到用户: {user_name}"
                        else:
                            return "格式错误，正确格式：#task 时间 周期 u[用户名] 密码 任务内容"

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
                
                # 检查是否在私聊中指定了用户或群组
                is_group = context.get("isgroup", False)
                group_name = None
                target_user = None

                if not is_group:
                    # 检查是否指定了用户
                    if event_str.startswith("u["):
                        # 从事件内容中提取用户名和密码，格式：u[用户名] 密码 其他内容
                        match = re.match(r'u\[([^\]]+)\]\s+(\S+)\s+(.*)', event_str)
                        if match:
                            user_name = match.group(1)
                            input_password = match.group(2)
                            event_str = match.group(3).strip()
                            logger.debug(f"[DifyTask] 从私聊中提取用户名: {user_name}, 实际内容: {event_str}")
                            
                            # 验证密码
                            password = self.plugin_config.get("task_list_password")
                            if not password:
                                return "未配置任务密码，无法创建指定用户的任务"
                            if input_password != password:
                                return "密码错误"
                            
                            # 查询用户信息
                            cursor.execute('SELECT wxid FROM contacts WHERE nickname = ?', (user_name,))
                            result = cursor.fetchone()
                            
                            if result:
                                target_user = result[0]
                                # 更新消息信息
                                if cmsg:
                                    # 创建新的 ChatMessage 对象，而不是使用字典
                                    new_msg = ChatMessage({})
                                    new_msg.from_user_id = target_user
                                    new_msg.actual_user_id = cmsg.from_user_id
                                    new_msg.to_user_id = cmsg.to_user_id
                                    new_msg.create_time = cmsg.create_time
                                    new_msg.is_group = False
                                    new_msg._prepared = True
                                    # 使用 ChatMessage 对象更新 context
                                    context["msg"] = new_msg
                                    logger.debug(f"[DifyTask] 找到用户: {user_name}, wxid: {target_user}")
                            else:
                                return f"未找到用户: {user_name}"
                        else:
                            return "格式错误，正确格式：#task 时间 周期 u[用户名] 密码 任务内容"
                    
                    # 现有的群组处理代码
                    elif event_str.startswith("g["):
                        # 从事件内容中提取群名和密码，格式：g[群名] 密码 其他内容
                        match = re.match(r'g\[([^\]]+)\]\s+(\S+)\s+(.*)', event_str)
                        if match:
                            group_name = match.group(1)
                            input_password = match.group(2)
                            event_str = match.group(3).strip()
                            logger.debug(f"[DifyTask] 从私聊中提取群名: {group_name}, 实际内容: {event_str}")
                            
                            # 验证密码
                            password = self.plugin_config.get("task_list_password")
                            if not password:
                                return "未配置任务密码，无法创建群组任务"
                            if input_password != password:
                                return "密码错误"
                            
                            # 查询群组信息
                            cursor.execute('SELECT wxid FROM groups WHERE nickname = ?', (group_name,))
                            result = cursor.fetchone()
                            
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
                            return "格式错误，正确格式：#task 时间 周期 g[群名] 密码 任务内容"
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
                    "msg": msg_info if 'msg_info' in locals() else context.get("msg", {})
                }
                logger.debug(f"[DifyTask] 上下文信息: {context_info}")
                
                # 保存到数据库
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
                # 使用原始时间来判断是否发生了调整
                if time_str != original_time:
                    return f"已创建任务（时间自动调整）: [{task_id}] {time_str} {circle_str} {event_str}"
                return f"已创建任务: [{task_id}] {time_str} {circle_str} {event_str}"

            finally:
                # 确保数据库连接被正确关闭
                conn.close()

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
                        # 如果任务时间已过期就删除
                        if now > task_date:
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
                        # 如果任务时间已过期就删除
                        if now > task_date:
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
            command = content[len(self.command_prefix):].strip()
            
            # 空命令显示帮助
            if not command:
                e_context['reply'] = Reply(ReplyType.TEXT, self.get_help_text())
                e_context.action = EventAction.BREAK_PASS
                return
            
            # 1. 先处理特殊命令（任务列表和取消任务）
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
            
            if command.startswith("取消任务 "):
                task_id = command.replace("取消任务 ", "", 1).strip()
                result = self._delete_task(task_id)
                e_context['reply'] = Reply(ReplyType.TEXT, result)
                e_context.action = EventAction.BREAK_PASS
                return
            
            # 2. 处理 cron 表达式命令
            if "cron[" in command:
                # 验证命令格式（不需要再导入 re）
                match = re.match(r'cron\[(.*?)\](?:\s+g\[(.*?)\])?\s+(.*)', command)
                if not match:
                    e_context['reply'] = Reply(ReplyType.TEXT, "命令格式错误，正确格式：\n$time cron[分 时 日 月 周] 事件内容\n或\n$time cron[分 时 日 月 周] g[群组] 事件内容")
                    e_context.action = EventAction.BREAK_PASS
                    return
                    
                cron_exp = match.group(1).strip()
                group_name = match.group(2)  # 可能是 None
                event_content = match.group(3).strip()
                
                # 验证事件内容不为空
                if not event_content:
                    e_context['reply'] = Reply(ReplyType.TEXT, "请输入事件内容")
                    e_context.action = EventAction.BREAK_PASS
                    return
                    
                # 验证 cron 表达式格式
                if not self._validate_cron_format(cron_exp):
                    e_context['reply'] = Reply(ReplyType.TEXT, "cron表达式格式错误，正确格式为：分 时 日 月 周\n例如：*/15 9-18 * * 1-5")
                    e_context.action = EventAction.BREAK_PASS
                    return
                    
                # 构建完整的事件字符串
                event_str = f"g[{group_name}] {event_content}" if group_name else event_content
                
                # 创建任务
                result = self._create_task("00:00", f"cron[{cron_exp}]", event_str, e_context['context'])
                e_context['reply'] = Reply(ReplyType.TEXT, result)
                e_context.action = EventAction.BREAK_PASS
                return
                
            # 3. 处理普通定时任务
            match = re.match(r'([^\s]+)\s+([^\s]+)(?:\s+g\[(.*?)\])?\s+(.*)', command)
            if not match:
                e_context['reply'] = Reply(ReplyType.TEXT, "命令格式错误，正确格式：\n周期 时间 事件内容\n或\n周期 时间 g[群组] 事件内容")
                e_context.action = EventAction.BREAK_PASS
                return
                
            circle_str = match.group(1).strip()
            time_str = match.group(2).strip()
            group_name = match.group(3)  # 可能是 None
            event_content = match.group(4).strip()
            
            # 验证事件内容不为空
            if not event_content:
                e_context['reply'] = Reply(ReplyType.TEXT, "请输入事件内容")
                e_context.action = EventAction.BREAK_PASS
                return
                
            # 验证基本格式
            is_valid, error_msg = self._validate_normal_format(circle_str, time_str)
            if not is_valid:
                e_context['reply'] = Reply(ReplyType.TEXT, error_msg)
                e_context.action = EventAction.BREAK_PASS
                return
                
            # 构建完整的事件字符串
            event_str = f"g[{group_name}] {event_content}" if group_name else event_content
            
            # 创建任务
            result = self._create_task(time_str, circle_str, event_str, e_context['context'])
            e_context['reply'] = Reply(ReplyType.TEXT, result)
            e_context.action = EventAction.BREAK_PASS

    def _adjust_time_for_conflicts(self, time_str: str, circle_str: str, cursor) -> str:
        """调整时间避免冲突，返回调整后的时间"""
        try:
            # 如果是cron表达式，不需要调整
            if circle_str.startswith("cron["):
                return time_str
            
            # 解析原始时间
            hour, minute = time_str.replace('：', ':').split(':')
            hour = int(hour)
            minute = int(minute)
            
            # 获取所有任务的时间
            cursor.execute('SELECT time FROM tasks')
            existing_tasks = cursor.fetchall()
            logger.debug(f"[DifyTask] 当前所有任务时间: {existing_tasks}")
            
            # 检查是否存在冲突
            while True:
                current_time = f"{hour:02d}:{minute:02d}"
                if not any(task[0] == current_time for task in existing_tasks):
                    break
                
                # 时间冲突，分钟数+1
                minute += 1
                if minute >= 60:
                    hour += 1
                    minute = 0
                if hour >= 24:
                    return None  # 无法调整
                
                logger.debug(f"[DifyTask] 尝试调整时间到: {hour:02d}:{minute:02d}")
            
            # 返回调整后的时间
            return f"{hour:02d}:{minute:02d}"
            
        except Exception as e:
            logger.error(f"[DifyTask] 调整时间失败: {e}")
            return None

    def _validate_cron_format(self, cron_exp):
        """验证 cron 表达式的基本格式"""
        try:
            parts = cron_exp.split()
            if len(parts) != 5:
                logger.debug(f"[DifyTask] cron表达式必须包含5个部分，当前: {len(parts)}")
                return False
            
            # 只验证格式，不验证具体值
            return True
        except Exception as e:
            logger.debug(f"[DifyTask] cron表达式格式验证失败: {e}")
            return False 

    def _validate_normal_format(self, circle_str, time_str):
        """验证普通定时任务的基本格式"""
        try:
            # 1. 验证时间格式 HH:mm
            if ':' not in time_str and '：' not in time_str:
                logger.debug(f"[DifyTask] 时间格式错误: {time_str}")
                return False, "时间格式错误，请使用 HH:mm 格式"
            
            # 2. 验证周期格式
            valid_circles = ["每天", "工作日", "今天", "明天", "后天"]
            if circle_str in valid_circles:
                return True, None
            
            # 每周几
            week_days = ["一", "二", "三", "四", "五", "六", "日"]
            if circle_str.startswith("每周"):
                day = circle_str[2:]
                if day in week_days:
                    return True, None
                return False, "每周后面必须是：一、二、三、四、五、六、日"
            
            # 具体日期 YYYY-MM-DD
            if len(circle_str) == 10:
                if not re.match(r'^\d{4}-\d{2}-\d{2}$', circle_str):
                    return False, "日期格式错误，正确格式：YYYY-MM-DD"
                return True, None
            
            return False, "周期格式错误，支持：每天、每周x、工作日、YYYY-MM-DD、今天、明天、后天"
        except Exception as e:
            logger.debug(f"[DifyTask] 格式验证失败: {e}")
            return False, f"格式验证失败: {str(e)}" 

    def _is_valid_task_data(self, time_str, circle_str, event_str):
        """验证任务数据的有效性"""
        try:
            if circle_str.startswith("cron["):
                # 验证 cron 表达式
                cron_exp = circle_str[5:-1].strip()
                try:
                    croniter(cron_exp)
                    return True
                except:
                    return False
            else:
                # 验证时间格式
                is_valid, _ = self._validate_time_format(time_str)
                if not is_valid:
                    return False
                    
                # 验证周期格式
                if not self._validate_circle_format(circle_str):
                    return False
                    
            return True
        except:
            return False 