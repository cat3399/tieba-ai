import aiotieba
from datetime import datetime
import json
import re
from threading import Thread
from queue import Queue
import websockets
import asyncio
import json
import requests
import mysql.connector
import time

uri = "ws://192.168.0.85:3002/event" # QQ机器人 WebSocket 服务器地址
BDUSS = "WlSZ****" # 贴吧 BDUSS
group_id =   # QQ 群号 
ba_name = "东北石油大学" # 贴吧吧名
qq_send_url = "http://192.168.0.85:2999/send_group_msg" # QQ机器人发送群消息接口地址


# 数据库配置 需要自行修改
db_config = {
    'host': '127.0.0.1',
    'user': 'hu',
    'password': '1',
    'database': 'tieba-nepu'
}

db = mysql.connector.connect(**db_config)


def get_fresh_db():
    return mysql.connector.connect(**db_config)

def time_stamp2time(time_stamp:datetime.timestamp):
    dt = datetime.fromtimestamp(time_stamp)
    formatted_date1 = dt.strftime(r"%Y-%m-%d %H:%M:%S")
    return formatted_date1

def qq_send(send_text:str):
    # 目标URL
    url = qq_send_url
    data = {
        "group_id": group_id,
        "message": send_text
    }
    response = requests.post(url, json=data)
    # print(response.text)

def get_text(data_json):
    text = ""
    for i in data_json:
        if i['type'] == "text":
            text += i['data']['text']
    return text

def del_by_id(ids):
    print("删除", ids)

    fresh_db = get_fresh_db()  # 重新获取数据库连接
    cursor = fresh_db.cursor(dictionary=True)
    get_sql = "SELECT tid, pid FROM pending_ids WHERE id = %s;"

    async def del_tid_or_pid_and_user(user_id:int, tid, pid=None):
        """合并删除 tid 和 pid 的异步方法"""
        async with aiotieba.Client(BDUSS=BDUSS) as client:
            if pid:
                response = await client.del_post(ba_name, tid, pid)
                response_user = await client.block(ba_name,user_id,day=10)
            else:
                response = await client.del_thread(ba_name, tid)
                response_user = await client.block(ba_name,user_id,day=10)
            # print(response)
            return response,response_user
    loop = asyncio.new_event_loop()  # 创建事件循环，避免 RuntimeError
    asyncio.set_event_loop(loop)
    if ids == "all":
        cursor.execute("select id from pending_ids where is_processed='0'")
        results = cursor.fetchall()
        print("result",)
        ids = [str(result['id']) for result in results ]
            
    for id in ids:
        print(f"正在删除 {id}")
        cursor.execute(get_sql, (id,))
        result = cursor.fetchone()  # 直接获取一行数据

        if not result:
            print(f"ID{id} 未找到，跳过")
            qq_send(f"ID{id} 未找到，跳过")
            continue

        tid, pid = result["tid"], result["pid"]
        cursor.execute("select user_id from pending_ids where id=%s",(id,))                                                            
        result = cursor.fetchone()
        print(result)
        user_id = int(result['user_id'])
        response,response_user = loop.run_until_complete(del_tid_or_pid_and_user(user_id, tid, pid))  # 统一调用删除函数
        print(response,response_user)
        if response:
            qq_send(f"删除ID {id} 成功")
            if pid:
                print("删除回复")
                cursor.execute("UPDATE post SET is_processed='1' WHERE tid=%s AND pid=%s", (tid, pid,))
                print(f"Rows updated: {cursor.rowcount}") 
            else:
                print("删除帖子")
                cursor.execute("UPDATE tid SET is_processed='1' WHERE tid=%s", (tid,))
            cursor.execute("UPDATE pending_ids SET is_processed='1' WHERE id=%s", (id,))
            if response_user:
                qq_send(f"封禁用户{user_id}成功")
                print(f"封禁用户{user_id}成功")
            else:
                qq_send(f"封禁用户{user_id}失败,查询日志获取详情")
                print(f"封禁用户{user_id}失败")
        else:
            qq_send("删除失败或已被删除, 查询日志获取详情")

    cursor.close()
    fresh_db.commit()

def search_by_id(ids):
    print("查询",ids)
    fresh_db = get_fresh_db()  # 重新获取数据库连接
    cursor = fresh_db.cursor(dictionary=True)
    if ids=="all":
        search_sql = "select * from pending_ids where is_processed='0'"
        cursor.execute(search_sql)
        results = cursor.fetchall()
        if results:
            for result in results:
                result['content'] = result['content'][:40]
                send_text = str(result)+f"\n直达链接:https://tieba.baidu.com/p/{result['tid']}"
                print(send_text)
                qq_send(str(send_text))
                time.sleep(1)
                print(result)
        else:
            print("所有内容已处理")
            qq_send("所有内容已处理")
    else:
        for id in ids:
            id = str(id)
            cursor.execute("select * from pending_ids where id=%s",(id,))
            result = cursor.fetchone()
            result['content'] = result['content'][:40]
            if result:
                send_text = str(result)+f"\n直达链接:https://tieba.baidu.com/p/{result['tid']}"
                print(send_text)
                qq_send(str(send_text))
            else:
                print(f"ID {id} 未找到，跳过")
                qq_send(f"ID {id} 未找到，跳过")
            time.sleep(1)
    cursor.close()

def ignore_by_id(ids):
    print("忽略",ids)
    fresh_db = get_fresh_db()  # 重新获取数据库连接
    cursor = fresh_db.cursor(dictionary=True)
    get_sql = "SELECT tid, pid FROM pending_ids WHERE id = %s;"
    if ids == "all":
        cursor.execute("SELECT id FROM pending_ids WHERE is_processed='0'")
        ids = [result['id'] for result in cursor.fetchall()]
    for id in ids:
        id = str(id)
        cursor.execute(get_sql, (id,))
        result = cursor.fetchone()  # 直接获取一行数据
        if not result:
            print(f"ID{id} 未找到，跳过")
            qq_send(f"ID{id} 未找到，跳过")
            continue
        tid, pid = str(result["tid"]), str(result["pid"])
        if pid:
            cursor.execute("UPDATE post SET is_processed='1' WHERE tid=%s AND pid=%s", (tid, pid))
        else:
            cursor.execute("UPDATE tid SET is_processed='1' WHERE tid=%s", (tid,))
        cursor.execute("UPDATE pending_ids SET is_processed='1' WHERE id=%s", (id,))
        if cursor.rowcount > 0:
            print(f"成功更新ID {id}状态")
            qq_send(f"成功更新ID {id}状态")
        else:
            print(f"更新状态出错,或ID {id}以被处理")
            qq_send(f"更新状态出错,或ID {id}以被处理")

    cursor.close()
    fresh_db.commit()


COMMAND_HANDLERS = [
    {
        'command': '删除',
        'allow_all': True,
        'handler': del_by_id,  # 假设的处理函数
        'error_msg': '删除指令格式错误! 正确格式: 删除 1 2 3'
    },
    {
        'command': '查询',
        'allow_all': True,
        'handler': search_by_id,  # 假设的处理函数
        'error_msg': '查询指令格式错误! 正确格式: 查询 all 或 查询 1 2 3'
    },
    {
        'command': '忽略',
        'allow_all': True,
        'handler': ignore_by_id,  # 假设的处理函数
        'error_msg': '查询指令格式错误! 正确格式: 查询 all 或 查询 1 2 3'
    }
]

def parse_arguments(arg_text: str, allow_all: bool = False):
    """
    解析纯参数部分（不包含指令名称）
    :param arg_text: 参数文本（如 'all' 或 '1 2 3'）
    :param allow_all: 是否允许 all 关键字
    :return: 数字列表/'all'/None
    """
    arg_text = arg_text.strip().lower()
    if not arg_text:
        return None
    
    if allow_all and arg_text == 'all':
        return 'all'
    
    if re.match(r'^\d+(?:\s+\d+)*$', arg_text):
        numbers = list(map(int, arg_text.split()))
        return numbers if numbers else None
    
    return None

# 消息处理线程（已重构）
def message_processor(message_queue):
    print('消息处理线程已启动')
    while True:
        data = message_queue.get()
        try:
            tmp_text = get_text(data['message']).strip()
            if tmp_text == "help" or tmp_text == "帮助":
                help_text = "指令列表: 删除/查询/忽略 id1,id2.id3..../all\n例如 删除 1 2 3 或者 删除 all\n删除指令会顺便封禁用户十天\n忽略指令会将id状态改为已处理"
                qq_send(help_text)
            # 遍历所有注册的指令
            for cmd in COMMAND_HANDLERS:
                # 匹配指令前缀（不区分大小写）
                pattern = rf'^{re.escape(cmd["command"])}\s+(.*)'
                match = re.match(pattern, tmp_text, re.IGNORECASE)
                if not match:
                    continue
                
                # 提取参数部分并解析
                arg_text = match.group(1).strip()
                result = parse_arguments(arg_text, cmd['allow_all'])
                
                if result is not None:
                    cmd['handler'](result)
                else:
                    print(cmd['error_msg'])
                    qq_send(cmd['error_msg'])
                
                break  # 匹配到指令后跳出循环
            
        except Exception as e:
            pass
        finally:
            message_queue.task_done()

# 消息处理队列

async def websocket_receiver(uri, message_queue):
    """
    websocket消息接收线程，接收消息后放入消息队列
    """
    try:
        async with websockets.connect(uri) as ws:
            print('websocket链接成功，消息接收线程已启动')
            while True:
                message = await ws.recv()
                message = json.loads(message)
                if "message_type" in message.keys():
                    if message['message_type'] == "group":
                        if message["group_id"] == group_id:
                            message_queue.put(message) # 将接收到的消息放入队列
                            print("有效")
    except Exception as e:
        print(f"WebSocket 接收线程发生错误: {e}")



if __name__ == "__main__":
    message_queue = Queue()
    # 创建并启动 websocket 接收线程
    websocket_thread = Thread(target=asyncio.run, args=(websocket_receiver(uri, message_queue),))
    websocket_thread.daemon = True # 设置为守护线程，主线程退出时自动退出
    websocket_thread.start()

    # 创建并启动 消息处理线程
    processor_thread = Thread(target=message_processor, args=(message_queue,))
    processor_thread.daemon = True # 设置为守护线程
    processor_thread.start()

    print("主线程启动，等待消息...")
    try:
        while True:
            time.sleep(3600) # 主线程保持运行，可以替换为其他需要主线程执行的任务，或者保持空转
    except KeyboardInterrupt:
        print("主线程接收到 KeyboardInterrupt，程序即将退出...")
    finally:
        print("程序退出。")
