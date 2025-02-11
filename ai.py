#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import traceback
import time
import ast
import requests
from datetime import datetime
import mysql.connector
from openai import OpenAI

# ---------------------------
# 配置部分
# ---------------------------
# DB_CONFIG = {
#     "host": "localhost",
#     "user": "hu",
#     "password": "1",
#     "database": "tieba-nepu"
# }
DB_CONFIG = {
    
}


# AI接口相关配置
API_KEY = "sk-****" # API 密钥
BASE_URL = "https://api.siliconflow.cn/v1"  # API 接口地址 兼容openai格式
MODEL = "deepseek-ai/DeepSeek-V3" # 模型名称

# QQ机器人通知相关配置
QQ_URL = "http://192.168.0.85:2999/send_group_msg" # QQ 机器人发送群消息接口地址
QQ_GROUP_ID =  # QQ 群号
MAX_AGO_TIME = 864000



# ---------------------------一般不需要修改以下内容---------------------------
def ai(text_list):
    """
    调用 AI 接口对文本列表进行审核，返回类似 python 列表的字符串，
    格式必须和输入的列表数量一致。
    """
    client = OpenAI(api_key=API_KEY, base_url=BASE_URL)
    # 黑白名单关键词设置（直接写入prompt中）
    black_word = " '鸡哥', '机械革命', '翼龙', '耳机', '华强北', 'dd' "
    white_word = " '弱智吧', '牛子', '635030267' "
    prompt = f'''
    任务描述：

    根据提供的文本列表,判断每段文本是否属于以下类别之一,并以python列表的形式输出对应的类别,但是不要使用代码块的形式：

    你审核的是一个高校的贴吧论坛
    1. 广告内容（包括硬广和软广）：
    - 硬广：直接推广产品或服务，包含明确的促销信息。
    - 软广（注意区分软广与学生之间的交易）：通过隐晦方式推广某些销售的品牌或产品，例如描述产品特点、制造期待感，或包含品牌/公司名称与正面评价。通常会包含品牌名，价格，或者打折等信息
    对于学生之间的交流，可能会被误判为软广，需要特别注意

    - 包含联系方式或者诱导“私信联系”通常归类为广告。
    - 比较礼貌的表示自己可以提供帮助通常归类为软广。
    - 含有以下关键词 {black_word} 每个关键词看作一个整体，不要分隔
    - 含有以下关键词 {white_word} 不要分析，直接认为是 0 

    2. 语言粗俗的对骂：
    - 含有歧视、恶意、不友好的词语。

    0. 其他内容：
    - 无法确定类别时,默认输出0。

    规则：
    - 如果属于多个类别，按优先级输出最重要的类别（优先级：广告 > 语言粗俗的对骂 )
    - 每个输入元素对应一个输出元素，输出数量必须与输入数量一致。
    以下内容不属于广告(此要求优先级高):
        - 学术、经验分享，专业选择推荐、求助,食堂推荐,学生间的二手交易，拼车等讨论。
        - 高校贴吧内普通的个人交易
    对于广告的判断，需要严格，广告概率大于0.5就应该判为广告
    示例输入：
    ['你好还需要吗', '收一个水壶','dd','来电气','购物上京东，私我']

    示例输出：
    [1, 0, 1, 0, 1]

    请严格遵循以上规则进行分类。
    输出只允许是类似python数组的字符串，不要输出其余内容，不要解释
    '''
    max_retries = 3  # 最大重试次数
    retry_delay = 2  # 初始重试延迟时间（秒）

    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model=MODEL,
                max_tokens=4096,
                temperature=0.2,
                messages=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": str(text_list)},
                ],
                stream=False
            )
            content = response.choices[0].message.content
            print(f"AI调用花费{response.usage.total_tokens} token")

            # 验证返回格式
            parsed = ast.literal_eval(content)
            if not isinstance(parsed, list):
                raise ValueError("返回结果不是列表类型")
            if len(parsed) != len(text_list):
                raise ValueError(f"返回长度不匹配，预期{len(text_list)}，实际{len(parsed)}")
            return content

        except (ValueError, SyntaxError, TypeError) as e:
            print(f"解析失败（第{attempt+1}次重试）：")
            traceback.print_exc()  # 打印完整的堆栈跟踪
            if content:
                print(f"AI回复：{content}")
            else:
                print("没有返回信息，大概是超时了，建议换其他家api或者稍后再试")
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
            else:
                raise  # 重新抛出异常，让调用者知道最终失败了

        except Exception as e:
            print(f"API请求失败（第{attempt+1}次重试）：")
            traceback.print_exc() # 打印完整的堆栈跟踪
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
            else:
                raise # 重新抛出异常

def qq_send(send_text: str):
    """调用 QQ 机器人接口发送群消息"""
    data = {
        "group_id": QQ_GROUP_ID,
        "message": send_text
    }
    try:
        response = requests.post(QQ_URL, json=data)
        # 如有需要，可根据 response.text 进行调试
    except Exception as e:
        print(f"发送QQ消息失败：{e}")

def time_stamp2time(time_stamp):
    dt = datetime.fromtimestamp(time_stamp)
    return dt.strftime(r"%Y-%m-%d %H:%M:%S")

# ---------------------------
# 主题贴审核（表 tid）
# ---------------------------
def audit_topics(db):
    cursor = db.cursor(dictionary=True)
    # 查询最近三天且未审核的主题贴
    sql_cmd = "SELECT tid, content, lable, create_time, user_id FROM tid"
    cursor.execute(sql_cmd)
    result = cursor.fetchall()
    if not result:
        print("没有主题贴数据")
        return
    qq_send("开始审核主题贴")
    print("开始审核主题贴")
    # 限制三天内的主题贴（时间戳单位：秒）
    lastest_time = int(max(r['create_time'] for r in result)) - MAX_AGO_TIME
    # 筛选待审核的主题贴，lable 为 '-1'
    db_dict = {r['tid']: [r['tid'], r['content'], r['create_time'], r['user_id']] 
               for r in result if int(r['create_time']) >= lastest_time and r['lable'] == '-1'}

    input_list = [[]]      # 用于存放分批的文本内容
    input_key_list = [[]]  # 对应的主题帖 tid
    not_input_list = []    # 长文本或异常文本，将无法直接 AI 审核的内容单独处理
    not_input_key_list = []

    keys = list(db_dict.keys())
    values = list(db_dict.values())

    for i in range(len(keys)):
        content = values[i][1]
        if len(content) < 1000:
            # 每批次控制内容总长度和批次数量（可根据需要调整）
            if len(str(input_list[-1])) + len(content) <= 1200 and len(input_list[-1]) < 2:
                input_list[-1].append(content)
                input_key_list[-1].append(keys[i])
            else:
                input_list.append([content])
                input_key_list.append([keys[i]])
        else:
            not_input_list.append(content)
            not_input_key_list.append(keys[i])
    
    # 对于无法审核的长文本内容，写入 pending_ids 表，lable 用 '8' 表示
    add_pending_sql = '''
        INSERT INTO pending_ids (tid, content, lable, type, is_tid, user_id)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE tid = tid
    '''
    if len(not_input_list):
        qq_send(f"主题贴中无法审核的内容数量: {len(not_input_list)}，请查看日志")
        print('无法审核的主题贴数量:', len(not_input_list))
        print('无法审核的主题贴内容:', not_input_list)
        for n in range(len(not_input_key_list)):
            tid = db_dict[not_input_key_list[n]][0]
            create_time = int(db_dict[not_input_key_list[n]][2])
            tmp_content = not_input_list[n]
            user_id = db_dict[not_input_key_list[n]][3]
            cursor.execute(add_pending_sql, (tid, tmp_content, '8', 'cant_post', '1', '0', user_id))
            inserted_id = cursor.lastrowid
            if inserted_id > 0:
                print(f"新插入的id为{inserted_id}")
            else:
                print("重复插入")

        db.commit()

    sql_update_lable = "UPDATE tid SET lable = %s, is_processed = %s WHERE tid = %s"
    good = True  # 用于标记是否存在问题帖子
    total_len = 0
    total_len_ai = 0

    if len(input_list[0]):
    # 遍历每一批次文本
        for idx in range(len(input_list)):
            current_item = input_list[idx]
            total_len += len(current_item)
            try:
                ai_response = ai(current_item)
            except Exception as e:
                error_msg = f"AI处理失败（主题贴批次）：{str(e)}"
                print(error_msg)
                qq_send(error_msg)
                continue

            try:
                ai_result = ast.literal_eval(ai_response)
            except Exception as e:
                print(f"解析AI返回结果失败：{e}")
                continue

            print("AI审核结果：", ai_result)
            if len(current_item) == len(ai_result):
                total_len_ai += len(ai_result)
                for j in range(len(ai_result)):
                    is_processed = 0
                    tid = db_dict[input_key_list[idx][j]][0]
                    tmp_content = current_item[j]
                    tmp_timestamp = int(db_dict[input_key_list[idx][j]][2])
                    user_id = db_dict[input_key_list[idx][j]][3]
                    # print("tmp_content",tmp_content)
                    # 如果 AI 返回 1（广告）或 2（不友好言论），写入 pending_ids 并发送 QQ 通知
                    if ai_result[j] == 1:
                        to_send_text = f'疑似广告贴\n具体内容: {tmp_content}\n位置 帖子tid: {tid}'
                        print(to_send_text)
                        cursor.execute(add_pending_sql, (tid, tmp_content, '1', 'bad_tid', '1', user_id))
                        inserted_id = cursor.lastrowid
                        if inserted_id > 0:
                            print(f"新插入的id为{inserted_id}")
                        else:
                            print("重复插入")
                        time.sleep(0.2)
                        tid_url = f"\n直达链接：https://tieba.baidu.com/p/{tid}\n发布时间：{time_stamp2time(tmp_timestamp)}\n id:{inserted_id}"
                        qq_send(to_send_text + tid_url)
                        good = False
                    elif ai_result[j] == 2:
                        to_send_text = f'疑似不友好言论贴\n具体内容: {tmp_content}\n位置 帖子tid: {tid}'
                        print(to_send_text)
                        cursor.execute(add_pending_sql, (tid, tmp_content, '2', 'bad_tid', '1', user_id))
                        inserted_id = cursor.lastrowid
                        if inserted_id > 0:
                            print(f"新插入的id为{inserted_id}")
                        else:
                            print("重复插入")
                        time.sleep(0.2)
                        tid_url = f"\n直达链接：https://tieba.baidu.com/p/{tid}\n发布时间：{time_stamp2time(tmp_timestamp)}\n id:{inserted_id}"
                        qq_send(to_send_text + tid_url)
                        good = False
                    elif ai_result[j] == 0:
                        is_processed = 1
                    # 更新 tid 表审核状态
                    cursor.execute(sql_update_lable, (str(ai_result[j]), str(is_processed), tid))
                    db.commit()
            else:
                print("AI返回结果长度与当前批次文本数量不匹配！")
        qq_send(f"主题贴待审核数量：{total_len}条，实际审核：{total_len_ai}条")
        print(f"主题贴待审核数量：{total_len}条，实际审核：{total_len_ai}条")
        if good:
            qq_send(f"{time_stamp2time(time.time())}\n主题贴中没有发现广告")
    else:
        print("没有新增主题贴")
        qq_send("没有新增主题贴")

    cursor.close()

# ---------------------------
# 回复审核（表 post）
# ---------------------------
def audit_replies(db):
    cursor = db.cursor(dictionary=True)
    # 查询最近三天且未审核的回复
    sql_cmd = "SELECT id, tid, floor, content, lable, pid, `timestamp`,user_id FROM post where lable='-1' and is_processed='0' "
    cursor.execute(sql_cmd)
    result = cursor.fetchall()
    if not result:
        print("没有回复数据")
        return

    # 限制三天内的回复（时间戳单位：秒）
    lastest_time = int(max(r['timestamp'] for r in result)) - MAX_AGO_TIME
    # 筛选待审核的回复，lable 为 '-1'
    db_dict = {r['id']: [r['tid'], r['content'], r['floor'], r['timestamp'], r['pid'], r['user_id']] 
               for r in result if int(r['timestamp']) >= lastest_time}
    qq_send("开始审核回复")
    print("开始审核回复")
    input_list = [[]]      # 分批审核文本内容
    input_key_list = [[]]  # 对应的回复 id
    not_input_list = []    # 无法直接审核的长文本
    not_input_key_list = []

    keys = list(db_dict.keys())
    values = list(db_dict.values())

    for i in range(len(keys)):
        content = values[i][1]
        # 此处对回复内容限制相对严格（长度<800），可根据实际情况调整
        if len(content) < 800:
            if len(str(input_list[-1])) + len(content) <= 1200 and len(input_list[-1]) < 10:
                input_list[-1].append(content)
                input_key_list[-1].append(keys[i])
            else:
                input_list.append([content])
                input_key_list.append([keys[i]])
        else:
            not_input_list.append(content)
            not_input_key_list.append(keys[i])
    
    # 对无法审核的回复内容写入 pending_ids 表，lable 用 '9' 表示
    add_pending_sql = '''
        INSERT INTO pending_ids (tid, pid, content, lable, type, is_tid, user_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE tid = tid
    '''
    if len(not_input_list):
        qq_send(f"回复中无法审核的内容数量: {len(not_input_list)}，请查看日志")
        print('无法审核的回复数量:', len(not_input_list))
        for n in range(len(not_input_key_list)):
            tid = db_dict[not_input_key_list[n]][0]
            create_time = int(db_dict[not_input_key_list[n]][3])
            pid = db_dict[not_input_key_list[n]][4]
            tmp_content = not_input_list[n]
            user_id = db_dict[not_input_key_list[n]][5]
            cursor.execute(add_pending_sql, (tid, pid, tmp_content, '9', 'cant_post', '0', user_id))
        db.commit()

    sql_update_lable = "UPDATE post SET lable = %s, is_processed = %s WHERE id = %s"
    good = True
    total_len = 0
    total_len_ai = 0
    if len(input_list[0]):
        for idx in range(len(input_list)):
            current_item = input_list[idx]
            total_len += len(current_item)
            try:
                ai_response = ai(current_item)
            except Exception as e:
                error_msg = f"AI处理失败（回复批次）：{str(e)}"
                print(error_msg)
                qq_send(error_msg)
                continue

            try:
                ai_result = ast.literal_eval(ai_response)
            except Exception as e:
                print(f"解析AI返回结果失败：{e}")
                continue

            print("AI审核回复结果：", ai_result)
            if len(current_item) == len(ai_result):
                total_len_ai += len(ai_result)
                for j in range(len(ai_result)):
                    is_processed = 0
                    tid = db_dict[input_key_list[idx][j]][0]
                    floor = db_dict[input_key_list[idx][j]][2]
                    create_time = int(db_dict[input_key_list[idx][j]][3])
                    pid = db_dict[input_key_list[idx][j]][4]
                    tmp_content = current_item[j]
                    user_id = db_dict[input_key_list[idx][j]][5]
                    # print("user_id::",user_id)
                    if ai_result[j] == 1:
                        to_send_text = (f'发现广告回复\n具体内容: {tmp_content}\n'
                                        f'位置 帖子tid: {tid} 第{floor}楼\n时间: {time_stamp2time(create_time)}')
                        print(to_send_text)
                        cursor.execute(add_pending_sql, (tid, pid, tmp_content, '1', 'bad_post', '0', user_id))
                        inserted_id = cursor.lastrowid
                        if inserted_id > 0:
                            print(f"新插入的id为{inserted_id}")
                        else:
                            print("重复插入")
                        time.sleep(0.1)
                        tid_url = f"\n直达链接：https://tieba.baidu.com/p/{tid}\n id:{inserted_id}"
                        qq_send(to_send_text + tid_url)
                        good = False
                    elif ai_result[j] == 2:
                        to_send_text = (f'发现不友好言论回复\n具体内容: {tmp_content}\n'
                                        f'位置 帖子tid: {tid} 第{floor}楼\n时间: {time_stamp2time(create_time)}')
                        print(to_send_text)
                        cursor.execute(add_pending_sql, (tid, pid, tmp_content, '2', 'bad_post', '0',user_id))
                        inserted_id = cursor.lastrowid
                        if inserted_id > 0:
                            print(f"新插入的id为{inserted_id}")
                        else:
                            print("重复插入")
                        time.sleep(0.1)
                        tid_url = f"\n直达链接：https://tieba.baidu.com/p/{tid}\n id:{inserted_id}"
                        qq_send(to_send_text + tid_url)
                        good = False
                    elif ai_result[j] == 0:
                        is_processed = 1
                    cursor.execute(sql_update_lable, (str(ai_result[j]), str(is_processed), int(input_key_list[idx][j])))
                    db.commit()
            else:
                print("AI返回结果长度与当前批次回复数量不匹配！")
        qq_send(f"回复待审核数量：{total_len}条，实际审核：{total_len_ai}条")
        print(f"回复待审核数量：{total_len}条，实际审核：{total_len_ai}条")
        if good:
            qq_send(f"{time_stamp2time(time.time())}\n回复中没有发现广告")
    else:
        print("没有新增回复")
        qq_send("没有新增回复")
    cursor.close()

# ---------------------------
# 主程序入口
# ---------------------------
def main():
    try:
        db = mysql.connector.connect(**DB_CONFIG)
    except Exception as e:
        print(f"数据库连接失败：{e}")
        return

    try:
        audit_topics(db)
    except Exception as e:
        print(f"审核主题贴时发生异常：{e}")
        qq_send(f"审核主题贴时发生异常：{e}")

    try:
        audit_replies(db)
    except Exception as e:
        print(f"审核回复时发生异常：{e}")
        qq_send(f"审核回复时发生异常：{e}")

    db.close()

if __name__ == '__main__':
    main()