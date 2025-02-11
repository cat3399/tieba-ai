import aiotieba
import asyncio
import mysql.connector
import time
from datetime import datetime

# -----------------------------
# 工具函数：时间戳转换为格式化时间字符串
# -----------------------------
def time_stamp2time(time_stamp):
    dt = datetime.fromtimestamp(time_stamp)
    return dt.strftime("%Y-%m-%d %H:%M:%S")

# -----------------------------
# 数据库连接配置（全局连接，两个扫描函数共用）
# -----------------------------

# 需要修改为自己的数据库连接信息
db = mysql.connector.connect(
    host="localhost",
    user="hu",
    password="1",
    database="tieba-tuc"
)

# 贴吧名称
ba_name = "东北石油大学"


max_reply = 500  # 回复数量上限
# -----------------------------
# 扫描新增主题帖函数（同步接口，内部使用异步调用 aiotieba）
# -----------------------------
def scan_new_threads():
    print("********正在扫描新增主题贴********")
    
    async def fetch_existing_threads():
        """
        从数据库中获取已记录的帖子 tid 和最后更新时间，
        返回一个字典，键为字符串形式的 tid。
        """
        cursor = db.cursor(dictionary=True)
        cursor.execute("SELECT tid, last_update_time FROM tid")
        result_list = cursor.fetchall()
        cursor.close()
        # 将 tid 转换为字符串，以便后续比较
        return {str(result['tid']): result['last_update_time'] for result in result_list}

    async def process_threads(client, tid_dict):
        cursor = db.cursor(dictionary=True)
        # 遍历贴吧前 19 页（可根据实际需求调整）
        for pn_num in range(1, 20):
            # 获取指定页数的帖子列表
            thread_list = await client.get_threads(ba_name, sort=5, pn=pn_num)
            for thread in thread_list:
                # 跳过置顶帖子
                if thread.is_top:
                    continue

                # 如果数据库中已存在该帖子，且数据库中的更新时间大于等于当前获取的更新时间，则认为后续帖子均已扫描过
                if str(thread.tid) in tid_dict and int(thread.last_time) <= int(tid_dict[str(thread.tid)]):
                    print(f"截至到 {thread.tid} 后续帖子均已扫描过")
                    cursor.close()
                    return

                # 获取发帖人的用户信息（昵称）
                user_info = await client.get_user_info(thread.author_id)
                nick_name = user_info.nick_name
                print(f"--帖子 tid: {thread.tid} 发帖人: {nick_name}\n帖子标题: {thread.title}\n创建时间: {time_stamp2time(thread.create_time)}  更新时间: {time_stamp2time(thread.last_time)}")

                # 将帖子信息插入或更新到数据库中
                # 注意：这里额外添加了 scan_timestamp 字段，用于后续回复扫描时判断是否有新回复
                tid_sql_cmd = """
                INSERT INTO tid (tid, nick_name, user_id, title, content, create_time, last_update_time, scan_timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    nick_name = VALUES(nick_name),
                    title = VALUES(title),
                    content = VALUES(content),
                    last_update_time = VALUES(last_update_time);
                """
                # 新帖时，scan_timestamp 可初始化为 0
                cursor.execute(tid_sql_cmd, (
                    thread.tid, 
                    nick_name, 
                    thread.author_id, 
                    thread.title, 
                    thread.text, 
                    thread.create_time, 
                    thread.last_time,
                    0
                ))

                if cursor.rowcount == 1:
                    print(f"新增帖子 tid: {thread.tid} 内容: {thread.text[:100]} 发布时间: {time_stamp2time(thread.create_time)}")
                elif cursor.rowcount == 2:
                    print(f"帖子 tid: {thread.tid} 标题: {thread.title} 有新回复!")
                print()
                db.commit()
            await asyncio.sleep(1)
        cursor.close()

    async def main():
        tid_dict = await fetch_existing_threads()
        async with aiotieba.Client() as client:
            await process_threads(client, tid_dict)

    # 使用 asyncio.run 调用内部的异步主函数
    asyncio.run(main())

# -----------------------------
# 扫描新增回复函数（同步接口，内部使用异步调用 aiotieba）
# -----------------------------
def scan_new_replies():
    print("********正在扫描新增回复********")
    cursor = db.cursor(dictionary=True)
    # 从数据库 tid 表中读取帖子信息，用于判断哪些帖子需要扫描回复
    sql_cmd = "SELECT tid, last_update_time, scan_timestamp FROM tid ORDER BY last_update_time DESC"
    cursor.execute(sql_cmd)
    sql_results = cursor.fetchall()
    current_timestamp = time.time()
    # 仅选取最后更新时间在最近10天内（864000秒）且上次扫描时间早于最后更新时间的帖子
    tid_list = [
        int(result['tid']) 
        for result in sql_results 
        if int(result['last_update_time']) + 864000 > int(current_timestamp) and int(result['scan_timestamp']) < int(result['last_update_time'])
    ]
    print(f"需要扫描的帖子 tid 列表: {tid_list}, 数量: {len(tid_list)}\n")

    # SQL 语句：插入或更新“楼层回复”
    post_sql_cmd = """
        INSERT INTO post (
            nick_name, user_id, content, floor, pid, created_time, tid, timestamp
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            nick_name = VALUES(nick_name),
            user_id = VALUES(user_id),
            content = VALUES(content),
            floor = VALUES(floor),
            pid = VALUES(pid),
            created_time = VALUES(created_time),
            tid = VALUES(tid),
            timestamp = VALUES(timestamp);
    """
    # SQL 语句：插入或更新“楼中楼回复”
    post_post_sql_cmd = """
        INSERT INTO post (
            nick_name, user_id, content, floor, pid, ppid, created_time, tid, is_reply_reply, timestamp
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            nick_name = VALUES(nick_name),
            user_id = VALUES(user_id),
            content = VALUES(content),
            floor = VALUES(floor),
            pid = VALUES(pid),
            ppid = VALUES(ppid),
            created_time = VALUES(created_time),
            tid = VALUES(tid),
            is_reply_reply = VALUES(is_reply_reply),
            timestamp = VALUES(timestamp);
    """

    async def main():
        post_num = 0
        try:
            async with aiotieba.Client() as client:
                for tid in tid_list:
                    total_page_num = 1
                    current_page_num = 0
                    while current_page_num < total_page_num:
                        current_page_num += 1
                        # 获取指定页数的帖子回复
                        posts = await client.get_posts(tid, pn=current_page_num)
                        total_page_num = posts.page.total_page
                        # 如果回复数量过多，只处理最后一页的数据
                        if posts.thread.reply_num > max_reply:
                            if total_page_num >= 2:
                                print(f"帖子 {tid} 回复过多, 仅处理最后一页数据")
                                current_page_num = total_page_num 
                            else:
                                print(f"逆天帖子, 第一页楼中楼数量超多 😡, tid: {tid}")
                                continue
                        print(f"tid: {tid} 共 {posts.page.total_page} 页, 当前处理第 {current_page_num} 页")
                        
                        # 处理每个回复
                        for post in posts:
                            cursor.execute(post_sql_cmd, (
                                post.user.nick_name, 
                                post.author_id, 
                                post.text, 
                                post.floor, 
                                post.pid, 
                                time_stamp2time(post.create_time), 
                                post.tid, 
                                post.create_time
                            ))
                            if cursor.rowcount == 1:
                                print(f"帖子 {post.tid} 新增楼层回复, 发送人: {post.user.nick_name}, 内容: {post.text}, 时间: {time_stamp2time(post.create_time)}")
                                post_num += 1
                            db.commit()
                            await asyncio.sleep(0.3)
                            # 如果该回复有“楼中楼”回复，则继续获取并写入
                            if post.reply_num:
                                comments = await client.get_comments(tid, post.pid)
                                for comment in comments:
                                    cursor.execute(post_post_sql_cmd, (
                                        comment.user.nick_name, 
                                        comment.author_id, 
                                        comment.text, 
                                        comment.floor, 
                                        comment.pid, 
                                        comment.ppid, 
                                        time_stamp2time(comment.create_time), 
                                        post.tid, 
                                        True, 
                                        comment.create_time
                                    ))
                                    if cursor.rowcount == 1:
                                        print(f"帖子 {post.tid} 新增楼中楼回复, 发送人: {comment.user.nick_name}, 内容: {comment.text}, 时间: {time_stamp2time(comment.create_time)}")
                                        post_num += 1
                                    db.commit()
                        await asyncio.sleep(0.2)
                    # 扫描完当前 tid 后更新其 scan_timestamp 字段
                    sql_update = "UPDATE tid SET scan_timestamp = %s WHERE tid = %s;"
                    new_timestamp = int(time.time())
                    cursor.execute(sql_update, (new_timestamp, str(tid)))
                    db.commit()
                    print(f"已更新 tid: {tid} 的扫描时间戳")
                    await asyncio.sleep(0.2)
        except Exception as e:
            print(f"扫描回复时出错: {e}")
        print("回复扫描完成")
        print(f"新增 {post_num} 条回复\n")
    await_main = main()  # 得到协程对象
    asyncio.run(await_main)
    cursor.close()

# -----------------------------
# 主入口：先扫描新增主题帖，再扫描新增回复
# -----------------------------
if __name__ == "__main__":
    # try:
    #     scan_new_threads()
    #     scan_new_replies()
    # finally:
    #     db.close()
    scan_new_threads()
    scan_new_replies()
    db.close()