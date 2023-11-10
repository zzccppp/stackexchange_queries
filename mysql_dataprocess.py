import mysql.connector

# 连接到数据库
db = mysql.connector.connect(
    host="localhost",
    port="3366",
    user="root",
    password="123456",
    database="stackexchange_cs"
)

cursor = db.cursor()

# 获取所有tags并存储在本地字典中
cursor.execute("SELECT Id, TagName FROM tags")
tags = cursor.fetchall()
tag_dict = {tag_name: tag_id for tag_id, tag_name in tags}

# 获取所有posts和它们的tags
cursor.execute("SELECT Id, Tags FROM posts WHERE Tags IS NOT NULL")
posts = cursor.fetchall()

# 存储批量插入的数据
post_tag_data = []

for post in posts:
    post_id, tags_str = post
    # 分割tags字符串并删除尖括号
    tags = tags_str.strip('>').strip('<').split('><')

    for tag in tags:
        # 从本地字典中获取tag的ID
        tag_id = tag_dict.get(tag)
        if tag_id:
            # 将post和tag的ID配对加入待插入列表
            post_tag_data.append((post_id, tag_id))

# 批量插入到post_tags
cursor.executemany("INSERT INTO post_tags (PostId, TagId) VALUES (%s, %s)", post_tag_data)

# 提交更改
db.commit()

# 关闭数据库连接
cursor.close()
db.close()