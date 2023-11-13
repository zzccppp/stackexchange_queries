import mysql.connector

db = mysql.connector.connect(
    host="localhost",
    port="3366",
    user="root",
    password="123456",
    database="stackexchange_cs",
)

cursor = db.cursor()

cursor.execute("SELECT Id, TagName FROM tags")
tags = cursor.fetchall()
tag_dict = {tag_name: tag_id for tag_id, tag_name in tags}

cursor.execute("SELECT Id, Tags FROM posts WHERE Tags IS NOT NULL")
posts = cursor.fetchall()

post_tag_data = []

for post in posts:
    post_id, tags_str = post
    tags = tags_str.strip(">").strip("<").split("><")

    for tag in tags:
        tag_id = tag_dict.get(tag)
        if tag_id:
            post_tag_data.append((post_id, tag_id))

cursor.executemany(
    "INSERT INTO post_tags (PostId, TagId) VALUES (%s, %s)", post_tag_data
)

db.commit()

cursor.close()
db.close()
