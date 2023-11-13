import pymysql
import random
from pymongo import MongoClient

connection = pymysql.connect(
    host="localhost",
    user="root",
    password="1234",
    database="cs_stack_exchange"
)

cursor = connection.cursor()


database_name = "stackexchange_cs"
mongo_client = MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client[database_name]
mongo_collection = mongo_db["share"]


cursor.execute("SELECT Id FROM posts WHERE posttypeId IN (1, 2) ORDER BY RAND() LIMIT 10000")
valid_postIds = [row[0] for row in cursor.fetchall()]

cursor.execute("SELECT Id FROM users ORDER BY RAND() LIMIT 10000")
valid_userIds = [row[0] for row in cursor.fetchall()]


for _ in range(10000):
    #Mysql
    postId = random.choice(valid_postIds)
    sharerId = random.choice(valid_userIds)
    sharedId = random.choice([uid for uid in valid_userIds if uid != sharerId])
    cursor.execute(f"INSERT INTO Share (postId, sharerId, sharedId) VALUES ({postId}, {sharerId}, {sharedId})")
    #Mongodb
    mongo_document = {
        "postId": postId,
        "sharerId": sharerId,
        "sharedId": sharedId
    }
    mongo_collection.insert_one(mongo_document)


connection.commit()
connection.close()

mongo_client.close()
