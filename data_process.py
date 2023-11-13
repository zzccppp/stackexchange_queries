from datetime import datetime
import xml.sax
from xml.sax import xmlreader
import json
from pymongo import MongoClient, UpdateOne
from tqdm import tqdm


def parse_int(s: str) -> int:
    try:
        return int(s)
    except ValueError:
        return None


def parse_datetime(s: str) -> datetime:
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def parse_tag(s: str) -> list:
    if s.startswith("<") and s.endswith(">"):
        return s[1:-1].split("><")
    else:
        return []


class UserXMLHandler(xml.sax.ContentHandler):
    def __init__(self):
        super().__init__()
        self.users = []
        self.current_user = {}

    def startElement(self, tag: str, attributes):
        if tag == "row":
            self.current_user = {
                "UserId": int(attributes["Id"]),
                "Reputation": int(attributes["Reputation"]),
                "CreationDate": datetime.fromisoformat(attributes["CreationDate"]),
                "DisplayName": attributes["DisplayName"],
                "LastAccessDate": datetime.fromisoformat(attributes["LastAccessDate"]),
                "WebsiteUrl": attributes.get("WebsiteUrl", ""),
                "Location": attributes.get("Location", ""),
                "AboutMe": attributes.get("AboutMe", ""),
                "Views": int(attributes["Views"]),
                "UpVotes": int(attributes["UpVotes"]),
                "DownVotes": int(attributes["DownVotes"]),
                "AccountId": parse_int(attributes.get("AccountId", "")),
            }

    def endElement(self, tag: str):
        if tag == "row":
            self.users.append(self.current_user)
            self.current_user = {}


class BadgeXMLHandler(xml.sax.ContentHandler):
    def __init__(self):
        super().__init__()
        self.badges = []
        self.current_badge = {}

    def startElement(self, tag: str, attributes):
        if tag == "row":
            self.current_badge = {
                "BadgeId": int(attributes["Id"]),
                "UserId": int(attributes["UserId"]),
                "Name": attributes["Name"],
                "Date": datetime.fromisoformat(attributes["Date"]),
                "Class": int(attributes["Class"]),
                "TagBased": attributes["TagBased"] == "True",
            }

    def endElement(self, tag: str):
        if tag == "row":
            self.badges.append(self.current_badge)
            self.current_badge = {}


class PostXMLHandler(xml.sax.ContentHandler):
    def __init__(self):
        super().__init__()
        self.posts = []
        self.current_post = {}

    def startElement(self, tag: str, attributes):
        if tag == "row":
            self.current_post = {
                "PostId": int(attributes["Id"]),
                "PostTypeId": int(attributes["PostTypeId"]),
                "AcceptedAnswerId": parse_int(attributes.get("AcceptedAnswerId", "")),
                "ParentId": parse_int(attributes.get("ParentId", "")),
                "CreationDate": parse_datetime(attributes["CreationDate"]),
                "Score": int(attributes["Score"]),
                "ViewCount": parse_int(attributes.get("ViewCount", "")),
                "Body": attributes["Body"],
                "OwnerUserId": parse_int(attributes.get("OwnerUserId", "")),
                "LastEditorUserId": parse_int(attributes.get("LastEditorUserId", "")),
                "LastEditDate": parse_datetime(attributes.get("LastEditDate", "")),
                "LastActivityDate": parse_datetime(attributes["LastActivityDate"]),
                "Title": attributes.get("Title", ""),
                # "Tags": attributes.get("Tags", ""),
                "Tags": parse_tag(attributes.get("Tags", "")),
                "AnswerCount": parse_int(attributes.get("AnswerCount", "")),
                "CommentCount": parse_int(attributes.get("CommentCount", "")),
                "FavoriteCount": parse_int(attributes.get("FavoriteCount", "")),
                "ClosedDate": parse_datetime(attributes.get("ClosedDate", "")),
                "CommunityOwnedDate": parse_datetime(
                    attributes.get("CommunityOwnedDate", "")
                ),
            }

    def endElement(self, tag: str):
        if tag == "row":
            self.posts.append(self.current_post)
            self.current_post = {}


class CommentXMLHandler(xml.sax.ContentHandler):
    def __init__(self):
        super().__init__()
        self.comments = []
        self.current_comment = {}

    def startElement(self, tag: str, attributes):
        if tag == "row":
            self.current_comment = {
                "CommentId": int(attributes["Id"]),
                "PostId": int(attributes["PostId"]),
                "Score": int(attributes["Score"]),
                "Text": attributes["Text"],
                "CreationDate": parse_datetime(attributes["CreationDate"]),
                "UserId": parse_int(attributes.get("UserId", "")),
            }

    def endElement(self, tag: str):
        if tag == "row":
            self.comments.append(self.current_comment)
            self.current_comment = {}


class TagXMLHandler(xml.sax.ContentHandler):
    def __init__(self):
        super().__init__()
        self.tags = []
        self.current_tag = {}

    def startElement(self, tag: str, attributes):
        if tag == "row":
            self.current_tag = {
                "TagId": int(attributes["Id"]),
                "TagName": attributes["TagName"],
                "Count": int(attributes["Count"]),
                "ExcerptPostId": parse_int(attributes.get("ExcerptPostId", "")),
                "WikiPostId": parse_int(attributes.get("WikiPostId", "")),
            }

    def endElement(self, tag: str):
        if tag == "row":
            self.tags.append(self.current_tag)
            self.current_tag = {}


class PostLinkXMLHandler(xml.sax.ContentHandler):
    def __init__(self):
        super().__init__()
        self.post_links = []
        self.current_post_link = {}

    def startElement(self, tag: str, attributes):
        if tag == "row":
            self.current_post_link = {
                "PostLinkId": int(attributes["Id"]),
                "CreationDate": parse_datetime(attributes["CreationDate"]),
                "PostId": int(attributes["PostId"]),
                "RelatedPostId": int(attributes["RelatedPostId"]),
                "LinkTypeId": int(attributes["LinkTypeId"]),
            }

    def endElement(self, tag: str):
        if tag == "row":
            self.post_links.append(self.current_post_link)
            self.current_post_link = {}


class VoteXMLHandler(xml.sax.ContentHandler):
    def __init__(self):
        super().__init__()
        self.votes = []
        self.current_vote = {}

    def startElement(self, tag: str, attributes):
        if tag == "row":
            self.current_vote = {
                "VoteId": int(attributes["Id"]),
                "PostId": int(attributes["PostId"]),
                "VoteTypeId": int(attributes["VoteTypeId"]),
                "UserId": parse_int(attributes.get("UserId", "")),
                "CreationDate": parse_datetime(attributes["CreationDate"]),
                "BountyAmount": parse_int(attributes.get("BountyAmount", "")),
            }

    def endElement(self, tag: str):
        if tag == "row":
            self.votes.append(self.current_vote)
            self.current_vote = {}


def user_xml2json(xml_path, json_path):
    parser = xml.sax.make_parser()
    handler = UserXMLHandler()
    parser.setContentHandler(handler)

    with open(xml_path, "r", encoding="utf-8") as file:
        parser.parse(file)

    with open(json_path, "w", encoding="utf-8") as file:
        json.dump(handler.users, file, indent=4)


def user2mongo(xml_path, db_name, collection_name, conn="mongodb://localhost:27017/"):
    parser = xml.sax.make_parser()
    handler = UserXMLHandler()
    parser.setContentHandler(handler)

    with open(xml_path, "r", encoding="utf-8") as file:
        parser.parse(file)

    client = MongoClient(conn)
    db = client[db_name]
    collection = db[collection_name]

    collection.insert_many(handler.users)

    collection.create_index("UserId")

    client.close()


def badge2mongo(xml_path, db_name, collection_name, conn="mongodb://localhost:27017/"):
    parser = xml.sax.make_parser()
    handler = BadgeXMLHandler()
    parser.setContentHandler(handler)

    with open(xml_path, "r", encoding="utf-8") as file:
        parser.parse(file)

    client = MongoClient(conn)
    db = client[db_name]
    collection = db[collection_name]

    operations = []
    for badge in handler.badges:
        badge_without_userid = badge.copy()
        del badge_without_userid["UserId"]
        operation = UpdateOne(
            {"UserId": badge["UserId"]}, {"$push": {"Badges": badge_without_userid}}
        )

        operations.append(operation)

    if operations:
        collection.bulk_write(operations, ordered=False)


def post2mongo(xml_path, db_name, collection_name, conn="mongodb://localhost:27017/"):
    parser = xml.sax.make_parser()
    handler = PostXMLHandler()
    parser.setContentHandler(handler)

    with open(xml_path, "r", encoding="utf-8") as file:
        parser.parse(file)

    client = MongoClient(conn)
    db = client[db_name]
    collection = db[collection_name]

    collection.insert_many(handler.posts)

    collection.create_index("PostId")
    collection.create_index("OwnerUserId")
    collection.create_index("CreationDate")
    collection.create_index("LastActivityDate")
    collection.create_index("Score")
    collection.create_index("ViewCount")


def comment2mongo(
    xml_path, db_name, collection_name, conn="mongodb://localhost:27017/"
):
    parser = xml.sax.make_parser()
    handler = CommentXMLHandler()
    parser.setContentHandler(handler)

    with open(xml_path, "r", encoding="utf-8") as file:
        parser.parse(file)

    client = MongoClient(conn)
    db = client[db_name]
    collection = db[collection_name]

    operations = []
    for comment in tqdm(handler.comments):
        comment_without_postid = comment.copy()
        del comment_without_postid["PostId"]
        operation = UpdateOne(
            {"PostId": comment["PostId"]},
            {"$push": {"Comments": comment_without_postid}},
        )

        operations.append(operation)

    if operations:
        collection.bulk_write(operations, ordered=False)


def tag2mongo(xml_path, db_name, collection_name, conn="mongodb://localhost:27017/"):
    parser = xml.sax.make_parser()
    handler = TagXMLHandler()
    parser.setContentHandler(handler)

    with open(xml_path, "r", encoding="utf-8") as file:
        parser.parse(file)

    client = MongoClient(conn)
    db = client[db_name]
    collection = db[collection_name]

    collection.insert_many(handler.tags)

    collection.create_index("TagId")


def postlink2mongo(
    xml_path, db_name, collection_name, conn="mongodb://localhost:27017/"
):
    parser = xml.sax.make_parser()
    handler = PostLinkXMLHandler()
    parser.setContentHandler(handler)

    with open(xml_path, "r", encoding="utf-8") as file:
        parser.parse(file)

    client = MongoClient(conn)
    db = client[db_name]
    collection = db[collection_name]

    operations = []
    for link in tqdm(handler.post_links):
        link_without_postid = link.copy()
        del link_without_postid["PostId"]
        operation = UpdateOne(
            {"PostId": link["PostId"]}, {"$push": {"PostLinks": link_without_postid}}
        )

        operations.append(operation)

    if operations:
        collection.bulk_write(operations, ordered=False)

def vote2mongo(xml_path, db_name, collection_name, conn="mongodb://localhost:27017/"):
    parser = xml.sax.make_parser()
    handler = VoteXMLHandler()
    parser.setContentHandler(handler)

    with open(xml_path, "r", encoding="utf-8") as file:
        parser.parse(file)

    client = MongoClient(conn)
    db = client[db_name]
    collection = db[collection_name]

    collection.insert_many(handler.votes)

    collection.create_index("VoteId")
    collection.create_index("PostId")
    collection.create_index("UserId")
    collection.create_index("CreationDate")
    collection.create_index("BountyAmount")

if __name__ == "__main__":
    # print("Parsing Users.xml...")
    # user2mongo("archive/cs/Users.xml", "stackexchange_cs", "Users")
    # print("Parsing Badges.xml...")
    # badge2mongo("archive/cs/Badges.xml", "stackexchange_cs", "Users")
    # print("Parsing Posts.xml...")
    # post2mongo("archive/cs/Posts.xml", "stackexchange_cs", "Posts")
    # print("Parsing Comment.xml...")
    # comment2mongo("archive/cs/Comments.xml", "stackexchange_cs", "Posts")
    # print("Parsing Tags.xml...")
    # tag2mongo("archive/cs/Tags.xml", "stackexchange_cs", "Tags")
    # print("Parsing PostLinks.xml...")
    # postlink2mongo("archive/cs/PostLinks.xml", "stackexchange_cs", "Posts")
    print("Parsing Votes.xml...")
    vote2mongo("archive/cs/Votes.xml", "stackexchange_cs", "Votes")
    pass
