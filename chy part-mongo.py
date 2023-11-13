import collections

from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
from scipy import stats
import numpy as np
import networkx as nx

database_name = "stackexchange_cs"

client = MongoClient("mongodb://localhost:27017/")
db = client[database_name]


def relation_len_score():
    pipeline = [
        {
            '$match': {
                'Body': {'$ne': None},
                'Score': {'$ne': None},
                'PostTypeId': 1
            }
        },
        {
            '$project': {
                'BodyLength': {'$strLenCP': '$Body'},
                'Score': 1,
                '_id': 0
            }
        }
    ]
    results = db.Posts.aggregate(pipeline)
    BodyLength_score = []

    for result in results:
        BodyLength_score.append([result['Score'], result['BodyLength']])
    BodyLength_score = np.array(BodyLength_score)
    # print(BodyLength_score[:,0])
    correlation_coefficient = np.corrcoef(BodyLength_score[:, 0], BodyLength_score[:, 1])[0, 1]
    print(f"The Pearson correlation coefficient is: {correlation_coefficient}")

    # polyfit
    coefficients = np.polyfit(BodyLength_score[:, 1], BodyLength_score[:, 0], 2)
    polynomial = np.poly1d(coefficients)
    x_values = np.linspace(BodyLength_score[:, 1].min(), BodyLength_score[:, 1].max(), )
    y_values = polynomial(x_values)
    plt.scatter(BodyLength_score[:, 1], BodyLength_score[:, 0], color='lightgreen', label='Data points')
    plt.plot(x_values, y_values, color='lightblue', label='Fit curve')
    plt.legend()

    plt.title('Linear Fit of Score vs. Body Length')
    plt.xlabel('Body Length')
    plt.ylabel('Score')

    plt.show()

    # posttypeId = 2
    pipeline = [
        {
            '$match': {
                'Body': {'$ne': None},
                'Score': {'$ne': None},
                'PostTypeId': 2
            }
        },
        {
            '$project': {
                'BodyLength': {'$strLenCP': '$Body'},
                'Score': 1,
                '_id': 0
            }
        }
    ]
    results = db.Posts.aggregate(pipeline)
    BodyLength_score = []

    for result in results:
        BodyLength_score.append([result['Score'], result['BodyLength']])
    BodyLength_score = np.array(BodyLength_score)
    # print(BodyLength_score[:,0])
    correlation_coefficient = np.corrcoef(BodyLength_score[:, 0], BodyLength_score[:, 1])[0, 1]
    print(f"The Pearson correlation coefficient is: {correlation_coefficient}")

    # polyfit
    coefficients = np.polyfit(BodyLength_score[:, 1], BodyLength_score[:, 0], 2)
    polynomial = np.poly1d(coefficients)
    x_values = np.linspace(BodyLength_score[:, 1].min(), BodyLength_score[:, 1].max(), )
    y_values = polynomial(x_values)
    plt.scatter(BodyLength_score[:, 1], BodyLength_score[:, 0], color='lightgreen', label='Data points')
    plt.plot(x_values, y_values, color='lightblue', label='Fit curve')
    plt.legend()

    plt.title('Linear Fit of Score vs. Body Length')
    plt.xlabel('Body Length')
    plt.ylabel('Score')

    plt.show()


def interesting_ques():
    pipeline = [
        {
            '$match': {
                'PostTypeId': 1,
                'AcceptedAnswerId': None
            }
        }, {
            '$project': {
                'Tags': 1,
                'PostId': 1,
                'Title': 1,
                'OwnerUserId': 1
            }
        }, {
            '$lookup': {
                'from': 'Users',
                'localField': 'OwnerUserId',
                'foreignField': 'UserId',
                'as': 'UserData'
            }
        }, {
            '$unwind': '$UserData'
        }, {
            '$unwind': '$Tags'
        }, {
            '$lookup': {
                'from': 'Tags',
                'localField': 'Tags',
                'foreignField': 'TagName',
                'as': 'TagData'
            }
        }, {
            '$unwind': '$TagData'
        }, {
            '$project': {
                'PostId': 1,
                'OwnerUserId': 1,
                'Tags': 1,
                'Title': 1,
                'Reputation': '$UserData.Reputation',
                'TagId': '$TagData.TagId'
            }
        }, {
            '$sort': {
                'Reputation': -1
            }
        }, {
            '$group': {
                '_id': '$TagId',
                'TagName': {
                    '$first': '$Tags'
                },
                'TagId': {
                    '$first': '$TagId'
                },
                'PostId': {
                    '$first': '$PostId'
                },
                'Title': {
                    '$first': '$Title'
                },
                'UserId': {
                    '$first': '$OwnerUserId'
                },
                'Reputation': {
                    '$first': '$Reputation'
                }
            }
        }, {
            '$project': {
                '_id': 0,
                'TagId': 1,
                'TagName': 1,
                'PostId': 1,
                'Title': 1,
                'UserId': 1,
                'Reputation': 1
            }
        }, {
            '$sort': {
                'TagId': 1
            }
        }
    ]

    results = db.Posts.aggregate(pipeline)
    print(f"TagId,TagName,PostId,Title,UserId,Reputation")
    count0 = 0
    for result in results:
        print(result)
        count0+=1
    print(count0)


def distribution():
    pipeline = [
        {
            '$match': {
                'PostTypeId': 1
            }
        }, {
            '$unwind': '$Tags'
        }, {
            '$project': {
                '_id': 0,
                'PostId': 1,
                'Tags': 1
            }
        }
    ]
    results = db.Posts.aggregate(pipeline)
    tuples_list = [tuple(doc.values()) for doc in results]
    post_tags = tuple(tuples_list)

    G = nx.Graph()
    tag_frequency = {}

    count = 0

    postid_to_tags = collections.defaultdict(set)
    for postid, tagname in post_tags:
        postid_to_tags[postid].add(tagname)
    print("collection setted")

    for postid, tags in postid_to_tags.items():
        for tagid in tags:
            if tagid not in G:
                G.add_node(tagid)
                # 初始化标签频率
                tag_frequency[tagid] = 1
            else:
                try:
                    tag_frequency[tagid] += 1
                except:
                    tag_frequency[tagid] = 1

            for tagid2 in tags:
                if tagid != tagid2:
                    if G.has_edge(tagid, tagid2):
                        G[tagid][tagid2]['weight'] += 1
                    else:
                        G.add_edge(tagid, tagid2, weight=1)
        count += 1
        if count % 1000 == 0:
            print(f"\rcount:{count}", end="", flush=True)

    # for u, v, d in G.edges(data=True):
    #     d['weight'] *= 300

    # print(G.edges)
    plt.figure(figsize=(55, 55))
    pos = nx.spring_layout(G, k=10, iterations=250)

    node_sizes = [tag_frequency[tag] * 60 for tag in G.nodes()]

    edges, weights = zip(*nx.get_edge_attributes(G, 'weight').items())
    # print(weights)
    max_weight = max(weights)
    min_weight = min(weights)
    adjusted_weights = [w + 1 - min_weight for w in weights]
    log_weights = [np.log(w) for w in adjusted_weights]  # 对数变换

    max_log_weight = max(log_weights)
    min_log_weight = min(log_weights)
    normalized_weights = [(w - min_log_weight) / (max_log_weight - min_log_weight) for w in log_weights]

    # print(normalized_weights)

    nx.draw(G, pos, node_size=node_sizes, node_color='lightblue', edgelist=edges, edge_color=normalized_weights,
            width=5.0, edge_cmap=plt.cm.YlGn)
    # label_pos = {k: [v[0], v[1] + 0.02] for k, v in pos.items()}
    label_pos = {k: [v[0], v[1]] for k, v in pos.items()}

    # labels_with_aliases = {node: aliases.get(node, node) for node in G.nodes()}
    nx.draw_networkx_labels(G, label_pos, font_size=12, font_color='darkblue')
    plt.show()


if __name__ == "__main__":
    relation_len_score()
    interesting_ques()
    distribution()
