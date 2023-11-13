import pymysql
import pymysql.cursors
import pandas as pd
import matplotlib.pyplot as plt
from scipy import stats
import numpy as np
import networkx as nx
import collections
import time


config = {
    'host': 'localhost',
    'user': 'root',
    'password': '1234',
    'database': 'cs_stack_exchange'
}


def relation_len_score():
    with connection.cursor() as cursor:
        correlation_query = """
        WITH LengthScores AS (
          SELECT
            LENGTH(Body) AS BodyLength,
            Score
          FROM
            posts
          WHERE
            Body IS NOT NULL AND
            Score IS NOT NULL AND
            PostTypeId = 1
        )
        SELECT
            *
            # (COUNT(*) * SUM(BodyLength * Score) - SUM(BodyLength) * SUM(Score)) /
            # (SQRT(COUNT(*) * SUM(BodyLength * BodyLength) - SUM(BodyLength) * SUM(BodyLength)) *
            #  SQRT(COUNT(*) * SUM(Score * Score) - SUM(Score) * SUM(Score))) AS correlation_coefficient
        FROM
            LengthScores;
        """

        df = pd.read_sql_query(correlation_query, connection)
        correlation_coefficient = df['BodyLength'].corr(df['Score'])
        print(f"The Pearson correlation coefficient is: {correlation_coefficient}")


        # polyfit
        coefficients = np.polyfit(df['BodyLength'], df['Score'], 2)
        polynomial = np.poly1d(coefficients)
        x_values = np.linspace(df['BodyLength'].min(), df['BodyLength'].max(), )
        y_values = polynomial(x_values)
        plt.scatter(df['BodyLength'], df['Score'], color='lightgreen', label='Data points')
        plt.plot(x_values, y_values, color='lightblue', label='Fit curve')
        plt.legend()

        plt.title('Linear Fit of Score vs. Body Length')
        plt.xlabel('Body Length')
        plt.ylabel('Score')

        plt.show()

        with connection.cursor() as cursor:
            correlation_query = """
            WITH LengthScores AS (
              SELECT
                CHAR_LENGTH(Body) AS BodyLength,
                Score
              FROM
                posts
              WHERE
                Body IS NOT NULL AND
                Score IS NOT NULL AND
                PostTypeId = 2
            )
            SELECT
                *
                # (COUNT(*) * SUM(BodyLength * Score) - SUM(BodyLength) * SUM(Score)) /
                # (SQRT(COUNT(*) * SUM(BodyLength * BodyLength) - SUM(BodyLength) * SUM(BodyLength)) *
                #  SQRT(COUNT(*) * SUM(Score * Score) - SUM(Score) * SUM(Score))) AS correlation_coefficient
            FROM
                LengthScores;
            """
            start_time = time.time()
            df = pd.read_sql_query(correlation_query, connection)
            end_time = time.time()

            timeslot = end_time -start_time
            print(timeslot)

            correlation_coefficient = df['BodyLength'].corr(df['Score'])
            print(f"The Pearson correlation coefficient is: {correlation_coefficient}")

            # polyfit
            coefficients = np.polyfit(df['BodyLength'], df['Score'], 2)
            polynomial = np.poly1d(coefficients)
            x_values = np.linspace(df['BodyLength'].min(), df['BodyLength'].max(), )
            y_values = polynomial(x_values)
            plt.scatter(df['BodyLength'], df['Score'], color='lightgreen', label='Data points')
            plt.plot(x_values, y_values, color='lightblue', label='Fit curve')
            plt.legend()

            plt.title('Linear Fit of Score vs. Body Length')
            plt.xlabel('Body Length')
            plt.ylabel('Score')

            plt.show()

        # # print(f"Correlation coefficient: {correlation_result}")
        #
        # average_length_query = """
        # SELECT Score, AVG(CHAR_LENGTH(Body)) AS average_length, COUNT(*) AS number_of_posts
        # FROM posts
        # WHERE Body IS NOT NULL AND Score IS NOT NULL
        # GROUP BY Score
        # ORDER BY Score;
        # """
        #
        # cursor.execute(average_length_query)
        # average_length_results = cursor.fetchall()
        # # print(f"Score,average_length,number_of_posts")
        # # for row in average_length_results:
        # #     print(row)
        #
        # scores = np.array([float(row[0]) for row in average_length_results])
        # average_lengths = np.array([float(row[1]) for row in average_length_results])
        # number_of_posts = np.array([float(row[2]) for row in average_length_results])
        # # print(number_of_posts)
        # coefficients = np.polyfit(scores, average_lengths, 2)
        #
        # polynomial = np.poly1d(coefficients)
        #
        # x_values = np.linspace(scores.min(), scores.max())
        # y_values = polynomial(x_values)
        #
        #
        # plt.scatter(scores, average_lengths, color='blue', label='Data points')
        #
        #
        # plt.plot(x_values, y_values, color='red', label='Fit curve')
        #
        # plt.legend()
        #
        # plt.title('Polynomial Fit of Average Length vs. Score')
        # plt.xlabel('Score')
        # plt.ylabel('Average Length')
        #
        # plt.show()


def interesting_ques():
    with connection.cursor() as cursor:
        query = """
                with tmp1 AS (
                SELECT t.Id AS TagId,
                t.TagName,
                p.Id AS PostId,
                p.Title,
                u.Id AS UserId,
                cast(u.Reputation as SIGNED)   as Reputation
                                 
                  FROM posts p
                           JOIN
                       users u ON p.OwnerUserId = u.Id
                           JOIN
                       post_tags pt ON p.Id = pt.PostId
                           JOIN
                       tags t ON pt.TagId = t.Id
                  WHERE p.AcceptedAnswerId IS NULL
                and p.posttypeId = 1)
                select TagId,
                       TagName,
                       PostId,
                       Title,
                       UserId,
                       Reputation
                       
                from (select
                    TagId,
                       TagName,
                       PostId,
                       Title,
                       UserId,
                       Reputation,
                    ROW_NUMBER() OVER (PARTITION BY TagId ORDER BY Reputation DESC) AS rn
                    from tmp1) as tmp2
                WHERE rn = 1
                ;
        
                """

        start_time = time.time()

        cursor.execute(query)
        result = cursor.fetchall()

        end_time = time.time()

        timeslot = end_time - start_time
        print(timeslot)

        print(f"TagId,TagName,PostId,Title,UserId,Reputation")
        print(result)
        print(len(result))

def distribution():
    with connection.cursor() as cursor:
        query = """
        select PostId,TagId,TagName
            from post_tags 
            join tags on post_tags.TagId = tags.Id
            join posts on Posts.Id =  post_tags.PostId
        where PostTypeId = 1
        """

        start_time = time.time()
        cursor.execute(query)

        end_time = time.time()

        timeslot = end_time - start_time
        print(timeslot)
        post_tags = cursor.fetchall()
        print(len(post_tags))
        #print(type(post_tags))
        #print(post_tags)

        aliases={}
        for _,tagid,tagname in post_tags:
            if tagid not in aliases.keys():
                aliases[tagid] = tagname



        G = nx.Graph()
        tag_frequency = {}
        count = 0

        postid_to_tags = collections.defaultdict(set)
        for postid, tagid, _ in post_tags:
            postid_to_tags[postid].add(tagid)
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
            count+=1
            if count %1000==0:
                print(f"\rcount:{count}",end="", flush=True)

        # for u, v, d in G.edges(data=True):
        #     d['weight'] *= 300

            #print(G.edges)
        plt.figure(figsize=(55, 55))
        pos = nx.spring_layout(G, k=10, iterations=250)
        # pos = nx.random_layout(G) #random pos
        # pos = nx.kamada_kawai_layout(G,pos = pos)

        # # 根据节点的度将节点分到不同的壳层
        # degree_shell = {}
        # for node, degree in G.degree():
        #     degree_shell.setdefault(degree, []).append(node)
        #
        # # 将壳层数据转换为列表格式
        # shells = list(degree_shell.values())
        #
        # pos = nx.shell_layout(G, shells)



        node_sizes = [tag_frequency[tag] * 60 for tag in G.nodes()]

        edges, weights = zip(*nx.get_edge_attributes(G, 'weight').items())
        #print(weights)
        max_weight = max(weights)
        min_weight = min(weights)
        adjusted_weights = [w + 1 - min_weight for w in weights]
        log_weights = [np.log(w) for w in adjusted_weights]  # 对数变换

        max_log_weight = max(log_weights)
        min_log_weight = min(log_weights)
        normalized_weights = [(w - min_log_weight) / (max_log_weight - min_log_weight) for w in log_weights]

        #print(normalized_weights)

        nx.draw(G, pos, node_size=node_sizes,node_color='lightblue', edgelist=edges, edge_color=normalized_weights, width=5.0, edge_cmap=plt.cm.YlGn)
        #label_pos = {k: [v[0], v[1] + 0.02] for k, v in pos.items()}
        label_pos = {k: [v[0], v[1] ] for k, v in pos.items()}


        labels_with_aliases = {node: aliases.get(node, node) for node in G.nodes()}
        nx.draw_networkx_labels(G, label_pos, labels=labels_with_aliases, font_size=12,font_color='darkblue')
        plt.show()


if __name__ == "__main__":
    connection = pymysql.connect(**config)
    relation_len_score()
    interesting_ques()
    distribution()


    # with connection.cursor() as cursor:
    #     query = """
    #     select count(*)
    #     from Posts
    #     where PostTypeId = 1
    #     """
    #     cursor.execute(query)
    #     print(cursor.fetchall())


    connection.close()
