#
from neo4j import GraphDatabase
from pymongo import MongoClient
import time


class Neo4jDataDriver:

    def __init__(self):
        self.driver = GraphDatabase.driver("bolt://localhost:7687", auth=('neo4j', 'gzcd15728'))

    def close(self):
        self.driver.close()


class MongoDataDriver:

    def __init__(self):
        self._client = MongoClient()
        self._database = self._client.relationData
        self.collection = self._database['testRun3']


def read_and_write():

    mongo_driver = MongoDataDriver()
    neo4j_driver = Neo4jDataDriver()

    with neo4j_driver.driver.session() as session:

        count = 0  # 用于计数
        start_time = time.time()  # 用于计时

        x = mongo_driver.collection.find(no_cursor_timeout=True)
        for each in x:
            start = each['startNode']
            end = each['endNode']
            rela = each['relation']
            if start == end:
                continue
            session.run("MERGE (a {name: $name}) set a:node_main "
                        "MERGE (b {name: $end}) set b:node "
                        "MERGE (a)-[:normal_rela {description: $description}]->(b)",
                        name=start, end=end, description=rela)

            end_time = time.time()  # 用于计时
            totaltime = end_time - start_time
            count = count + 1
            per_time = totaltime / count
            print("has processed : {0}, per time : {1}, total time : {2}".format(count, per_time, totaltime))

        x.close()


if __name__ == '__main__':
    read_and_write()
