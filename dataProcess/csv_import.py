#
import neo4j
import csv
import time

file_path = input()

driver = neo4j.GraphDatabase.driver("bolt://localhost:7687", auth=('neo4j', 'gzcd15728'))

with driver.session() as session:
    with open(file_path, 'r') as f:
        lines = csv.reader(f)
        count = 0
        start_time = time.time()
        for line in lines:
            if line[0] == line[2]:
                continue  # 起点终点一致的全部舍弃
            if count == 0:
                count = 1
                continue  # 舍弃第一行
            session.run("MERGE (a {name: $name}) set a:node_main "
                        "MERGE (b {name: $end}) set b:node "
                        "MERGE (a)-[:normal_rela {description: $description}]->(b)",
                        name=line[0], end=line[2], description=line[2])

            count = count + 1
            end_time = time.time()
            total_time = end_time - start_time
            per_time = total_time / count
            print("has processed : {0}, per time : {1}, total time : {2}".format(count, per_time, total_time))



