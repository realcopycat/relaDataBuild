#
from pymongo import MongoClient as MC
import csv

client = MC()
collection = client['relationData']['testRun3']
output_node_path = '../data/node1.csv'
output_relation_path = '..data/rela1.csv'


total_node_set = set()
id_head = "testRun3_"  # 以collection 的名字作为id 的开头
id_count = 0  # 用于作为唯一标示
node_dict = dict()  # 全程使用键值对来实现索引
relation_dict_list = list()

cur = collection.find(batch_size=50,)
with open(output_node_path, 'w') as node_file:

    csv_point = csv.writer(node_file)
    csv_point.writerow([":ID", "name"])  # 写入表头

    for x in cur:

        if x['startNode'] not in total_node_set:
            total_node_set.add(x['startNode'])
            id_count = id_count + 1
            tmp_start_node_id = id_head + str(id_count)
            node_dict[x['startNode']] = tmp_start_node_id
            csv_point.writerow([tmp_start_node_id, x['startNode']])  # 写入文件

        if x['endNode'] not in total_node_set:
            total_node_set.add(x['endNode'])
            id_count = id_count + 1
            tmp_end_node_id = id_head + str(id_count)
            node_dict[x['endNode']] = tmp_end_node_id
            csv_point.writerow([tmp_end_node_id, x['endNode']])  # 写入文件

        if x['endNode'] != x['startNode']:
            start_node_id = node_dict[x['startNode']]
            end_node_id = node_dict[x['endNode']]
            relation_dict_list.append({
                'start_id': start_node_id,
                'end_id': end_node_id,
                'relation': x['relation']
            })

        print('this round had process: {0}'.format(id_count))

        # if id_count >= 1000:
        #   break

# 至此应该已经获取了所有的数据 都是list of dict模式













