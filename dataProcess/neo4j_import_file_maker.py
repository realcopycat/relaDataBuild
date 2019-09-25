#
from pymongo import MongoClient as MC
import csv
import re

client = MC()
collection = client['relationData']['testRun2']
output_node_path = '../data/node3.csv'
output_relation_path = '../data/rela3.csv'

total_node_set = set()
id_head = "testRun2_"  # 以collection 的名字作为id 的开头
id_count = 0  # 用于作为唯一标示
node_dict = dict()  # 全程使用键值对来实现索引
relation_dict_list = list()

cur = collection.find(batch_size=50)
with open(output_node_path, 'w') as node_file:
    with open(output_relation_path, 'w') as rela_file:
        csv_point = csv.writer(node_file)
        csv_point.writerow([":ID", "name", ":LABEL"])  # 写入表头
        csv_point_rela = csv.writer(rela_file)
        csv_point_rela.writerow([":START_ID", 'description', ":END_ID", ":TYPE"])

        for x in cur:

            if x['startNode'] not in total_node_set:
                total_node_set.add(x['startNode'])
                id_count = id_count + 1
                tmp_start_node_id = id_head + str(id_count)
                node_dict[x['startNode']] = tmp_start_node_id
                format_check_name = re.sub(r'\s+', '', x['startNode'])
                csv_point.writerow([tmp_start_node_id, format_check_name, 'main_node'])  # 写入文件

            if x['endNode'] not in total_node_set:
                total_node_set.add(x['endNode'])
                id_count = id_count + 1
                tmp_end_node_id = id_head + str(id_count)
                node_dict[x['endNode']] = tmp_end_node_id
                format_check_name = re.sub(r'\s+', '', x['endNode'])
                csv_point.writerow([tmp_end_node_id, format_check_name, 'node'])  # 写入文件

            if x['endNode'] != x['startNode']:
                start_node_id = node_dict[x['startNode']]
                end_node_id = node_dict[x['endNode']]
                format_check_name = re.sub(r'\s+', '', x['relation'])
                csv_point_rela.writerow(
                    [start_node_id, format_check_name, end_node_id, 'normal_rela'])

            print('this round had process: {0}'.format(id_count))

            # if id_count >= 1000:
            #   break

# 至此应该已经获取了所有的数据 都是list of dict模式
