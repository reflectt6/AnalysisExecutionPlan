# This is a sample Python script.
from utils.analysis_utils import *

HDFS_ROOT = 'hdfs://server1:9000/'
HISTORY_JSON_PATH = f"{HDFS_ROOT}/spark2-history-json/"

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    logs_name = get_spark_logs_name()
    for log_name in logs_name:
        history_json_path = f"{HISTORY_JSON_PATH}/{log_name}.json"
        _, metrics_text, physical_plan, _, _ = get_history_json(history_json_path)
        nodes = get_node_structure(physical_plan)
        metrics_nodes = get_node_metrics(metrics_text)

        for node in nodes:
            toDels = []
            candidates_node = MetricNode.node_cache.get(node.name)
            if candidates_node is None:
                continue
            for candidate_node in candidates_node:
                match = True
                addition = {}
                for key in node.para.keys():
                    if candidate_node.desc.__contains__(key):
                        # TODO 判断相等
                        if candidate_node.desc.get(key) != node.para.get(key):
                            match = False
                            break
                    else:
                        addition = node.para.get(key)
                if match:
                    candidate_node.desc += addition
                    toDels.append(candidate_node)
                print()
            for toDel in toDels:
                del MetricNode.node_cache[toDel]
        print()
