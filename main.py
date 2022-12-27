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
            find = False
            candidates_node = MetricNode.node_cache.get(node.name)
            if candidates_node is None:
                continue
            for candidate_node in candidates_node:
                if find:
                    break
                match = True
                # addition = {}
                for key in node.para.keys():
                    if candidate_node.desc.__contains__(key):
                        # TODO 判断相等
                        one = candidate_node.desc.get(key)
                        other = node.para.get(key)
                        if not ((isinstance(one, str) and isinstance(other, str) and one in other)
                                or (isinstance(one, list) and isinstance(other, list) and one == other)):
                            match = False
                            break
                    # else:
                    #     addition[key] = other
                if match:
                    candidate_node.desc = {**node.para, **candidate_node.desc}
                    toDels.append(candidate_node)
                    find = True
                    print("matched: " + str(one) + "|||||||||" + str(other))
            # for toDel in toDels:
        # del MetricNode.node_cache[toDel]
        print()
