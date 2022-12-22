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
        for metrics_node in metrics_nodes:
            colls = MetricNode.node_cache.get(metrics_node.name)
            if colls is None:
                continue
            for col in colls:
                # TODO
                print()

        print()