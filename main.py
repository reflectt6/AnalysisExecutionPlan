# This is a sample Python script.
from utils.analysis_utils import *

HDFS_ROOT = 'hdfs://server1:9000/'
HISTORY_JSON_PATH = f"{HDFS_ROOT}/spark2-history-json/"

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    logs_name = get_spark_logs_name()
    all_candidate_views = []
    for log_name in logs_name:
        history_json_path = f"{HISTORY_JSON_PATH}/{log_name}.json"
        _, metrics_text, physical_plan, _, _ = get_history_json(history_json_path)
        nodes = get_node_structure(physical_plan)
        MetricNode.node_cache = get_node_metrics(metrics_text)
        complete_information(nodes)
        contribute_sql(MetricNode.node_cache.get('0'))
        candidate_views = get_candidate_views(MetricNode.node_cache.get('0'))
        sqls = fill_sql(candidate_views)
        accumulate_to_join(MetricNode.node_cache.get('0'))
        all_candidate_views += candidate_views
        print()

