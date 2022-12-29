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
        MetricNode.node_cache = get_node_metrics(metrics_text)
        complete_information(nodes)
        contribute_sql(MetricNode.node_cache.get('0'))
        candidate_views = get_candidate_views(MetricNode.node_cache.get('0'))

        sqls = []
        for candidate_view in candidate_views:
            sqls.append(generate_sql(candidate_view))
        for i in range(len(sqls)):
            tmp = sqls[i]
            res = re.search(r'#\d+L*', tmp)
            while res is not None:
                tmp = tmp.replace(res.group(), '')
                res = re.search(r'#\d+L*', tmp)
            sqls[i] = tmp
        print()
