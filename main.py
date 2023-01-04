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
        if metrics_text == '':
            print_err_info(f'[empty history] {history_json_path} is empty.')
        nodes = get_node_structure(physical_plan)
        MetricNode.node_cache = get_node_metrics(metrics_text)
        complete_information(nodes)
        contribute_sql(MetricNode.node_cache.get('0'))
        candidate_views = get_candidate_views(MetricNode.node_cache.get('0'))
        sqls = fill_sql(candidate_views)
        accumulate_all(MetricNode.node_cache.get('0'))
        all_candidate_views += candidate_views
        print()
    views_set = set(all_candidate_views)
    count = {}
    for view1 in all_candidate_views:
        if view1 not in views_set:
            continue
        for view2 in all_candidate_views:
            if view1 == view2:
                continue
            res = compare_view(view1, view2)
            if res is True:
                views_set.remove(view2)
                if count.get(view1) is None:
                    count[view1] = 1
                else:
                    count[view1] = count[view1] + 1
                print("[remove view]")
    print()
