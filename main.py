# This is a sample Python script.
from analysis_utils import *

HDFS_ROOT = 'hdfs://server1:9000/'
HISTORY_JSON_PATH = f"{HDFS_ROOT}/spark2-history-json/"

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    logs_name = get_spark_logs_name()
    history_json_path = f"{HISTORY_JSON_PATH}/{logs_name[0]}.json"
    query, node, physical, dot, materialized_views = parse_history_json(history_json_path)
