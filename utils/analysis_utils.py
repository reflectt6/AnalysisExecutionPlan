import json
import subprocess
import re
import time

SECONDS_PER_MINUTE = 60


def get_spark_logs_name(start_time='1940-01-01 00:00', end_time='2500-01-01 00:00',
                        spark_history_path='/spark2-history'):
    """
    获取spark日志
    :param start_time: 日志开始时间段
    :param end_time: 日志结束时间段
    :param spark_history_path: spark日志存储路径
    :return:
    """
    (ret, logs_name, err) = run_cmd(['hdfs', 'dfs', '-ls', spark_history_path])
    start_time = time_str_to_int(start_time) - SECONDS_PER_MINUTE
    end_time = time_str_to_int(end_time) + SECONDS_PER_MINUTE
    yarn_logs = []
    assert ret == 0
    for item in logs_name[1:]:
        log_path = item.split(' ')[-1]
        log_name = log_path.split('/')[-1]
        match = re.search(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}', item)
        if match and 'inprogress' not in log_path and 'local' not in log_path:
            cur_time = time_str_to_int(match.group())
            if start_time < cur_time < end_time:
                yarn_logs.append(log_name)
    return yarn_logs


def run_cmd(command_list):
    """
    在系统上运行shell指令
    :param command_list: 指令列表
    :return:返回值、输出、错误输出
    """
    print('Running system command: {0}'.format(' '.join(command_list)))
    proc = subprocess.Popen(command_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate()
    ret = proc.returncode
    out = out.decode('utf-8').split('\n')
    return ret, out, err


def time_str_to_int(time_str, ft='%Y-%m-%d %H:%M'):
    """
    时间字符串转时间戳
    :param time_str: 时间字符串，例如：1940-01-01 00:00
    :param ft: 时间格式，默认格式为：%Y-%m-%d %H:%M
    :return:
    """
    return int(time.mktime(time.strptime(time_str, ft)))


def parse_history_json(history_json_path):
    """
    解析json获取spark任务信息，json的生成需要使用日志解析jar包（地址待补充）
    :param history_json_path:
    :return:
    """
    (ret, out, err) = run_cmd(['hdfs', 'dfs', '-cat', history_json_path])
    if ret != 0 or len(out) == 0 or out[0] == '[]':
        return "", "", "", "", ""
    jsons = json.loads(out[0])
    # 一个执行计划中，每一条sql会被解析为一个json，这里默认一个任务中只有一条sql
    first_json = jsons[0]
    return first_json['original query'], first_json['node metrics'], first_json['physical plan'], \
        first_json['dot metrics'], first_json['materialized views']


def parse_node_info(physical_plan):
    """
    解析physical_plan文件,该文件结构如下：
    1、物理计划图
    2、每个节点的详细信息，主要包括输入，输出，filter等结构信息
    3、subgraph的存储关系
    :param physical_plan:
    :return:
    """
    context = iter(physical_plan[physical_plan.find('\n(1)') + 1:].split('\r\n'))
    # for line in context:

