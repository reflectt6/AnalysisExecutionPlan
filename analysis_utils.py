import subprocess
import re
import time

SECONDS_PER_MINUTE = 60


def get_spark_logs(start_time='1940-01-01 00:00', end_time='2500-01-01 00:00', spark_history_path='/spark2-history'):
    """
    获取spark日志
    :param start_time: 日志开始时间段
    :param end_time: 日志结束时间段
    :param spark_history_path: spark日志存储路径
    :return:
    """
    (ret, logs_name, err) = run_cmd(['hdfs', 'dfs', '-ls', spark_history_path])
    start_time = int(time.strptime(start_time, "%Y-%m-%d %H:%M")) - SECONDS_PER_MINUTE
    end_time = int(time.strptime(end_time, "%Y-%m-%d %H:%M")) + SECONDS_PER_MINUTE
    yarn_logs = {}
    assert ret == 0
    for item in logs_name[1:]:
        log_path = item.split(' ')[-1]
        log_name = log_path.split('/')[-1]
        match = re.search(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}', item)
        if match and 'inprogress' not in log_path and 'local' not in log_path:
            cur_time = int(time.strptime(match.group(), "%Y-%m-%d %H:%M"))
            if start_time < cur_time < end_time:
                yarn_logs[log_name] = cur_time
    return yarn_logs


def run_cmd(command_list):
    """
    在系统上运行shell指令
    :param command_list: 指令列表
    :return:返回值、输出、错误输出
    """
    print('Running system command: {0}'.format(''.join(command_list)))
    proc = subprocess.Popen(command_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate()
    ret = proc.returncode
    out = out.decode('utf-8').split('\n')
    return ret, out, err
