import subprocess
import re


def get_yarn_logs(start_time=float('inf'), end_time=float('inf'), spark_history_path='/spark2-history'):
    (ret, logs, err) = run_cmd(['hdfs', 'dfs', '-ls', spark_history_path])
    yarn_logs = {}
    assert ret == 0
    for log in logs[1:]:
        log_hdfs_path = log.split(':')[-1].split(' ')[-1]
        match_str = re.search(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}', log)
        if match_str and 'inprogress' not in log_hdfs_path and 'local' not in log_hdfs_path:
            timestamp = match_str.group()
            if start_time < timestamp < end_time:
                yarn_logs[log_hdfs_path.split('/')[-1]] = timestamp
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
