import json
import subprocess
import re
import time

from utils.structure import PhysicalPlanNode, MetricNode, Attribute

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


def get_history_json(history_json_path):
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


def get_node_structure(physical_plan):
    """
    解析physical_plan文件,该文件结构如下：
    1、物理计划图
    2、每个节点的详细信息，主要包括输入，输出，filter等结构信息
    3、subgraph的存储关系
    :param physical_plan:
    :return:
    """

    def parse_physical_plan(node_structure):
        """
        解析 ·删掉physical_plan前面的树形图和后面的subgraph之后· 剩余的结构信息
        :param node_structure:
        :return:
        """
        parameter = {}
        for line in node_structure:
            # line = str(len)
            head = re.match(r"([A-Za-z]+( [A-Za-z]+)*) *(\[\d+])*:", line)
            if head is None:
                print_err_info(f"line:<{line}> Unable to extract field headers. ")
                continue
            head = head.group()
            if line.startswith('Output'):
                parameter[Attribute.OUTPUT.value] = processing_bracket_list(line.replace(head, ''))
            elif line.startswith('Input'):
                parameter[Attribute.INPUT.value] = processing_bracket_list(line.replace(head, ''))
            elif line.startswith('Batched'):
                parameter[Attribute.BATCHED.value] = line.replace(head, '').strip()
            elif line.startswith('Arguments'):
                # TODO[node structure]情况复杂，需要完善（当前策略就是不解析，后面和metrics做匹配也方便）
                parameter[Attribute.ARGUMENTS.value] = line.replace(head, '').strip()
            elif line.startswith('Result'):
                parameter[Attribute.RESULT.value] = processing_bracket_list(line.replace(head, ''))
            elif line.startswith('Aggregate Attributes'):
                parameter[Attribute.AGGREGATE.value] = processing_bracket_list(line.replace(head, ''))
            elif line.startswith('Functions'):
                parameter[Attribute.FUNCTION.value] = processing_bracket_list(line.replace(head, ''))
            elif line.startswith('Keys'):
                parameter[Attribute.KEYS.value] = processing_bracket_list(line.replace(head, ''))
            elif line.startswith('Join condition'):
                parameter[Attribute.JOIN_CONDITION.value] = processing_bracket_list(line.replace(head, ''))
            elif line.startswith('Left keys'):
                parameter[Attribute.LEFT_KEYS.value] = processing_bracket_list(line.replace(head, ''))
            elif line.startswith('Right keys'):
                parameter[Attribute.RIGHT_KEYS.value] = processing_bracket_list(line.replace(head, ''))
            elif line.startswith('Condition'):
                # TODO[node structure]
                parameter[Attribute.CONDITION.value] = processing_bracket_list(line.replace(head, ''))
            elif line.startswith('ReadSchema'):
                parameter[Attribute.READ_SCHEMA.value] = line.replace(head, '').strip()
            elif line.startswith('PushedFilters'):
                parameter[Attribute.PUSHED_FILTERS.value] = processing_bracket_list(line.replace(head, ''))
            elif line.startswith('Location'):
                parameter[Attribute.LOCATION.value] = line.replace(head, '').split(' ')[2].strip('[').strip(']')
            elif line.startswith('PartitionFilters'):
                parameter[Attribute.PARTITION_FILTERS.value] = processing_bracket_list(line.replace(head, ''))
            else:
                print_err_info(f"line:<{line}> Unconsidered field header.")
                continue
        return parameter

    # remove physical tree
    contexts = physical_plan[physical_plan.find('\n(1)') + 1:].strip('\n')
    # remove Subqueries
    sub_num = contexts.find("===== Subqueries =====")
    if sub_num != -1:
        contexts = contexts[:sub_num].strip('\n')
    nodes = []
    for context in contexts.split('\n\n'):
        # get name
        lines = context.split('\n')
        start = lines[0].find(')') + 2
        end = lines[0].find('[')
        if end == -1:
            end = len(lines[0])
        name = lines[0][start:end]
        # get parameter
        para = parse_physical_plan(lines[1:])
        nodes.append(PhysicalPlanNode(name, para))
    return nodes


def get_node_metrics(node_metrics):
    """
    解析node_metrics文件，树节点关系、子图关系、以及运行代价信息
    :param node_metrics:
    :return:
    """
    start_size = len("[PlanMetric]\n")
    edge_tag = re.search(r"  \d->\d;", node_metrics)
    subgraph_tag = re.search(r"\[SubGraph]\n", node_metrics)
    assert (edge_tag is not None)
    assert (subgraph_tag is not None)
    metrics = node_metrics[start_size:edge_tag.span()[0] - 4].split('\n\n\n\n')
    edge = node_metrics[edge_tag.span()[0]:subgraph_tag.span()[0]]
    subgraph = node_metrics[subgraph_tag.span()[1]:]
    metric_nodes = parse_metrics_text(metrics)
    build_tree_with_edge_text(edge, metric_nodes)


def parse_metrics_text(metrics):
    """
    解析代价信息
    :param metrics:
    :return:
    """
    metric_nodes = {}
    for metric in metrics:
        lines = metric.split('\n')
        start_id = lines[0].find('id:')
        start_name = lines[0].find('name:')
        start_desc = lines[0].find('desc:')
        nid = lines[0][start_id + len('id:'): start_name].strip()
        name = lines[0][start_name + len('name:'): start_desc].strip()
        desc = lines[0][start_desc + len('desc:'):].strip()
        parse_metric_desc(desc)
        # TODO[后续处理时间信息]
        info = lines[1:]
        metric_nodes[nid] = MetricNode(nid, name, desc, info)
    return metric_nodes


def parse_metric_desc(desc):
    """
    解析代价信息中的node tag，用来和结构信息对应
    :param desc:
    :return:
    """
    para = {}
    if "FileScan" in desc:
        # 处理file scan的情况
        scan_tag = re.search(r"FileScan .+\[.*] ", desc)
        assert scan_tag is not None
        para[Attribute.OUTPUT.value] = processing_bracket_list('[' + scan_tag.group().split('[')[1])
        desc = desc[scan_tag.span()[1]:]
        front = re.search(r"\w+: ", desc)
        while front is not None:
            desc = desc[front.span()[1]:]
            behind = re.search(r"\w+: ", desc)
            if behind is not None:
                para[front.group().replace(":", "").strip()] = canonicalize(desc[:behind.span()[0]])
                front = behind
            else:
                para[front.group().replace(":", "").strip()] = desc
                break
        print()


def build_tree_with_edge_text(edge, metric_nodes):
    item = re.search(r"\d+->\d+", edge)
    while item is not None:
        relation = item.group().split('->')
        metric_nodes[relation[0]].parents_node.append(relation[1])
        metric_nodes[relation[1]].children_node.append(relation[0])
        edge = edge[item.span()[1]:]
        item = re.search(r"\d+->\d+", edge)


def processing_bracket_list(string):
    """
    处理用中括号[]括起来的列表
    :param string:
    :return:
    """
    # 只处理带中括号的
    if '[' not in string:
        return string
    stan = string.strip(' ').strip('[').strip(']')
    if ',' in stan:
        return stan.split(',')
    return stan


def print_err_info(info):
    print('\033[1;31;40m' + 'error occur' + '\033[0m')
    print('\033[1;31;40m' + info + '\033[0m')


def canonicalize(item):
    """
    规范化字符串
    :return:
    """
    return item.strip.strip('..., ').strip('...,').strip(' :').strip(': ') \
        .strip(', ').strip(' ,').strip(' ').strip(',').strip(':')
