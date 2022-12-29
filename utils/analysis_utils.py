import json
import subprocess
import re
import time

from utils.structure import PhysicalPlanNode, MetricNode, Attribute, SQLContribute

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
        parameter_tag = {}
        for line in node_structure:
            # line = str(len)
            head = re.match(r"([A-Za-z]+( [A-Za-z]+)*) *(\[\d+])*:", line)
            if head is None:
                print_err_info(f"line:<{line}> Unable to extract field headers. ")
                continue
            head = head.group()
            if line.startswith('Output'):
                parameter[Attribute.OUTPUT.value] = parse_bracket_list(line.replace(head, ''))
                parameter_tag[Attribute.OUTPUT.value] = canonicalize(line.replace(head, '')).replace(' ', '')
            elif line.startswith('Input'):
                parameter[Attribute.INPUT.value] = parse_bracket_list(line.replace(head, ''))
                parameter_tag[Attribute.INPUT.value] = canonicalize(line.replace(head, '')).replace(' ', '')
            elif line.startswith('Batched'):
                parameter[Attribute.BATCHED.value] = canonicalize(line.replace(head, ''))
                parameter_tag[Attribute.BATCHED.value] = canonicalize(line.replace(head, '')).replace(' ', '')
            elif line.startswith('Arguments'):
                # TODO[node structure]情况复杂，需要完善（当前策略就是不解析，后面和metrics做匹配也方便）
                parameter[Attribute.ARGUMENTS.value] = canonicalize(line.replace(head, ''))
                parameter_tag[Attribute.ARGUMENTS.value] = canonicalize(line.replace(head, '')).replace(' ', '')
            elif line.startswith('Result'):
                parameter[Attribute.RESULT.value] = parse_bracket_list(line.replace(head, ''))
                parameter_tag[Attribute.RESULT.value] = canonicalize(line.replace(head, '')).replace(' ', '')
            elif line.startswith('Aggregate Attributes'):
                parameter[Attribute.AGGREGATE.value] = parse_bracket_list(line.replace(head, ''))
                parameter_tag[Attribute.AGGREGATE.value] = canonicalize(line.replace(head, '')).replace(' ', '')
            elif line.startswith('Functions'):
                parameter[Attribute.FUNCTION.value] = parse_bracket_list(line.replace(head, ''))
                parameter_tag[Attribute.FUNCTION.value] = canonicalize(line.replace(head, '')).replace(' ', '')
            elif line.startswith('Keys'):
                parameter[Attribute.KEYS.value] = parse_bracket_list(line.replace(head, ''))
                parameter_tag[Attribute.KEYS.value] = canonicalize(line.replace(head, '')).replace(' ', '')
            elif line.startswith('Join condition'):
                parameter[Attribute.JOIN_CONDITION.value] = parse_bracket_list(line.replace(head, ''))
                parameter_tag[Attribute.JOIN_CONDITION.value] = canonicalize(line.replace(head, '')).replace(' ', '')
            elif line.startswith('Left keys'):
                parameter[Attribute.LEFT_KEYS.value] = parse_bracket_list(line.replace(head, ''))
                parameter_tag[Attribute.LEFT_KEYS.value] = canonicalize(line.replace(head, '')).replace(' ', '')
            elif line.startswith('Right keys'):
                parameter[Attribute.RIGHT_KEYS.value] = parse_bracket_list(line.replace(head, ''))
                parameter_tag[Attribute.RIGHT_KEYS.value] = canonicalize(line.replace(head, '')).replace(' ', '')
            elif line.startswith('Condition'):
                # TODO[node structure]
                parameter[Attribute.CONDITION.value] = parse_bracket_list(line.replace(head, ''))
                parameter_tag[Attribute.CONDITION.value] = canonicalize(line.replace(head, '')).replace(' ', '')
            elif line.startswith('ReadSchema'):
                parameter[Attribute.READ_SCHEMA.value] = canonicalize(line.replace(head, ''))
                parameter_tag[Attribute.READ_SCHEMA.value] = canonicalize(line.replace(head, '')).replace(' ', '')
            elif line.startswith('PushedFilters'):
                parameter[Attribute.PUSHED_FILTERS.value] = parse_bracket_list(line.replace(head, ''))
                parameter_tag[Attribute.PUSHED_FILTERS.value] = canonicalize(line.replace(head, '')).replace(' ', '')
            elif line.startswith('Location'):
                # parameter[Attribute.LOCATION.value] = line.replace(head, '').split(' ')[2].strip('[').strip(']')
                # tmp = line.replace(head, '').split(' ')
                parameter[Attribute.LOCATION.value] = line.replace(head, '').replace(' ', '')
                parameter_tag[Attribute.LOCATION.value] = line.replace(head, '').replace(' ', '').replace(' ', '')
            elif line.startswith('PartitionFilters'):
                parameter[Attribute.PARTITION_FILTERS.value] = parse_bracket_list(line.replace(head, ''))
                parameter_tag[Attribute.PARTITION_FILTERS.value] = canonicalize(line.replace(head, '')).replace(' ', '')
            else:
                print_err_info(f"line:<{line}> Unconsidered field header.")
                continue
        return parameter, parameter_tag

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
        para, para_tag = parse_physical_plan(lines[1:])
        nodes.append(PhysicalPlanNode(name, para, para_tag))
    return nodes


def get_node_metrics(metrics_nodes):
    """
    解析node_metrics文件，树节点关系、子图关系、以及运行代价信息
    :param metrics_nodes:
    :return:
    """
    start_size = len("[PlanMetric]\n")
    edge_tag = re.search(r"  \d->\d;", metrics_nodes)
    subgraph_tag = re.search(r"\[SubGraph]\n", metrics_nodes)
    assert (edge_tag is not None)
    assert (subgraph_tag is not None)
    metrics = metrics_nodes[start_size:edge_tag.span()[0] - 4].split('\n\n\n\n')
    edge = metrics_nodes[edge_tag.span()[0]:subgraph_tag.span()[0]]
    # TODO [subgraph]
    subgraph = metrics_nodes[subgraph_tag.span()[1]:]
    metric_nodes = parse_metrics_text(metrics)
    build_tree_with_edge_text(edge, metric_nodes)
    return metric_nodes


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
        desc, desc_tag = parse_metric_desc(name, desc)
        if isinstance(desc_tag, str):
            desc_tag = desc_tag.replace(' ', '')
        else:
            for key in desc_tag.keys():
                desc_tag[key] = desc_tag[key].replace(' ', '')
        # TODO[后续处理时间信息]
        info = lines[1:]
        ins_node = MetricNode(nid, name, desc, info, desc_tag)
        metric_nodes[nid] = ins_node
        if "WholeStageCodegen" in name or "Sort" == name or \
                "SubqueryBroadcast" == name or "ReusedExchange" == name:
            continue
        # cache
        if MetricNode.union_cache.get(name) is None:
            MetricNode.union_cache[name] = [ins_node]
        else:
            MetricNode.union_cache.get(name).append(ins_node)
    return metric_nodes


def parse_metric_desc(name, desc):
    """
    解析代价信息中的node tag，用来和结构信息对应
    :param name:
    :param desc:
    :return:
    """
    if "SubqueryBroadcast" == name or \
            "ReusedExchange" == name or \
            "ColumnarToRow" == name or \
            "WholeStageCodegen" in name or \
            "Sort" == name:
        return desc, desc
    para = {}
    para_tag = {}
    if "FileScan" in desc:
        # 处理file scan的情况
        scan_tag = re.search(r"FileScan .+\[.*?] ", desc)
        assert scan_tag is not None
        para[Attribute.OUTPUT.value] = parse_bracket_list('[' + scan_tag.group().split('[')[1])
        para_tag[Attribute.OUTPUT.value] = canonicalize('[' + scan_tag.group().split('[')[1])
        desc = desc[scan_tag.span()[1]:]
        front = re.search(r"\w+: ", desc)
        while front is not None:
            desc = desc[front.span()[1]:]
            behind = re.search(r"\w+: ", desc)
            if behind is not None:
                para[canonicalize(front.group())] = canonicalize(desc[:behind.span()[0]])
                para_tag[canonicalize(front.group())] = canonicalize(desc[:behind.span()[0]])
                front = behind
            else:
                para[canonicalize(front.group())] = canonicalize(desc)
                para_tag[canonicalize(front.group())] = canonicalize(desc)
                break
    elif "Filter" == name:
        para[Attribute.CONDITION.value] = canonicalize(desc.replace(name, ""))
        para_tag[Attribute.CONDITION.value] = canonicalize(desc.replace(name, ""))
    elif "Project" == name:
        para[Attribute.OUTPUT.value] = parse_bracket_list(desc.replace(name, ""))
        para_tag[Attribute.OUTPUT.value] = canonicalize(desc.replace(name, ""))
    elif "Exchange" == name:
        para[Attribute.ARGUMENTS.value] = canonicalize(desc.replace(name, ""))
        para_tag[Attribute.ARGUMENTS.value] = canonicalize(desc.replace(name, ""))
    elif "BroadcastExchange" == name:
        para[Attribute.ARGUMENTS.value] = canonicalize(desc.replace(name, ""))
        para_tag[Attribute.ARGUMENTS.value] = canonicalize(desc.replace(name, ""))
    elif "SortMergeJoin" == name or "BroadcastHashJoin" == name:
        infos = canonicalize(desc.replace(name, ""))
        left_keys = re.search(r"\[.*?], ", infos)
        assert left_keys is not None
        para[Attribute.LEFT_KEYS.value] = parse_bracket_list(canonicalize(left_keys.group()))
        para_tag[Attribute.LEFT_KEYS.value] = canonicalize(canonicalize(left_keys.group()))
        infos = infos[left_keys.span()[1]:]
        right_keys = re.search(r"\[.*?], ", infos)
        assert right_keys is not None
        para[Attribute.RIGHT_KEYS.value] = parse_bracket_list(canonicalize(right_keys.group()))
        para_tag[Attribute.RIGHT_KEYS.value] = canonicalize(canonicalize(right_keys.group()))
        infos = infos[right_keys.span()[1]:]
        has_next = re.search(r", ", infos)
        if has_next is not None:
            para[Attribute.JOIN_TYPE.value] = canonicalize(infos[:has_next.span()[0]])
            para_tag[Attribute.JOIN_TYPE.value] = canonicalize(infos[:has_next.span()[0]])
        else:
            para[Attribute.JOIN_TYPE.value] = canonicalize(infos)
            para_tag[Attribute.JOIN_TYPE.value] = canonicalize(infos)
        has_condition = re.search(r"(\(.*\)){1}", infos)
        if has_condition is not None:
            para[Attribute.JOIN_CONDITION.value] = has_condition.group()
            para_tag[Attribute.JOIN_CONDITION.value] = has_condition.group()
    elif "HashAggregate" == name or "TakeOrderedAndProject" == name:
        key_value = re.search(r"\w+=\[.*?]", desc)
        while key_value is not None:
            key = get_attribute_enum(key_value.group().split('=')[0])
            if key is not None:
                value = parse_bracket_list(key_value.group().split('=')[1])
                para[key.value] = value
                para_tag[key.value] = canonicalize(key_value.group().split('=')[1])
            desc = desc[key_value.span()[1]:]
            key_value = re.search(r"\w+=\[.*?]", desc)
    else:
        print_err_info(f"[metrics error] {name} is not considered.")
    return para, para_tag


def complete_information(nodes):
    """
    根据physical plan中的信息补全metrics节点
    :param nodes:
    :return:
    """
    num = 0
    for node in nodes:
        num += 1
        toDels = []
        find = False
        candidates_node = MetricNode.union_cache.get(node.name)
        if candidates_node is None:
            continue
        for candidate_node in candidates_node:
            if find:
                break
            match = True
            for key in node.para_tag.keys():
                if candidate_node.desc_tag.__contains__(key):
                    one = candidate_node.desc_tag.get(key)
                    other = node.para_tag.get(key)
                    if not (isinstance(one, str) and isinstance(other, str) and one in other):
                        match = False
                        break
            if match:
                candidate_node.desc = {**candidate_node.desc, **node.para}
                toDels.append(candidate_node)
                find = True
                print("[" + str(num) + "]matched: " + node.name)
            else:
                print("[" + str(num) + "]Not matched: " + node.name)


def contribute_sql(root):
    children = root.children_node
    for child in children:
        contribute_sql(MetricNode.node_cache.get(child))

    # copy child contribute_sql
    if isinstance(children, list) and len(children) == 1:
        child_ctr = MetricNode.node_cache.get(children[0]).contribute_sql
        for key in child_ctr.keys():
            root.contribute_sql[key] = child_ctr[key].copy()

    if "Scan" in root.name:
        output = root.desc.get(Attribute.OUTPUT.value)
        if isinstance(output, str):
            output = [output]
        table = root.name.split(' ')[2]
        if isinstance(table, str):
            table = [table]

        if output is not None:
            root.contribute_sql[SQLContribute.SELECT.value] += output
        root.contribute_sql[SQLContribute.FROM.value] += table
        # TODO Partition Filter 和 Pushed Filter待解析
    elif "Filter" == root.name:
        condition = root.desc.get(Attribute.CONDITION.value)
        if isinstance(condition, str):
            condition = [condition]

        if condition is not None:
            root.contribute_sql[SQLContribute.WHERE.value] += condition
    elif "Project" == root.name:
        output = root.desc.get(Attribute.OUTPUT.value)
        if isinstance(output, str):
            output = [output]

        if output is not None:
            root.contribute_sql[SQLContribute.SELECT.value] = output
    elif "SortMergeJoin" == root.name or "BroadcastHashJoin" == root.name:
        join_type = root.desc.get(Attribute.JOIN_TYPE.value)
        left_keys = root.desc.get(Attribute.LEFT_KEYS.value)
        right_keys = root.desc.get(Attribute.RIGHT_KEYS.value)
        join_condition = root.desc.get(Attribute.JOIN_CONDITION.value)
        assert join_type is not None
        conditions = []
        if left_keys is not None and right_keys is not None:
            if isinstance(left_keys, str):
                conditions.append(left_keys + " = " + right_keys)
            else:
                for i in range(len(left_keys)):
                    conditions.append(left_keys[i] + " = " + right_keys[i])
        if join_condition != 'None':
            conditions.append(join_condition)

        root.contribute_sql[SQLContribute.JOIN_TYPE.value] = [join_type]
        root.contribute_sql[SQLContribute.JOIN_CONDITION.value] = conditions
        root.contribute_sql[SQLContribute.SUBQUERY.value].append(
            generate_sql(MetricNode.node_cache.get(root.children_node[0])))
        root.contribute_sql[SQLContribute.SUBQUERY.value].append(
            generate_sql(MetricNode.node_cache.get(root.children_node[1])))
    elif "HashAggregate" == root.name:
        keys = root.desc.get(Attribute.KEYS.value)
        if isinstance(keys, str):
            keys = [keys]
        result = root.desc.get(Attribute.RESULT.value)
        if isinstance(result, str):
            result = [result]

        if keys is not None:
            root.contribute_sql[SQLContribute.GROUP_BY.value] = keys
        if result is not None:
            root.contribute_sql[SQLContribute.SELECT.value] = result
    elif "TakeOrderedAndProject" == root.name:
        output = root.desc.get(Attribute.OUTPUT.value)
        if isinstance(output, str):
            output = [output]
        order_by = root.desc.get(Attribute.ORDER_BY.value)
        if isinstance(order_by, str):
            order_by = [order_by]

        if output is not None:
            root.contribute_sql[SQLContribute.SELECT.value] = output
        if order_by is not None:
            root.contribute_sql[SQLContribute.ORDER_BY.value] = order_by
    elif "Union" == root.name:
        # TODO
        print("Union TODO")
    else:
        print_err_info(f"[node ignore] {root.name} can not be deal.")


def generate_sql(node):
    def general(node, sql):
        # Where
        where = node.contribute_sql[SQLContribute.WHERE.value]
        if len(where) > 0:
            sql += "Where "
            for w in where:
                sql += w + ' and '
            sql = canonicalize(sql) + ' '

        # group by
        group_by = node.contribute_sql[SQLContribute.GROUP_BY.value]
        if len(group_by) > 0:
            sql += "Group by "
            for g in group_by:
                sql += g + ', '
            sql = canonicalize(sql) + ' '

        # order by
        order_by = node.contribute_sql[SQLContribute.ORDER_BY.value]
        if len(order_by) > 0:
            sql += "Order by "
            for o in order_by:
                sql += o + ', '
            sql = canonicalize(sql)
        return canonicalize(sql)

    if len(node.contribute_sql[SQLContribute.SUBQUERY.value]) == 0:
        # 除了join的情况拼接
        # Select
        sql = "SELECT "
        select = node.contribute_sql[SQLContribute.SELECT.value]
        if len(select) != 0:
            for sel in select:
                sql += sel + ", "
        else:
            sql += "*"
        sql = canonicalize(sql) + ' '

        # From
        fromm = node.contribute_sql[SQLContribute.FROM.value]
        sql += "FROM "
        assert len(fromm) > 0
        for fro in fromm:
            sql += fro + ', '
        sql = canonicalize(sql) + ' '
        return general(node, sql)
    else:
        # join场景拼接
        left_table = 'sub' + str(accumulator(MetricNode))
        right_table = 'sub' + str(accumulator(MetricNode))
        join_type = node.contribute_sql[SQLContribute.JOIN_TYPE.value][0]
        if 'Inner' in join_type:
            join_type = 'JOIN'
        elif 'LeftOuter' in join_type:
            join_type = 'LEFT JOIN'
        elif 'LeftSemi' in join_type:
            join_type = 'SEMI JOIN'
        elif 'LeftAnti' in join_type:
            join_type = 'ANTI JOIN'
        elif 'FullOuter' in join_type:
            join_type = 'FULL JOIN'
        else:
            print_err_info(join_type + 'is not supported.')
        # Select
        sql = "SELECT "
        select = node.contribute_sql[SQLContribute.SELECT.value]
        if len(select) != 0:
            for sel in select:
                sql += sel + ", "
        else:
            sql += "*"
        sql = canonicalize(sql) + ' '

        # From
        # sql += f'From (' + node.contribute_sql[SQLContribute.SUBQUERY.value][0] + f') as {left_table} {join_type} (' + \
        #        node.contribute_sql[SQLContribute.SUBQUERY.value][1] + f') as {right_table} '
        sql += f'From (' + node.contribute_sql[SQLContribute.SUBQUERY.value][0] + f') {join_type} (' + \
               node.contribute_sql[SQLContribute.SUBQUERY.value][1] + f') '

        return general(node, sql)


def get_candidate_views(root):
    def get_candidate_view(root, candidate_views):
        if len(root.children_node) > 0:
            for child in root.children_node:
                get_candidate_view(MetricNode.node_cache.get(child), candidate_views)
        if 'Join' in root.name or 'HashAggregate' == root.name:
            candidate_views.append(root)

    candidate_views = []
    get_candidate_view(root, candidate_views)
    return candidate_views


def accumulator(clz):
    clz.accumulator = clz.accumulator + 1
    return clz.accumulator


def build_tree_with_edge_text(edge, metric_nodes):
    item = re.search(r"\d+->\d+", edge)
    while item is not None:
        relation = item.group().split('->')
        metric_nodes[relation[0]].parents_node.append(relation[1])
        metric_nodes[relation[1]].children_node.append(relation[0])
        edge = edge[item.span()[1]:]
        item = re.search(r"\d+->\d+", edge)


def parse_bracket_list(string):
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
        stan = stan.split(',')
        for i in range(len(stan)):
            stan[i] = canonicalize(stan[i])
        # stan.sort()
    else:
        stan = [stan]
    return stan


def print_err_info(info):
    print('\033[1;31;40m' + 'error occur' + '\033[0m')
    print('\033[1;31;40m' + info + '\033[0m')


def canonicalize(item):
    """
    规范化字符串
    :return:
    """
    if isinstance(item, str):
        item = item.strip('.,: ')
        if item.endswith('and ') or item.endswith('AND '):
            item = item[:-4]
        if item.endswith('and') or item.endswith('AND'):
            item = item[:-3]
        return item
    else:
        return item


def get_attribute_enum(string):
    """
    获取字符串对应的枚举类
    :param string:
    :return:
    """
    if isinstance(string, str):
        for attr in Attribute:
            if string.lower() == attr.value.lower():
                return attr
    return None
