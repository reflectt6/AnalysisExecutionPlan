from enum import Enum


class PhysicalPlanNode(object):
    def __init__(self, name, para, para_tag):
        self.name = name
        self.para = para
        self.para_tag = para_tag


class MetricNode(object):
    node_cache = {}

    def __init__(self, nid, name, desc, time_info, desc_tag):
        self.nid = nid
        self.name = name
        self.desc = desc
        self.desc_tag = desc_tag
        self.time_info = time_info
        self.children_node = []
        self.parents_node = []


class Attribute(Enum):
    OUTPUT = 'Output'
    INPUT = 'Input'
    BATCHED = 'Batched'
    ARGUMENTS = 'Arguments'
    RESULT = 'result'
    AGGREGATE = 'Aggregate Attributes'
    FUNCTION = 'Functions'
    KEYS = 'Keys'
    JOIN_CONDITION = 'Join condition'
    LEFT_KEYS = 'Left keys'
    RIGHT_KEYS = 'Right keys'
    CONDITION = 'Condition'
    READ_SCHEMA = 'ReadSchema'
    PUSHED_FILTERS = 'PushedFilters'
    LOCATION = 'Location'
    PARTITION_FILTERS = 'PartitionFilters'
    JOIN_TYPE = 'Join Type'
