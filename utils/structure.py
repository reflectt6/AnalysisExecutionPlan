from enum import Enum


class PhysicalPlanNode(object):
    def __init__(self, name, para):
        self.name = name
        self.para = para


class MetricNode(object):

    def __init__(self, nid, name, desc, time_info):
        self.nid = nid
        self.name = name
        self.desc = desc
        self.time_info = time_info
        self.children_node = []
        self.parents_node = []


class Attribute(Enum):
    OUTPUT = 'output'
    INPUT = 'input'
    BATCHED = 'batched'
    ARGUMENTS = 'arguments'
    RESULT = 'result'
    AGGREGATE = 'aggregate attributes'
    FUNCTION = 'functions'
    KEYS = 'keys'
    JOIN_CONDITION = 'join condition'
    LEFT_KEYS = 'left keys'
    RIGHT_KEYS = 'right keys'
    CONDITION = 'condition'
    READ_SCHEMA = 'readSchema'
    PUSHED_FILTERS = 'pushedFilters'
    LOCATION = 'location'
    PARTITION_FILTERS = 'partitionFilters'

