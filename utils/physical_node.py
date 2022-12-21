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
