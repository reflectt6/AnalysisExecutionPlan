class SparkPhysicalNode(object):
    def __init__(self, nid, name):
        self.children_node = []
        self.parents_node = []
        self.id = nid
        self.name = name
        self.para = ()
