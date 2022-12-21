class SparkPhysicalNode(object):
    children_node = []
    parents_node = []
    id = -1

    def __init__(self, name, para):
        self.name = name
        self.para = para
