class Novatool(object):

    def __init__(self):
        self._nodetool = ['novatool', 'localhost']

    @property
    def nodetool(self):
        return self._nodetool
