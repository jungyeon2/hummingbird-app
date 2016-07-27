import json


class Settings:
    settings = None

    def __init__(self, filename):
        self.settings = json.load(open(filename, 'r'))

    def foo(self):
        print self.settings.keys()
