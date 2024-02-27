import json

class DataObject:
    def __init__(self, base_name, url=None, headline=None, summary=None, date=None, copyright=None):
        self.base_name = base_name
        self.url = url
        self.headline = headline
        self.summary = summary
        self.date = date
        self.copyright = copyright

    def __str__(self):
        return f"DataObject({self.base_name}, URL={self.url}, Headline={self.headline}, Summary={self.summary}, Date={self.date}, Copyright={self.copyright})"

    def to_json(self):
        return json.dumps(self.__dict__, indent=4)