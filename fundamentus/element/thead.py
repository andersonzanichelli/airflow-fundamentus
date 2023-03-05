class THead:

    def extract(self, table):
        return [th.text for th in table.findAll("th")]
