class TBody:

    def extract(self, table):
        tbody = table.find("tbody")
        trs = tbody.findAll("tr")

        rows = []
        for row in trs:
            rows.append([td.text for td in row.findAll("td")])

        return rows
