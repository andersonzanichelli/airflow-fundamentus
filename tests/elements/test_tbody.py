import pytest
from bs4 import BeautifulSoup

from fundamentus.element.tbody import TBody

@pytest.mark.parametrize(
    ("expected", "path"),
    [
        ([["MLFT4", "22,39", "2.952,91", "11,77", "45,292", "0,00%"], ["BBTG12", "3,15", "66.496,50", "26.598,60", "0,000", "0,00%"]], "tests/resources/tbody_1.html")
    ],
)
def test_extract_tbody(expected, path):

    with open(path, "r") as f:
        contents = f.read()
        soup = BeautifulSoup(contents, features="html.parser")
        table = soup.find("table")

        rows = TBody().extract(table)

        assert expected == rows