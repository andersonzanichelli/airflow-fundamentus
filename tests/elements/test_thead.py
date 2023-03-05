import pytest
from bs4 import BeautifulSoup

from fundamentus.element.thead import THead

@pytest.mark.parametrize(
    ("expected", "path"),
    [
        (["Papel", "Cotação", "P/L", "P/VP", "PSR", "Div.Yield"], "tests/resources/thead_1.html"),
        (["P/Ativo", "P/Cap.Giro","P/EBIT", "P/Ativ Circ.Liq", "EV/EBIT", "EV/EBITDA"], "tests/resources/thead_2.html"),
    ],
)
def test_extract_tbody(expected, path):

    with open(path, 'r') as f:
        contents = f.read()
        soup = BeautifulSoup(contents, features="html.parser")
        actual = THead().extract(soup)
        assert expected == actual