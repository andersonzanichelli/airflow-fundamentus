import requests
from bs4 import BeautifulSoup
from pandas import DataFrame

from fundamentus.element.thead import THead
from fundamentus.element.tbody import TBody
from fundamentus.config import BASE_URL

def scraping():
    agent = {"User-Agent":'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'}
    page = requests.get(BASE_URL, headers=agent)
    soup = BeautifulSoup(page.text, features="html.parser")

    #with open("fundamentus.html", "r") as f:
    #    contents = f.read()
    #    soup = BeautifulSoup(contents, features="html.parser")

    table = soup.find('table')
    thead = THead().extract(table)
    tbody = TBody().extract(table)

    return thead, tbody