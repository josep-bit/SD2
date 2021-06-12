import logging
import requests
from bs4 import BeautifulSoup
from lithops.storage.cloud_proxy import open
from lithops import FunctionExecutor
from lithops.utils import setup_lithops_logger

setup_lithops_logger(logging.DEBUG)
import re

# global list
link = []
result = []


def urls(url):
    global link, result
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'html.parser')
    links = soup.find_all('div', {'class': 'article-left'})
    for i, enter in enumerate(links):
        link.append(enter.find('a'))
    result = map(separate, link)
    return result


def stringtag(value):
    return re.sub(r'<[^>]*?>', '', value)


def infototext(url):
    t = []
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'html.parser')
    text = soup.find_all('p', {'data-gtm-element-container': 'modulo-texto-link', 'class': 'paragraph'})
    for i, enter in enumerate(text):
        p = str(enter)
        p = stringtag(p)
        t.append(p)
    return t


def getfile(t, file):
    with open(file, 'w') as f:
        for i in t:
            f.write(i + '\n')
    ok = 'ok'
    return ok


def separate(tag):
    return str(tag).split('"')[1]


def getlink(url):
    for i in range(1, 40):
        x = url + str(i)
        urls(x)


if __name__ == '__main__':
    url = 'https://stories.lavanguardia.com/search?q=covid&author=&section=&startDate=&endDate=&sort=&page='
    getlink(url)
    j = 1
    for i in result:
        print(j)
        with FunctionExecutor() as exe:
            t = infototext(i)
            file = 'data' + str(j) + '.txt'
            exe.call_async(getfile, (t, file))
            print(exe.get_result())
            j += 1
