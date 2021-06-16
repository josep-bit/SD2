import logging
import requests
from bs4 import BeautifulSoup
from lithops.storage.cloud_proxy import open
from lithops import FunctionExecutor
from lithops.utils import setup_lithops_logger

setup_lithops_logger(logging.DEBUG)
import re


def getlink(url):
    result = []
    for i in range(1, 40):
        x = url + str(i)
        for i in urls(x):
            result.append(i)
    return result


def urls(url):
    link = []
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'html.parser')
    links = soup.find_all('div', {'class': 'article-left'})
    for i, enter in enumerate(links):
        link.append(enter.find('a'))
    result = map(separate, link)
    return list(result)


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


def separate(tag):
    return str(tag).split('"')[1]


def save(result):
    j = 1
    for i in result:
        t = infototext(i)
        file = 'data' + str(j) + '.txt'
        getfile(t, file)
        j += 1
    ok = 'ok'
    return ok


if __name__ == '__main__':
    with FunctionExecutor() as exe:
        url = 'https://stories.lavanguardia.com/search?q=covid&author=&section=&startDate=&endDate=&sort=&page='
        exe.call_async(getlink, url)
        r = exe.get_result()
        exe.call_async(save, r)
        print(exe.get_result())



