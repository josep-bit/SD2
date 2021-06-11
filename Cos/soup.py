import requests
from bs4 import BeautifulSoup
import re
# global list
link = []
result = []


class Scrapy:

    def urls(self, url):
        global link, result
        r = requests.get(url)
        soup = BeautifulSoup(r.text, 'html.parser')
        links = soup.find_all('div', {'class': 'article-left'})
        for i, enter in enumerate(links):
            link.append(enter.find('a'))
        result = map(self.separate, link)
        return result

    def stringtag(self,value):
        return re.sub(r'<[^>]*?>', '', value)

    def infototext(self, url, file):
        r = requests.get(url)
        soup = BeautifulSoup(r.text, 'html.parser')
        text = soup.find_all('p', {'data-gtm-element-container': 'modulo-texto-link', 'class': 'paragraph'})
        for i, enter in enumerate(text):
            p = str(enter)
            p = self.stringtag(p)
            with open(file, 'w') as f:
                f.write(p)
        f.close()


    def separate(self, tag):
        return str(tag).split('"')[1]

    def getlink(self, url):
        for i in range(1, 40):
            urls = url + str(i)
            self.urls(urls)


if __name__ == '__main__':
    s = Scrapy()
    url = 'https://www.lavanguardia.com/television/20210611/7521629/lagrimas-directo-iker-jimenez-muerte-anna-olivia.html'
    s.infototext(url, 'a.txt')
