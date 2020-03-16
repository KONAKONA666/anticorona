import os
import csv
import time
import logging
import itertools

import asyncio
import aiohttp
import concurrent.futures

from bs4 import BeautifulSoup

#BASE_URL = r'https://www.nur.kz'

#log = logging.getLogger(__name__)
#logging.basicConfig(level=logging.INFO)

#limit = asyncio.Semaphore(10)

class BaseParser:
    def __init__(self, base_url, loop, redis_conn, time_sleep=60*60, limit=10, max_workers=4):
        self.loop = loop
        self.base_url = base_url
        self.time_sleep = time_sleep
        self.limit = asyncio.Semaphore(limit)
        self.max_workers = max_workers
        #self.redis_conn = redis_conn

    async def get_html(self, client: aiohttp.ClientSession, url: str):
        limit = self.limit
        with await limit:
            async with client.get(url) as r:
                text = await r.text()
                return text

    async def parse(self):
        urls = self.get_urls()
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor: 
            while True:
                pages = await self.process_tag_pages(urls)
                tasks = [self.loop.run_in_executor(executor, self.get_links, page) for page in pages]
                links = list(itertools.chain.from_iterable(await asyncio.gather(*tasks)))
              #  log.info('Len links {}'.format(len(links)))
                articles_raw = await self.process_tag_pages(links)
                article_tasks = [self.loop.run_in_executor(executor, self.process_article, page) for page in articles_raw]
                data_csv = await asyncio.gather(*article_tasks)
#                log.info('Complited: {}'.format(len(data_csv)))
                #await asyncio.sleep(self.time_sleep)
                return data_csv

    async def process_tag_pages(self, urls):
        async with aiohttp.ClientSession() as client:
            coroutunes = [self.get_html(client, url) for url in urls]
            pages_html = await asyncio.gather(*coroutunes)
            return pages_html



class NurParser(BaseParser):
    def __init__(self, max_page=2 ,*args, **kwargs):
        super(NurParser, self).__init__(*args, **kwargs)
        self.max_page = max_page

    def get_urls(self):
        return [r'https://www.nur.kz/tag/2019-ncov.html?page={}'.format(i) for i in range(1, self.max_page)]

    def get_links(self, html):
        soup = BeautifulSoup(html, 'lxml')
        curr_links = soup.select('.block-infinite__item a')
        return [a.get('href') for a in curr_links]

    def process_article(self, html):
        soup = BeautifulSoup(html, 'lxml')
        title = soup.select('h1')[0].text
        try:
            time = soup.select('.layout-article-page__content time')[0].get('datetime')
        except:
            return ['', '', '', '']
        text = " ".join([p.text for p in soup.select('artice > p')])
    #with open('tengri.csv', 'a') as csvf:
    #    writer = csv.writer(csvf)
    #    writer.writerow(['tengri', title.encode('utf-8'), time.encode('utf-8'), text.encode('utf-8')])
        #log.info('{} is completed'.format(title))
        text = ''.join([p.text for p in soup.select('article > p')])
        return [self.base_url, title, time, text]


class TengriParser(BaseParser):
    def __init__(self, max_page=2 ,*args, **kwargs):
        super(TengriParser, self).__init__(*args, **kwargs)
        self.max_page = max_page

    def get_urls(self):
        return [r'https://tengrinews.kz/tag/%D0%9A%D0%BE%D1%80%D0%BE%D0%BD%D0%B0%D0%B2%D0%B8%D1%80%D1%83%D1%81-%D0%B2-%D0%9A%D0%B0%D0%B7%D0%B0%D1%85%D1%81%D1%82%D0%B0%D0%BD%D0%B5/?page={}'.format(i) for i in range(1, self.max_page)]

    def get_links(self, html):
        soup = BeautifulSoup(html, 'lxml')
        curr_links = soup.select('.tn-news-author-list .tn-news-author-list-item a')
        return [self.base_url + a.get('href') for a in curr_links]

    def process_article(self, html):
        soup = BeautifulSoup(html, 'lxml')
        title = soup.select('h1')[0].text
        try:
            time = soup.select('.tn-data-list time')[0].get('datetime')
        except:
            return ['', '', '', '']
        text = " ".join([p.text for p in soup.select('.tn-news-content .tn-news-text > p')])
    #with open('tengri.csv', 'a') as csvf:
    #    writer = csv.writer(csvf)
    #    writer.writerow(['tengri', title.encode('utf-8'), time.encode('utf-8'), text.encode('utf-8')])
        #log.info('{} is completed'.format(title))
        #print(text)
        return [self.base_url, title, time, text]


if __name__ == '__main__':
    pass
