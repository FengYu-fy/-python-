#coding=utf-8  
import re  
import urlparse  
from bs4 import BeautifulSoup as BS  
  
class HtmlParser(object):  
    def parser(self, page_url, html_cont):  
        if page_url is None or html_cont is None:  
            return  
        soup = BS(html_cont, 'html.parser', from_encoding='utf-8')  
        new_urls = self._get_new_urls(page_url, soup)  
        new_data = self._get_new_data(page_url, soup)  
        return new_urls, new_data  
  
    def _get_new_urls(self, page_url, soup):  
        new_urls = set()  
        # 抽取符合要求的a标签  
        links = soup.find_all('a', href=re.compile(r'/item/.*'))  
        for link in links:  
            # 提取href属性  
            new_url = link['href']  
            # 拼接成完整网址  
            new_full_url = urlparse.urljoin(page_url, new_url)  
            new_urls.add(new_full_url)  
        return new_urls  
  
    def _get_new_data(self, page_url, soup):  
        data = {}  
        data['url'] = page_url  
        title = soup.find('dd', class_='lemmaWgt-lemmaTitle-title').find('h1')  
        data['title'] = title.get_text()  
        summary = soup.find('div', class_='lemma-summary')  
        # 获取到tag中包含的所有文版内容包括子孙tag中的内容,并将结果作为Unicode字符串返回  
        data['summary'] = summary.get_text()  
        return data