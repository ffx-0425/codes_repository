# -*- encoding: utf-8 -*-
# Created on 2018-04-16 10:38:15
# Project: industry_eiu

from pyspider.libs.base_handler import every
import redis
from bs4 import BeautifulSoup
from plugin.handler.abc_base_handler import AbcBaseHandler
from plugin.utils.common_utils import get_report
from plugin.configs import configs
import logging
import requests


class EiuHandler(AbcBaseHandler):
    source_identity = 'industry_eiu'
    ds_id = '96'
    r_db = redis.StrictRedis(**configs['redis'])
    item_temp = {
        'dev_mail': 'ffxie@abcft.com',
        'market': u'一级市场',
        'file_type': '.pdf',
        'report_category': u'行业研报',
        'sitename': 'http://www.eiu.com',
        'source': u'eiu',
        'payment_type': u'免费',
    }

    @every(minutes=24 * 60)
    def on_start(self):
        task_seq = self.gen_task_seq()
        self.init_crawler_log(task_seq)
        meta = {'task_seq': task_seq}
        loginpage = "https://www.eiu.com/Login.aspx?mode=up"
        r = requests.get(url=loginpage)
        if not r:
            return False
        soup = BeautifulSoup(r.text, "html5lib", from_encoding='utf8')
        __VIEWSTATE = soup.find(id="__VIEWSTATE").get("value")
        __EVENTVALIDATION = soup.find(id="__EVENTVALIDATION").get("value")
        data = {
            "username": "888888@163.com",
            "password": "Data2017",
            "__VIEWSTATE": __VIEWSTATE,
            "__EVENTVALIDATION": __EVENTVALIDATION
        }
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Cache-Control": "max-age=0",
            "Content-type": "application/x-www-form-urlencoded",
            "Referer": "https://www.eiu.com/Login.aspx?mode=up",
            "Upgrade-Insecure-Requests": "1"
        }
        loginurl = "https://www.eiu.com/Login.aspx?mode=up"
        self.crawl(loginurl, force_update=True, data=data, headers=headers, save={'meta': meta}, callback=self.login,
                   validate_cert=False)

    def login(self, response):
        if response.status_code == 200:
            meta = response.save['meta']
            content_url = "http://www.eiu.com/landing.aspx?topic=special_reports"
            r = requests.get(content_url)
            soup = BeautifulSoup(r.text, "html5lib", from_encoding='utf8')
            abstract = soup.find_all(class_="abstract")
            for i in abstract:
                pdfDownload = i.find("h3").find("a").get("href")
                if "http://www.eiu.com/public/topical_report.aspx?campaignid=" in pdfDownload:
                    report = {}
                    report['title'] = i.find("h3").find("a").string
                    report['summary'] = i.find(class_="snippet").string
                    cookies = dict(response.cookies)
                    cookie_str = ''.join([str(x) for x in cookies.items()]).replace("', '", "=").replace("')('",
                                                                                                         ";").replace(
                        "('", '').replace("')", '')
                    cookie = {
                        'Cookie': cookie_str
                    }

                    self.crawl(pdfDownload, callback=self.pdf_page, headers=cookie,
                               save={'meta': meta, 'report': report}, validate_cert=False,
                               proxy=self.get_random_proxy())

    def pdf_page(self, response):
        meta = response.save['meta']
        task_seq = meta.get('task_seq')
        report = response.save['report']
        content = response.content
        pdfsoup = BeautifulSoup(content, 'html.parser', from_encoding='utf-8')
        file_url = pdfsoup.find('a', class_="btn-blue report-download").get("href")
        if self.is_data_existed(file_url):
            logging.info('file existed#' + file_url)
            return
        item = get_report()
        item.update(self.item_temp)
        item.update(report)
        item['file_url'] = file_url
        item['source_url'] = response.url
        redis_data = {'extension': item['file_type'], 'item': item,
                      'requests_data': {'url': item['file_url']}, 'content_type': self.CONTENT_TYPE_PDF}
        self.log_new_data(redis_data, task_seq, file_url)


