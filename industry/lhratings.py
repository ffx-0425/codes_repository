#!/usr/bin/env python
# coding=utf-8
# auther:yxpeng

from bs4 import BeautifulSoup
import redis
from plugin.handler.abc_base_handler import AbcBaseHandler
from plugin.utils.common_utils import get_report, change_url, standardized_item, format_time
from plugin.configs import configs
from urlparse import urljoin
import logging
from datetime import datetime
import re
import sys

reload(sys)
sys.setdefaultencoding('utf-8')


class CicirHandler(AbcBaseHandler):
    source_identity = 'industry_lhratings'
    ds_id = '151'
    r_db = redis.StrictRedis(**configs['redis'])
    item_temp = {
        'dev_mail': 'yxpeng@abcft.com',
        'market': u'二级市场',
        'file_type': u'.pdf',
        'report_category': u'行业分析',
        'sitename': u'http://www.lhratings.com',
        'source': u'lhratings',
        'payment_type': u'免费',
    }
    start_url = 'http://www.lhratings.com/research/index.html'

    # 先登录获取cookie
    def on_start(self):
        data = {
            "username": "yli@abcft.com",
            "action": "login",
            "password": "Data2017"
        }
        self.crawl('http://www.lhratings.com/user/login.html', callback=self.on_second, data=data, validate_cert=False)

    def on_second(self, response):
        task_seq = self.gen_task_seq()
        self.init_crawler_log(task_seq)
        meta = {'task_seq': task_seq}

        cookie_dic = {'cookie': response.cookies['JSESSIONID']}

        for num in range(1, 27):
            self.crawl(self.start_url, params={'page': num}, age=10 * 24 * 60 * 60, auto_recrawl=True,
                       force_update=True, callback=self.handle_items, save={'meta': meta, 'cookie_dic': cookie_dic},
                       validate_cert=False)

    def handle_items(self, response):
        header = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36',
            'cookie': 'JSESSIONID=' + response.save['cookie_dic']['cookie']
        }
        soup = BeautifulSoup(response.content, 'lxml')
        tbody = soup.find_all('tbody')[0]
        for i in tbody.find_all('tr'):
            href = i.find_all('td')[1].find('a').get("href")
            if not href:
                continue
            report = get_report()
            report.update(self.item_temp)
            report.update({
                'file_url': href,
                'title': i.find_all('td')[1].find('a').text,
                'time': format_time(i.find_all('td')[3].text.strip(), '%Y-%m-%d')
            })
            if self.is_data_existed(report['file_url']):
                logging.info('file existed#' + report['file_url'])
                continue
            redis_data = {'extension': report['file_type'], 'item': report,
                          'requests_data': {'url': report['file_url'], 'headers': header},
                          'content_type': self.CONTENT_TYPE_PDF}
            # 去重，文件队列，源更新
            self.log_new_data(redis_data, response.save['meta']['task_seq'], report['file_url'])




