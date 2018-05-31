from bs4 import BeautifulSoup
import redis
from plugin.handler.abc_base_handler import AbcBaseHandler
from plugin.utils.common_utils import get_report, format_time
from plugin.configs import configs
from urlparse import urljoin
import logging
import sys

reload(sys)
sys.setdefaultencoding('utf-8')
import re
import requests


class CicirHandler(AbcBaseHandler):
    source_identity = 'industry_madisonboom'
    r_db = redis.StrictRedis(**configs['redis'])
    item_temp = {
        'dev_mail': 'yxpeng@abcft.com',
        'market': '二级市场',
        'file_type': '.pdf',
        'report_category': '行业分析',
        'sitename': 'http://www.madisonboom.com',
        'source': 'madisonboom',
        'payment_type': '免费',
    }
    headers = {
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Host': 'pan.baidu.com',
        'Origin': 'https://pan.baidu.com',
        'Referer': 'https://pan.baidu.com/s/16vXHQ5E3Fx2m614oX22LTA',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36''',
    }
    start_url = 'http://www.madisonboom.com/category/knowleage/report/page/%s/'
    pdf_requests_url = 'https://pan.baidu.com/api/sharedownload?sign={sign}&timestamp={timestamp}&channel=chunlei&web=1&app_id=250528&bdstoken=null&logid={logid}&dp-callid=0&clienttype=0'

    def on_start(self):
        task_seq = self.gen_task_seq()
        self.init_crawler_log(task_seq)
        meta = {'task_seq': task_seq}
        for i in range(1, 29):
            self.crawl(self.start_url % str(i), age=10 * 24 * 60, auto_recrawl=True, force_update=True,
                       callback=self.handle_items, save=meta, validate_cert=False,
                       proxy='http://cj514w:cj514w@27.50.154.20:808')

    def handle_items(self, response):
        soup = BeautifulSoup(response.content, 'html.parser')
        for i in soup.select('#gallery_list_elements li'):
            try:
                # title=i.select('h3 a')[0].text
                report = get_report()
                report.update(self.item_temp)
                report.update({
                    'source_url': i.select('h3 a')[0]['href'],
                    'time': i.select('.info span')[0].text.strip(),
                })
                self.crawl(report['source_url'], callback=self.yunpan_page, validate_cert=False,
                           save={'meta': response.save, 'report': report},
                           proxy='http://cj514w:cj514w@27.50.154.20:808')
            except:
                pass

    def yunpan_page(self, response):
        soup = BeautifulSoup(response.content, 'html.parser')
        for n, i in enumerate(soup.select('.content_goods a')):
            try:
                #提取码和验证码 有待完善
                print re.findall('密码：(.*?)</strong>', str(soup), re.S)[n]
            except:
                pass
            if 'pan' in i['href']:
                yunpan_url = i['href']
                self.crawl(yunpan_url, callback=self.pdf_page, validate_cert=False,
                           save={'meta': response.save, 'report': response.save['report']},
                           proxy='http://cj514w:cj514w@27.50.154.20:808')

    def pdf_page(self, response):
        report = response.save['report']
        soup = BeautifulSoup(response.content, 'html.parser')
        report['title'] = soup.select('title')[0].text.split('_免费')[0]
        if 'pdf' not in report['title']:
            return  # 不是pdf
        data = {
            'encrypt': '0',
            'product': 'share',
            'uk': '2811459421',
            'primaryid': str(re.findall('shareid":(.*?),"', str(soup))[0]),
            'fid_list': '[' + str(re.findall('fs_id":(.*?),"', str(soup))[0]) + ']',
            'path_list': '',
        }

        timestamp = re.findall('timestamp":(.*?),"', str(soup))[0]
        sign = re.findall('sign":"(.*?)","', str(soup))[0]
        logid = re.findall('logid=(.*?)&dp', str(soup))[0]

        requests_url = self.pdf_requests_url.format(timestamp=timestamp, sign=sign, logid=logid)
        self.crawl(requests_url, callback=self.down_page, validate_cert=False, data=data, headers=self.headers,
                   save={'meta': response.save, 'report': report}, proxy='http://cj514w:cj514w@27.50.154.20:808')

    def down_page(self, response):
        report = response.save['report']
        report['file_url'] = response.json['list'][0]['dlink']
        if self.is_data_existed(report['file_url']):
            logging.info('file existed#' + report['file_url'])
            return
        redis_data = {'extension': report['file_type'], 'item': report,
                      'requests_data': {'url': report['file_url']}, 'content_type': self.CONTENT_TYPE_PDF}
        # 去重，文件队列，源更新
        self.log_new_data(redis_data, response.save['meta']['task_seq'], report['file_url'])

