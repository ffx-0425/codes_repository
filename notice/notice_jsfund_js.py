#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2018-03-16 19:12:25
# Project: notice_jsfund_js


from pyspider.libs.proxy_helper import ProxyHelper
from pyspider.libs.base_handler import *
import random
import urllib2
from bs4 import BeautifulSoup
from pyspider.libs.chx.uploader import Uploader
from pyspider.libs.chx import tool
import requests
import json
import os
import uuid
import oss2
import datetime
import time
import re
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

class Handler(BaseHandler):
    crawl_config = {
        'validate_cert': False,
        "headers": {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36'},
        'timeout' : 1200,
        'connect_timeout': 1000,
        "retries": 10,
        "itag": 'v2.3'
    }
   
       

    
    @every(minutes=24 * 60)
    def on_start(self):
        time.sleep(2)
        self.crawl('http://www.jsfund.cn/izenportal/service/portal/findActionInfo?callback=jQuery112008388390334697426_1514358536709type=1&catalogid=156%2C157%2C158%2C1045%2C1046%2C1047&startDate=2017-6-27&seq=pb', callback=self.index_page, timeout=15 * 60)
        
        self.crawl('http://www.jsfund.cn/izenportal/service/portal/findActionInfo?callback=jQuery1120010726443164709565_1514353354744type=1&catalogid=976&seq=pb', callback=self.index_page,timeout=15 * 60)
                                   
        
    @config(age=1 * 24 * 60 * 60)
    def index_page(self, response):      
        head = 'http://www.jsfund.cn/'
        content = response.content
        data = response.content.split('"prop2":"')
        for i in range(1,len(data)):
            report = {}
            
            #时间
            time = response.content.split('"publishdate":"')[i].split('"')[0]
            time = eval("u"+"\'"+time+"\'")
            
            #基金代码
            stock_code = response.content.split('"prop2":"')[i].split('"')[0]
            report['stock_code'] = stock_code                                   
            
            #公告页面链接
            source_url = head + response.content.split('"url":"')[i].split('"')[0]
            
            report['time'] = time
            report['plate'] = u'基金'

            self.crawl('http://www.jsfund.cn/izenportal/service/left/getFundLeft?callback=jQuery112008388390334697426_1514358536709type=1&list=only#'+str(i), callback=self.stock_page,save = {'report':report,'source_url':source_url}, timeout=15 * 60)
     
    
    @config(age=1 * 24 * 60 * 60)
    def stock_page(self, response):
        report = response.save['report']
        source_url = response.save['source_url']        
        content = response.content
        
        data = response.content.split('"fundshortname":"')
        stock = {}
        for i in range(1,len(data)):                  
            
            #基金名称
            stock_name = response.content.split('"fundshortname":"')[i].split('"')[0]
            #基金代码
            stock_code = response.content.split('"fundcode":"')[i].split('"')[0]
            
            stock[stock_code] = stock_name    
            #基金名称
        if len(report['stock_code'])==0:
            report['stock_name'] = ''
        elif len(report['stock_code'])==6:
            report['stock_name'] = stock[report['stock_code']]
        elif len(report['stock_code'])>6:            
            stock_code_list = report['stock_code'].split(',')
            stock_name = ''            
            for i in range(len(stock_code_list)):                    
                stock_name += (stock[stock_code_list[i]]+',')
            report['stock_name'] = stock_name[:-1]
            
        self.crawl(source_url,save=report,callback=self.source_page, timeout=15 * 60)
    
    @config(age=1 * 24 * 60 * 60)
    def source_page(self, response): 
        content = response.content
        soup = BeautifulSoup(content, 'lxml', from_encoding='utf8')  
        if 'njbg' in response.url:
            report = response.save
            date = {}
            date['type'] ='年金报告'
            pdf_div = soup.find('div',class_='mm_r_box3')
            pdf_title_div = soup.find('div',class_='mm_r_title1')
            
            #文件链接
            file_url = pdf_div.find('a')['href']
            date['file_url'] = file_url
            
            #标题
            date['title'] = pdf_title_div.get_text()
            
            #阿里云OSS 的存储完整路径
            date['oss_path'] = get_oss_path(date['file_url']) 
            date['source_url'] = response.url
            self.crawl(date['file_url'], save={'item1': report,'date':date}, callback=self.upload1_item, timeout=15 * 60,oss_path=date['oss_path'], validate_cert=False)
        
        else:
            if soup.find('div',id='printcontent') != None:
                report = response.save
                report['type'] = soup.find('div',class_='mm_r_title1 mm_333').get_text()                 
                report['title'] = soup.find('div',id='printcontent').find('h2',class_='article_title').get_text()                     
                try:  
                    items = soup.find('div',id='printcontent').find_all('a')
                    for item in items:
                        pdf_url = item['href']
                        if 'AttachDownLoad' in pdf_url:
                            reporti = {} 
                            #pdf_url = re.search('http.*?jsp\?id=.*?"',str(soup.find('div',id='printcontent'))).group().replace('"','') 
                            reporti['file_url'] = pdf_url
                            reporti['oss_path'] = get_oss_path(reporti['file_url']) 
                            reporti['source_url'] = response.url
                            self.crawl(reporti['file_url'], save={'item2': report,'reporti':reporti}, callback=self.upload2_item,oss_path=reporti['oss_path'],validate_cert=False)
                except: 
                    reporti = {} 
                    reporti['file_url'] = response.url
                    reporti['source_url'] = response.url
                    self.crawl(reporti['file_url'], save={'item': report,'report':reporti}, callback=self.handle_result, timeout=15 * 60, validate_cert=False,itag = 'v1.2')
                         
    
    def handle_result(self, response):
            
            soup = BeautifulSoup(response.content, 'lxml')        
            content = str(soup.find('div',id = 'printcontent')).replace(str(soup.find('div',id = 'printcontent').find('div',class_ = 'articleinfo')),'').replace('http://嘉实基金/','#').replace(str(soup.find('h2',class_ = 'article_title')),'').replace(str(soup.find('div',id = 'printcontent').find('div',class_ = 'articleinfo')),'')
            report1 = response.save['item']
            report2 = response.save['report']
            item = dict(report1, **report2) 
            item['oss_path'] = get_oss_path2(item['file_url'])   
            item.update({
            'file_size':len(content)
            })
            item['time'] = datetime.datetime.strptime(item['time'],"%Y-%m-%d %H:%M:%S")
            upload_html(item['oss_path'],content)
            item['timestamp'] = int(time.time() * 1000)
            item['src_id'] = 'jsfund'+str(item['timestamp'])+str(random.randint(10,1000))
            rst = upload_html(item['oss_path'],content)
            if rst == 200:
                print 'ok ok ok ok ok ok'
                item['file_type'] = '.shtml' 
                item['downloaded'] = True
                item['org_id'] = '' 
                item['storage'] = "oss"
                item['column'] = '基金公告'
                item['export_flag'] = False
                item['export_version'] = 0
                item['category_id'] = ''
                return item
            else:
                print '失败'  
    
    def upload1_item(self, response):
        # 上传已下载的文件.
        if response.save and 'item1' in response.save and 'upload_result' in response.save:
            
            report1 = response.save['item1']
            report2 = response.save['date']
            report = dict(report1, **report2) 

            upload_result = response.save['upload_result']
            if response.save['upload_result']['status'] == 200:
                report['downloaded'] = True
                report['file_size'] =len(requests.get(response.url).content)                  
                report['org_id'] = ''
                report['file_type'] = '.pdf'
                report['time'] = datetime.datetime.strptime(report['time'],"%Y-%m-%d %H:%M:%S")
                #基金
                report['plate'] = "基金"
                report['storage'] = "oss"
                report['timestamp'] =int(time.time() * 1000)
                report['src_id'] = 'jsfund'+str(report['timestamp'])+str(random.randint(10, 1000))
                report['column'] = '基金公告'
                report['export_flag'] = False
                report['export_version'] = 0
                report['category_id'] = ''
                return report
        else:
            raise Exception('failed to upload!!, nothing in response.save') 
            
    def upload2_item(self, response):
        # 上传已下载的文件.
        if response.save and 'item2' in response.save and 'upload_result' in response.save:
            
            report1 = response.save['item2']
            report2 = response.save['reporti']
            report = dict(report1, **report2) 

            upload_result = response.save['upload_result']
            if response.save['upload_result']['status'] == 200:
                report['downloaded'] = True
                report['file_size'] =len(requests.get(response.url).content)	                
                report['org_id'] = ''
                report['file_type'] = '.pdf'
                report['time'] = datetime.datetime.strptime(report['time'],"%Y-%m-%d %H:%M:%S")
                #基金
                report['plate'] = "基金"
                report['storage'] = "oss"
                report['column'] = '基金公告'
                report['export_flag'] = False
                report['export_version'] = 0
                report['category_id'] = ''
                report['timestamp'] =int(time.time() * 1000)
                report['src_id'] = 'jsfund'+str(report['timestamp'])+str(random.randint(10, 1000))
                return report
        else:
            raise Exception('failed to upload!!, nothing in response.save')    

def get_oss_path(file_url):
    return 'jsfund/{}.pdf'.format(uuid.uuid3(uuid.NAMESPACE_DNS, u'{}_{}.pdf'.format('3j695', file_url).encode('utf-8')))

def get_file_size(response):
    return response.headers.get('Content-Length', '')
        
def get_oss_path2(file_url):
    return 'jsfund/{}.shtml'.format(uuid.uuid3(uuid.NAMESPACE_DNS, u'{}_{}.shtml'.format('juhk7', file_url).encode('utf-8')))   

def upload_html(oss_path,content):
    temp = '''<!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Document</title>
    </head>
    <body>
    {}
    </body>
    </html>
        '''
    
    config = {
    "key": "LTAIkAyF3705B6J7",
    "secret": "itevqE2BRRStNgLe1FAb3d3qMHgrUV",
    "bucket_name": "abc-crawler",
    "endpoint": "http://oss-cn-hangzhou.aliyuncs.com"
  }
    bucket = oss2.Bucket(oss2.Auth(config['key'], config['secret']), config['endpoint'],
                                  config['bucket_name'])
    resp = bucket.put_object(oss_path, temp.format(content), headers={'Content-Type'	
:'text/html'})
    return resp.status  
        