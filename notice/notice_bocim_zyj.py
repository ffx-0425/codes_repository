#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2018-03-21 11:16:02
# Project: notice_bocim_zyj

import random
from pyspider.libs.base_handler import *
from bs4 import BeautifulSoup
import re
import oss2
import json
import os
import uuid
import requests
import datetime
import time

ROOTURL = 'http://www.bocim.com/FundSite/actions/fundProducts!fundNetValueList.html'
JJGG='http://www.bocim.com/FundSite/actions/special/specialdeposit!fundBulletinPage.html?curPage={}&fundCode={}&title=&starttime=&endtime=&type=0'
JJJB='http://www.bocim.com/FundSite/actions/special/specialdeposit!fundBulletinPage.html?curPage={}&fundCode={}&title=&starttime=&endtime=&type=206'
JJBNB='http://www.bocim.com/FundSite/actions/special/specialdeposit!fundBulletinPage.html?curPage={}&fundCode={}&title=&starttime=&endtime=&type=205'
JJNB='http://www.bocim.com/FundSite/actions/special/specialdeposit!fundBulletinPage.html?curPage={}&fundCode={}&title=&starttime=&endtime=&type=204'
FLWJ='http://www.bocim.com/FundSite/actions/special/specialdeposit!fundBulletinPage.html?curPage={}&fundCode={}&title=&starttime=&endtime=&type=209'


class Handler(BaseHandler):
      
    crawl_config = {
        'timeout' : 300,
        'connect_timeout': 30,
        'validate_cert' : False,
        "headers": {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36'
        },
        "itag": 'v8.2',
        "retries": 10,
        "timeout": 300,
    }
    
    @every(minutes=24 * 60)
    def on_start(self):
        time.sleep(20)
        
        self.crawl(ROOTURL,callback=self.get_massage,itag = 'v1.3')
        for i in range(1,26):
            self.crawl('http://www.bocim.com/FundSite/actions/infor/informationsection!bulletinPage.html?curPage=%s&btype=102&title=&starttime=&endtime='%i,callback=self.get_massage1,itag = 'v2.1') 
             
        
    @config(age=1 * 24 * 60 * 60)
    def get_massage(self,response):
        time.sleep(20)
        content = response.content
            
        soup = BeautifulSoup(content,'html.parser',from_encoding='utf-8')
        items = soup.select('.fundName')
        for item in items:
            data = {}
            data['stock_code'] = item.select('a')[0].get('href')[-6:]
            data['stock_name'] = item.select('a')[0].get('title')
            
            
            for index in range(40):
                self.crawl(JJGG.format(index,data['stock_code']) ,save=data,callback=self.get_pdf,itag = 'v3')
            for index in range(10):
                self.crawl(JJJB.format(index,data['stock_code']) ,save=data,callback=self.get_pdf,itag = 'v2.7')            
                self.crawl(JJBNB.format(index,data['stock_code']) ,save=data,callback=self.get_pdf,itag = 'v2.5')
                self.crawl(JJNB.format(index,data['stock_code']) ,save=data,callback=self.get_pdf,itag = 'v2.6')            
                self.crawl(FLWJ.format(index,data['stock_code']) ,save=data,callback=self.get_pdf,itag = 'v2.1')
                
    @config(age=1 * 24 * 60 * 60)
    def get_massage1(self,response):
        time.sleep(21)
        content = response.content
          
        soup = BeautifulSoup(content,'html.parser',from_encoding='utf-8')
        items = soup.select('ul li')
        for item in items:
            data = {}
            data['stock_code'] = ''
            data['stock_name'] = ''
            data['type'] = u'投资有道'
            data['time'] = item.find('span').get_text()
            id_num = item.find('a')['href'].split('id=')[1]
            data['source_url'] = 'http://www.bocim.com/Channel/'+item.find('a')['href']
            file_url = 'http://www.bocim.com/FundSite/actions/infor/informationsection!getBulletincontext.html?id=%s'%id_num
            self.crawl(file_url,save=data,callback=self.get_pdf1,itag = 'v2')
    
    @config(age=1 * 24 * 60 * 60)
    @catch_status_code_error        
    def get_pdf1(self,response):
        time.sleep(10)
        data = response.save
        content = response.content
        soup = BeautifulSoup(content,'html.parser',from_encoding='utf-8')
        data['title'] = soup.find('div','tit').text
        try:
            data['file_url'] = 'http://www.bocim.com'+soup.find('a','xz')['href']
            data['oss_path'] = get_oss_path(data['file_url'])
            self.crawl(data['file_url'] ,save={'item': data},callback=self.upload_item,oss_path=data['oss_path'],itag = 'v2.3')
        except:
            None
    
    @config(age=1 * 24 * 60 * 60)
    @catch_status_code_error            
    def get_pdf(self,response):
        time.sleep(10)
        data = response.save
        content = response.content
        soup = BeautifulSoup(content,'html.parser',from_encoding='utf-8')
        
        stock_cod = response.save['stock_code']
        stock_name = response.save['stock_name']
        
        typenum = response.url[-3:]

            
        
        lies = soup.select('li')
        for li in lies:
            
            data = {}
            if typenum=='e=0':
                data['type'] = u'基金公告'
            if typenum=='206':
                data['type'] = u'基金季报'    
            if typenum=='205':
                data['type'] = u'基金半年报'    
            if typenum=='204':
                data['type'] = u'基金年报'    
            if typenum=='209':
                data['type'] = u'法律文件' 
                
            data['stock_code'] = stock_cod
            data['stock_name'] = stock_name
            data['time'] = li.select('span')[0].get_text()
            print data['time']
            data['title'] = li.select('a')[0].get_text()                    
            url ='http://www.bocim.com/Channel/'+li.select('a')[0].get('href')
            ff=re.findall('(.*?id=)', url)[0]
            
            data['source_url'] = 'http://www.bocim.com/Channel/'+li.find('a')['href']
            
            num = url.replace(ff,'')
            data['file_url'] = r'http://www.bocim.com/FundSite/actions/infor/informationsection!testDownload.action?id='+num  
            data['oss_path'] = get_oss_path(data['file_url'])
            self.crawl(data['file_url'] ,save={'item': data},callback=self.upload_item,oss_path=data['oss_path'],itag = 'v2.3')
     
    
    @catch_status_code_error   
    def handle_result(self, response):
        if response.status_code != 404:
            data1 = response.save['data1']
            soup = BeautifulSoup(requests.get(data1['file_url']).content, 'lxml')         
            item = response.save['item'] 
            content = str(soup.find('div',class_ = 'Section1')).replace(item['title'],'')
            item['oss_path'] = get_oss_path2(data1['file_url']) 
            item.update({
                'file_size':len(content)
            })
            if int(item['file_size']) != 4 :
                item['time'] = datetime.datetime.strptime(item['time'],"%Y-%m-%d")
                upload_html(item['oss_path'],content)
                item['timestamp'] = int(time.time() * 1000)
                item['src_id'] = 'ccbfund'+str(item['timestamp'])+str(random.randint(10,1000))
                rst = upload_html(item['oss_path'],content)
                if rst == 200:
                    item['downloaded'] = True
                    item =self.fill_data(item)
                    return item
                else:
                    print '失败'    
            
    @catch_status_code_error
    def upload_item(self, response):
        print response.status_code
        if response.status_code == 200:
            data = response.save['item']
            upload_result = response.save['upload_result']
            if int(upload_result['pdf_page_num']) > 0:
                if response.save['upload_result']['status'] == 200:
                    data['file_type'] = '.pdf'
                    data['file_size'] =len(requests.get(response.url).content)                      
                    data['downloaded'] = True
                    data['time'] = format_time(data['time'])
                    data =self.fill_data(data)
                    return data
                else:
                    raise Exception('failed to upload!')
            else:
                print response.url
                data['file_type'] = '.html'
                data['file_url'] = data['source_url']
                data1 = {}
                data1['file_url'] = response.url
                self.crawl(data['file_url'],save={'item':data,'data1':data1},callback=self.handle_result,itag = 'v1.5')

    def fill_data(self,data):
        
        data['plate'] = u'基金'
        data['storage'] = 'oss'
        data['org_id'] = ''
        data['timestamp'] = int(round(time.time()*1000))
        data['src_id'] = 'bocim'+str(data['timestamp'])+str(random.randint(10,1000))
        data['column'] = u'基金公告'
        data['export_flag'] = False
        data['export_version'] = 0
        data['category_id'] = ''

        return data
    
   
    
    
def format_time(time_str):
    time_str = time_str.replace('-','')
    return datetime.datetime.strptime(time_str,"%Y%m%d")

def get_file_size(response):
    return response.headers.get('Content-Length', '')



def get_oss_path(file_url):
    return 'bocim/{}.pdf'.format(uuid.uuid3(uuid.NAMESPACE_DNS, u'{}_{}.pdf'.format('yxpeng', file_url).encode('utf-8')))


def get_oss_path2(file_url):
    return 'bocim/{}.html'.format(uuid.uuid3(uuid.NAMESPACE_DNS, u'{}_{}.html'.format('master', file_url).encode('utf-8')))   

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


























