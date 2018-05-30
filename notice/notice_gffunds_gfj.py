#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2018-03-21 16:12:10
# Project: notice_gffunds_gfj

from pyspider.libs.base_handler import *
from pyspider.libs.proxy_helper import ProxyHelper
from bs4 import BeautifulSoup
import random
import requests
import uuid
import time 
import sys
import json
import oss2
import re
import datetime
reload(sys)
sys.setdefaultencoding('utf8')

urlcode='http://www.gffunds.com.cn/funds/?fundcode=%s'
url='http://www.gffunds.com.cn/funds/cpmp/allFunds.js'

class Handler(BaseHandler):
    crawl_config = {
        'itag' : 'v3.36'
    }

    @every(minutes=24 * 60)
    def on_start(self):
        time.sleep(6)
        self.crawl(url, callback=self.index_page)

    @config(age=1 * 24 * 60 * 60)
    def index_page(self, response):
        soup=BeautifulSoup(response.content,'html.parser')
        cc=str(soup).split('var funds = ')[1]
        for i in str(soup).split('=')[1].split(','):
            
            if 'fName' in i:
                codename=i.split('fName:')[1].strip("'").split("'")[1]
                d['codename']=codename
            
            if 'fCode' in i:
                d={}
                code=i.split('fCode:')[1].strip("'").split("'")[1]
                
                d['code']=code
            for i in (228364, 200445, 236293, 207589):
                url ='http://www.gffunds.com.cn/was5/web/search?channelid=%s&page=0&searchword={}'.format(str(code))%str(i) 
                d['url']=url
                

                self.crawl(url,save=d,callback=self.detail_page,itag = 'v9')
                
                
    @config(age=1 * 24 * 60 * 60)            
    @catch_status_code_error
    def detail_page(self,response):
        time.sleep(6)
        d=response.save
        
        soup=BeautifulSoup(response.content,'html.parser')
        num=str(soup).split('"RECORDNUM":')[1].split(',"RECORDS"')[0].strip('"')
        if (int(num)%8) !=0:
            pagenum = int(num) / 8 + 1
        else:
            pagenum = int(num) / 8
        print pagenum

        for i in range(1,pagenum+1):
            allurl=response.url.split('page=0')[0]+'page=%s'%i+response.url.split('page=0')[1]
            
            channelid=allurl.split('id=')[1].split('&page')[0]
            if channelid=='200445':
                d['type']='法律文件'
            if channelid=='228364':
                d['type']='重大事件'
            if channelid=='236293':
                d['type']='定期报告'
            if channelid=='207589':
                d['type']='临时文件'
            self.crawl(allurl,save=d,callback=self.page_page)
            
            
            
            
            
    @config(age=1 * 24 * 60 * 60)        
    @catch_status_code_error
    def page_page(self,response):
        time.sleep(6)
        
        soup=BeautifulSoup(response.content,'html.parser')
        c=str(soup).split('"RECORDS":')[1]
        
        for i in re.findall('{(.*?)}',c)[1:]:
            data={}
            a = json.loads('{'+i+'}')
            
            
            url=a['DOCPUBURL']
            title=a['DOCTITLE']
            tim=a['DOCRELTIME']
            #tim=2015.10.09 09:15:00 dt =2015-10-09 09:15:00
            data['type']=response.save['type']
            data['time']=tim
            data['org_id']=''
            data['stock_code']=response.save['code']
            data['stock_name']=response.save['codename']
            data['column']='基金公告'
            data['plate']='基金'
            data['timestamp'] = int(time.time() * 1000)
            data['src_id'] = 'gffunds'+str(data['timestamp'])+str(random.randint(10,1000))
            data['title']=title
            data['file_url']=a['DOCPUBURL']  
            try:
                if 'df' in url[-2:]:
                    data['source_url'] = response.url
                    data['oss_path'] = get_oss_path(url)
                    data['file_type']='.pdf'
                    self.crawl(url,save={'item': data},callback=self.upload_item,oss_path=data['oss_path'])
                if 'c' in url[-1]:
                    data['source_url'] = response.url
                    data['oss_path'] = get_oss_path2(url)
                    data['file_type']='.doc'
                    self.crawl(url,save={'item': data},callback=self.upload_item,oss_path=data['oss_path'])
                if 'x' in url[-1]:
                    data['source_url'] = response.url
                    data['oss_path'] = get_oss_path3(url)
                    data['file_type']='.docx'

                    self.crawl(url,save={'item': data},callback=self.upload_item,oss_path=data['oss_path'])

                if '.shtml' in url:

                    self.crawl(url,save={'item': data},callback=self.handle_result) 
            except:
                None
                
    @catch_status_code_error                    
    def handle_result(self, response):       
         if response.status_code != 404:   
            soup = BeautifulSoup(response.content, 'lxml')
            try:                  
                content=re.sub('<a(.*?)</a>','',str(soup.find('div',id= 'new_hzcon'))).replace('附件一：','').replace('附件二：','').replace('附件三：','').replace('附件：','')                
            except:
                content = str(soup.find('div',id= 'new_hzcon')).replace('width="100%','width=50%').replace('height: 13.5pt','height: 5.5pt')
            item = response.save['item'] 
            item['oss_path'] = get_oss_path4(item['file_url']) 
            item.update({
                'file_size':len(content)
            })
            if int(item['file_size']) != 4 :
                item['time'] = datetime.datetime.strptime(item['time'],"%Y.%m.%d %H:%M:%S")
                upload_html(item['oss_path'],content)
                item['timestamp'] = int(time.time() * 1000)
                item['src_id'] = 'gffunds'+str(item['timestamp'])+str(random.randint(10,1000))
                rst = upload_html(item['oss_path'],content)
                if rst == 200:
                    item['downloaded'] = True
                    item['source_url'] = response.url 
                    item['file_type'] = '.shtml' 
                    item['org_id'] = '' 
                    item['storage'] = "oss"
                    item['column'] = '基金公告'
                    item['export_flag'] = False
                    item['export_version'] = 0
                    item['category_id'] = ''
                    return item
                else:
                    print '失败'  
                
    @catch_status_code_error            
    def upload_item(self, response):
        if response.status_code == 200:
            # 上传已下载的文件.
            if response.save and 'item' in response.save and 'upload_result' in response.save:
                data = response.save['item']
                upload_result = response.save['upload_result']
                if upload_result['status'] == 200:
                    data['downloaded'] = True
                    data['category_id']=''
                    data['export_version']=0
                    data['export_flag']= False
                    data['timestamp'] = int(time.time() * 1000)
                    data['src_id'] = 'gffunds'+str(data['timestamp'])+str(random.randint(10,1000))
                    data['time'] = datetime.datetime.strptime(data['time'],"%Y.%m.%d %H:%M:%S")
                    data['org_id']=''
                    data['storage']='oss'
                    try:
                        data['file_size'] =int(get_file_size(response))
                    except:
                        data['file_size'] = 0

                    return data
                else:
                    raise Exception('failed to upload!')
            else:
                raise Exception('failed to upload!!, nothing in response.save')  

def get_file_size(response):
    return response.headers.get('Content-Length', '')

def get_oss_path(file_url):
    return 'gffunds/{}.pdf'.format(uuid.uuid3(uuid.NAMESPACE_DNS, u'{}_{}.pdf'.format('yxpeng', file_url).encode('utf-8')))  
def get_oss_path2(file_url):
    return 'gffunds/{}.doc'.format(uuid.uuid3(uuid.NAMESPACE_DNS, u'{}_{}.doc'.format('yxpeng', file_url).encode('utf-8'))) 
def get_oss_path3(file_url):
    return 'gffunds/{}.docx'.format(uuid.uuid3(uuid.NAMESPACE_DNS, u'{}_{}.docx'.format('yxpeng', file_url).encode('utf-8'))) 
                
def get_oss_path4(file_url):
    return 'gffunds/{}.shtml'.format(uuid.uuid3(uuid.NAMESPACE_DNS, u'{}_{}.shtml'.format('master', file_url).encode('utf-8')))   

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
                        
                
                
                
                
   