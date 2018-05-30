#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2018-03-27 12:30:36
# Project: notice_chinaamc_hx

from bs4 import BeautifulSoup
from pyspider.libs.base_handler import *
from pyspider.libs.proxy_helper import ProxyHelper
import time
import random
import uuid
from urlparse import urljoin
import sys
import oss2
import re
import datetime
reload(sys)
sys.setdefaultencoding('utf8')
import requests
head='http://fund.chinaamc.com'
urljs='http://www.chinaamc.com/indexfundvalue.js'
url='http://www.chinaamc.com/fund/%s/index.shtml'
urlpage='http://fund.chinaamc.com/portal/cn/ggjjzcms/jjggList.jsp?fundcode=%s'
ubc='http://www.chinaamc.com/fund/510330/index.shtml'


#验证大小
env = "offline"    
ALI_DEST_OSS_CONFIG = {
        'key': 'LTAITN0hCn7KBUzK',
        'secret': 'c8SOHjg15bkkW3AxQmbDyyDQA8fnNI',
        'bucket': 'abc-crawler',
        'endpoint': 'oss-cn-hangzhou-internal.aliyuncs.com' if env == "online" else 'oss-cn-hangzhou.aliyuncs.com'}
ali_source_bucket = oss2.Bucket(oss2.Auth(ALI_DEST_OSS_CONFIG['key'],ALI_DEST_OSS_CONFIG['secret']),ALI_DEST_OSS_CONFIG['endpoint'],ALI_DEST_OSS_CONFIG['bucket'])

class Handler(BaseHandler):
    crawl_config = {
        'itag' : 'v9.6',
         'timeout' : 10000,
         'connect_timeout': 9000,
         'validate_cert' : False
    }
      
    @every(minutes=24 * 60)
    def on_start(self):
        time.sleep(10)
        self.crawl(urljs,callback=self.index_page)

    @config(age=1 * 24 * 60 * 60)
    def index_page(self, response):
        time.sleep(5)
        soup=BeautifulSoup(response.content,'html.parser')
        self.crawl(ubc,save=ubc,callback=self.detail_page)
        for i in range(1,7):

            code=str(soup).split('=')[i][1:7]    
            u=url%code.replace('\r\n','')
            self.crawl(u,save=url%code,callback=self.detail_page ,itag = 'v8')
        for i in re.findall('noturl#(.*?)华', str(soup)):
            code=i.strip(':')
            
            u=url%code.replace('\r\n','')
            self.crawl(u,save=url%code,callback=self.detail_page , itag = 'v7')
            
    @config(age=1 * 24 * 60 * 60)            
    def detail_page(self, response): 
        time.sleep(10)
        r = requests.get(response.url)
        code = r.status_code
        if code != 200:
            print "无此网页"
        else:
            #u=response.save
            print response.url,'response'
            shuju={}
            soup=BeautifulSoup(response.content,'html.parser')
            name=str(soup.find('span',class_='text').text.split('(')[0]).replace('\n','')

            code=soup.find('span',class_='text').text.split('(')[1].split(')')[0]
            shuju['name']=name
            shuju['code']=code
            shuju['url']=urlpage%code
            #基金type  shuju['type']=soup.find('span',class_='vam').text
            print urlpage%code      
            self.crawl(urlpage%code,save=shuju,callback=self.getpage_page)
    
    @config(age=1 * 24 * 60 * 60)
    @catch_status_code_error
    def getpage_page(self,response):
        time.sleep(10)
        print response.save
        shuju=response.save
        soup=BeautifulSoup(response.content,'html.parser')
        
        pagenum=int(soup.find('p').find_all('strong')[-1].text)
        for i in range(0,pagenum):
            
            ulist=shuju['url']+'&index={}&keywords='.format(str(i))
            print ulist,'ulist'
            self.crawl(ulist,save=shuju,callback=self.geturl_page)
            
            
    @config(age=1 * 24 * 60 * 60)        
    def geturl_page(self,response):
        time.sleep(10)
        time.sleep(5)
        shuju=response.save
        soup=BeautifulSoup(response.content,'html.parser')
        for i in soup.find('ul').find_all('a'):                   
            self.crawl(i.get('href'),save=shuju,callback=self.getpdf_page)
    
    
    @config(age=1 * 24 * 60 * 60)
    @catch_status_code_error
    def getpdf_page(self,response):
        time.sleep(5)       
        shuju=response.save
        soup=BeautifulSoup(response.content,'html.parser')
        try:
            tim=soup.find('em',class_='gray2 p_r30').text.split('：')[1]
            urls1=soup.find(id='page_detail').find_all('a') 
            for url1 in urls1:
                title=url1.text.split('.')[0]
                tex=url1.text
                data={}
                url = url1.get('href')
                if 'http' in url:
                    url=url1.get('href')  
                else:
                    url=head+url1.get('href') 
                
                data['org_id']=''
                data['time']=tim
                
                data['stock_code']=shuju['code']
                data['stock_name']=shuju['name'].replace('\n','')
                data['type']='公告'
                data['column']='基金公告'
                data['plate']='基金'
               
                data['title']=title
                data['source_url'] = response.url
                if 'pdf' in tex:
                    data['file_url']  = url
                    data['file_type']='.pdf'
                    data['oss_path'] = get_oss_path(url)
                    self.crawl(data['file_url'],save={'item': data},callback=self.upload_item,oss_path=data['oss_path'])
                   
                if 'doc' in tex: 
                    data['file_type']='.doc'
                    file_url = url
                    self.crawl(file_url,save={'item': data},callback=self.doc_item,allow_redirects=False)
                
        except:
            pass
        
        
    @catch_status_code_error
    def doc_item(self,response):    
        data=response.save['item']
        soup=BeautifulSoup(response.content,'html.parser')
        try:
            data['file_url'] = soup.find('a')['href']
            data['oss_path'] = get_oss_path2(data['file_url'])
            self.crawl(data['file_url'],save={'item': data},callback=self.upload_item,oss_path=data['oss_path'],itag = 'v5.0')
        except:
            data['file_url'] = response.url
            data['oss_path'] = get_oss_path2(data['file_url'])
            self.crawl(data['file_url'],save={'item': data},callback=self.upload_item,oss_path=data['oss_path'],itag = 'v9.0')
        
    def upload_item(self, response):
        if response.status_code == 200:
            
            oss_url = response.save['item']['oss_path']          
            oss_size = int(ali_source_bucket.head_object(oss_url).content_length)
            if oss_size > 30000 :
                print oss_size

                if response.save and 'item' in response.save and 'upload_result' in response.save:
                    oss_url = urljoin('http://abc-crawler.oss-cn-hangzhou.aliyuncs.com/',response.save['item']['oss_path'])
                    if str(requests.get(oss_url).headers.get('Content-Type', '')) == 'application/x-empty':
                        None
                    else:
                        data = response.save['item']
                        upload_result = response.save['upload_result']

                        if upload_result['status'] == 200:
                            data['downloaded'] = True
                            data['timestamp'] = int(time.time() * 1000)
                            data['src_id'] = 'chinaamc'+str(data['timestamp'])+str(random.randint(10,1000))
                            data['category_id']=''
                            data['export_version'] = 0
                            data['export_flag'] = False
                            data['org_id']=''
                            data['storage']='oss'
                            data['file_size'] =len(requests.get(response.url).content)  
                            data['time'] = format_time(data['time'])
                            return data
                        else:
                            raise Exception('failed to upload!')
                else:
                    raise Exception('failed to upload!!, nothing in response.save')
    
def format_time(time_str):
    #time_str = time_str.replace('-','')
    return datetime.datetime.strptime(time_str,"%Y-%m-%d")



def get_oss_path(file_url):
    return 'chinaamc/{}.pdf'.format(uuid.uuid3(uuid.NAMESPACE_DNS, u'{}_{}.pdf'.format('yxpeng', file_url).encode('utf-8')))       
def get_oss_path2(file_url):
    return 'chinaamc/{}.doc'.format(uuid.uuid3(uuid.NAMESPACE_DNS, u'{}_{}.doc'.format('yxpeng', file_url).encode('utf-8')))  
            
            
def get_file_size(response):
    return response.headers.get('Content-Length', '')          
            
            
            
            
            
            
            
            
            
            
            
            
        
        
        
        
        
        


        
        
        

          
            
            
            
            
            
            
            
        
        
        
        
        
        


        
        
        

