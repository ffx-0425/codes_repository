#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2018-02-01 14:55:55
# Project: notice_jxfunds


from pyspider.libs.base_handler import *
from pyspider.libs.proxy_helper import ProxyHelper
from bs4 import BeautifulSoup
import random
import requests
import uuid
import time 
import json
import re
from urlparse import urljoin
import datetime

start_url='http://www.jxfunds.com.cn/front/index_10022.jhtml'

pdf_url='http://www.jxfunds.com.cn/front/getFundNoticeByPage_%s.jhtml?pageVo.pageNo=0'
pdf2_url='http://www.jxfunds.com.cn/front/getFundLegalByPage_%s.jhtml?pageVo.pageNo=0'
class Handler(BaseHandler):
    crawl_config = {
        #"itag": 'v1'
        #'proxy': random.choice(ProxyHelper().get_proxies()),
        'validate_cert' : False,
        'connect_timeout': 3000,#单位秒
        "timeout": 3000,#默认值：120
    }

    @every(minutes=24 * 60)
    def on_start(self):
        self.crawl(start_url, callback=self.index_page)

    @config(age=24 * 60)
    def index_page(self, response):
        soup=BeautifulSoup(response.content,'html.parser')
        for n,i in enumerate(soup.select('tr a')[1:]):
            if n%2==0:
                data={}
                u=changeurl(i['href']).split('Detail_')[1].split('.')[0]
                url=pdf_url%u
                url2=pdf2_url%u
                name=i.text.strip()
                code=i['href'].split('_')[-1].split('.')[0]
                data['stock_name']=name
                data['stock_code']=code
                
                self.crawl(url,save=data,callback=self.detail_page)
                self.crawl(url2,save=data,callback=self.detail_page)
    @config(age=24 * 60)
    def detail_page(self, response):
        url=response.url.replace('pageNo=0','pageNo={}')
        pagenum=int(response.json['paginationStr'].split('共<span>')[1].split('</span>')[0])
        for i in range(1,pagenum+1):
            data=response.save.copy()
            self.crawl(url.format(str(i)),save=data,callback=self.page_page)
    #                               可固定  fundNoticeId   
    #http://www.jxfunds.com.cn/front/fundNoticeDetail_100238_11131.jhtml
    @config(age=24 * 60)
    def page_page(self, response):
        for i in range(0,10):
            data=response.save.copy()
            try:
                js=response.json['list'][i]
                fundNoticeId=js['fundNoticeId']
                data['title']=js['title']
                data['time']= js['releaseDate']
                if '1' in js['noticeType']:
                    data['type']='基金公告' 
                if '2' in js['noticeType']:
                    data['type']='法律文件'    
                url='http://www.jxfunds.com.cn/front/fundNoticeDetail_100238_%s.jhtml'%fundNoticeId
                self.crawl(url,save=data,callback=self.dwon_page)
            except:
                pass
    
    @config(age=24 * 60)
    def dwon_page(self, response):
        soup=BeautifulSoup(response.content,'html.parser')
        data=response.save.copy()
        url=changeurl(soup.select('.download-content a')[0]['href'])
        data['file_url'] =url
        data['oss_path'] = get_oss_path(url)
        data['source_url']=response.url
        self.crawl(url,save={'item': data},callback=self.upload_item,oss_path=data['oss_path'])     
    @config(age=24 * 60)    
    def upload_item(self, response):
        # 上传已下载的文件.
        if response.save and 'item' in response.save and 'upload_result' in response.save:
            data = response.save['item']
            upload_result = response.save['upload_result']
            if upload_result['status'] == 200:
                #时间格式在最后转 之前转会传成str
                data['time']=format_time(data['time'])
                #基金名称+时间戳+999随机数                
                data['src_id'] ='fund_'+data['file_url'].split('.')[1]+str(int(round(time.time()*100)))+str(random.randint(0,999))
                data['file_size'] =len(requests.get(response.url).content)
                data['downloaded']=True
                data=self.fill_data(data)
                return data
            else:
                raise Exception('failed to upload!')
        else:
            raise Exception('failed to upload!!, nothing in response.save')     
        
    def fill_data(self,data):
        data['file_type'] = '.pdf'
        data['plate'] = u'基金'
        data['storage'] = 'oss'
        data['org_id'] = ''
        data['timestamp'] = int(round(time.time()*1000))
        data['column'] = u'基金公告'
        data['export_flag'] = False
        data['export_version'] = 0
        data['category_id'] = ''

        return data  
    
def changeurl(url):
    #依赖 start_url   最好带'http://' 
    head='http://'+start_url.split('/')[2]
    if head in url:
        return url
    else:
        if url.startswith('/'):
            return '%s%s' %(head,url)
         #片段url不以/开头 添加head+/
        else:
            if url.startswith('http'):
                body=url.replace('http//','').replace('http://','')
                #http//192.168.1.113/main/qxjj/001040/jjzy.shtml
                if '1' in body:
                    
                    return head+body.replace(body.split('/')[0],'')
                else:
                    #http://data/201407010853572014
                    return head+body.replace(body.split('/')[0],'')
            return head+'/'+url
    
def format_time(time_str):    
    try:
        return datetime.datetime.strptime(time_str,"%Y-%m-%d")    
    except:
        return datetime.datetime.strptime(time_str,"%Y-%m-%d %H:%M:%S")
def get_file_size(response):
    return response.headers.get('Content-Length', '')

def get_oss_path(file_url):
    if file_url.endswith('pdf'):
        return 'jxfunds/{}.pdf'.format(uuid.uuid3(uuid.NAMESPACE_DNS, u'{}_{}.pdf'.format('yxpeng', file_url).encode('utf-8')))           
    elif file_url.endswith('doc'):
        return 'jxfunds/{}.doc'.format(uuid.uuid3(uuid.NAMESPACE_DNS, u'{}_{}.doc'.format('yxpeng', file_url).encode('utf-8')))  