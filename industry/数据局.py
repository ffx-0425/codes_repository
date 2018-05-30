#! /usr/bin/env python
# -*- coding: utf-8 -*-

from Research_basis import Research_basis
from base.Tool import *
from bs4 import BeautifulSoup
import datetime
import re

class www_shujuju_cn(Research_basis):
    """
    毕马威 研报爬虫
    """
    #数据库表名
    tabname = "industry.report.shujuju"
    #开发者邮箱
    dev_mail = "hwang@abcft.com"
    #源名称
    source = "数据局"
    #报告所属行业
    report_industry = "行业分析"
    #站点host
    sitename = "http://www_shujuju_cn/"
    #是否免费
    payment_type = "免费"
    market = "一级市场"

    def _login(self, uk, pic_str):
        Tool = net_Tool()
        self.logger.info("验证码正确")
        password = Tool.RSA(key_bytes=uk, message="123456")
        data = {
            "username": "13264616071",
            "password": "",
            "code": pic_str,
            "password_2": password
        }
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Host ': 'www.shujuju.cn',
            'Upgrade-Insecure-Requests': '1',
            "Referer": "networks://www.shujuju.cn/login",
            "X-Requested-With": "XMLHttpRequest",
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0'
        }
        loginr = self.http.send_http(method="post", url="http://www.shujuju.cn/login/execute", data=data, headers=headers)

        if loginr.status_code == 200:
            self.logger.info("登陆成功")
            return True
        else:
            self.logger.error("登陆失败")
            return False

    def login(self):
        """
        登陆数据局
        :return:
        """
        loginurl = "http://www.shujuju.cn/login"
        headers = self.http.default_headers
        self.logger.info("request login page ....")
        r = self.http.send_http(method="get", url=loginurl, headers=headers)
        soup = BeautifulSoup(r.text, 'html5lib', from_encoding='utf8')
        if not r.status_code == 200:
            return False
        uk = bytes(soup.find(id="uk").get("value"))
        self.logger.info("request Verification code image")
        imgdata = self.http.send_http(method="get", url="http://www.shujuju.cn/code?type=login", headers=headers)
        self.logger.info("提取验证码")
        captcha = self.Verification(imgdata=imgdata.content, code=1902)
        if not captcha.get("pic_str"):
            self.VerificationError(captcha.get("pic_str"))
            self.logger.info("验证码错误")
            return False
        data = {
            "code": captcha.get("pic_str")
        }
        self.logger.info("提交验证码")
        validator = self.http.send_http(method="post", url="http://www.shujuju.cn/validator/module/1/3",
                                        data=data, headers=headers)
        if "success" in validator.text:
            return self._login(uk=uk, pic_str=captcha.get("pic_str"))
        else:
            self.VerificationError(captcha.get("im_id"))
            self.logger.info("验证码错误")
            return False

    def getpdfdata(self, file_url):
        download_headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q = 0.8",
            "Accept-Encoding": "gzip,deflate",
            "Accept-Language": "zh-CN,zh;q = 0.8",
            "Connection": "keep-alive",
            "Host": "www.shujuju.cn",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36",
        }
        f = self.http.send_http(method="get", url=file_url, isproxie=False, headers=download_headers)
        return f.content

    def content(self, page):
        headers = self.http.default_headers
        url = "http://www.shujuju.cn/lecture/browe?page="+str(page)
        self.logger.info("当前页面"+ str(url))
        r = self.http.send_http(method="get", url=url, headers=headers)
        soup = BeautifulSoup(r.text, 'html5lib', from_encoding='utf8')
        articles = soup.find_all(class_="small-info clearfix")
        for i in articles:
            report_img = i.find(class_="information-small-img").find("img").get("src")
            file_name = i.find(class_="textdescription-small-info").find("h3").string
            source_url = "http://www.shujuju.cn"+i.find(class_="textdescription-small-info").find("h3").find("a").get("href")
            detail = self.http.send_http(method="get", url=source_url)
            detailsoup = BeautifulSoup(detail.text, 'html5lib', from_encoding='utf8')
            _time = re.search(".*?(\d{4}-\d{2}-\d{2}).*?", detailsoup.find(class_="report-time").string).group(1)
            time = datetime.datetime.strptime(_time, "%Y-%m-%d")
            try:
                file_url = "http://www.shujuju.cn" + detailsoup.find(class_="embed-link").get("href")
            except Exception:
                continue
            file_type = "pdf"
            file_data = self.getpdfdata(file_url=file_url)
            if file_data:
                self.save(file_data=file_data, file_url=file_url, source_url=source_url, file_name=file_name,
                          file_type=file_type, time=time, report_img=report_img)

    def runall(self):
        """
        全量入口
        :return:
        """
        while True:
            if self.login():
                for i in range(8, 322):
                    self.content(page=i)
                break

    def runadd(self):
        """
        全量入口
        :return:
        """
        if self.login():
            for i in range(1, 5):
                self.content(page=i)
if __name__ == '__main__':
    app = www_shujuju_cn()
    app.runall()
