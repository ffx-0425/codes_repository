# -*- coding:utf-8 -*-
from spiders.app.BaseApp import BaseApp
from spiders.config import config
from spiders.Tools.uploader import Uploader
from spiders.Tools.MySQLTool import MySQLTool
from datetime import datetime, timedelta
from urllib import urlencode
from urlparse import urljoin
from bs4 import BeautifulSoup
import requests
import logging
import random
import uuid
import time
import sys
import re
import os
import json

reload(sys)
sys.setdefaultencoding("utf-8")

# 通过此处定义是否为全量更新
all_update = True


# INSERT INTO `spiders_task`(`app_name`, `interval`, `start`, `max_time`) VALUES ('QuanSpider', 43200000, 1526451579, 259200000);
class QuanSpider(object, BaseApp):
    table_name = "notice_roadshow"
    base_url = "http://rs.p5w.net"

    def __init__(self):
        super(QuanSpider, self).__init__()
        self.mysql_tool = MySQLTool(**config["MYSQL_SPIDER_WATCHER"])
        self.task_table = "spiders_task"
        self.response = self.http_tool(use_proxy=True, sleep_time=0.1)
        self.uploader_tool = Uploader()
        self.logger = logging.getLogger("QuanSpider")
        self.ali_conn = self.connect_oss()

    def run(self, update_pid=False):
        self.mongodb = self.mongodb_tool(db_choice="original_data")
        logging.info("update_pid:{}".format(update_pid))
        update_sql = "update {} set pid=%s, start=%s, end=%s where app_name=%s".format(self.task_table)
        logging.debug("update_sql:{},pid:{}".format(update_sql, os.getpid()))
        update_sql_result = self.mysql_tool.exec_mysql(update_sql, [os.getpid(), int(time.time()), None, "QuanSpider"])
        logging.debug("update_sql_result:{}".format(update_sql_result))
        self.mysql_tool.exec_mysql(update_sql, [os.getpid(), int(time.time()), None, "QuanSpider"])
        self.mysql_tool.mysqlconn.close()
        rs_type_category = {
            4: ["IPO路演", "S016001"],
            22: ["可转债路演", "S016001"],
            2: ["摇号抽签", "S016001"],
            9: ["上市仪式", "S016001"],
            5: ["业绩说明会", "S016002"],
            3: ["集体接待日", "S016002"],
            12: ["投资者说明会", "S016002"],
            14: ["资产重组说明会", "S016002"],
            8: ["公开致歉会", "S016002"],
            6: ["上市公司再融资路演", "S016001"],
            15: ["股东大会", "S016002"],
            7: ["项目路演", "S016001"],
            32: ["新财富分析师路演", "S016001"],
            13: ["财经节目", "S016003"],
            1: ["论坛峰会", "S016003"],
            16: ["人物访谈", "S016003"],
            17: ["发布会", "S016002"],
            18: ["培训会", "S016002"],
            10: ["投资者回馈", "S016002"],
            11: ["其他路演", "S016001"]
        }
        rs_types = [4, 22, 2, 9, 5, 3, 12, 14, 8, 6, 15, 7, 32, 13, 1, 16, 17, 18, 10, 11]
        for r in rs_types:
            last_file_url = self.mongodb.select_field(tablename=self.table_name,
                                                      find={"category_id": rs_type_category[r][1],
                                                            "time": datetime.now() - timedelta(days=3)},
                                                      select_field={"file_url": 1,
                                                                    "_id": 0}).sort("time", -1)
            last_file_url = list(last_file_url)
            if last_file_url:
                last_file_url = last_file_url[0]["file_url"]
            else:
                last_file_url = None
            self.spider_list(r, last_file_url)

    @staticmethod
    def clip_html(conference_detail, people_basic, collect_qa, conference_list=None):
        """
        :param conference_list:
        :param conference_detail: 活动介绍内容
        :param people_basic: 人物信息表内容
        :param collect_qa: 互动交流内容
        :return:
        """
        html = """<html>
                        <head>
                        <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
                        <meta http-equiv="X-UA-Compatible" content="IE=edge,chrome=1">
                        <meta http-equiv="Pragma" content="no-cache">  
                        <meta http-equiv="Cache-Control" content="no-cache">
                        <meta http-equiv="Expires" content="0" />
                        </head>
                            <body style="">
                                <div>
                                    <h2><b>活动介绍：</b></h2>
                                        {}
                                </div>
                                
                                <div>
                                    <h2><b>活动议程：</b></h2>
                                        {}
                                </div>

                                <div class="people_basic_info">
                                    <div class=""><span class=""><h2><b>嘉宾介绍</b></h2></span> </div>
                                        <ul id="people_data">
                                            {}
                                        </ul>
                                </div>

                                <div id="collect_aq">
                                    <div class=""><span class=""><h2><b>互动交流</b></h2></span> </div>
                                    <ul>
                                        {}
                                    </ul>
                                </div>
                            </body>
                    </html>
                """
        conference_detail = conference_detail.replace("\r\n", "").replace("\r", "").replace("\n", "") if conference_detail else "<li>暂无活动内容详情</li>"
        conference_list = "\n".join(conference_list) if conference_list else "<li>暂无活动议程</li>"
        people_basic_content = "\n".join(people_basic) if people_basic else "<li>暂无嘉宾参与</li>"
        collect_qa_content = ""
        for qa in collect_qa:
            for q in qa.values():
                collect_qa_content += "{}\n".format(q)
        collect_qa_content_result = collect_qa_content if collect_qa_content else "<li>暂无交流问答</li>"
        return html.format(conference_detail, conference_list, people_basic_content, collect_qa_content_result)

    def parse_id(self, r, **kwargs):
        """
        解析网页中的roadshowId
        :param r:
        :return:
        """
        if isinstance(r, requests.Response):
            res = r
        elif isinstance(r, (str, unicode)):
            res = self.response.get_response(method="get", url=r)
            if not res:
                return None
        else:
            return None
        soup = BeautifulSoup(res.content, "lxml")
        # show_id = soup.find("input", attrs={"type": "hidden", "id": "roadshowId"})
        show_id = soup.find("input", attrs=kwargs)
        return show_id.get("value")

    def get_collect_qa(self, show_id):
        url = "http://rs.p5w.net/roadshowLive/getCollectQA.shtml"
        data = {
            "roadshowId": show_id,
            "flag": "qa"
        }
        res = self.response.get_response("post", url, data=data)
        if not res:
            return None
        res = json.loads(res.content)
        all_aq = []
        for r in res.get("rows", list()):
            question = '<li class="chat_1" style="display: list-item;"><span class="ask_people">{}<i><b> {} </b></i> {}<span class="chat_time">{}</span></span><div class="chat_content"><p>{}</p></div></li>'.format(
                r.get("speakUserName", ""),
                "问" if r.get("questionGuestName") else "",
                r.get("questionGuestName") if r.get("questionGuestName") else "",
                r.get("speakTime") if r.get("speakTime") else "",
                r.get("speakContent") if r.get("speakContent") else "")
            if r.get("replyList", [dict()])[0].get("questionGuestName"):
                reply = '<li class="chat_2 clearfix" style="display: list-item;"><span class="ask_people">{}<span class="chat_time"> {} </span></span><div class="chat_content"><p>{}</p></div></li>'.format(
                    r.get("replyList", [dict()])[0].get("questionGuestName"),
                    r.get("replyList", [dict()])[0].get("speakTime"),
                    r.get("replyList", [dict()])[0].get("speakContent"))
                all_aq.append({"question": question, "reply": reply})
            else:
                all_aq.append({"question": question})
        return all_aq

    def get_collect_qa_next(self, show_id):
        url = "http://rs.p5w.net/roadshowLive/getNInteractionDatas.shtml"
        all_aq = []
        page = 0
        while True:
            logging.debug("请求qa页数:{}".format(page))
            data = {
                "roadshowId": show_id,
                "isPagination": 1,
                "type": 1,
                "page": page,
                "rows": 10
            }
            res = self.response.get_response("post", url, data=data)
            if not res:
                break
            res = json.loads(res.content)
            if not res.get("rows", list()):
                break
            for r in res.get("rows", list()):
                question = '<li class="chat_1" style="display: list-item;"><span class="ask_people">{}<i><b> {} </b></i> {}<span class="chat_time">{}</span></span><div class="chat_content"><p>{}</p></div></li>'.format(
                    r.get("speakUserName", ""),
                    "问" if r.get("questionGuestName") else "",
                    r.get("questionGuestName") if r.get("questionGuestName") else "",
                    r.get("speakTime") if r.get("speakTime") else "",
                    r.get("speakContent") if r.get("speakContent") else "")
                if r.get("replyList", [dict()])[0].get("questionGuestName"):
                    reply = '<li class="chat_2 clearfix" style="display: list-item;"><span class="ask_people">{}<span class="chat_time"> {} </span></span><div class="chat_content"><p>{}</p></div></li>'.format(
                        r.get("replyList", [dict()])[0].get("questionGuestName"),
                        r.get("replyList", [dict()])[0].get("speakTime"),
                        r.get("replyList", [dict()])[0].get("speakContent"))
                    all_aq.append({"question": question, "reply": reply})
                else:
                    all_aq.append({"question": question})
            page += 1
        return all_aq

    def get_collect_qa_sec(self, base_url):

        all_aq = []
        page = 1
        while True:
            data = {
                "pageNo": page,
                "now": int(time.time() * 1000)
            }
            url = base_url + "/bbs/question_page.asp?" + urlencode(data)
            logging.debug("url:{},请求qa页数:{}".format(url, page))
            response = self.response.get_response("post", url, data=data, encoding="gbk")

            if not response:
                break
            res = BeautifulSoup(response.text, "lxml")
            rows = res.find_all("q_and_r")
            if not rows:
                break
            for row in rows:
                question = '<li class="chat_1" style="display: list-item;"><span class="ask_people">{}<i><b> {} </b></i> {}<span class="chat_time">{}</span></span><div class="chat_content"><p>{}</p></div></li>'
                try:
                    if len(row.contents[0].contents) == 4:
                        question = question.format(row.contents[0].contents[0], "", "", "", row.contents[0].contents[2])
                    elif len(row.contents[0].contents) == 5:
                        question = question.format(row.contents[0].contents[0], "问", row.contents[0].contents[4], "",
                                                   row.contents[0].contents[1])
                except Exception as e:
                    self.logger.error("获得question失败,base_url:{},msg:{}".format(base_url, e))
                    continue

                if len(row.contents) == 2:
                    reply = '<li class="chat_2 clearfix" style="display: list-item;"><span class="ask_people">{}<span class="chat_time"> {} </span></span><div class="chat_content"><p>{}</p></div></li>'
                    try:
                        reply = reply.format(row.contents[1].contents[1], "", row.contents[1].contents[2])
                        all_aq.append({"question": question, "reply": reply})
                    except Exception as e:
                        self.logger.error("获取reply失败,base_url:{},msg:{}".format(base_url, e))
                        all_aq.append({"question": question})
                all_aq.append({"question": question})
            re_pattern = re.compile("<q_page>(\d+)</q_page>")
            page_result = re_pattern.findall(response.text)
            if page_result and int(page_result[0]) <= page:
                break
            page += 1
        return all_aq

    def get_conference_detail(self, r):
        """
        通过给到url或者网页内容获取活动介绍内容
        :param r:
        :return:
        """
        if isinstance(r, requests.Response):
            res = r
        elif isinstance(r, (str, unicode)):
            res = self.response.get_response(method="get", url=r)
            if not res:
                return None
        else:
            return None
        soup = BeautifulSoup(res.content, "lxml")
        detail_0 = soup.find("div", class_="hdjs clearfix scrollpublic")
        detail_1 = soup.find("div", id="hd_js")
        detail = detail_0 if detail_0 else detail_1
        if detail:
            return str(detail).replace("<b>活动介绍：</b>", "")
        detail_2 = soup.find("div", class_="bar aboutgs")
        if detail_2:
            try:
                return detail_2.get_text()
            except Exception as e:
                self.logger.warning("fail get conference detail,msg:{}".format(e))

    def get_stock_code(self, r):
        """
        通过给到url或者网页内容获取活动公司的stock_code和stock_name
        :param r:
        :return:
        """
        if isinstance(r, requests.Response):
            res = r
        elif isinstance(r, (str, unicode)):
            res = self.response.get_response(method="get", url=r)
            if not res:
                return None
        else:
            return None
        soup = BeautifulSoup(res.content, "lxml")
        detail_0 = soup.find("span", id="company_code")
        result_dict = dict()
        if detail_0:
            result = detail_0.get_text().split("\r\n")
            result_strip = []
            for r in result:
                if r.strip():
                    result_strip.append(r.strip())
            if len(result_strip) != 2:
                return result_dict
            result_dict["stock_name"] = result_strip[0].strip()
            result_dict["stock_code"] = result_strip[1].strip()
            return result_dict
        else:
            detail_1 = soup.find("title")
            re_pattern = re.compile("\((\d{6})\)")
            stock_code_result = re_pattern.findall(str(detail_1))
            if stock_code_result:
                result_dict["stock_code"] = stock_code_result[0]
                return result_dict

    def get_people_basic(self, show_id):
        """
        通过主题获得嘉宾介绍相关信息
        :param show_id:
        :return:
        """
        url = "http://rs.p5w.net/controller/roadShowActivities/singleGuestList.shtml"

        data = {
            "roadshowId": show_id,
        }
        res = self.response.get_response("post", url, data=data)
        if not res:
            return None
        try:
            res = json.loads(res.text.replace("null(", "").replace(")\n", ""))
        except:
            return None
        all_people = []
        for r in res.get("result", []):
            all_people.append(
                '<li><h4>{}<br>{}</h4><p style="">{}</p></li>'.format(r.get("guestName"), r.get("position"),
                                                                      r.get("vipDesc")))
        return all_people

    def get_people_basic_sec(self, file_url):
        """
        通过主题获得嘉宾介绍相关信息例如:http://roadshow2008.p5w.net/2010/hwwj/
        :param file_url:
        :return:
        """
        all_people = []
        url = file_url + "/jbjs.htm"
        res = self.response.get_response("get", url)
        if not res:
            return None
        soup = BeautifulSoup(res.content, "lxml")
        try:
            soup = soup.find("td", class_="content")
            result = soup.find_all("tr")
            for r in result:
                all_people.append(
                    '<li><h4>{}<br>{}</h4><p style="">{}</p></li>'.format(r.select("td")[0].get_text(), "",
                                                                          r.select("td")[1].get_text()))
            return all_people
        except Exception as e:
            self.logger.error("获取嘉宾介绍相关信息失败,url:{},msg:{}".format(file_url, e))
            return None

    def get_conference_list(self, show_id):
        """
        通过show_id获取活动议程
        :param show_id:
        :return:
        """
        url = "http://rs.p5w.net/controller/roadShowActivities/conferenceList.shtml"
        data = {"roadshowId": show_id}
        res = self.response.get_response("post", url, data=data)
        if not res:
            return None
        try:
            res_json = json.loads(res.text.replace("null(", "").replace(")\n", ""))
        except Exception as e:
            self.logger.error("get conference list is error, res:{}, msg:{}".format(res.text, e))
            return None
        if "result" not in res_json:
            return None
        conference_list = []
        for j in res_json["result"]:
            conference_list.append("<li><i>{}  </i>{}</li>".format(j.get("meetingTime", ""), j.get("content", "")))
        return conference_list

    def get_html(self, file_url):
        """
        通过给到file_url获取到相关的html
        :param file_url: file_url
        :return:
        """
        res = self.response.get_response("get", file_url)
        if not res:
            return None
        show_id = self.parse_id(res, **{"type": "hidden", "id": "roadshowId"})
        conference_detail = self.get_conference_detail(res)
        people_basic = self.get_people_basic(show_id)
        qa = self.get_collect_qa(show_id)
        all_qa = qa if qa else self.get_collect_qa_next(show_id)
        return self.clip_html(conference_detail, people_basic, all_qa)

    def uploader(self, oss_path, html_content):
        result = self.uploader_tool.update_file(html_content, oss_path, content_type="text/html")
        self.logger.info("oss_path:{},result:{}".format(oss_path, result))
        return result

    def normalize_item(self, **kwargs):
        data = dict()
        rs_type_category = {
            4: ["IPO路演", "S016001"],
            22: ["可转债路演", "S016001"],
            2: ["摇号抽签", "S016001"],
            9: ["上市仪式", "S016001"],
            5: ["业绩说明会", "S016002"],
            3: ["集体接待日", "S016002"],
            12: ["投资者说明会", "S016002"],
            14: ["资产重组说明会", "S016002"],
            8: ["公开致歉会", "S016002"],
            6: ["上市公司再融资路演", "S016001"],
            15: ["股东大会", "S016002"],
            7: ["项目路演", "S016001"],
            32: ["新财富分析师路演", "S016001"],
            13: ["财经节目", "S016003"],
            1: ["论坛峰会", "S016003"],
            16: ["人物访谈", "S016003"],
            17: ["发布会", "S016002"],
            18: ["培训会", "S016002"],
            10: ["投资者回馈", "S016002"],
            11: ["其他路演", "S016001"]
        }
        rs_type = kwargs.get("rs_type")
        file_url = kwargs.get("file_url")
        show_id = kwargs.get("show_id")
        html_result = self.response.get_response("get", file_url)
        stock_result = self.get_stock_code(html_result)
        conference_detail = self.get_conference_detail(html_result)
        conference_list = self.get_conference_list(show_id)
        people_basic = self.get_people_basic(show_id)
        qa = self.get_collect_qa(show_id)
        all_qa = qa if qa else self.get_collect_qa_next(show_id)
        oss_path_result = self.clip_html(conference_detail, people_basic, all_qa, conference_list)
        oss_path = "{}.html".format(str(uuid.uuid3(uuid.NAMESPACE_DNS, str(file_url))))
        data["file_type"] = ".html"
        data["org_id"] = None
        data["time"] = kwargs.get("time")
        data["file_size"] = self.get_file_size(self.ali_conn, oss_path)
        data["src_id"] = "rs_{}_{}".format(int(time.time() * 1000), random.randint(0, 1000))
        data["title"] = kwargs.get("title")
        data["type"] = rs_type_category[rs_type][0]
        data["plate"] = None  # 无法获取
        data["timestamp"] = int(time.time())
        data["stock_code"] = stock_result.get("stock_code") if stock_result else None
        data["stock_name"] = stock_result.get("stock_name") if stock_result else None
        data["industry"] = None
        data["file_url"] = file_url
        data["column"] = "会议路演"
        data["oss_path"] = oss_path
        data["export_flag"] = False
        data["export_version"] = 0
        data["category_id"] = rs_type_category[rs_type][1]
        data["source_url"] = file_url
        data["downloaded"] = self.uploader(oss_path, oss_path_result)
        if self.mongodb.count_table_num(tablename=self.table_name, find={"file_url": file_url}) > 0:
            self.mongodb.save_mongodb_time(data={k: v for k, v in data.items() if k != "src_id"},
                                           find={"file_url": file_url},
                                           tablename=self.table_name)
        else:
            self.mongodb.save_mongodb_time(data=data,
                                           find={"file_url": file_url},
                                           tablename=self.table_name)

    def normalize_item_sec(self, **kwargs):
        data = dict()
        rs_type_category = {
            4: ["IPO路演", "S016001"],
            22: ["可转债路演", "S016001"],
            2: ["摇号抽签", "S016001"],
            9: ["上市仪式", "S016001"],
            5: ["业绩说明会", "S016002"],
            3: ["集体接待日", "S016002"],
            12: ["投资者说明会", "S016002"],
            14: ["资产重组说明会", "S016002"],
            8: ["公开致歉会", "S016002"],
            6: ["上市公司再融资路演", "S016001"],
            15: ["股东大会", "S016002"],
            7: ["项目路演", "S016001"],
            32: ["新财富分析师路演", "S016001"],
            13: ["财经节目", "S016003"],
            1: ["论坛峰会", "S016003"],
            16: ["人物访谈", "S016003"],
            17: ["发布会", "S016002"],
            18: ["培训会", "S016002"],
            10: ["投资者回馈", "S016002"],
            11: ["其他路演", "S016001"]
        }
        rs_type = kwargs.get("rs_type")
        file_url = kwargs.get("file_url")
        html_result = self.response.get_response("get", file_url)
        stock_result = self.get_stock_code(html_result)
        conference_detail = ""
        people_basic = self.get_people_basic_sec(file_url)
        all_qa = self.get_collect_qa_sec(file_url)
        oss_path_result = self.clip_html(conference_detail, people_basic, all_qa)
        oss_path = "{}.html".format(str(uuid.uuid3(uuid.NAMESPACE_DNS, str(file_url))))
        data["file_type"] = ".html"
        data["org_id"] = None
        data["time"] = kwargs.get("time")
        data["file_size"] = self.get_file_size(self.ali_conn, oss_path)
        data["src_id"] = "rs_{}_{}".format(int(time.time() * 1000), random.randint(0, 1000))
        data["title"] = kwargs.get("title")
        data["type"] = rs_type_category[rs_type][0]
        data["plate"] = None  # 无法获取
        data["timestamp"] = int(time.time())
        data["stock_code"] = stock_result.get("stock_code") if stock_result else None
        data["stock_name"] = stock_result.get("stock_name") if stock_result else None
        data["industry"] = None
        data["file_url"] = file_url
        data["column"] = "会议路演"
        data["oss_path"] = oss_path
        data["export_flag"] = False
        data["export_version"] = 0
        data["category_id"] = rs_type_category[rs_type][1]
        data["source_url"] = file_url
        data["downloaded"] = self.uploader(oss_path, oss_path_result)
        if self.mongodb.count_table_num(tablename=self.table_name, find={"file_url": file_url}) > 0:
            self.mongodb.save_mongodb_time(data={k: v for k, v in data.items() if k != "src_id"},
                                           find={"file_url": file_url},
                                           tablename=self.table_name)
        else:
            self.mongodb.save_mongodb_time(data=data,
                                           find={"file_url": file_url},
                                           tablename=self.table_name)

    def spider_list(self, rs_type, last_file_url):
        url = "http://rs.p5w.net/roadshow/getRoadshowList.shtml"
        count = 0
        # count = 151
        max_page = 3
        rows = 9
        has_more = True
        while has_more:
            data = {"companyType": "",
                    "roadshowDate": "",
                    "roadshowType": rs_type,
                    "roadshowTitle": "",
                    "start": "",
                    "end": "",
                    "stationId": "",
                    "page": count,
                    "rows": rows,
                    }
            logging.debug("请求参数data:{}".format(data))
            res = self.response.get_response("post", url, data=data)
            if not res:
                break
            type_max_pge = {
                4: 151
            }
            if rs_type in type_max_pge:
                if count >= type_max_pge[rs_type]:
                    break
            res = json.loads(res.text)
            logging.debug("res:{}".format(res))
            # 此类情况异常，直接翻到下一页
            if "rows" not in res:
                if count >= max_page:
                    break
                else:
                    continue
            if not res.get("rows"):
                logging.info("rows is None, break")
                break
            # 重新定义最大页数
            # max_page = ((res.get("total", 0) if res.get("total", 0) else 0) / rows)
            for r in res.get("rows"):
                file_url = urljoin(self.base_url, r.get("roadshowUrl"))
                if file_url == last_file_url and not all_update:
                    self.logger.info("发现重复文件,跳过rs_type:{}本次更新".format(rs_type))
                    has_more = False
                    break
                if r.get("roadshowUrl"):
                    data = {
                        "file_url": file_url,
                        "rs_type": rs_type,
                        "show_id": r.get("pid"),
                        "time": r.get("roadshowDates"),
                        "title": r.get("roadshowTitle")
                    }
                    self.normalize_item(**data)
                else:
                    data = {
                        "file_url": r.get("roadshowActiveHis"),
                        "rs_type": rs_type,
                        "show_id": r.get("pid"),
                        "time": r.get("roadshowDates"),
                        "title": r.get("roadshowTitle")
                    }
                    self.normalize_item_sec(**data)
            count += 1

    def test_get_collect_qa(self, url):
        show_id = self.parse_id(url, **{"type": "hidden", "id": "roadshowId"})
        all_aq = self.get_collect_qa(show_id)
        print all_aq

    def test_get_collect_qa_next(self, url):
        show_id = self.parse_id(url, **{"type": "hidden", "id": "roadshowId"})
        all_aq_next = self.get_collect_qa_next(show_id)
        print all_aq_next

    def test_get_people_basic(self, url):
        show_id = self.parse_id(url, **{"type": "hidden", "id": "roadshowId"})
        people_basic = self.get_people_basic(show_id)
        content = "\n".join(people_basic)
        print content

    def test_clip_html(self):
        source_url = "http://rs.p5w.net/html/76604.shtml"
        res = self.response.get_response("get", source_url)
        show_id = self.parse_id(res, **{"type": "hidden", "id": "roadshowId"})
        conference_detail = self.get_conference_detail(res)
        people_basic = self.get_people_basic(show_id)
        conference_list = self.get_conference_list(show_id)
        qa = self.get_collect_qa(show_id)
        all_qa = qa if qa else self.get_collect_qa_next(show_id)
        with open("D:\\industry_project\\IndustrySpider\\test.html", "wb") as f:
            f.write(self.clip_html(conference_detail, people_basic, all_qa, conference_list))

    def test_stock_code(self):
        source_url = "http://rs.p5w.net/html/49550.shtml"
        source_url = "http://roadshow2008.p5w.net/2010/hwwj/"
        res = self.response.get_response("get", source_url)
        result = self.get_stock_code(res)
        print result

    def test_get_collect_qa_sec(self):
        res = self.get_collect_qa_sec("http://roadshow2008.p5w.net/2010/hwwj")
        print res

    def test_get_people_basic_sec(self):
        source_url = "http://roadshow2008.p5w.net/2010/hwwj/"
        self.get_people_basic_sec(source_url)

    def test_url(self):
        file_url = "http://roadshow2008.p5w.net/2010/crty/jbjs.htm"
        r = self.response.get_response("get", file_url)
        s = BeautifulSoup(r.content, "lxml")
        print s.get_text()

    def test_normalize_item(self):
        show_id = "00011E0EB2D5D40445498E6ED276CA9BECB7"
        data = {
            "file_url": "http://rs.p5w.net/html/76604.shtml",
            "rs_type": 4,
            "show_id": show_id,
            "time": "",
            "title": "",
        }
        self.normalize_item(**data)

    def test_get_conference_list(self):
        print self.get_conference_list("00011E0EB2D5D40445498E6ED276CA9BECB7")


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        filemode="a",
                        filename="quan_spider.log",
                        format='%(asctime)s %(filename)s [line:%(lineno)d] %(levelname)s %(message)s', )
    QuanSpider().test_clip_html()
    # QuanSpider().test_clip_html()

    # QuanSpider().run()
