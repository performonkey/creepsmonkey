#! /usr/bin/env python
#coding: utf-8

import re
import optparse
import sqlite3
import socket
import logging
import urllib2, urlparse
from Queue import Queue, Empty
from threading import Thread, Lock
from httplib import BadStatusLine
from bs4 import BeautifulSoup

visited_links = []
queue = Queue()
db_queue = Queue()

def fetchPage(deep, url, log):
    '''抓取传入的URL的HTML源码 '''
    if not url.startswith('http://') and not url.startswith('https://'):
        url = 'http://'+url

    log.info(u"获取HTML源码: (%d)%s" % (deep, url))
    headers = {
    'Referer':'http://www.cnbeta.com/articles',
    'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'
    }
    log.debug(u'设定请求头部: (%d)%s' %(deep, url))
    req = urllib2.Request(url, headers = headers)

    log.debug(u'下载页面: (%d)%s' %(deep, url))
    try:
        response = urllib2.urlopen(req, timeout=20)
    except urllib2.URLError as e:
        if hasattr(e, 'reason'):
            log.error("We failed to reach: (%d)%s \n Reason: %s" %(deep, url, e.reason))
        elif hasattr(e, 'code'):
            log.error("Couldn't fulfill the request: (%d)%s\n Error code: %s" %(deep, url, e.code))
    except BadStatusLine:
        log.warn("Could not fetch: (%d)%s" %(deep, url))
    except UnicodeError:
        log.error("UnicodeError: (%d)%s" %(deep, url))
        #queue.put((deep, url.encode('raw_unicode_escape')))
    except Exception:
        pass
    else:
        log.debug(u'获取源码成功: (%d)%s' %(deep, url))

        try:
            return response.read()
        except urllib2.socket.timeout:
            log.error('Timeout: %s' %url)
            return ''



def processHtml(response, options_url, url, log):
    '''处理获取的HTML源码，从中过滤出和需爬去域名的相同域的链接'''
    links = []
    real_link = []
    same_site = []
    try:
        html = BeautifulSoup(response, 'lxml')
    except TypeError:
        html = BeautifulSoup(str(response), 'lxml')

    log.debug(u'从源码中过滤出<a>标签: %s' %url)
    for a in html.find_all("a"):
        try:
            links.append(a["href"])
        except Exception:
            continue
    log.debug(u'<a>过滤完毕: %s' %url)

    #检测是否为URL
    for i in set(links):
        log.debug(u'检测是否为URL: %s' %i)
        if i.startswith('http') or i.startswith('?') or i.startswith('/'):    #判断是否是正确的URL
            if not i.startswith('http'):    #如果是相对地址则转换成绝对地址
                if not i.startswith('/'):
                    i = '/'+i
                absolute_path = urlparse.urljoin(url, i)
                i = absolute_path
                log.debug(u'转换为绝对地址: %s' %i)
            if i in visited_links:    #检查是否是已爬取的URL
                log.debug(u'已访问过此网址: %s' %i)
                continue
            log.debug(u'为合法URL: %s' %i)
            real_link.append(i)
        else:
            log.debug(u'为非法URL: %s' %i)

    #检测URL是否和输入的URl为同一域
    for u in real_link:
        #将URL的域名部分以"."分割，转换为列表
        log.debug(u'检测是否为同一域: %s' %u)
        separate_input_url = (urlparse.urlparse(options_url).netloc).split(".")
        separate_url = (urlparse.urlparse(u).netloc).split(".")

        si = len(separate_input_url) - 1
        su = len(separate_url) - 1

        #对比列表，如果相同则为同一域的域名
        while((separate_url[su]).lower() == (separate_input_url[si]).lower()):
            if (si == 1):
                separate_input_url[0] == "www"
                same_site.append(u)
                log.debug(u'和所爬取的URL为同一域: %s' %u)
                break
            if (si == 0):
                same_site.append(u)
                log.debug(u'和所爬取的URL为同一域: %s' %u)
                break
            si = si - 1
            su = su - 1
    visited_links.extend(same_site)

    return same_site


def save2Db(options_deep, options_dbfile, log):
    '''将传入的URL和深度存入指定数据库‘'''
    log.debug(u'连接数据库')
    with sqlite3.connect(options_dbfile, isolation_level=None, check_same_thread=False) as connect_db:
        db = connect_db.cursor()
        log.debug(u'连接数据库成功')
        log.debug(u'建立表格')
        try:
            db.execute("CREATE TABLE links(hierarchy INTEGER, URL TEXT)")
        except sqlite3.OperationalError:
            log.debug(u'存在已建立的表格')
        else:
            log.debug(u'表格建立成功')

        while True:
            try:
                (deep, url) = db_queue.get(True, 30)
            except Empty:
                log.debug('Dbqueue is Empty')
                break
            except Exception as err:
                log.debug('DB: Exception %s' %err)
                continue
            else:
                if deep <= (options_deep+1):
                    log.debug(u'存入%s' %url)
                    try:
                        db.execute("INSERT INTO links (hierarchy, URL) VALUES(?, ?)",(int(deep), url))
                    except Exception:
                        pass
                    log.debug(u'存入成功: %s' %url)
                    print(deep, url)
                    db_queue.task_done()
                else:
                    break



def checkKeyword(options_keyword, req):
    '''检查是否需要查找关键字，如果需要则搜索'''
    if options_keyword == "":
        return True
    else:
        if req.find(options_keyword) != -1:
            return True
        else:
            return False


def logSet(log_level):
    '''设置日志记录等级'''
    levels = {1:logging.DEBUG,
            2:logging.INFO,
            3:logging.WARNING,
            4:logging.ERROR,
            5:logging.CRITICAL}

    log = logging.getLogger('creepsmonkey')
    log.setLevel(levels.get(log_level, "logging.WARNING"))
    fh = logging.FileHandler("log", mode='w', encoding='UTF-8')
    formatter = logging.Formatter("%(levelname)s - %(asctime)s - %(message)s")
    fh.setFormatter(formatter)
    log.addHandler(fh)

    return log


def getUrl(options_url, options_keyword, options_deep, log):
    while True:
        try:
            (deep, url) = queue.get(True, 30)
        except Empty:
            log.debug('queue is Empty')
            break
        except Exception:
            continue
        else:
            if deep <= options_deep:     #判断是否达到需爬需层级数
                response = fetchPage(deep, url, log)

                if checkKeyword(options_keyword, response):
                    log.debug(u'解析HTML源码: %s' %url)
                    urls = processHtml(response, options_url, url, log)

                    for i in urls:    #将爬出的URL加进列队
                        if deep < options_deep:
                            queue.put((deep+1, i))
                        db_queue.put((deep+1, i))
            queue.task_done()


if __name__ == "__main__":
    option = optparse.OptionParser()
    option.add_option("-u", "--url", dest="url", default="http://www.baidu.com", type="string", help=u"所需爬取的URL")
    option.add_option("-d", "--deep", dest="deep", default=0, type="int", help=u"所需爬取的深度")
    option.add_option("-t", "--threadpool", dest="threadpool", default=10, type="int", help=u"所需的线程的线程数")
    option.add_option("-f", "--dbfile", dest="dbfile", default="dbfile", type="string", help=u"指定数据库名")
    option.add_option("-k", "--keyword", dest="keyword", default="", type="string", help=u"指定页面需包含的关键字")
    option.add_option("-l", "--loglevel", dest="loglevel", default="1", type="int", help=u"设置日志记录等级，1-5，数字越大越详细")
    (options, args) = option.parse_args()

    log = logSet(options.loglevel)
    queue.put((-1, options.url))
    visited_links.append(options.url)

    for i in range(options.threadpool):
        t = Thread(target=getUrl, args=(options.url, options.keyword, options.deep, log))
        t.daemon = True
        t.start()
    log.debug(u'开始爬取')

    for u in range(options.threadpool):
        tdb = Thread(target=save2Db, args=(options.deep, options.dbfile, log))
        tdb.daemon = True
    tdb.start()

    queue.join()
    db_queue.join()

    log.info(u'共爬取Link: %s个' %len(visited_links))
