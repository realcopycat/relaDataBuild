import scrapy
# import re
from pymongo import MongoClient as MC
from bs4 import BeautifulSoup as BS
# from urllib import parse as urlcode
# import jieba as JB
from athena_1.component.ParseTool import detectKeySentence ,tagCheck
import time


class BaikeRelaPicker(scrapy.Spider):
    name = 'relaPicker_multi'

    def __init__(self):
        self.limit = 5000 * 2000
        self.totalCount = 0
        self.entry = [str(input())]
        print(self.entry)
        self.start_time = time.time()

    def start_requests(self):
        """
        设定初始的url
        """
        entry_url = self.entry

        for each in entry_url:
            yield scrapy.Request(url=each, callback=self.parse, dont_filter=True)

    def parse(self, response):
        """
        主解析函数
        """
        potentiallink = self.mainContentParse(response)  # 此处就已经将所有的页面内可用的关系都爬取下来了

        for each in potentiallink:
            if self.totalCount < self.limit:  # 如未达总量项限制 则继续
                self.totalCount = self.totalCount + 1
                yield scrapy.Request(url=each, callback=self.parse)

        return None

    def mainContentParse(self, response):
        """
        主体内容解析
        """
        soup = BS(response.text, features='lxml')  # 生成BS对象 为进一步解析提供基础
        if tagCheck(soup) is False:
            return list()  # 如果标签检查无法通过 ，则不进行筛选
        startTag = soup.find('div', 'top-tool')  # 找到开始的标签
        tagIter = startTag.next_siblings  # 设置迭代器
        self.title = soup.find('dd', 'lemmaWgt-lemmaTitle-title').h1.string  # 提取页面的title,使其可以全局访问
        resListDict, link = self.relationSearch(tagIter)  # 内容关系提取
        self.inject_mongo(resListDict)  # 数据注入数据库

        return link  # 最后返回这个link以供parse来进一步搜索

    def relationSearch(self, tagIter):
        """
        抽取文段中可能的关系，并生成需要进一步检查的url，还要调用其他函数来对URL来进一步搜索
        :param tagIter: 迭代器
        :return: 字典列表
        """
        res = list()  # 标准的、带有链接的提取关系结果
        link = list()  # 用于检查确实值得进一步搜索的页面

        for each in tagIter:
            try:  # 此处是为了应付each根本就是个空的情况
                className = each['class']
            except (TypeError, KeyError) as e:
                continue

            if isinstance(each['class'], list):
                if each['class'][0] == 'para':  # 确认确实为段落的区块之后，进行解析
                    tmp_res, tmp_links = detectKeySentence(each, self.title)
                    res.extend(tmp_res)
                    link.extend(tmp_links)
                else:
                    continue
            else:
                continue

        self.end_time = time.time()
        dur_time = (self.end_time - self.start_time) / 60
        print('current count : {0} , current cost time : {1} mins'.format(self.totalCount, dur_time))

        return res, link

    def inject_mongo(self, dataList):
        """
        注入mongoDB
        :param dataList: 数据列表
        :return:
        """
        DBclient = MC()  # 打开数据库链接
        database = DBclient.relationData
        dataCollection = database.testRun5

        for x in dataList:
            if dataCollection.find_one(x) is None:
                dataCollection.insert_one(x)  # 插入数据

        return None

