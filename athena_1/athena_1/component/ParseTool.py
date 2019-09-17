#
import jieba as JB
import re
# import urllib.request
from urllib import parse as urlcode
from bs4 import BeautifulSoup as BS
import urllib3
import logging


logging.getLogger("urllib3").setLevel(logging.WARNING)

STOPWORDS_FILE = './athena_1/component/stopwords.txt'


def detectKeySentence(eachPara, title):
    """
    :param title: 该页面的标题
    :param eachPara: class为para的div元素
    :return:
    """

    # print(os.getcwd())  # ..../spiderApp/athena_1 本地调试时的结果

    rawParaText = ''.join(x for x in eachPara.strings)  # 合成完整的文段
    sentenceList = rawParaText.split('。')  # 按照句号分句 形成列表
    elements_a = eachPara.find_all('a')  # 找到a标签 返回列表
    elements_a_link = [('https://baike.baidu.com' + x.get('href')) \
                       for x in elements_a if re.search('/item/',
                                                        x.get('href') if isinstance(x.get('href'),
                                                                                    str) else 'please do not')]
    # 形成待爬取的标签
    elementsTextList_a = [x.string for x in elements_a if x.string != u'\xa0']  # 获取a标签内的文字列表 并去除脚注

    result_ListOfDict = list()
    result_ListOfPossibleUrl = list()

    for each_sentence in sentenceList:
        """
        part 1  通过构建url 侦测可以建立节点的名词
        """

        tmpCutResList = list(set(JB.lcut(each_sentence, cut_all=True)))  # 使用列表储存分词的结果 并实现去重

        count = 0  # 计数系统 用于log输出

        for eachWord in tmpCutResList:

            if eachWord == title:
                continue

            if wordFilter(eachWord) is False:  # 如未能通过过滤器 则直接终止
                continue

            tmp_url = 'https://baike.baidu.com/item/' + urlcode.quote(eachWord)

            count = count + 1  # 用于log
            # print("checking url" + str(count))  # 用于log

            if urlCheck(tmp_url):
                result_ListOfPossibleUrl.append(tmp_url)
                result_ListOfDict.append({
                    'startNode': title,
                    'relation': each_sentence,
                    'endNode': eachWord
                })

        """
        part2  通过直接解构已经标记好的连接，形成一部分字典
        """
        for each_a in elementsTextList_a:
            try:
                if each_a not in each_sentence:
                    continue  # 如该标签文字不在这个句子里，那么就直接继续循环
                else:
                    tmpResDict = {
                        'startNode': title,
                        'relation': each_sentence,
                        'endNode': each_a
                    }
                    result_ListOfDict.append(tmpResDict)
            except TypeError as e:  # 此处except是因为each_a 有可能出现NONE的情况
                continue

    result_ListOfPossibleUrl.extend(elements_a_link)

    return result_ListOfDict, result_ListOfPossibleUrl


def urlCheck(url):
    """
    检测url是不是真真确确的百科页面
    :param url: url
    :return:
    """
    http = urllib3.PoolManager()
    returnContent = http.request('GET', url)
    tmp_data = BS(returnContent.data, features="lxml")  # 直接转换为bs对象
    test = tmp_data.find('h2', 'title-text')  # 使用网页中的"目录"标签来确认其确实为百科页面
    if test is None:
        return False
    else:
        if tagCheck(tmp_data):
            return True
        else:
            return False


def wordFilter(word):
    """
    此处为单词过滤装置，滤除明显无效的字符串
    :param word:
    :return: bool
    """
    with open(STOPWORDS_FILE, 'r') as f:
        for line in f:
            if word in line:
                return False

    try:
        float(word)  # 检测是否为纯数字
        return False
    except ValueError as e:
        pass

    return True


def tagCheck(body):
    """
    对词条标签进行检查
    :param body: bs
    :return:
    """
    tagList = ['自然', '科学', '社会', '科技产品', '学科', '疾病', '医学',
               '非生物', '医学术语', '生活', '书籍', '语言', '人物']  # 预制词汇
    tags = body.find_all('span', 'taglist')  # 搜寻词条标签

    if tags is not None:
        for each in tags:
            each = re.sub(r'\s', '', each.text)
            if each in tagList:
                return True  # 只要有一项符合即刻
    else:
        return False
