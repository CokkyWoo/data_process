#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
 @ User:  pwu
 @ Date:  2017/10/13
 @ Time:  11:32
 @ Usage: 对各种类型进行处理
"""
from config import *
import datetime
import re
from py_mysql import *
import pymongo

collection_deal = 'deal_alltest'
collection_deal_investors = 'deal_investors_sample'
collection_company = 'company_dealtest'
collection_investor = 'investor_dealtest'

mongo_uri = MONGO_URI
mongo_db = MONGO_DATABASE

mysql_client = MysqlClient()
client = pymongo.MongoClient(mongo_uri)
db = client[mongo_db]

# 获取当前时间，并转化为datetime格式
def get_time():
    # TODO  切换时区，转成UTC时间
    now = datetime.datetime.now()
    format = '%Y-%m-%d %H:%M:%S'
    return now.strftime(format)

def get_company_status(status_str):
    status_rule = {
        1:'运营中',
        2:'未上线',
        3:'已关闭',
        4:'已转型'
    }
    try:
        for k,v in status_rule.items():
            if status_str == v:
                status = k
        return status
    except:
        print('!!!----status maybe error!')

# 将'年-月-日'格式的字符串转化为datetime格式
def format_time(time_str):
    try:
        date_time = datetime.datetime.strptime(time_str, '%Y-%m-%d')
        return date_time
    except:
        print('!!!! wrong time string!')

def get_round(round_str):
    round = '不明确'
    # if round_str == '':
    #     return round
    round_rule = {
        '':'不明确',
        'SEED':'种子轮',
        'ANGEL':'天使轮',
        'PREA':'Pre-A轮',
        'A':'A轮',
        'A+':'A+轮',
        'B':'B轮',
        'B+':'B+轮',
        'C':'C轮',
        'C+': 'C+轮',
        'D':'D轮',
        'D+':'D+轮',
        'E':'E轮',
        'F':'F轮-上市前',
        'SI':'战略投资',
        'IPO':'IPO上市',
        'LISTED':'IPO上市后',
        'M&A':'收购',
    }
    try:
        for k,v in round_rule.items():
            if round_str == v:
                round = k
                break
            else:
                round = round_str
    except:
        print('!!! ---- round is error!')
    return round

def get_currency(currency_str):
    currency = ''
    currency_rule = {
        'CNY':'人民币',
        'USD':'美元',
        'EUR':'欧元',
        'JPY':'日元',
        'GBP':'英镑',
        'TWD':'新台币',
        'HKD':'港元',
        'INR':'卢比',
        'yyy':'其他', # 人工填写的其他
        'zzz':'', # 空值
    }
    try:
        for k,v in currency_rule.items():
            if currency_str== v:
                currency = k
                break
            else:
                currency = 'xxx' # 未添加的币种
        return currency
    except:
        print('!!! ---- currency is error!')

def get_amount(amount_str):
    # 1. 判断字符串不包含数字，则返回amount=0
    # 2. 包含数字时，识别匹配单位'万''亿'
    # 3. 将'亿'转换为'万'
    # TODO mysql中该字段为整型，无法区分情况
    amount = 0
    if amount_str.isalpha():
        if amount_str == '未透露':
            amount = 0 #-1
    else:
        num_decimal = re.compile(r'[^\d.]+')
        num = num_decimal.sub('', amount_str)
        unit_str = re.compile(r'[^\u4e00-\u9fa5]')
        unit = unit_str.sub('',amount_str)
        if unit == '亿':
            amount = int(float(num)*10000)
        elif unit == '万':
            amount = num
        else:
            amount = 0 #-2
    return amount

# check当前 mongodb中数据是否已经添加（传入mongoDB id,name,mongoDB 集合名，mysql表名）,返回mysql中的id值
def check_data(params):
    #  params{'id':int, 'tbl':'','collection':'', 'name':''}
    result_db = db[params['collection']].find_one({params['name']:params['id']})
    if result_db:
        if result_db['plus_id'] != 0:
            result = result_db['plus_id']
            # print('@1---MongoDB collection('+params['collection']+') \''+params['name']+'\':'+str(params['id'])+ ' in mysql:'+params['tbl']+'('+str(result)+')')
        else:
            result = -1 # 爬取未插入
            print('@2---MongoDB collection('+params['collection']+') \''+params['name']+'\':'+str(params['id'])+', but not exist in mysql:'+params['tbl'])
    else:
        result = -2 # 未爬取
        print('@3---MongoDB collection('+params['collection']+') \''+params['name']+'\'not exist'+str(params['id']))
    return result

# 对比交易和公司中的公司名，如不同，更新为deal中的值,com_id为mysql中com_plus_id
def update_com_name(com_plus_id, deal_cname):
    result = db[collection_company].find_one({'plus_id':com_plus_id})
    com_cname = result['com_name']
    if com_cname != deal_cname:
        try:
            sql = "UPDATE companies_company SET name = ('{}') where id = ({})".format(deal_cname,com_plus_id)
            if mysql_client.query(sql) == True:
                print('%%--- company('+str(com_plus_id)+') updated: '+ deal_cname)
                db[collection_company].update({'plus_id':com_plus_id}, {"$set": {'com_name': deal_cname,'process_status': 2,}})
                print('%%---updated in MongoDB! : '+ deal_cname)
            else:
                print('%%---!!! company('+ str(com_plus_id)+') can not update in mysql!')
        except:
            print('%%---exception mysql not save')

def main():
    # print('Enter :')
    # str = input()
    # format_time(str) #完成
    # get_round(str) #完成
    # get_currency(str) #完成
    # get_amount(str) #完成
    # update_com_name(1,'美丽说') #完成
    # check_data() 完成
    # dict = {
    #     'id':3299620,
    #     'name':'com_id',
    #     'collection':'company_dealtest',
    #     'tbl':'companies_company'
    # }
    # print('reslut = '+str(check_data(dict)))


if __name__ == '__main__':
    main()

