#!/usr/bin/env python
# -*- coding: utf-8 -*-

from config import *
from py_mysql import *
from pipeline import *
import pymongo

collection_deal = 'deal_sample'
collection_deal_investors = 'deal_investors_sample'
mongo_uri = MONGO_URI
mongo_db = MONGO_DATABASE

mysql_client = MysqlClient()
client = pymongo.MongoClient(mongo_uri)
db = client[mongo_db]

# 取investor值
def invest_with(params):
    return None

# 插入deal表和deal_invstors
def insert_deal():
    for i in db[collection_deal].find({}).sort('deal_id', pymongo.ASCENDING):
        # status[0: 初始化保存 1：处理插入mysql中deal完成 2:处理插入mysql中deal_investors完成
        if i['status'] == 0:
            amount = get_amount(i['amount_str'])
            amount_str = i['amount_str']
            round = get_round(i['round'])
            currency = get_currency(i['currency'])
            finance_time = format_time(i['date'])
            test = str(i['investors']).replace('\'','\\\'')
            remark = 'it{deal_id:'+str(i['deal_id'])+';investors:'+test+';};'
            description = ''
            created_time = get_time()
            updated_time = get_time()
            is_active = 1
            com_name = i['com_name'].replace('\'','\\\'')
            dict = {'id':i['com_id'],'name':'com_id','collection':collection_company,'tbl':'companies_company'}
            company_id = check_data(dict)
            # TODO 当company_id发现mysql中未插入时需要处理
            try:
                update_com_name(company_id, com_name)
                sql = "INSERT INTO deals_deal(amount_str, round, currency, finance_time, remark, description,\
                    created_time, updated_time, is_active, company_id, amount) values ( \
                    '{}','{}','{}','{}','{}','{}','{}','{}',{},{},{});".format(
                    amount_str, round, currency, finance_time,remark,description,created_time,
                    updated_time,is_active,company_id,amount)
                if mysql_client.insert(sql) != False:
                    plus_id = mysql_client.conn.insert_id()
                    print('---- deal '+str(i['deal_id'])+' saved to mysql,and plus id:'+str(plus_id))
                    db[collection_deal].update(i, {"$set": {'status': 1,'plus_id':plus_id}})
                else:
                    print('### deal ' + str(i['deal_id']) + ' can not saved to mysql!')
            except:
                print('exception mysql not save')
        else:
            print('!!!! alert deal ' + str(i['deal_id']) + '  have been saved before!')
    client.close()

def main():
    insert_deal()


if __name__ == '__main__':
    main()

