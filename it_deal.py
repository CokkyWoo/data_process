#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pipeline import *
import pymongo


def insert_deal_investors(plus_deal_id,investors):
    try:
        for dict_id,dict_name in investors.items():
            dict = {'id':int(dict_id), 'name': 'invst_id', 'collection': collection_investor, 'tbl': 'investors_investor'}
            investor_id = check_data(dict)
            if investor_id > 0:
                try:
                    sql = "INSERT INTO deals_deal_investors(deal_id,investor_id) values ({},{});".format(
                        plus_deal_id, investor_id)
                    if mysql_client.insert(sql) != False:
                        plus_deal_investor_id = mysql_client.conn.insert_id()
                        print('#1---- deal_investor ' + str(plus_deal_id) + ' saved to deal_investor,and plus id:' + str(plus_deal_investor_id))
                        remark_origin = db[collection_deal].find_one({'plus_id':plus_deal_id})['remark']
                        remark_for_mongodb = remark_origin+'deal_investor_plus_id:'+str(plus_deal_investor_id)+';'
                        db[collection_deal].update({'plus_id':plus_deal_id}, {"$set": {'status':2,'remark':remark_for_mongodb}})
                    else:
                        print('### deal_investor can not saved to mysql!')
                except:
                    print('exception mysql not save')
            else:
                continue
    except:
        db[collection_deal].update({'plus_id': plus_deal_id}, {"$set": {'status': 2, 'remark':'no data insert to investors'}})

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
            if company_id > 0:
                try:
                    update_com_name(company_id, com_name)
                    sql = "INSERT INTO deals_deal(amount_str, round, currency, finance_time, remark, description,\
                        created_time, updated_time, is_active, company_id, amount) values ( \
                        '{}','{}','{}','{}','{}','{}','{}','{}',{},{},{});".format(
                        amount_str, round, currency, finance_time,remark,description,created_time,
                        updated_time,is_active,company_id,amount)
                    if mysql_client.insert(sql) != False:
                        plus_id = mysql_client.conn.insert_id()
                        print('+1---- deal '+str(i['deal_id'])+' saved to mysql,and plus id:'+str(plus_id))
                        db[collection_deal].update(i, {"$set": {'status': 1,'plus_id':plus_id}})
                        insert_deal_investors(plus_id,i['investors'])
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

