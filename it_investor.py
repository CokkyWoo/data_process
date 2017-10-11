#!/usr/bin/env python
# -*- coding: utf-8 -*-

from config import *
from py_mysql import *
import pymongo
import datetime

collection_name = 'investor'
mongo_uri = MONGO_URI
mongo_db = MONGO_DATABASE

mysql_client = MysqlClient()
client = pymongo.MongoClient(mongo_uri)
db = client[mongo_db]

def get_time():
    now = datetime.datetime.now()
    format = '%Y-%m-%d %H:%M:%S'
    return now.strftime(format)

def insert_item():
    # 1. 读取数据并判断其是否已经添加
    # 2. 已添加则读取下一条数据
    # 3. 未添加则处理数据并插入mysql
    for i in db[collection_name].find({}).sort('invst_id', pymongo.ASCENDING):
        if i['is_process'] == 0:
            name = i['invst_name']
            short_name = ''
            try:
                type = i['invst_type']
            except:
                type = ''
            try:
                image_uris = i['invst_logo']
            except:
                image_uris = ''
            try:
                description = i['invst_des']
            except:
                description = ''
            try:
                website = i['web_site']
            except:
                website = ''
            try:
                tags = i['tags']
            except:
                tags =''
            try:
                interests = i['fav_rounds']
            except:
                interests =''
            remark = 'it{invst_id:'+str(i['invst_id'])+';};'
            created_time = get_time()
            updated_time = get_time()
            is_active = 1
            try:
                sql = "INSERT INTO investors_investor(name, short_name, type, image_uris, description, website, tags, interests,\
                    remark, is_active, created_time, updated_time) values ('{}','{}','{}','{}','{}','{}','{}','{}','{}',\
                    {},'{}','{}');".format(name, short_name, type, image_uris, description, website, tags, interests,remark,
                                           is_active, created_time, updated_time)
                # print(sql)
                mysql_client.query(sql)
                print('@@@@@investor '+str(i['invst_id'])+' saved to mysql')
                db[collection_name].update(i, {"$set": {'is_process': 1}})
            except:
                print('exception mysql not save')
        else:
            print('!!!!alert investor ' + str(i['invst_id']) + 'have been saved before!')

    client.close()

def main():
    insert_item()


if __name__ == '__main__':
    main()


