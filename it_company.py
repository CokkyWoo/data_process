#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pipeline import *
import pymongo

collection_name = 'company'
mongo_uri = MONGO_URI
mongo_db = MONGO_DATABASE

mysql_client = MysqlClient()
client = pymongo.MongoClient(mongo_uri)
db = client[mongo_db]
status = 0

def insert_item():
    # 1. 读取数据并判断其是否已经添加
    # 2. 已添加则读取下一条数据
    # 3. 未添加则处理数据并插入mysql
    for i in db[collection_name].find({}).sort('com_id', pymongo.ASCENDING):
        # status[0: 初始化保存 1：处理插入mysql完成】
        if i['process_status'] == 0:
            name = i['com_name'].replace('\'','\\\'')
            website = ''
            tags = ''
            try:
                image_uris = i['com_logo_archive'].strip()
            except:
                image_uris = ''
            try:
                description = i['com_des'].replace('\'','\\\'')
            except:
                description = ''
            try:
                scale = i['com_scale'].strip()
            except:
                scale = ''
            try:
                location = i['com_addr']
            except:
                location =''
            try:
                sector = i['com_cat_name']
            except:
                sector =''
            try:
                subsector = i['com_sub_cat_name']
            except:
                subsector =''
            status = get_company_status(i['com_status'])
            if i['com_born_date']:
                remark = 'it{com_id:' + str(i['com_id']) + ';born_date:' + i['com_born_date'] +';};'
            else:
                remark = 'it{com_id:'+str(i['com_id'])+';};'
            created_time = get_time()
            updated_time = get_time()
            is_active = 1
            try:
                sql = "INSERT INTO companies_company( name,website,tags,image_uris,description,scale,location, \
                    sector,subsector,remark,created_time,updated_time,is_active,status) values ('{}','{}','{}',\
                    '{}','{}','{}','{}','{}','{}','{}','{}','{}',{},{});".format(
                    name,website,tags,image_uris,description,scale,location,sector,subsector,
                    remark,created_time,updated_time,is_active,status)
                if mysql_client.insert(sql) != False:
                    plus_id = mysql_client.conn.insert_id()
                    print('---- com '+str(i['com_id'])+' saved to mysql,and plus id:'+str(plus_id))
                    db[collection_name].update(i, {"$set": {'process_status': 1,'plus_id':plus_id}})
                else:
                    print('### company ' + str(i['com_id']) + ' can not saved to mysql!')
            except:
                print('exception mysql not save')
        else:
            print('!!!! alert company ' + str(i['com_id']) + '  have been saved before!')
    client.close()

def main():
    insert_item()


if __name__ == '__main__':
    main()

