#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import json
import numpy as np
import os
import sys
import mysql.connector
import mysql
from time import time
from tqdm import tqdm

# for fetching the all market price related details.
def fetch_benchmarkers_mp():
    #get token from benchmarkers
    phone_no=''
    passwd=''
    myurl=''
    head={"phoneNumber": '{}'.format(phone_no),"password": '{}'.format(passwd)}
    getdata = requests.post(myurl,data=head)
    response=getdata.text
    response=json.loads(response)
    token=str(response['data']['token'])

    try:
        #get all market price available in benchmarkers API
        myurl='http://52.163.118.48:8010/api/v1/price'
        head = {'Authorization': '{}'.format(token)}
        getdata = requests.get(myurl,headers=head)
        response=getdata.text
        response=json.loads(response)
        print('[info] : BenchMarkers Market Price values fetched!! :)')
        return response
    except:
        print("[info] : Error fetching Price Vaules.. Network time out may be.. or Force terminated program")
        return None


# In[2]:


# for connecting DataBase
def db_connect():
    try:
        mydb = mysql.connector.connect(
            host="",
            database="",
            user="",
            password="")
        print('[info] : Database Connection Successful!!! :)')
        return mydb,1;
    except:
        print('[info] : Database Connection Failed.. :(')
        return False,0
        


# In[3]:


#updates location table
def update_vamp_location_table(location):
    
    # checks if location name already exists
    location=location.upper()
    query="select vl_id from iwap_vamp_location where vl_name='{}'".format(location)
    mycursor = mydb.cursor(buffered=True)
    mycursor.execute(query)
    l= [x for x in mycursor]
    
    # if location not exists, then
    if l == []:
        #print('New location is available: ',location)
        #get max_id to insert new record with new id
        query='select max(vl_id) from iwap_vamp_location;'
        mycursor = mydb.cursor(buffered=True)
        mycursor.execute(query)
        max_id=[x[0] for x in mycursor]
        
        if max_id == [None]:
            # if max_id ==None (or) first record to insert into table
            query="insert into iwap_vamp_location values (1,'{}','1');".format(location)
            #print(query)
            mycursor = mydb.cursor(buffered=True)
            mycursor.execute(query)
            #print('first tuple added into the table..')
            return 1
        else:
            # if max_id !=None / not a first record to insert into table
            max_id=max_id[0]+1
            query="insert into iwap_vamp_location values ({},'{}','1');".format(str(max_id),location)
            #print(query)
            mycursor = mydb.cursor(buffered=True)
            mycursor.execute(query)
            #print('New tuple added')
            return max_id
    else:
        # else location already exist, then mark it as available by '1'
        query="update iwap_vamp_location set vl_avail='1' where vl_id={};".format(str(l[0][0]))
        mycursor = mydb.cursor(buffered=True)
        mycursor.execute(query)
        return l[0][0]


# In[4]:


def update_vamp_market_table(market,vl_id,latlong):
    # keeps only the first words and discards the rest. eg 'kaleswara' for 'kalesware Rao market'
    market=market.split(' ')[0].upper()
    query="select vm_id from iwap_vamp_market where vm_name='{}'".format(market)
    mycursor = mydb.cursor(buffered=True)
    mycursor.execute(query)
    l= [x for x in mycursor]
    if l == []:
        #print('New market is available: ',market)
        query='select max(vm_id) from iwap_vamp_market;'
        mycursor = mydb.cursor(buffered=True)
        mycursor.execute(query)
        max_id=[x[0] for x in mycursor]

        if max_id == [None]:
            query="insert into iwap_vamp_market values (1,'{}',{},{},{},'1');".format(market,str(vl_id),latlong[0],latlong[1])
            #print(query)
            mycursor = mydb.cursor(buffered=True)
            mycursor.execute(query)
            #print('first tuple added into the table..')
            return 1
        else:
            max_id=max_id[0]+1
            query="insert into iwap_vamp_market values ({},'{}',{},{},{},'1');".format(str(max_id),market,str(vl_id),latlong[0],latlong[1])
            #print(query)
            mycursor = mydb.cursor(buffered=True)
            mycursor.execute(query)
            #print('New tuple added')
            return max_id
    else:
        query="update iwap_vamp_market set vm_avail='1',vm_latitude={},vm_longitude={} where vm_id={};".format(latlong[0],latlong[1],str(l[0][0]))
        mycursor = mydb.cursor(buffered=True)
        mycursor.execute(query)
        return l[0][0]


# In[5]:


def update_vamp_category_table(category):
    query="select vc_id from iwap_vamp_category where vc_name='{}'".format(category)
    mycursor = mydb.cursor(buffered=True)
    mycursor.execute(query)
    l= [x for x in mycursor]
    
    if l == []:
        #print('New category is available: ',category)
        query='select max(vc_id) from iwap_vamp_category'
        mycursor = mydb.cursor(buffered=True)
        mycursor.execute(query)
        max_id=[x[0] for x in mycursor]

        if max_id == [None]:
            query="insert into iwap_vamp_category values (1,'{}','1');".format(category)
            #print(query)
            mycursor = mydb.cursor(buffered=True)
            mycursor.execute(query)
            #print('first tuple added into the table..')
            return 1
        else:
            max_id=max_id[0]+1
            query="insert into iwap_vamp_category values ({},'{}','1');".format(str(max_id),category)
            #print(query)
            mycursor = mydb.cursor(buffered=True)
            mycursor.execute(query)
            #print('New tuple added')
            return max_id
    else:
        query="update iwap_vamp_category set vc_avail='1' where vc_id={};".format(str(l[0][0]))
        mycursor = mydb.cursor(buffered=True)
        mycursor.execute(query)
        return l[0][0]


# In[6]:


def update_vamp_item_table(item,vc_id):
    query="select vi_id from iwap_vamp_item where vi_name='{}'".format(item)
    mycursor = mydb.cursor(buffered=True)
    mycursor.execute(query)
    l= [x for x in mycursor]
    
    if l == []:
        #print('New item is available: ',item)
        query='select max(vi_id) from iwap_vamp_item;'
        mycursor = mydb.cursor(buffered=True)
        mycursor.execute(query)
        max_id=[x[0] for x in mycursor]

        if max_id == [None]:
            query="insert into iwap_vamp_item values (1,'{}',{},'1');".format(item,str(vc_id))
            #print(query)
            mycursor = mydb.cursor(buffered=True)
            mycursor.execute(query)
            #print('first tuple added into the table..')
            return 1
        else:
            max_id=max_id[0]+1
            query="insert into iwap_vamp_item values ({},'{}',{},'1');".format(str(max_id),item,str(vc_id))
            #print(query)
            mycursor = mydb.cursor(buffered=True)
            mycursor.execute(query)
            #print('New tuple added')
            return max_id
    else:
        query="update iwap_vamp_item set vi_avail='1' where vi_id={};".format(str(l[0][0]))
        mycursor = mydb.cursor(buffered=True)
        mycursor.execute(query)
        return l[0][0]


# In[7]:


# handles the duplicate record especially 18.33 18.33 like float errors, causing duplicate records. by,
# 18.33 not equals 18.33 computationally
# so tries to add a duplicate -- error occurs, since we set unique constraint for 
# unique(vl_id,vm_id,vc_id,vi_id,price_perkg,updatedAt) attributes
# when 
def new_update_vamp_market_price_table(vl_id,vm_id,vc_id,vi_id,price_perkg,updatedAt):
    try:
        query="select vmp_id from iwap_vamp_market_price where vl_id='{}' and vm_id='{}' and vc_id='{}' and vi_id='{}' and vmp_price_perkg={} and vmp_updatedAt='{}'".format(str(vl_id),str(vm_id),str(vc_id),str(vi_id),price_perkg,updatedAt)
        mycursor = mydb.cursor(buffered=True)
        mycursor.execute(query)
        l= [x for x in mycursor]
        #print(l)
    
        if l == []:
            #print('New market price is available')
            query='select max(vmp_id) from iwap_vamp_market_price'
            mycursor = mydb.cursor(buffered=True)
            mycursor.execute(query)
            max_id=[x[0] for x in mycursor]

            if max_id == [None]:
                query="insert into iwap_vamp_market_price values (1,'{}','{}','{}','{}','{}','{}');".format(str(vi_id),str(vc_id),str(vm_id),str(vl_id),price_perkg,updatedAt)
                #print(query)
                mycursor = mydb.cursor(buffered=True)
                mycursor.execute(query)
                #print('first tuple added into the table..')
                return 1
            else:
                max_id=max_id[0]+1
                query="insert into iwap_vamp_market_price values ({},'{}','{}','{}','{}','{}','{}');".format(str(max_id),str(vi_id),str(vc_id),str(vm_id),str(vl_id),price_perkg,updatedAt)
                #print(query)
                mycursor = mydb.cursor(buffered=True)
                mycursor.execute(query)
                #print('New tuple added')
                return max_id
        else:
            query="update iwap_vamp_market_price set vmp_price_perkg='{}' where vmp_id='{}';".format(price_perkg,str(l[0][0]))
            mycursor = mydb.cursor(buffered=True)
            mycursor.execute(query)
            return l[0][0]
            print('already exists')
    except IntegrityError as e:
        print("!!Discarded duplicate record : ",vl_id,vm_id,vc_id,vi_id,price_perkg,updatedAt)


# In[8]:


def master_update(response):
    
    # rather renew the entire DB, just can mark whether market, location,.. is avail or not.
    # so before we make all to be not avail by marking '0', 
    # then if any new loc or marketfound in newly updated list, then smiple mark that alone as '1'
    # others still which not found remains avail as '0'
    
    query='delete from iwap_vamp_market_price'
    mycursor = mydb.cursor(buffered=True)
    mycursor.execute(query)
    
    query="update iwap_vamp_market set vm_avail='0';"
    mycursor = mydb.cursor(buffered=True)
    mycursor.execute(query)
    
    query="update iwap_vamp_location set vl_avail='0';"
    mycursor = mydb.cursor(buffered=True)
    mycursor.execute(query)
    
    query="update iwap_vamp_category set vc_avail='0';"
    mycursor = mydb.cursor(buffered=True)
    mycursor.execute(query)
    
    print('[info] : Updating changes in database..')
    print('no of items : ',len(response['data']))
    # from response variable(or market price data)
    # extract each item,category,location,market,updated-time
    for i in tqdm(range(0,len(response['data']))):
        item=response['data'][i]['itemName']
        location=response['data'][i]['location']['name']
        market=response['data'][i]['marketName']
        category=response['data'][i]['categoryName']
        price_perkg=response['data'][i]['pricePerKg']
        market_geolocation=response['data'][i]['market']['marketGeoLocation']['coordinates']
        updatedAt=response['data'][i]['updatedAt']
        # converting from '2021-02-12T02:24:17.056Z' to '2021-02-12 02:24:17' format
        updatedAt=updatedAt.split('T')
        time=updatedAt[1].split('.')[0]
        updatedAt=updatedAt[0]+' '+time

        ###########--Updates tables--------location->market, category->item------####
        # updates vamp_location table---------------------------------------------
        vl_id=update_vamp_location_table(location)
        # updates vamp_market table-----------------------------------------------
        #####first fetches the location id which is used in insertion of market row as foreign key
        if vl_id == None:
            mycursor = mydb.cursor(buffered=True)
            query="select vl_id from iwap_vamp_location where vl_name='{}'".format(location)
            mycursor.execute(query)
            vl_id=[x[0] for x in mycursor]
            vl_id=vl_id[0]
        vm_id=update_vamp_market_table(market,vl_id,market_geolocation)
        # updates vamp_category table----------------------------------------------
        vc_id=update_vamp_category_table(category)
        #updates vamp_item table---------------------------------------------------
        #####first fetches the category id which is used in insertion of item row as foreign key
        if vc_id == None:
            mycursor = mydb.cursor(buffered=True)
            query="select vc_id from iwap_vamp_category where vc_name='{}'".format(category)
            mycursor.execute(query)
            vc_id=[x[0] for x in mycursor]
            vc_id=vc_id[0]
        vi_id=update_vamp_item_table(item,vc_id)
    
        new_update_vamp_market_price_table(vl_id,vm_id,vc_id,vi_id,price_perkg,updatedAt)
    # after all updated the db, commited the changes permanently at end.
    mydb.commit()
    print("[info] : Changes were Committed in DB..")


# In[9]:


def master_function():
    # for checking whether new list were avail from benchmarkers
    # here just comparing the previous data and now fetched data.
    # if same then no changes, else db should be updated.
    if not os.path.exists('pre_response.json'):
        pre_res={}
        with open('pre_response.json','w') as p_res:
            json.dump(pre_res,p_res)
        p_res.close()
        
    with open('pre_response.json','r') as p_res:
        p_res=json.load(p_res)
    
    # fetched new list data from BenchMarkers    
    response=fetch_benchmarkers_mp()
    
    # comparing with previous data. 
    if p_res != response and response != None:
        master_update(response)
    else:
        print('[info] : No updates in the BenchMarkers API')

    with open('pre_response.json','w') as p_res:
        json.dump(response,p_res)


# In[10]:


# Trying to connect Database
mydb,connected=db_connect()
# if DataBase connection successful then, run master_function.
if connected:
    try:
        master_function()
    except Exception as e:
        print("Error in updating!!!")
        ex_type, ex_value, ex_traceback = sys.exc_info()
        print(ex_value)
        mydb.close()
        del mydb


# In[ ]:




